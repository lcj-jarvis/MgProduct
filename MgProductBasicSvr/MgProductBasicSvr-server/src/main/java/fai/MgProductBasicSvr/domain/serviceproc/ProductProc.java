package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductProc {
    public ProductProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    public ProductProc(int flow, int aid, TransactionCtrl tc, String xid, boolean withSaga) {
        this.m_flow = flow;
        this.m_dao = ProductDaoCtrl.getInstance(flow, aid);
        this.xid = xid;
        if(!Str.isEmpty(xid)) {
            this.m_sagaDao = ProductSagaDaoCtrl.getInstance(flow, aid);
            this.withSaga = withSaga;
        }
        init(tc);
    }

    public int addProduct(int aid, Param pdData) {
        int rt;
        if(Str.isEmpty(pdData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdData);
        }
        int count = getPdCount(aid);
        if(count >= ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        int pdId = creatAndSetId(aid, pdData);
        rt = m_dao.insert(pdData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 使用分布式事务时，记录下新增数据的主键
        if(withSaga) {
            Param pdSaga = new Param();
            pdSaga.assign(pdData, ProductEntity.Info.AID);
            pdSaga.assign(pdData, ProductEntity.Info.PD_ID);

            long branchId = RootContext.getBranchId();
            pdSaga.setString(BasicSagaEntity.Common.XID, xid);
            pdSaga.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
            pdSaga.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.ADD);

            // 插入
            addSaga(aid, pdSaga);
        }
        return pdId;
    }

    public FaiList<Integer> batchAddProduct(int aid, FaiList<Param> pdDataList) {
        int rt;
        if(pdDataList == null || pdDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, pdDataList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdDataList);
        }
        int count = getPdCount(aid);
        if(count > ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        FaiList<Integer> pdIdList = new FaiList<>();
        int maxId = m_dao.getId(aid);
        for(Param pdData : pdDataList) {
            int pdId = pdData.getInt(ProductEntity.Info.PD_ID, ++maxId);
            if(pdId > maxId) {
                maxId = pdId;
            }
            pdData.setInt(ProductEntity.Info.PD_ID, pdId);
            pdIdList.add(pdId);
        }

        rt = m_dao.batchInsert(pdDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        m_dao.updateId(aid, maxId, false);
        return pdIdList;
    }

    public void setSingle(int aid, int pdId, ParamUpdater recvUpdater) {
        FaiList<ParamUpdater> updaters = new FaiList<>();
        updaters.add(recvUpdater);
        Set<String> updateFields = Utils.validUpdaterList(updaters, ProductEntity.UPDATE_FIELDS, null);
        if (Utils.isEmptyList(updateFields)) {
            return;
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.EQ, pdId);

        updateProduct(aid, matcher, recvUpdater);
    }

    public void setProducts(int aid, FaiList<Integer> pdIds, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        updateProduct(aid, matcher, updater);
    }

    public static ParamUpdater assignUpdate(int flow, int aid, ParamUpdater recvUpdater) {
        int rt;
        if (recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "updater=null;aid=%d;", flow, aid);
        }
        Param recvInfo = recvUpdater.getData();
        String name = recvInfo.getString(ProductEntity.Info.NAME);
        if (name != null && !MgProductCheck.checkProductName(name)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, name not valid;flow=%d;aid=%d;name=%s", flow, aid, name);
        }

        Param data = new Param();
        for(String field : ProductEntity.UPDATE_FIELDS) {
            data.assign(recvInfo, field);
        }

        ParamUpdater updater = new ParamUpdater(data);
        updater.add(recvUpdater.getOpList(ProductEntity.Info.FLAG));
        updater.add(recvUpdater.getOpList(ProductEntity.Info.FLAG1));

        if(updater.isEmpty()) {
            Log.logDbg("no pd basic field changed;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        data.setCalendar(ProductEntity.Info.UPDATE_TIME, Calendar.getInstance());

        return updater;
    }

    private int creatAndSetId(int aid, Param info) {
        int rt;
        Integer pdId = info.getInt(ProductEntity.Info.PD_ID, 0);
        if(pdId <= 0) {
            pdId = m_dao.buildId(aid, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            pdId = m_dao.updateId(aid, pdId, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        info.setInt(ProductEntity.Info.PD_ID, pdId);
        return pdId;
    }

    private int updateProduct(int aid, ParamMatcher matcher, ParamUpdater updater) {
        int rt;
        if(updater == null || updater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, updater is null;flow=%d;aid=%d;", m_flow, aid);
        }
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;", m_flow, aid);
        }
        matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);

        // 使用分布式事务时，记录下修改的数据及主键
        if(withSaga) {
            FaiList<ParamUpdater> updaters = new FaiList<>();
            updaters.add(updater);
            Set<String> updateFields = Utils.validUpdaterList(updaters, ProductEntity.UPDATE_FIELDS, null);
            // 没有可修改的字段
            if (Utils.isEmptyList(updateFields)) {
                return 0;
            }
            // 加上主键信息，一起查出来
            updateFields.add(ProductEntity.Info.AID);
            updateFields.add(ProductEntity.Info.PD_ID);

            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, new FaiList<>(updateFields));
            // 修改的matcher 没有命中数据
            if(list.isEmpty()) {
                return 0;
            }
            long branchId = RootContext.getBranchId();
            for(Param info : list) {
                info.setString(BasicSagaEntity.Common.XID, xid);
                info.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.UPDATE);
            }
            // 插入
            addSagaList(aid, list);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.update(updater, matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updateProduct error;flow=%d;aid=%d;", m_flow, aid);
        }
        return refRowCount.value;
    }

    public void delProduct(int aid, int pdId) {
        int rt;
        if(pdId <= 0) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdId=%s", m_flow, aid, pdId);
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product list error;flow=%d;aid=%d;pdId=%d;", m_flow, aid, pdId);
        }
        Log.logStd("del product ok;aid=%d;pdId=%d;", aid, pdId);
    }

    public int deleteProductList(int aid, int tid, FaiList<Integer> pdIds, boolean softDel) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        matcher.and(ProductEntity.Info.SOURCE_TID, ParamMatcher.EQ, tid);
        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductEntity.Info.STATUS, ProductValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            return updateProduct(aid, matcher, updater);
        }

        // 开启了分布式事务，记录下删除的数据
        if(withSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            if(list.isEmpty()) {
                return 0;
            }
            long branchId = RootContext.getBranchId();
            for(Param info : list) {
                info.setString(BasicSagaEntity.Common.XID, xid);
                info.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.DEL);
            }
            // 插入
            addSagaList(aid, list);
        }
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product list error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }

        return refRowCount.value;
    }

    // 清空指定aid+unionPriId的数据
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "clearAcct error, unionPriIds is null;flow=%d;aid=%d;sourceUnionPriId=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;sourceUnionPriId=%s;", m_flow, aid, unionPriIds);
        }
        // 处理下idBuilder
        restoreMaxId(aid, false);
        Log.logStd("clearAcct ok;flow=%d;aid=%d;sourceUnionPriId=%s;", m_flow, aid, unionPriIds);
    }

    public void restoreMaxId(int aid, boolean needLock) {
        m_dao.restoreMaxId(aid, needLock);
        m_dao.clearIdBuilderCache(aid);
    }

    public int getPdCount(int aid) {
        // 从缓存中获取
        Param dataStatus = getDataStatus(aid);
        Integer count = dataStatus.getInt(DataStatus.Info.TOTAL_SIZE);
        if(count == null) {
            throw new MgException(Errno.ERROR, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
        }
        return count;
    }

    public Param getProductInfo(int aid, int pdId) {
        Param info = ProductCacheCtrl.InfoCache.getCacheInfo(aid, pdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> pdIds = new HashSet<>();
        pdIds.add(pdId);
        FaiList<Param> list = getList(aid, pdIds);
        if(Util.isEmptyList(list)) {
            return new Param();
        }
        info = list.get(0);
        return info;
    }

    public FaiList<Param> getProductList(int aid, FaiList<Integer> pdIdList) {
        int rt;
        if(pdIdList == null || pdIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "pdIdList is empty;aid=%d;pdIdList=%s;", aid, pdIdList);
        }
        HashSet<Integer> pdIds = new HashSet<Integer>(pdIdList);

        return getList(aid, pdIds);
    }

    public Param getDataStatus(int aid) {
        Param statusInfo = ProductCacheCtrl.DataStatusCache.get(aid);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductCacheCtrl.DataStatusCache.expire(aid, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getPdCountFromDb(aid);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductCacheCtrl.DataStatusCache.add(aid, statusInfo);
        return statusInfo;
    }

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.STATUS, ParamMatcher.NE, ProductValObj.Status.DEL);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        return listRef.value;
    }

    private FaiList<Param> getList(int aid, HashSet<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        List<String> pdIdStrs = pdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductCacheCtrl.InfoCache.getCacheList(aid, pdIdStrs);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == pdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        //searchArg.matcher.and(ProductEntity.Info.STATUS, ParamMatcher.NE, ProductValObj.Status.DEL);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        // 添加到缓存
        ProductCacheCtrl.InfoCache.addCacheList(aid, tmpRef.value);

        return list;
    }

    // saga补偿
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("pd need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(BasicSagaEntity.Common.SAGA_OP)));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(BasicSagaValObj.SagaOp.ADD));

        // 回滚修改操作
        rollback4Update(aid, groupBySagaOp.get(BasicSagaValObj.SagaOp.UPDATE));

        // 回滚删除操作
        rollback4Delete(aid, groupBySagaOp.get(BasicSagaValObj.SagaOp.DEL));
    }

    /**
     * 新增的数据补偿：根据主键删除
     */
    private void rollback4Add(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        int rt;
        FaiList<Integer> pdIds = Utils.getValList(new FaiList<>(list), ProductEntity.Info.PD_ID);

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;xid=%s;pdIds=%s;", m_flow, aid, xid, pdIds);
        }

        restoreMaxId(aid, false);
        Log.logStd("rollback add pd ok;aid=%d;xid=%s;pdIds=%s;", aid, xid, pdIds);
    }

    /**
     * 这边一个请求只会做一次修改操作
     * 所有被修改的数据字段都是一致的
     * 所以这里直接拿第一个被修改数据的字段做补偿
     * 如果可能会有多次修改，且每次修改字段不一致的，不能采用这种方式
     */
    private void rollback4Update(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        Param first = list.get(0);
        Set<String> keySet = first.keySet();
        keySet.remove(ProductEntity.Info.AID);
        keySet.remove(ProductEntity.Info.PD_ID);
        keySet.remove(BasicSagaEntity.Common.XID);
        keySet.remove(BasicSagaEntity.Common.BRANCH_ID);
        keySet.remove(BasicSagaEntity.Common.SAGA_OP);
        FaiList<String> keyList = new FaiList<>(keySet);

        FaiList<Param> dataList = new FaiList<>();
        for(Param info : list) {
            int pdId = info.getInt(ProductEntity.Info.PD_ID);
            Param data = new Param();
            // for updater
            for(String key : keyList) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(ProductEntity.Info.AID, aid);
            data.setInt(ProductEntity.Info.PD_ID, pdId);
            dataList.add(data);
        }
        ParamUpdater updater = new ParamUpdater();
        for(String key : keyList) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.EQ, "?");

        int rt = m_dao.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "update pd error;flow=%d;aid=%d;dataList=%s;", m_flow, aid, dataList);
        }
        Log.logStd("update pd rollback ok;flow=%d;aid=%d;dataList=%s;", m_flow, aid, dataList);
    }

    /**
     * 删除的数据补偿：插入已删除的数据
     */
    private void rollback4Delete(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }

        for(Param relInfo : list) {
            relInfo.remove(BasicSagaEntity.Common.XID);
            relInfo.remove(BasicSagaEntity.Common.BRANCH_ID);
            relInfo.remove(BasicSagaEntity.Common.SAGA_OP);
        }
        int rt = m_dao.batchInsert(new FaiList<>(list), null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "add list error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, xid, list);
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(BasicSagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(BasicSagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = m_sagaDao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get saga list error;aid=%d;xid=%s;branchId=%d;", aid, xid, branchId);
        }
        return tmpRef.value;
    }

    private int getPdCountFromDb(int aid) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
        }
        return countRef.value;
    }

    private void addSaga(int aid, Param sagaInfo) {
        if(!withSaga || Str.isEmpty(sagaInfo)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }
        int rt = m_sagaDao.insert(sagaInfo);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga insert product error;flow=%d;aid=%d;sagaInfo=%s;", m_flow, aid, sagaInfo);
        }
    }

    private void addSagaList(int aid, FaiList<Param> list) {
        if(!withSaga || Utils.isEmptyList(list)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }
        int rt = m_sagaDao.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga insert product error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductDaoCtrl err;");
        }
        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductDaoCtrl err;");
        }
    }

    private int m_flow;
    private String xid;
    private boolean withSaga;
    private ProductDaoCtrl m_dao;
    private ProductSagaDaoCtrl m_sagaDao;
}
