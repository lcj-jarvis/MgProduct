package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductRelSagaDaoCtrl;
import fai.app.DocOplogDef;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductRelProc {

    public ProductRelProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    public ProductRelProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_dao = ProductRelDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(m_xid)) {
            this.m_sagaDao = ProductRelSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
        }
        init(tc);
    }

    public int addProductRel(int aid, int unionPriId, Param relData) {
        int rt;
        if(Str.isEmpty(relData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        int count = getPdRelCount(aid, unionPriId);
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductRelValObj.Limit.COUNT_MAX);
        }
        int rlPdId = createAndSetRlPdId(aid, unionPriId, relData);
        rt = m_dao.insert(relData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product rel error;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        // 使用分布式事务时，记录下新增数据的主键
        if(addSaga) {
            Param pdRelSaga = new Param();
            pdRelSaga.assign(relData, ProductRelEntity.Info.AID);
            pdRelSaga.assign(relData, ProductRelEntity.Info.UNION_PRI_ID);
            pdRelSaga.assign(relData, ProductRelEntity.Info.PD_ID);

            long branchId = RootContext.getBranchId();
            pdRelSaga.setString(BasicSagaEntity.Common.XID, m_xid);
            pdRelSaga.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
            pdRelSaga.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.ADD);

            // 插入
            addSaga(aid, pdRelSaga);
        }
        return rlPdId;
    }

    public FaiList<Integer> batchAddProductRel(int aid, int unionPriId, Param pdInfo, FaiList<Param> relDataList) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
        }

        int count = getPdRelCount(aid, unionPriId);
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductRelValObj.Limit.COUNT_MAX);
        }

        int maxId = m_dao.getId(aid, unionPriId);
        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
            if(!Str.isEmpty(pdInfo)) {
                relData.assign(pdInfo, ProductRelEntity.Info.PD_ID);
                relData.assign(pdInfo, ProductRelEntity.Info.PD_TYPE);
            }
            Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(curPdId == null) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
            }

            int rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, ++maxId);
            if(rlPdId > maxId) {
                maxId = rlPdId;
            }
            rlPdIds.add(rlPdId);
        }

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        m_dao.updateId(aid, unionPriId, maxId, false);
        return rlPdIds;
    }

    public int getPdRelCount(int aid, int unionPriId) {
        Param dataStatus = getDataStatus(aid, unionPriId);
        Integer count = dataStatus.getInt(DataStatus.Info.TOTAL_SIZE);
        if(count == null) {
            throw new MgException(Errno.ERROR, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return count;
    }

    // 清空指定aid+unionPriId的数据
    public int clearData(int aid, int unionPriId, boolean softDel) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        // 记录要同步到es的数据
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        FaiList<String> fields = new FaiList<>();
        fields.add(ProductRelEntity.Info.PD_ID);
        FaiList<Param> list = searchFromDb(aid, searchArg, fields);
        ESUtil.batchPreLog(aid, list, DocOplogDef.Operation.DELETE_ONE);

        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            return updatePdRel(aid, matcher, updater);
        }
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;", m_flow, aid, unionPriId);
        }
        // 处理下idBuilder
        restoreMaxId(aid, unionPriId, false);
        Log.logStd("clearData ok;flow=%d;aid=%d;unionPriId=%s;delCnt=%s;", m_flow, aid, unionPriId, refRowCount.value);

        return refRowCount.value;
    }

    // 清空指定aid+unionPriId的数据
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "clearAcct error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);

        // 记录要同步给es的数据
        FaiList<Param> list = searchFromDb(aid, matcher, Utils.asFaiList(ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID));
        ESUtil.batchPreLog(aid, list, DocOplogDef.Operation.DELETE_ONE);

        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
        }
        // 处理下idBuilder
        for(int unionPriId : unionPriIds) {
            restoreMaxId(aid, unionPriId, false);
        }
        Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
    }

    public void restoreMaxId(int aid, int unionPriId, boolean needLock) {
        m_dao.restoreMaxId(aid, unionPriId, needLock);
        m_dao.clearIdBuilderCache(aid, unionPriId);
    }

    public int delProductRel(int aid, int unionPriId, FaiList<Integer> pdIds, boolean softDel) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;pdIds=%s", m_flow, aid, unionPriId, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            return updatePdRel(aid, matcher, updater);
        }
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;pdIds=%d;", m_flow, aid, unionPriId, pdIds);
        }
        return refRowCount.value;
    }

    /**
     * 根据pdId, 删除所有关联数据
     * @return 返回被删除的数据
     */
    public void delProductRelByPdId(int aid, FaiList<Integer> pdIds, boolean softDel) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }
        FaiList<FaiList<Integer>> batchPdIds = Utils.splitList(pdIds, BATCH_DEL_SIZE);
        for(FaiList<Integer> tmpPdIds : batchPdIds) {
            ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, tmpPdIds);

            if(softDel) {
                Param updateInfo = new Param();
                updateInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
                ParamUpdater updater = new ParamUpdater(updateInfo);
                updatePdRel(aid, matcher, updater);
                continue;
            }

            // 开启了分布式事务，需要记录删除的数据
            if(addSaga) {
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
                rt = m_dao.select(searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    throw new MgException(rt, "get unionPriId error;flow=%d;aid=%d;tmpPdIds=%d;", m_flow, aid, tmpPdIds);
                }
                FaiList<Param> oldList = listRef.value;
                if(Utils.isEmptyList(oldList)) {
                    return;
                }

                for(Param info : oldList) {
                    long branchId = RootContext.getBranchId();
                    info.setString(BasicSagaEntity.Common.XID, m_xid);
                    info.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
                    info.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.DEL);
                }
                // 插入
                addSagaList(aid, oldList);
            }
            rt = m_dao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del product rel error;flow=%d;aid=%d;tmpPdIds=%d;", m_flow, aid, tmpPdIds);
            }
        }
    }

    public void setSingle(int aid, int unionPriId, Integer pdId, ParamUpdater recvUpdater) {
        if (recvUpdater == null || recvUpdater.isEmpty()) {
            return;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.EQ, pdId);

        updatePdRel(aid, matcher, recvUpdater);
    }

    public void setPdRels(int aid, int unionPriId, FaiList<Integer> pdIds, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        updatePdRel(aid, matcher, updater);
    }

    public static ParamUpdater assignUpdate(int flow, int aid, ParamUpdater recvUpdater) {
        int rt;
        if (recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "updater=null;aid=%d;", flow, aid);
        }
        Param recvInfo = recvUpdater.getData();

        Param data = new Param();
        for(String field : ProductRelEntity.UPDATE_FIELDS) {
            data.assign(recvInfo, field);
        }
        ParamUpdater updater = new ParamUpdater(data);
        updater.add(recvUpdater.getOpList(ProductRelEntity.Info.FLAG));
        if(updater.isEmpty()) {
            Log.logDbg("no pd rel field changed;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        Calendar now = Calendar.getInstance();
        data.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        data.setCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        return updater;
    }

    private int updatePdRel(int aid, ParamMatcher matcher, ParamUpdater updater) {
        int rt;
        if(updater == null || updater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, updater is null;flow=%d;aid=%d;", m_flow, aid);
        }
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;", m_flow, aid);
        }
        matcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);


        // 使用分布式事务时，记录下修改的数据及主键
        if(addSaga) {
            FaiList<ParamUpdater> updaters = new FaiList<>();
            updaters.add(updater);
            Set<String> validUpdaterFields = Utils.validUpdaterList(updaters, ProductRelEntity.UPDATE_FIELDS, null);
            // 加上主键信息，一起查出来
            validUpdaterFields.add(ProductRelEntity.Info.AID);
            validUpdaterFields.add(ProductRelEntity.Info.UNION_PRI_ID);
            validUpdaterFields.add(ProductRelEntity.Info.PD_ID);

            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> oldList = searchFromDb(aid, searchArg, new FaiList<>(validUpdaterFields));

            if(oldList.isEmpty()) {
                Log.logStd("update not found;aid=%d;matcher=%s;update=%s;", aid, matcher.toJson(), updater.toJson());
                return 0;
            }

            FaiList<Param> sagaList = new FaiList<>();
            for(Param info : oldList) {
                Param sagaInfo = new Param();
                Utils.assign(sagaInfo, info, validUpdaterFields);

                long branchId = RootContext.getBranchId();
                sagaInfo.setString(BasicSagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
                sagaInfo.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.UPDATE);
                sagaList.add(sagaInfo);
            }
            // 插入
            addSagaList(aid, sagaList);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.update(updater, matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRel error;flow=%d;aid=%d;", m_flow, aid);
        }
        return refRowCount.value;
    }

    // saga补偿
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("rel info need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
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
        Map<Integer, List<Param>> groupByUnionPriId = list.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductRelEntity.Info.UNION_PRI_ID)));
        for(Integer unionPriId : groupByUnionPriId.keySet()) {
            List<Param> batchList = groupByUnionPriId.get(unionPriId);
            FaiList<Integer> pdIds = Utils.getValList(new FaiList<>(batchList), ProductRelEntity.Info.PD_ID);

            ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
            rt = m_dao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;pdIds=%s;", m_flow, aid, unionPriId, pdIds);
            }

            restoreMaxId(aid, unionPriId, false);
            Log.logStd("rollback add ok;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
            // es同步数据 预处理
            ESUtil.batchPreLog(aid, unionPriId, pdIds, DocOplogDef.Operation.DELETE_ONE);
        }
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
        keySet.remove(ProductRelEntity.Info.AID);
        keySet.remove(ProductRelEntity.Info.UNION_PRI_ID);
        keySet.remove(ProductRelEntity.Info.PD_ID);
        keySet.remove(BasicSagaEntity.Common.XID);
        keySet.remove(BasicSagaEntity.Common.BRANCH_ID);
        keySet.remove(BasicSagaEntity.Common.SAGA_OP);
        FaiList<String> keyList = new FaiList<>(keySet);

        FaiList<Param> dataList = new FaiList<>();
        for(Param info : list) {
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            Param data = new Param();
            // for updater
            for(String key : keyList) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(ProductRelEntity.Info.AID, aid);
            data.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(ProductRelEntity.Info.PD_ID, pdId);
            dataList.add(data);
        }
        ParamUpdater updater = new ParamUpdater();
        for(String key : keyList) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.EQ, "?");

        int rt = m_dao.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRel error;flow=%d;aid=%d;dataList=%s;", m_flow, aid, dataList);
        }

        // es同步数据 预处理
        ESUtil.batchPreLog(aid, new FaiList<>(list), DocOplogDef.Operation.UPDATE_ONE);

        Log.logStd("updatePdRel rollback ok;flow=%d;aid=%d;dataList=%s;", m_flow, aid, dataList);
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
        int rt = m_dao.batchInsert(new FaiList<>(list), null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "add list error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }

        // es同步数据 预处理
        ESUtil.batchPreLog(aid, new FaiList<>(list), DocOplogDef.Operation.UPDATE_ONE);

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    public Param getProductRel(int aid, int unionPriId, int pdId) {
        int rt;
        if(pdId <= 0) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;pdId=%s", m_flow, aid, unionPriId, pdId);
        }
        Param info = ProductRelCacheCtrl.InfoCache.getCacheInfo(aid, unionPriId, pdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> pdIds = new HashSet<>();
        pdIds.add(pdId);
        FaiList<Param> relList = getListByPdId(aid, unionPriId, pdIds);
        if(relList == null || relList.isEmpty()) {
            return new Param();
        }

        return relList.get(0);
    }

    public FaiList<Param> getProductRelList(int aid, int unionPriId, FaiList<Integer> pdIdList) {
        int rt;
        if(Utils.isEmptyList(pdIdList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIdList is empty;aid=%d;pdIdList=%s;", aid, pdIdList);
        }
        HashSet<Integer> pdIds = new HashSet<Integer>(pdIdList);

        return getListByPdId(aid, unionPriId, pdIds);
    }

    public FaiList<Param> getBoundUniPriIds(int aid, FaiList<Integer> pdIds) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        //searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        //只查aid+pdId+unionPriId+rlPdId
        int rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getRlPdInfoByPdIds fail;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);

        }

        return listRef.value;
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductRelCacheCtrl.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductRelCacheCtrl.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getPdRelCountFromDB(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductRelCacheCtrl.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    /**
     * 在 aid 下搜索
     */
    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        //searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;match=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;match=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        return listRef.value;
    }
    public FaiList<Param> searchFromDb(int aid, ParamMatcher matcher, FaiList<String> selectFields) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        return searchFromDb(aid, searchArg, selectFields);
    }

    private int getPdRelCountFromDB(int aid, int unionPriId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }

        return countRef.value;
    }

    private int createAndSetRlPdId(int aid, int unionPriId, Param relData) {
        int rt;
        Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, 0);
        if(rlPdId <= 0) {
            rlPdId = m_dao.buildId(aid, unionPriId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }else {
            rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }
        relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
        return rlPdId;
    }

    public Integer getPdId(int aid, int unionPriId, int sysType, int rlPdId) {
        HashSet<Integer> rlPdIds = new HashSet<>();
        rlPdIds.add(rlPdId);
        FaiList<Integer> list = getPdIds(aid, unionPriId, sysType, rlPdIds);
        if(Utils.isEmptyList(list)) {
            return null;
        }

        return list.get(0);
    }

    public FaiList<Integer> getPdIds(int aid, int unionPriId, int sysType, HashSet<Integer> rlPdIds) {
        FaiList<Param> list = getPdIdRels(aid, unionPriId, sysType, rlPdIds);

        return Utils.getValList(list, ProductRelEntity.Info.PD_ID);
    }

    public FaiList<Param> getPdIdRels(int aid, int unionPriId, int sysType, HashSet<Integer> rlPdIds) {
        FaiList<Param> list = ProductRelCacheCtrl.PdIdCache.getCacheList(aid, unionPriId, sysType, rlPdIds);
        if(list == null) {
            list = new FaiList<>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == rlPdIds.size()) {
            return list;
        }

        // 拿到未缓存的rlPdId list
        FaiList<Integer> noCacheIds = new FaiList<Integer>();
        noCacheIds.addAll(rlPdIds);
        for(Param info : list) {
            Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
            noCacheIds.remove(rlPdId);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        searchArg.matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheIds);

        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = m_dao.select(searchArg, tmpRef, ProductRelEntity.Info.RL_PD_ID, ProductRelEntity.Info.PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select pdId error;aid=%d;uid=%d;sysType=%d;rlPdIds=%s;", aid, unionPriId, sysType, rlPdIds);
        }

        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
            // 添加到缓存
            ProductRelCacheCtrl.PdIdCache.addCacheList(aid, unionPriId, sysType, tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        return list;
    }

    private FaiList<Param> getListByPdId(int aid, int unionPriId, HashSet<Integer> pdIds) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, pdIds is empty;aid=%d;uid=%s;", aid, unionPriId);
        }
        List<String> pdIdsStrs = pdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.InfoCache.getCacheList(aid, unionPriId, pdIdsStrs);
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
            Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        //searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return list;
        }

        // 添加到缓存
        ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, tmpRef.value);

        return list;
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(BasicSagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(BasicSagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = m_sagaDao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get saga list error;aid=%d;xid=%s;branchId=%d;", aid, xid, branchId);
        }
        return tmpRef.value;
    }

    private void addSaga(int aid, Param sagaInfo) {
        if(!addSaga || Str.isEmpty(sagaInfo)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }
        int rt = m_sagaDao.insert(sagaInfo);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga insert productRel error;flow=%d;aid=%d;sagaInfo=%s;", m_flow, aid, sagaInfo);
        }
    }

    public void addSagaList(int aid, FaiList<Param> list) {
        if(!addSaga || Utils.isEmptyList(list)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }
        int rt = m_sagaDao.batchInsert(list, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga insert productRel error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }
        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductRelSagaDaoCtrl err;");
        }
    }

    private int m_flow;
    private boolean addSaga;
    private String m_xid;
    private ProductRelDaoCtrl m_dao;
    private ProductRelSagaDaoCtrl m_sagaDao;
    private static final int BATCH_DEL_SIZE = 500;
}
