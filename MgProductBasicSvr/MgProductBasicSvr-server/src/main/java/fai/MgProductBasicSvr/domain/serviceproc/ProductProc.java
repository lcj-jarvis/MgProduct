package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.GfwUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.bak.ProductBakDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductSagaDaoCtrl;
import fai.app.DocOplogDef;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductProc {
    public ProductProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    // 备份还原用
    public ProductProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
        this.m_flow = flow;
        this.m_dao = ProductDaoCtrl.getInstance(flow, aid);
        if(useBak) {
            this.m_bakDao = ProductBakDaoCtrl.getInstance(flow, aid);
        }
        init(tc);
    }

    // 分布式事务用
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

    public void backupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        if(Utils.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }

        // 查出当前的数据
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = searchFromDbWithDel(aid, searchArg, null);
        if(fromList.isEmpty()) {
            return;
        }

        Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
        Calendar maxUpdateTime = null;
        Calendar minUpdateTime = null;
        for (Param fromInfo : fromList) {
            fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
            newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
            Calendar updateTime = fromInfo.getCalendar(ProductEntity.Info.UPDATE_TIME);
            if(minUpdateTime == null || updateTime.before(minUpdateTime)) {
                minUpdateTime = updateTime;
            }
            if(maxUpdateTime == null || updateTime.after(maxUpdateTime)) {
                maxUpdateTime = updateTime;
            }
        }

        // 查出已有的备份数据，和当前数据比较，若当前数据已经存在备份数据，则更新backupIdFlag，否则新增备份数据
        SearchArg oldBakArg = new SearchArg();
        oldBakArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        oldBakArg.matcher.and(ProductEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        if(minUpdateTime != null) {
            oldBakArg.matcher = new ParamMatcher(ProductEntity.Info.UPDATE_TIME, ParamMatcher.GE, minUpdateTime);
        }
        if(maxUpdateTime != null) {
            oldBakArg.matcher = new ParamMatcher(ProductEntity.Info.UPDATE_TIME, ParamMatcher.LE, maxUpdateTime);
        }
        oldBakArg.matcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        FaiList<Param> oldBakList = searchBakList(aid, oldBakArg);

        Set<String> oldBakUniqueKeySet = new HashSet<String>((int)(oldBakList.size()/0.75f)+1);
        for (Param oldBak : oldBakList) {
            oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
        }
        // 获取交集，说明剩下的这些是要合并的备份数据
        oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);

        if(!oldBakUniqueKeySet.isEmpty()){
            // 合并标记
            ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);

            // 合并条件
            ParamMatcher mergeMatcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

            FaiList<Param> dataList = new FaiList<Param>();
            for (String bakUniqueKey : oldBakUniqueKeySet) {
                String[] keys = bakUniqueKey.split(DELIMITER);
                Calendar updateTime = Calendar.getInstance();
                updateTime.setTimeInMillis(Long.valueOf(keys[1]));
                Param data = new Param();

                // mergeUpdater start
                data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                // mergeUpdater end

                // mergeMatcher start
                data.setInt(ProductEntity.Info.AID, aid);
                data.setInt(ProductEntity.Info.PD_ID, Integer.valueOf(keys[0]));
                data.setCalendar(ProductEntity.Info.UPDATE_TIME, updateTime);
                // mergeMatcher end

                dataList.add(data);
            }
            rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
            if(rt != Errno.OK) {
                throw new MgException(rt, "merge bak update err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }

        // 移除掉合并的数据，剩下的就是需要新增的备份数据
        newBakUniqueKeySet.removeAll(oldBakUniqueKeySet);

        for (int j = fromList.size(); --j >= 0;) {
            Param formInfo = fromList.get(j);
            if(newBakUniqueKeySet.contains(getBakUniqueKey(formInfo))){
                // 置起当前备份标识
                formInfo.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                continue;
            }
            fromList.remove(j);
        }

        if(fromList.isEmpty()) {
            return;
        }
        // 批量插入备份表
        rt = m_bakDao.batchInsert(fromList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchInsert bak err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }

        Log.logStd("backupData ok;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
        ParamMatcher delMatcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
        rt = m_bakDao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        Log.logStd("del rel bak ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
    }

    public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_dao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "relDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        if(Utils.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }

        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        rt = m_dao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "delete err;delMatcher=%s;backupId=%d;backupFlag=%d;", delMatcher.toJson(), backupId, backupFlag);
        }

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = searchBakList(aid, bakSearchArg);
        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        // 还原备份数据
        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_dao.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        m_dao.restoreMaxId(aid, false);
        m_dao.clearIdBuilderCache(aid);
    }

    public int addProduct(int aid, int tid, int siteId, Param pdData) {
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
            pdSaga.setString(SagaEntity.Common.XID, xid);
            pdSaga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            pdSaga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            pdSaga.setCalendar(SagaEntity.Common.SAGA_TIME, Calendar.getInstance());

            // 插入
            addSaga(aid, pdSaga);
        }
        GfwUtil.preWriteGfwLog(aid, tid, siteId, pdData);
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
        FaiList<Param> sagaList = new FaiList<>();
        FaiList<Integer> pdIdList = new FaiList<>();
        int maxId = m_dao.getId(aid);
        for(Param pdData : pdDataList) {
            int pdId = pdData.getInt(ProductEntity.Info.PD_ID, ++maxId);
            if(pdId > maxId) {
                maxId = pdId;
            }
            pdData.setInt(ProductEntity.Info.PD_ID, pdId);
            pdIdList.add(pdId);
            // 使用分布式事务时，记录下新增数据的主键
            if(withSaga) {
                Param pdSaga = new Param();
                pdSaga.assign(pdData, ProductEntity.Info.AID);
                pdSaga.assign(pdData, ProductEntity.Info.PD_ID);

                long branchId = RootContext.getBranchId();
                pdSaga.setString(SagaEntity.Common.XID, xid);
                pdSaga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                pdSaga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                pdSaga.setCalendar(SagaEntity.Common.SAGA_TIME, Calendar.getInstance());

                sagaList.add(pdSaga);
            }
        }

        rt = m_dao.batchInsert(pdDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        m_dao.updateId(aid, maxId, false);

        // 使用分布式事务时，记录下新增数据的主键
        addSagaList(aid, sagaList);
        return pdIdList;
    }

    // 临时方法，门店迁移数据
    public void batchSet(int aid, FaiList<Param> list) {
        int rt;
        Param first = list.get(0);
        Set<String> keySet = first.keySet();
        keySet.remove(ProductEntity.Info.AID);
        keySet.remove(ProductEntity.Info.PD_ID);
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

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.EQ, "?");

        ParamUpdater updater = new ParamUpdater();
        for(String key : keyList) {
            updater.getData().setString(key, "?");
        }

        rt = m_dao.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchUpdate product error;flow=%d;aid=%d;", m_flow, aid);
        }
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
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
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
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            }
            // 插入
            addSagaList(aid, list);
        }

        return delProduct(aid, matcher);
    }

    public int delProduct(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK){
            throw new MgException(rt, "del product list error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
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
        if(Utils.isEmptyList(list)) {
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

        return searchFromDbWithDel(aid, searchArg, selectFields);
    }

    public FaiList<Param> searchFromDbWithDel(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();

        // 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tablename
        m_dao.setTableName(aid);

        int rt = m_dao.select(searchArg, listRef, selectFields);

        // 查完之后恢复最初的tablename
        m_dao.restoreTableName();

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
    public void rollback4Saga(int aid, long branchId, ProductRelProc relProc) {
        FaiList<Param> list = getSagaList(aid, xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("pd need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚修改操作
        rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE), relProc);

        // 回滚删除操作
        rollback4Delete(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));
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
    private void rollback4Update(int aid, List<Param> list, ProductRelProc relProc) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        Param first = list.get(0);
        Set<String> keySet = first.keySet();
        keySet.remove(ProductEntity.Info.AID);
        keySet.remove(ProductEntity.Info.PD_ID);
        keySet.remove(SagaEntity.Common.XID);
        keySet.remove(SagaEntity.Common.BRANCH_ID);
        keySet.remove(SagaEntity.Common.SAGA_OP);
        keySet.remove(SagaEntity.Common.SAGA_TIME);
        FaiList<String> keyList = new FaiList<>(keySet);

        FaiList<Integer> pdIds = new FaiList<>();
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

        // 同步数据给es 预处理
        preLog4ES(aid, pdIds, relProc);
    }

    /**
     * 只有rollback4Update 会调用
     * 因为除了更新操作，新增和删除必然会操作到关系表对应的数据
     * 所以在操作关系表数据的时候，同步给es就行了
     */
    private void preLog4ES(int aid, FaiList<Integer> pdIds, ProductRelProc relProc){
        if(Utils.isEmptyList(pdIds) || relProc == null) {
            return;
        }
        SearchArg relSearch = new SearchArg();
        relSearch.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        relSearch.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        FaiList<String> fields = new FaiList<>();
        fields.add(ProductRelEntity.Info.UNION_PRI_ID);
        fields.add(ProductRelEntity.Info.PD_ID);
        // 根据pdIds，反查关系表，得到unionPirId 和 pdId的绑定关系
        FaiList<Param> relList = relProc.searchFromDb(aid, relSearch, fields);

        ESUtil.batchPreLog(aid, relList, DocOplogDef.Operation.UPDATE_ONE);
    }

    /**
     * 删除的数据补偿：插入已删除的数据
     */
    private void rollback4Delete(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }

        for(Param relInfo : list) {
            relInfo.remove(SagaEntity.Common.XID);
            relInfo.remove(SagaEntity.Common.BRANCH_ID);
            relInfo.remove(SagaEntity.Common.SAGA_OP);
            relInfo.remove(SagaEntity.Common.SAGA_TIME);
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
        searchArg.matcher.and(SagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
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
            throw new MgException("registered ProductSagaDaoCtrl err;");
        }
        if(m_bakDao != null && !tc.register(m_bakDao)) {
            throw new MgException("registered ProductBakDaoCtrl err;");
        }
    }

    private FaiList<Param> searchBakList(int aid, SearchArg searchArg) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bakDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        return listRef.value;
    }

    private static String getBakUniqueKey(Param fromInfo) {
        return fromInfo.getInt(ProductEntity.Info.PD_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductEntity.Info.UPDATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";

    private int m_flow;
    private String xid;
    private boolean withSaga;
    private ProductDaoCtrl m_dao;
    private ProductSagaDaoCtrl m_sagaDao;
    private ProductBakDaoCtrl m_bakDao;
}
