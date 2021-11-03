package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.bak.ProductRelBakDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductRelSagaDaoCtrl;
import fai.app.DocOplogDef;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.middleground.FaiValObj;
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

public class ProductRelProc {

    public ProductRelProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    // 备份还原用
    public ProductRelProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
        this.m_flow = flow;
        this.m_dao = ProductRelDaoCtrl.getInstance(flow, aid);
        if(useBak) {
            this.m_bakDao = ProductRelBakDaoCtrl.getInstance(flow, aid);
        }
        init(tc);
    }

    // 分布式事务用
    public ProductRelProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_dao = ProductRelDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(m_xid)) {
            this.m_sagaDao = ProductRelSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
            this.sagaMap = new HashMap<>();
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

        for(int unionPriId : unionPriIds) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            FaiList<Param> fromList = searchFromDbWithDel(aid, searchArg, null);
            if(fromList.isEmpty()) {
                continue;
            }

            Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
            Calendar maxUpdateTime = null;
            Calendar minUpdateTime = null;
            for (Param fromInfo : fromList) {
                fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
                newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));

                Calendar updateTime = fromInfo.getCalendar(ProductRelEntity.Info.UPDATE_TIME);
                if(minUpdateTime == null || updateTime.before(minUpdateTime)) {
                    minUpdateTime = updateTime;
                }
                if(maxUpdateTime == null || updateTime.after(maxUpdateTime)) {
                    maxUpdateTime = updateTime;
                }
            }

            SearchArg oldBakArg = new SearchArg();
            oldBakArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
            oldBakArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            if(minUpdateTime != null) {
                oldBakArg.matcher = new ParamMatcher(ProductRelEntity.Info.UPDATE_TIME, ParamMatcher.GE, minUpdateTime);
            }
            if(maxUpdateTime != null) {
                oldBakArg.matcher = new ParamMatcher(ProductRelEntity.Info.UPDATE_TIME, ParamMatcher.LE, maxUpdateTime);
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
                ParamMatcher mergeMatcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductRelEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

                FaiList<Param> dataList = new FaiList<Param>();
                for (String bakUniqueKey : oldBakUniqueKeySet) {
                    String[] keys = bakUniqueKey.split(DELIMITER);
                    Calendar updateTime = Calendar.getInstance();
                    updateTime.setTimeInMillis(Long.valueOf(keys[2]));
                    Param data = new Param();

                    // mergeUpdater start
                    data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                    // mergeUpdater end

                    // mergeMatcher start
                    data.setInt(ProductRelEntity.Info.AID, aid);
                    data.setInt(ProductRelEntity.Info.UNION_PRI_ID, Integer.valueOf(keys[0]));
                    data.setInt(ProductRelEntity.Info.PD_ID, Integer.valueOf(keys[1]));
                    data.setCalendar(ProductRelEntity.Info.UPDATE_TIME, updateTime);
                    // mergeMatcher end

                    dataList.add(data);
                }
                rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
                if(rt != Errno.OK) {
                    throw new MgException(rt, "merge bak update err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
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
                continue;
            }
            // 批量插入备份表
            rt = m_bakDao.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "batchInsert bak err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
            }
        }

        Log.logStd("backupData ok;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
        ParamMatcher delMatcher = new ParamMatcher(MgBackupEntity.Info.AID, ParamMatcher.EQ, aid);
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
        ParamMatcher delMatcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_dao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "restore del old err;delMatcher=%s;backupId=%d;backupFlag=%d;", delMatcher, backupId, backupFlag);
        }

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        bakSearchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        bakSearchArg.matcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        FaiList<Param> fromList = searchBakList(aid, bakSearchArg);
        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_dao.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        for(int unionPriId : unionPriIds) {
            m_dao.restoreMaxId(aid, unionPriId, false);
            m_dao.clearIdBuilderCache(aid, unionPriId);
        }
    }

    public void cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId) {
        ParamMatcher delMatcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, toUnionPriId);

        int rt = m_dao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "clear old list error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }

        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, fromUnionPriId);
        FaiList<Param> list = searchFromDb(aid, matcher, null);
        if(list.isEmpty()) {
            return;
        }
        Calendar now = Calendar.getInstance();
        for(Param info : list) {
            info.setInt(ProductRelEntity.Info.UNION_PRI_ID, toUnionPriId);
            info.setCalendar(ProductRelEntity.Info.CREATE_TIME, now);
            info.setCalendar(ProductRelEntity.Info.UPDATE_TIME, now);
        }
        // 记录要同步给es的数据
        ESUtil.batchPreLog(aid, list, DocOplogDef.Operation.UPDATE_ONE);
        // insert
        rt = m_dao.batchInsert(list, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "cloneBizBind error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
        // 处理idBuidler
        restoreMaxId(aid, toUnionPriId, false);

        Log.logStd("cloneBizBind ok;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
    }

    public FaiList<Param> setPdStatus(int aid, int ownUnionPriId, FaiList<Integer> unionPriIds, FaiList<Integer> pdIds, int status) {
        if(Utils.isEmptyList(unionPriIds) || Utils.isEmptyList(pdIds)) {
            throw new MgException(Errno.ARGS_ERROR, "arg error;aid=%s;uids=%s;pdIds=%s;status=%s;", aid, unionPriIds, pdIds, status);
        }
        HashSet<Pair<Integer, Integer>> addSet = new HashSet();
        for(int unionPriId : unionPriIds) {
            for(int pdId : pdIds) {
                addSet.add(new Pair<>(unionPriId, pdId));
            }
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        FaiList<Param> list = searchFromDbWithDel(aid, searchArg, Utils.asFaiList(ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID));
        for(Param info : list) {
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            addSet.remove(new Pair<>(unionPriId, pdId));
        }

        FaiList<Param> addList = new FaiList<>();
        Calendar now = Calendar.getInstance();
        if(!addSet.isEmpty()) {
            HashSet<Integer> addPdIds = new HashSet<>();
            for(Pair pair : addSet) {
                addPdIds.add((Integer) pair.second);
            }
            FaiList<Param> sourceList = getListByPdId(aid, ownUnionPriId, addPdIds);
            Map<Integer, Param> pdId_pdInfo = Utils.getMap(sourceList, ProductRelEntity.Info.PD_ID);
            HashSet<Integer> needAddUnionPriIds = new HashSet<>();
            for(Pair pair : addSet) {
                int unionPriId = (int) pair.first;
                int pdId = (int) pair.second;
                Param info = new Param();
                Param sourceInfo = pdId_pdInfo.get(pdId);
                info.assign(sourceInfo);
                info.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
                info.setCalendar(ProductRelEntity.Info.CREATE_TIME, now);
                info.setCalendar(ProductRelEntity.Info.UPDATE_TIME, now);
                addList.add(info);
                needAddUnionPriIds.add(unionPriId);
            }
            int rt = m_dao.batchInsert(addList, null, false);
            if(rt != Errno.OK) {
                throw new MgException(rt, "batch insert err;aid=%d;addList=%s;", aid, addList);
            }
            // 处理idBuidler
            for(int unionPirId : needAddUnionPriIds) {
                restoreMaxId(aid, unionPirId, false);
            }
            // 记录要同步给es的数据
            ESUtil.batchPreLog(aid, addList, DocOplogDef.Operation.UPDATE_ONE);
        }

        Param updaterInfo = new Param();
        updaterInfo.setCalendar(ProductRelEntity.Info.UPDATE_TIME, now);
        updaterInfo.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        updaterInfo.setInt(ProductRelEntity.Info.STATUS, status);
        ParamUpdater updater = new ParamUpdater(updaterInfo);

        int count = updatePdRel(aid, matcher, updater);

        Log.logStd("set status ok;aid=%s;update cnt=%s;updater=%s;matcher=%s;", aid, count, updater.toJson(), matcher.toJson());

        return addList;
    }

    public int addProductRel(int aid, int tid, int unionPriId, Param relData) {
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
        int rlPdId = createAndSetRlPdId(aid, tid, unionPriId, relData);
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
            pdRelSaga.setString(SagaEntity.Common.XID, m_xid);
            pdRelSaga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            pdRelSaga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            pdRelSaga.setCalendar(SagaEntity.Common.SAGA_TIME, Calendar.getInstance());

            // 插入
            addSaga(aid, pdRelSaga);
        }
        return rlPdId;
    }

    public FaiList<Integer> batchAddProductRel(int aid, int tid, int unionPriId, FaiList<Param> relDataList) {
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

        FaiList<Integer> rlPdIds = batchCreatAndSetId(aid, tid, unionPriId, relDataList);

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        // 使用分布式事务时，记录下新增数据的主键
        if(addSaga) {
            FaiList<Param> pdRelSagaList = new FaiList<>();
            Calendar now = Calendar.getInstance();
            for(Param relData : relDataList) {
                Param pdRelSaga = new Param();
                pdRelSaga.assign(relData, ProductRelEntity.Info.AID);
                pdRelSaga.assign(relData, ProductRelEntity.Info.UNION_PRI_ID);
                pdRelSaga.assign(relData, ProductRelEntity.Info.PD_ID);

                long branchId = RootContext.getBranchId();
                pdRelSaga.setString(SagaEntity.Common.XID, m_xid);
                pdRelSaga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                pdRelSaga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                pdRelSaga.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                pdRelSagaList.add(pdRelSaga);
            }
            // 插入
            addSagaList(aid, pdRelSagaList);
        }

        return rlPdIds;
    }

    public void insert4Clone(int aid, FaiList<Integer> unionPriIds, FaiList<Param> relDataList) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
        }

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        for(Integer unionPriId : unionPriIds) {
            m_dao.restoreMaxId(aid, unionPriId, false);
            m_dao.clearIdBuilderCache(aid, unionPriId);
        }
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
        fields.add(ProductRelEntity.Info.UNION_PRI_ID);
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
        return delProductRel(aid, matcher);
    }

    public int delProductRel(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK){
            throw new MgException(rt, "delProductRel error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
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
                FaiList<Param> oldList = searchFromDb(aid, searchArg, null);
                if(Utils.isEmptyList(oldList)) {
                    return;
                }

                Calendar now = Calendar.getInstance();
                for(Param info : oldList) {
                    long branchId = RootContext.getBranchId();
                    info.setString(SagaEntity.Common.XID, m_xid);
                    info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                    info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
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

    public void batchSet(int aid, FaiList<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        Param first = list.get(0);
        Set<String> keySet = first.keySet();
        keySet.remove(ProductRelEntity.Info.AID);
        keySet.remove(ProductRelEntity.Info.UNION_PRI_ID);
        keySet.remove(ProductRelEntity.Info.PD_ID);
        keySet.remove(ProductRelEntity.Info.RL_PD_ID);
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

        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
    }

    public void batchSet(int aid, FaiList<Integer> unionPriIds, FaiList<Integer> pdIds, ParamUpdater updater) {
        if(Utils.isEmptyList(unionPriIds) || Utils.isEmptyList(pdIds) || updater == null || updater.isEmpty()) {
            return;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        updatePdRel(aid, matcher, updater);

        // es同步数据 预处理
        for(int unionPriId : unionPriIds) {
            ESUtil.batchPreLog(aid, unionPriId, pdIds, DocOplogDef.Operation.UPDATE_ONE);
        }

        Log.logStd("batchSet ok;flow=%d;aid=%d;uid=%s;pdIds=%s;update=%s;", m_flow, aid, unionPriIds, pdIds, updater.toJson());
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

    public int getMaxSort(int aid, int unionPriId) {
        String sortCache = ProductRelCacheCtrl.SortCache.get(aid, unionPriId);
        if(!Str.isEmpty(sortCache)) {
            return Parser.parseInt(sortCache, ProductRelValObj.Default.SORT);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        FaiList<Param> resList = searchFromDbWithDel(aid, searchArg, Utils.asFaiList("max(sort) as sort"));
        if (Utils.isEmptyList(resList)) {
            Log.logDbg(Errno.NOT_FOUND, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return ProductRelValObj.Default.SORT;
        }

        Param info = resList.get(0);
        int sort = info.getInt(ProductRelEntity.Info.SORT, ProductRelValObj.Default.SORT);
        // 添加到缓存
        ProductRelCacheCtrl.SortCache.set(aid, unionPriId, sort);
        return sort;
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
            preAddUpdateSaga(aid, updater, matcher);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.update(updater, matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRel error;flow=%d;aid=%d;", m_flow, aid);
        }
        return refRowCount.value;
    }
    // 预记录修改操作数据 for saga
    private void preAddUpdateSaga(int aid, ParamUpdater updater, ParamMatcher matcher) {
        FaiList<ParamUpdater> updaters = new FaiList<>();
        updaters.add(updater);
        Set<String> validUpdaterFields = new HashSet<>();
        validUpdaterFields.addAll(ProductRelEntity.UPDATE_FIELDS);
        // 加上主键信息，一起查出来
        validUpdaterFields.add(ProductRelEntity.Info.AID);
        validUpdaterFields.add(ProductRelEntity.Info.UNION_PRI_ID);
        validUpdaterFields.add(ProductRelEntity.Info.PD_ID);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        FaiList<Param> oldList = searchFromDbWithDel(aid, searchArg, new FaiList<>(validUpdaterFields));

        if(oldList.isEmpty()) {
            Log.logStd("update not found;aid=%d;matcher=%s;update=%s;", aid, matcher.toJson(), updater.toJson());
            return;
        }

        Calendar now = Calendar.getInstance();
        for(Param info : oldList) {
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            PrimaryKey primaryKey = new PrimaryKey(aid, unionPriId, pdId);
            if(sagaMap.containsKey(primaryKey)) {
                continue;
            }

            long branchId = RootContext.getBranchId();
            info.setString(SagaEntity.Common.XID, m_xid);
            info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, info);
        }
    }
    // 事务的最后调用
    public void transactionEnd(int aid) {
        // db记录 修改操作 涉及的数据 for saga
        if(addSaga && sagaMap != null && !sagaMap.isEmpty()) {
            addSagaList(aid, new FaiList<>(sagaMap.values()));
        }
    }
    private Map<PrimaryKey, Param> sagaMap;
    private class PrimaryKey {
        int aid;
        int unionPirId;
        int pdId;

        public PrimaryKey(int aid, int unionPirId, int pdId) {
            this.aid = aid;
            this.unionPirId = unionPirId;
            this.pdId = pdId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    unionPirId == that.unionPirId &&
                    pdId == that.pdId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, unionPirId, pdId);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", unionPirId=" + unionPirId +
                    ", pdId=" + pdId +
                    '}';
        }
    }

    // saga补偿
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("rel info need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 保证顺序：回滚删除操作 -> 回滚新增操作 -> 回滚修改操作
        // 回滚删除操作
        rollback4Delete(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚修改操作
        rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE));
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
        keySet.remove(SagaEntity.Common.XID);
        keySet.remove(SagaEntity.Common.BRANCH_ID);
        keySet.remove(SagaEntity.Common.SAGA_OP);
        keySet.remove(SagaEntity.Common.SAGA_TIME);
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
            relInfo.remove(SagaEntity.Common.XID);
            relInfo.remove(SagaEntity.Common.BRANCH_ID);
            relInfo.remove(SagaEntity.Common.SAGA_OP);
            relInfo.remove(SagaEntity.Common.SAGA_TIME);
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

    public FaiList<Param> getProductRelListWithDel(int aid, int unionPriId, FaiList<Integer> pdIdList) {
        int rt;
        if(Utils.isEmptyList(pdIdList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIdList is empty;aid=%d;pdIdList=%s;", aid, pdIdList);
        }
        HashSet<Integer> pdIds = new HashSet<Integer>(pdIdList);

        return getListByPdIdWithDel(aid, unionPriId, pdIds);
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

        //只查aid+pdId+unionPriId+rlPdId
        FaiList<Param> list = searchFromDb(aid, searchArg, Utils.asFaiList(ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID));
        if(list == null) {
            list = new FaiList<>();
        }
        if(list.isEmpty()) {
            Log.logDbg(Errno.NOT_FOUND, "not found;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }

        return list;
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
     * 在 aid 下搜索, 不包含软删数据
     * 是否获取软删通过区分接口控制
     * 不用参数控制的原因：
     * 如若一个业务之前没有软删，后面支持软删，通过参数控制的方式，业务方需要全面修改接口使用
     * 获取软删场景是较少的，所以一般默认使用过滤软删接口，需要获取软删数据的使用另外的接口
     */
    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        // TODO 等门店那边接入完成，需要获取软删数据的请求走了专门的接口之后，这个status的限制需要在线上开放
        if(MgProductCheck.isDev()) {
            searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        }

        return searchFromDbWithDel(aid, searchArg, selectFields);
    }
    /**
     * 在 aid 下搜索, 不包含软删数据
     */
    public FaiList<Param> searchFromDb(int aid, ParamMatcher matcher, FaiList<String> selectFields) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        return searchFromDb(aid, searchArg, selectFields);
    }
    /**
     * 在 aid 下搜索，包含软删数据
     */
    public FaiList<Param> searchFromDbWithDel(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        // 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tablename
        m_dao.setTableName(aid);

        int rt = m_dao.select(searchArg, listRef, selectFields);

        // 查完之后恢复之前的tablename
        m_dao.restoreTableName();

        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;match=%s;selectFields=%s;", m_flow, aid, searchArg.matcher.toJson(), selectFields);
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

    private FaiList<Integer> batchCreatAndSetId(int aid, int tid, int unionPriId, FaiList<Param> relDataList) {
        // 如果是门店通的请求，为了保证同一个商品在所有门店下id的一致性
        // 门店通的rlPdId 直接使用 pdId
        if(tid == FaiValObj.TermId.YK) {
            for(Param relData : relDataList) {
                // TODO 如果设置了rlPdId，则不处理。门店完全接入后，可以去掉continue这个逻辑
                int curRlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, 0);
                if(curRlPdId > 0) {
                    continue;
                }
                Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
                if(curPdId == null) {
                    throw new MgException(Errno.ARGS_ERROR, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
                }
                relData.setInt(ProductRelEntity.Info.RL_PD_ID, curPdId);
            }
        }
        int maxId = m_dao.getId(aid, unionPriId);
        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
            Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(curPdId == null) {
                throw new MgException(Errno.ARGS_ERROR, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
            }

            int rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, ++maxId);
            if(rlPdId > maxId) {
                maxId = rlPdId;
            }
            rlPdIds.add(rlPdId);
        }

        m_dao.updateId(aid, unionPriId, maxId, false);
        return rlPdIds;
    }
    private int createAndSetRlPdId(int aid, int tid, int unionPriId, Param relData) {
        int rt;
        Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, 0);
        // 如果是门店通的请求，为了保证同一个商品在所有门店下id的一致性
        // 门店通的rlPdId 直接使用 pdId
        // TODO 如果设置了rlPdId，则不处理。门店完全接入后，可以去掉rlPdId <= 0这个判断
        if(rlPdId <= 0 && tid == FaiValObj.TermId.YK) {
            Integer pdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(pdId == null) {
                throw new MgException(Errno.ARGS_ERROR, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
            }
            relData.setInt(ProductRelEntity.Info.RL_PD_ID, pdId);
            rlPdId = pdId;
        }
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

    public Map<Integer, Integer> getPdIdRelMap(int aid, int unionPriId, int sysType, HashSet<Integer> rlPdIds) {
        FaiList<Param> list = getPdIdRels(aid, unionPriId, sysType, rlPdIds);
        return Utils.getMap(list, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
    }

    public Map<Integer, Integer> getRlPdIdRelMap(int aid, int unionPriId, int sysType, HashSet<Integer> rlPdIds) {
        FaiList<Param> list = getPdIdRels(aid, unionPriId, sysType, rlPdIds);
        return Utils.getMap(list, ProductRelEntity.Info.RL_PD_ID, ProductRelEntity.Info.PD_ID);
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

        FaiList<Param> tmpList = searchFromDbWithDel(aid, searchArg, Utils.asFaiList(ProductRelEntity.Info.RL_PD_ID, ProductRelEntity.Info.PD_ID));

        if(!Utils.isEmptyList(tmpList)) {
            list.addAll(tmpList);
            // 添加到缓存
            ProductRelCacheCtrl.PdIdCache.addCacheList(aid, unionPriId, sysType, tmpList);
        }

        if (list.isEmpty()) {
            Log.logDbg(Errno.NOT_FOUND, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        return list;
    }

    private FaiList<Param> getListByPdIdWithDel(int aid, int unionPriId, HashSet<Integer> pdIds) {
        return getList(aid, unionPriId, pdIds, true);
    }

    private FaiList<Param> getListByPdId(int aid, int unionPriId, HashSet<Integer> pdIds) {
        return getList(aid, unionPriId, pdIds, false);
    }

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> pdIds, boolean withDel) {
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
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);

        FaiList<Param> tmpList = withDel ? searchFromDbWithDel(aid, searchArg, null) : searchFromDb(aid, searchArg, null);
        if(!Utils.isEmptyList(tmpList)) {
            list.addAll(tmpList);
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return list;
        }

        // 添加到缓存
        ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, tmpList);

        return list;
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(SagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
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
        if(m_bakDao != null && !tc.register(m_bakDao)) {
            throw new MgException("registered ProductRelBakDaoCtrl err;");
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
        return fromInfo.getInt(ProductRelEntity.Info.UNION_PRI_ID) +
                DELIMITER +
                fromInfo.getInt(ProductRelEntity.Info.PD_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductEntity.Info.UPDATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";

    private int m_flow;
    private boolean addSaga;
    private String m_xid;
    private ProductRelDaoCtrl m_dao;
    private ProductRelSagaDaoCtrl m_sagaDao;
    private ProductRelBakDaoCtrl m_bakDao;
    private static final int BATCH_DEL_SIZE = 500;
}
