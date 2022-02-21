package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindGroupDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.bak.ProductBindGroupBakDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductBindGroupSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductBindGroupProc {
    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, false);
    }

    // 备份还原
    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
        this.m_flow = flow;
        this.m_dao = ProductBindGroupDaoCtrl.getInstance(flow, aid);
        if(useBak) {
            this.m_bakDao = ProductBindGroupBakDaoCtrl.getInstance(flow, aid);
        }
        init(tc);
    }

    // 分布式事务
    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_dao = ProductBindGroupDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(m_xid)) {
            this.m_sagaDao = ProductBindGroupSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
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
            searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            FaiList<Param> fromList = searchFromDb(aid, searchArg, null);
            if(fromList.isEmpty()) {
                continue;
            }

            Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
            Calendar maxCreateTime = null;
            Calendar minCreateTime = null;
            for (Param fromInfo : fromList) {
                fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
                newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));

                Calendar createTime = fromInfo.getCalendar(ProductBindGroupEntity.Info.CREATE_TIME);
                if(minCreateTime == null || createTime.before(minCreateTime)) {
                    minCreateTime = createTime;
                }
                if(maxCreateTime == null || createTime.after(maxCreateTime)) {
                    maxCreateTime = createTime;
                }
            }

            SearchArg oldBakArg = new SearchArg();
            oldBakArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            oldBakArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            if(minCreateTime != null) {
                oldBakArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.CREATE_TIME, ParamMatcher.GE, minCreateTime);
            }
            if(maxCreateTime != null) {
                oldBakArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.CREATE_TIME, ParamMatcher.LE, maxCreateTime);
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
                ParamMatcher mergeMatcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductBindGroupEntity.Info.CREATE_TIME, ParamMatcher.EQ, "?");

                FaiList<Param> dataList = new FaiList<Param>();
                for (String bakUniqueKey : oldBakUniqueKeySet) {
                    String[] keys = bakUniqueKey.split(DELIMITER);
                    Calendar createTime = Calendar.getInstance();
                    createTime.setTimeInMillis(Long.valueOf(keys[2]));
                    Param data = new Param();

                    // mergeUpdater start
                    data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                    // mergeUpdater end

                    // mergeMatcher start
                    data.setInt(ProductBindGroupEntity.Info.AID, aid);
                    data.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, Integer.valueOf(keys[0]));
                    data.setInt(ProductBindGroupEntity.Info.PD_ID, Integer.valueOf(keys[1]));
                    data.setCalendar(ProductBindGroupEntity.Info.CREATE_TIME, createTime);
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
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
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
    }

    public FaiList<Param> getPdBindGroupList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        if(Utils.isEmptyList(pdIds)) {
            throw new MgException(Errno.ARGS_ERROR, "get pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        return getList(aid, unionPriId, new HashSet<>(pdIds));
    }

    public FaiList<Param> getPdBindGroupList(int aid, FaiList<Integer> unionPriIds, FaiList<Integer> pdIds) {
        if (Utils.isEmptyList(unionPriIds)) {
            throw new MgException(Errno.ARGS_ERROR, "unionPriIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "dao.select err;flow=%d;aid=%d;matcher=%s", m_flow, aid, searchArg.matcher);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        return listRef.value;
    }

    public void cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId) {
        ParamMatcher delMatcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, toUnionPriId);

        int rt = m_dao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "clear old list error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }

        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, fromUnionPriId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        FaiList<Param> list = searchFromDb(aid, searchArg, null);
        if(list.isEmpty()) {
            return;
        }
        Calendar now = Calendar.getInstance();
        for(Param info : list) {
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, toUnionPriId);
            info.setCalendar(ProductBindPropEntity.Info.CREATE_TIME, now);
        }
        rt = m_dao.batchInsert(list, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "cloneBizBind error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
        Log.logStd("cloneBizBind ok;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
    }

    public void updateBindGroupList(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Integer> rlGroupIdList) {
        if(rlGroupIdList == null) {
            return;
        }
        HashSet<Integer> pdIds = new HashSet<Integer>();
        pdIds.add(pdId);
        FaiList<Param> list = getList(aid, unionPriId, pdIds);
        FaiList<Integer> delGroupIds = new FaiList<>();
        FaiList<Integer> oldGroupIds = Utils.getValList(list, ProductBindGroupEntity.Info.RL_GROUP_ID);
        for(Integer rlGroupId : oldGroupIds) {
            if(rlGroupIdList.contains(rlGroupId)) {
                // 移除已存在的rlGroupId，rlGroupIdList中剩下的就是要新增的
                rlGroupIdList.remove(rlGroupId);
            }else {
                // 传过来的rlGroupIdList中不存在的就是要删除的
                delGroupIds.add(rlGroupId);
            }
        }

        // 新增
        if(!rlGroupIdList.isEmpty()) {
            addPdBindGroupList(aid, unionPriId, sysType, rlPdId, pdId, rlGroupIdList);
        }

        // 删除
        if(!delGroupIds.isEmpty()) {
            delPdBindGroupList(aid, unionPriId, pdId, delGroupIds);
        }
    }

    public void updateBindGroupList(int aid, int unionPriId, FaiList<Integer> pdIds, FaiList<Param> newList) {
        if(newList == null) {
            return;
        }
        // 删除旧数据
        delPdBindGroupList(aid, unionPriId, pdIds);
        // 添加新设置的数据
        if(!newList.isEmpty()) {
            batchBindGroupList(aid, unionPriId, newList);
        }
    }

    public void addPdBindGroupList(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Integer> rlGroupIdList) {
        int rt;
        if(Utils.isEmptyList(rlGroupIdList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<>();
        for(int rlGroupId : rlGroupIdList) {
            Param info = new Param();
            info.setInt(ProductBindGroupEntity.Info.SYS_TYPE, sysType);
            info.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
            info.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
            addList.add(info);
        }
        batchBindGroupList(aid, unionPriId, addList);
    }

    public void batchBindGroupList(int aid, int unionPriId, FaiList<Param> infoList) {
        int rt;
        if(Utils.isEmptyList(infoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        FaiList<Param> addList = new FaiList<>();
        FaiList<Param> sagaList = new FaiList<>();
        Calendar now = Calendar.getInstance();
        for(Param info : infoList) {
            int rlPdId = info.getInt(ProductBindGroupEntity.Info.RL_PD_ID, 0);
            int rlGroupId = info.getInt(ProductBindGroupEntity.Info.RL_GROUP_ID, 0);
            int pdId = info.getInt(ProductBindGroupEntity.Info.PD_ID, 0);
            if(rlPdId <= 0 || rlGroupId < 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;info=%s;", m_flow, aid, info);
            }
            int sysType = info.getInt(ProductBindGroupEntity.Info.SYS_TYPE, 0);
            Param addData = new Param();
            addData.setInt(ProductBindGroupEntity.Info.AID, aid);
            addData.setInt(ProductBindGroupEntity.Info.SYS_TYPE, sysType);
            addData.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
            addData.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
            addData.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);
            addData.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
            addData.setCalendar(ProductBindGroupEntity.Info.CREATE_TIME, now);
            addList.add(addData);

            // 开启了分布式事务，记录添加数据的主键
            if(addSaga) {
                Param sagaInfo = new Param();
                sagaInfo.setInt(ProductBindGroupEntity.Info.AID, aid);
                sagaInfo.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
                sagaInfo.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
                sagaInfo.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);

                sagaInfo.setString(SagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(SagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                sagaInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                sagaInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaList.add(sagaInfo);
            }
        }
        rt = m_dao.batchInsert(addList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind group error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 使用分布式事务时，记录新增的数据
        addSagaList(aid, sagaList);
        Log.logStd("batch add bind groups ok;aid=%d;uid=%d;addList=%s;", aid, unionPriId, addList);
    }

    public void insert4Clone(int aid, FaiList<Param> dataList) {
        int rt;
        if(Utils.isEmptyList(dataList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;dataList=%s;", m_flow, aid, dataList);
        }

        rt = m_dao.batchInsert(dataList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert pd bind group error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public int delPdBindGroup(int aid, int unionPriId, ParamMatcher matcher) {
        if(matcher == null) {
            matcher = new ParamMatcher();
        }
        matcher.and(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        return delPdBindGroup(aid, matcher);
    }

    public int delPdBindGroupList(int aid, FaiList<Integer> pdIds) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            long branchId = RootContext.getBranchId();
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, m_xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            }
            // 插入
            addSagaList(aid, list);
        }

        return delPdBindGroup(aid, matcher);
    }

    public int delPdBindGroupList(int aid, int unionPriId, int pdId, FaiList<Integer> rlGroupIds) {
        int rt;
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }

        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, m_xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            }
            // 插入
            addSagaList(aid, list);
        }

        return delPdBindGroup(aid, matcher);
    }

    public int delPdBindGroupList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, m_xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            }
            // 插入
            addSagaList(aid, list);
        }

        return delPdBindGroup(aid, matcher);
    }

    public int delPdBindGroupListByRlGroupIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds) {
        int rt;
        if(Utils.isEmptyList(rlGroupIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;uid=%d;rlGroupIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);

        return delPdBindGroup(aid, matcher);
    }

    public int delPdBindGroup(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK){
            throw new MgException(rt, "delPdBindGroup error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delPdBindGroup ok;flow=%d;aid=%s;matcher=%s;", m_flow, aid, matcher);
        return refRowCount.value;
    }

    // 清空指定aid+unionPriId的数据
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "clearAcct error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
        }
        Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
    }

    public FaiList<Integer> getRlPdIdsByGroupId(int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds) {
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "args error;rlGroupIds is null;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select error;flow=%d;aid=%d;unionPriId=%d;rlGroupIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param info : listRef.value) {
            rlPdIds.add(info.getInt(ProductBindGroupEntity.Info.RL_PD_ID));
        }
        return rlPdIds;
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductBindGroupCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductBindGroupCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getCountFromDB(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductBindGroupCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        // 存在克隆场景需要拿其他aid的数据，设置下表名
        m_dao.setTableName(aid);
        int rt = m_dao.select(searchArg, listRef, selectFields);
        // 查完之后恢复下表名
        m_dao.restoreTableName();

        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        return listRef.value;
    }

    private int getCountFromDB(int aid, int unionPriId) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if(countRef.value == null) {
            countRef.value = 0;
        }
        return countRef.value;
    }

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> pdIds) {
        if(Utils.isEmptyList(pdIds)) {
            throw new MgException(Errno.ARGS_ERROR, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, pdIds);
        }
        Ref<FaiList<Integer>> noCacheIdsRef = new Ref<>();
        // 缓存中获取
        FaiList<Param> list = ProductBindGroupCache.getCacheList(aid, unionPriId, new FaiList<>(pdIds), noCacheIdsRef);
        if(list == null) {
            list = new FaiList<>();
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = noCacheIdsRef.value;
        if(Utils.isEmptyList(noCacheIds)) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        FaiList<Param> tmpList = searchFromDb(aid, searchArg, null);
        if(!Utils.isEmptyList(tmpList)) {
            list.addAll(tmpList);
            Map<Integer, List<Param>> groupByPdId = tmpList.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductBindGroupEntity.Info.PD_ID)));
            for(Integer pdId : groupByPdId.keySet()) {
                // 添加到缓存
                ProductBindGroupCache.addCacheList(aid, unionPriId, pdId, new FaiList<>(groupByPdId.get(pdId) ));
                boolean remove = noCacheIds.remove(pdId);
            }
        }

        // 添加空缓存
        ProductBindGroupCache.EmptyCache.addCacheList(aid, unionPriId, noCacheIds);

        return list;
    }

    /**
     * saga补偿
     * 绑定关系表只会有新增和删除操作
     */
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("bind group need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

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
        for(Param info : list) {
            int unionPriId = info.getInt(ProductBindGroupEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(ProductBindGroupEntity.Info.PD_ID);
            int rlGroupId = info.getInt(ProductBindGroupEntity.Info.RL_GROUP_ID);

            ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.EQ, rlGroupId);

            rt = m_dao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del bind group error;flow=%d;aid=%d;xid=%s;matcher=%s;", m_flow, aid, m_xid, matcher.toJson());
            }
        }

        Log.logStd("rollback add ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
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

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(SagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = m_sagaDao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get saga list error;aid=%d;xid=%s;branchId=%d;", aid, xid, branchId);
        }
        return tmpRef.value;
    }

    private void addSagaList(int aid, FaiList<Param> sagaList) {
        // 是否开启了分布式事务
        if(!addSaga || Utils.isEmptyList(sagaList)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }

        int rt = m_sagaDao.batchInsert(sagaList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga batch insert product bind group error;flow=%d;aid=%d;xid=%s;", m_flow, aid, m_xid);
        }
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        // 事务注册失败则设置dao为null，防止在TransactionCtrl无法控制的情况下使用dao
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }

        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductBindGroupSagaDaoCtrl err;");
        }

        if(m_bakDao != null && !tc.register(m_bakDao)) {
            throw new MgException("registered ProductBindGroupBakDaoCtrl err;");
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
        return fromInfo.getInt(ProductBindGroupEntity.Info.UNION_PRI_ID) +
                DELIMITER +
                fromInfo.getInt(ProductBindGroupEntity.Info.PD_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductBindGroupEntity.Info.CREATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";

    private int m_flow;
    private String m_xid;
    private boolean addSaga;
    private ProductBindGroupBakDaoCtrl m_bakDao;
    private ProductBindGroupSagaDaoCtrl m_sagaDao;
    private ProductBindGroupDaoCtrl m_dao;
}
