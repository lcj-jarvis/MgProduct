package fai.MgProductGroupSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelValObj;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelBakDaoCtrl;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelCache;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProductGroupRelProc {

    public ProductGroupRelProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_relDao = ProductGroupRelDaoCtrl.getInstance(flow, aid);
        this.m_bakDao = ProductGroupRelBakDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    /**
     * 新增商品分类关联
     * @return rlGroupId
     */
    public int addGroupRelInfo(int aid, int unionPriId, Param info) {
        int rt;
        int count = getCount(aid, unionPriId);
        if(count >= ProductGroupRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupRelValObj.Limit.COUNT_MAX);
        }
        int rlGroupId = creatAndSetId(aid, unionPriId, info);
        rt = m_relDao.insert(info);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        return rlGroupId;
    }

    public void cloneData(int aid, int fromAid, Map<Integer, Integer> cloneUnionPriIds) {
        int rt;
        if(cloneUnionPriIds == null || cloneUnionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "cloneUnionPriIds is null;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        if(m_relDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        FaiList<Integer> fromUnionPriIds = new FaiList<>(cloneUnionPriIds.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, fromAid);
        matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, fromUnionPriIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        // 根据fromAid设置表名，默认表名是根据aid生成的
        m_relDao.setTableName(fromAid);
        Ref<FaiList<Param>> dataListRef = new Ref<>();
        rt = m_relDao.select(searchArg, dataListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException("select clone data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }

        // 根据aid设置表名
        m_relDao.setTableName(aid);
        FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUnionPriIds.values());
        ParamMatcher delMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
        rt = m_relDao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("del old data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        // 组装数据
        for(Param data : dataListRef.value) {
            int fromUnionPriId = data.getInt(ProductGroupRelEntity.Info.UNION_PRI_ID);
            int toUnionPriId = cloneUnionPriIds.get(fromUnionPriId);
            data.setInt(ProductGroupRelEntity.Info.AID, aid);
            data.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, toUnionPriId);
        }
        // 批量插入
        if(!dataListRef.value.isEmpty()) {
            rt = m_relDao.batchInsert(dataListRef.value);
            if(rt != Errno.OK) {
                throw new MgException("batch insert err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
            }
        }
        for(int unionPriId : toUnionPriIds) {
            rt = m_relDao.restoreMaxId(unionPriId, false);
            if(rt != Errno.OK) {
                throw new MgException("restoreMaxId err;flow=%d;aid=%d;fromAid=%d;curUid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, unionPriId, cloneUnionPriIds);
            }
            m_relDao.clearIdBuilderCache(aid, unionPriId);
        }
    }

    public void insert4Clone(int aid, FaiList<Integer> unionPriIds, FaiList<Param> list) {
        if(Util.isEmptyList(list)) {
            Log.logStd("incrementalClone list is empty;aid=%d;uid=%s;", aid, unionPriIds);
            return;
        }
        int rt = m_relDao.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        for(Integer unionPriId : unionPriIds) {
            rt = m_relDao.restoreMaxId(unionPriId, false);
            if(rt != Errno.OK) {
                throw new MgException("restoreMaxId err;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
            m_relDao.clearIdBuilderCache(aid, unionPriId);
        }
    }

    public FaiList<Integer> batchAddGroupRel(int aid, int unionPriId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "infoList is null;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        int count = getCountFromDb(aid, unionPriId) + infoList.size();
        if(count > ProductGroupRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupRelValObj.Limit.COUNT_MAX);
        }
        FaiList<Integer> rlIds = new FaiList<>();
        for(Param info : infoList) {
            int rlGroupId = creatAndSetId(aid, unionPriId, info);
            rlIds.add(rlGroupId);
        }
        rt = m_relDao.batchInsert(infoList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        return rlIds;
    }

    private int getCountFromDb(int aid, int unionPriId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_relDao.selectCount(searchArg, countRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select rel count error;flow=%d;aid=%d;unionPriId=%d", m_flow, aid, unionPriId);
        }
        return countRef.value;
    }

    public void setGroupRelList(int aid, int unionPriId, int sysType, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> groupUpdaterList) {
        int rt;
        FaiList<Param> oldInfoList = getList(aid, unionPriId);
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int rlGroupId = updateInfo.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
            ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.EQ, rlGroupId);
            matcher.and(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
            Param oldInfo = Misc.getFirst(oldInfoList, matcher);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            int groupId = oldInfo.getInt(ProductGroupRelEntity.Info.GROUP_ID);
            oldInfo = updater.update(oldInfo, true);
            Param data = new Param();

            //只能修改rlFlag、sort、status、parentId
            int sort = oldInfo.getInt(ProductGroupRelEntity.Info.SORT, 0);
            int rlFlag = oldInfo.getInt(ProductGroupRelEntity.Info.RL_FLAG, 0);
            int status = oldInfo.getInt(ProductGroupRelEntity.Info.STATUS, 0);
            int parentId = oldInfo.getInt(ProductGroupRelEntity.Info.PARENT_ID, 0);
            data.setInt(ProductGroupRelEntity.Info.SORT, sort);
            data.setInt(ProductGroupRelEntity.Info.RL_FLAG, rlFlag);
            data.setInt(ProductGroupRelEntity.Info.STATUS, status);
            data.setInt(ProductGroupRelEntity.Info.PARENT_ID, parentId);
            data.setCalendar(ProductGroupRelEntity.Info.UPDATE_TIME, now);

            data.assign(oldInfo, ProductGroupRelEntity.Info.AID);
            data.assign(oldInfo, ProductGroupRelEntity.Info.UNION_PRI_ID);
            data.assign(oldInfo, ProductGroupRelEntity.Info.RL_GROUP_ID);
            data.assign(oldInfo, ProductGroupRelEntity.Info.SYS_TYPE);
            dataList.add(data);
            if(groupUpdaterList != null) {
                updateInfo.setInt(ProductGroupRelEntity.Info.GROUP_ID, groupId);
                groupUpdaterList.add(updater);
            }
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList emtpy;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return;
        }

        ParamMatcher doBatchMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, "?");

        Param item = new Param();
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        item.setString(ProductGroupRelEntity.Info.SORT, "?");
        item.setString(ProductGroupRelEntity.Info.RL_FLAG, "?");
        item.setString(ProductGroupRelEntity.Info.STATUS, "?");
        item.setString(ProductGroupRelEntity.Info.PARENT_ID, "?");
        item.setString(ProductGroupRelEntity.Info.UPDATE_TIME, "?");
        rt = m_relDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product group error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    public void delGroupList(int aid, int unionPriId, FaiList<Integer> delRlIdList, int sysType, boolean softDel) {
        int rt;
        if(delRlIdList == null || delRlIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delRlIdList);
        }

        ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.IN, delRlIdList);

        if (softDel) {
            softDelGroupRelList(aid, matcher);
        } else {
            delGroupRelList(aid, matcher);
        }
    }

    private void softDelGroupRelList(int aid, ParamMatcher matcher) {
        int rt;
        if (matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }
        ParamUpdater updater = new ParamUpdater(new Param().setInt(ProductGroupRelEntity.Info.STATUS, ProductGroupRelValObj.Status.DEL));
        rt = m_relDao.update(updater, matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "soft del error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public void delGroupRelList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }

        rt = m_relDao.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
    }

    public FaiList<Param> getGroupRelList(int aid, int unionPriId) {
        return getList(aid, unionPriId);
    }

    public FaiList<Param> searchFromDb(int aid, int unionPriId, SearchArg searchArg) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        Ref<FaiList<Param>> listRef = new Ref<>();
        // 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tablename
        m_relDao.setTableName(aid);
        int rt = m_relDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        // 查完之后恢复之前的tablename
        m_relDao.restoreTableName();
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        return listRef.value;
    }

    public Set<Integer> backupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        if(Util.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        Set<Integer> groupIds = new HashSet<>();
        for(int unionPriId : unionPriIds) {
            FaiList<Param> fromList = searchFromDb(aid, unionPriId, null);
            if(fromList.isEmpty()) {
                continue;
            }

            Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
            FaiList<Calendar> updateTimeList = new FaiList<>();
            for (Param fromInfo : fromList) {
                fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
                newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
                updateTimeList.add(fromInfo.getCalendar(ProductGroupRelEntity.Info.UPDATE_TIME));
                groupIds.add(fromInfo.getInt(ProductGroupRelEntity.Info.GROUP_ID));
            }

            SearchArg oldBakArg = new SearchArg();
            oldBakArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            oldBakArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
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
                ParamMatcher mergeMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductGroupRelEntity.Info.GROUP_ID, ParamMatcher.EQ, "?");
                mergeMatcher.and(ProductGroupRelEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

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
                    data.setInt(ProductGroupRelEntity.Info.AID, aid);
                    data.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, Integer.valueOf(keys[0]));
                    data.setInt(ProductGroupRelEntity.Info.GROUP_ID, Integer.valueOf(keys[1]));
                    data.setCalendar(ProductGroupRelEntity.Info.UPDATE_TIME, updateTime);
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

        return groupIds;
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
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
        if(m_relDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "relDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        if(Util.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }

        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        delGroupRelList(aid, delMatcher);

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        FaiList<Param> fromList = searchBakList(aid, bakSearchArg);
        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_relDao.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        for(int unionPriId : unionPriIds) {
            m_relDao.restoreMaxId(unionPriId, false);
            m_relDao.clearIdBuilderCache(aid, unionPriId);
        }
    }

    public FaiList<Param> searchBakList(int aid, SearchArg searchArg) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);

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

    private int creatAndSetId(int aid, int unionPriId, Param info) {
        int rt = Errno.OK;
        Integer rlGroupId = info.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
        if(rlGroupId <= 0) {
            rlGroupId = m_relDao.buildId(aid, unionPriId, false);
            if (rlGroupId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlGroupId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            rlGroupId = m_relDao.updateId(aid, unionPriId, rlGroupId, false);
            if (rlGroupId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        info.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, rlGroupId);

        return rlGroupId;
    }

    public Integer getId(int aid, int unionPriId) {
        return m_relDao.getId(aid, unionPriId);
    }

    private FaiList<Param> getList(int aid, int unionPriId) {
        // 从缓存获取数据
        FaiList<Param> list = ProductGroupRelCache.InfoCache.getCacheList(aid, unionPriId);
        if(!Util.isEmptyList(list)) {
            return list;
        }
        LockUtil.GroupRelLock.readLock(aid);
        try {
            // check again
            list = ProductGroupRelCache.InfoCache.getCacheList(aid, unionPriId);
            if(!Util.isEmptyList(list)) {
                return list;
            }

            // db中获取
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            Ref<FaiList<Param>> listRef = new Ref<>();
            int rt = m_relDao.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            list = listRef.value;
            if(list == null) {
                list = new FaiList<Param>();
            }
            if (list.isEmpty()) {
                rt = Errno.NOT_FOUND;
                Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
                return listRef.value;
            }
            // 添加到缓存
            ProductGroupRelCache.InfoCache.addCacheList(aid, unionPriId, list);
        }finally {
            LockUtil.GroupRelLock.readUnLock(aid);
        }

        return list;
    }

    public int getCount(int aid, int unionPriId) {
        FaiList<Param> list = getList(aid, unionPriId);

        if(list == null) {
            return 0;
        }
        return list.size();
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductGroupRelCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductGroupRelCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getCount(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductGroupRelCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public int getMaxSort(int aid, int unionPriId) {
        String sortCache = ProductGroupRelCache.SortCache.get(aid, unionPriId);
        if(!Str.isEmpty(sortCache)) {
            return Parser.parseInt(sortCache, ProductGroupRelValObj.Default.SORT);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_relDao.select(searchArg, listRef, "max(sort) as sort");
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return ProductGroupRelValObj.Default.SORT;
        }

        Param info = listRef.value.get(0);
        int sort = info.getInt(ProductGroupRelEntity.Info.SORT, ProductGroupRelValObj.Default.SORT);
        // 添加到缓存
        ProductGroupRelCache.SortCache.set(aid, unionPriId, sort);
        return sort;
    }

    public FaiList<Integer> getIdsByRlIds(int aid, int unionPriId, FaiList<Integer> rlIdList, int sysType) {
        if(rlIdList == null || rlIdList.isEmpty()) {
            return null;
        }
        FaiList<Param> list = getList(aid, unionPriId);
        FaiList<Integer> idList = new FaiList<Integer>();
        if(list.isEmpty()) {
            return idList;
        }
        ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlIdList);
        matcher.and(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        list = Misc.getList(list, matcher);
        for(int i = 0; i < list.size(); i++) {
            Param info = list.get(i);
            idList.add(info.getInt(ProductGroupRelEntity.Info.GROUP_ID));
        }
        return idList;
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_relDao.clearIdBuilderCache(aid, unionPriId);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if(transactionCrtl == null) {
            return;
        }
        if(!transactionCrtl.register(m_relDao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }
        if(!transactionCrtl.register(m_bakDao)) {
            throw new MgException("registered ProductGroupRelBakDaoCtrl err;");
        }
    }

    private static String getBakUniqueKey(Param fromInfo) {
        return fromInfo.getInt(ProductGroupRelEntity.Info.UNION_PRI_ID) +
                DELIMITER +
                fromInfo.getInt(ProductGroupRelEntity.Info.GROUP_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductGroupRelEntity.Info.UPDATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";
    private int m_flow;
    private ProductGroupRelDaoCtrl m_relDao;
    private ProductGroupRelBakDaoCtrl m_bakDao;
}
