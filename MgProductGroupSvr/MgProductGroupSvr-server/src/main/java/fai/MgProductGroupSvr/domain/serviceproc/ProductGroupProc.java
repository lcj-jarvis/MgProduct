package fai.MgProductGroupSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.common.ProductGroupCheck;
import fai.MgProductGroupSvr.domain.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupValObj;
import fai.MgProductGroupSvr.domain.repository.ProductGroupBakDaoCtrl;
import fai.MgProductGroupSvr.domain.repository.ProductGroupCache;
import fai.MgProductGroupSvr.domain.repository.ProductGroupDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductGroupProc {
    public ProductGroupProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductGroupDaoCtrl.getInstance(flow, aid);
        this.m_bakDao = ProductGroupBakDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    /**
     * 添加商品分类数据
     * @return 商品分类id
     */
    public int addGroup(int aid, int unionPriId, Param info, boolean isCheck) {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;info=%s", m_flow, aid, info);
        }

        int count = getGroupCount(aid);
        if(count >= ProductGroupValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupValObj.Limit.COUNT_MAX);
        }
        // 检查名称
        if (isCheck) {
            int sysType = info.getInt(ProductGroupEntity.Info.SYS_TYPE, ProductGroupValObj.SysType.PRODUCT);
            String name = info.getString(ProductGroupEntity.Info.GROUP_NAME);
            isNameExist(aid, unionPriId, sysType, name);
        }

        int groupId = creatAndSetId(aid, info);
        rt = m_daoCtrl.insert(info);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product group error;flow=%d;aid=%d;groupId=%d;", m_flow, aid, groupId);
        }

        return groupId;
    }

    /**
     * 修改商品分类
     */
    public void setGroupList(int aid, int unionPriId, int sysType, FaiList<ParamUpdater> updaterList, boolean isCheck) {
        int rt;
        FaiList<String> nameList = new FaiList<>();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            String name = updateInfo.getString(ProductGroupEntity.Info.GROUP_NAME);
            if(name != null && !ProductGroupCheck.isNameValid(name)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, name);
            }
            nameList.add(name);
        }
        if (isCheck) {
            isNameExist(aid, unionPriId, sysType, nameList);
        }

        FaiList<Param> oldList = getGroupList(aid);
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int groupId = updateInfo.getInt(ProductGroupEntity.Info.GROUP_ID, 0);
            Param oldInfo = Misc.getFirstNullIsEmpty(oldList, ProductGroupEntity.Info.GROUP_ID, groupId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            oldInfo = updater.update(oldInfo, true);
            Param data = new Param();
            data.assign(oldInfo, ProductGroupEntity.Info.ICON_LIST);
            data.assign(oldInfo, ProductGroupEntity.Info.GROUP_NAME);
            data.assign(oldInfo, ProductGroupEntity.Info.FLAG);
            data.setCalendar(ProductGroupEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductGroupEntity.Info.AID);
            data.assign(oldInfo, ProductGroupEntity.Info.GROUP_ID);
            dataList.add(data);
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList emtpy;flow=%d;aid=%d;", m_flow, aid);
            return;
        }

        ParamMatcher doBatchMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.EQ, "?");

        Param item = new Param();
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        item.setString(ProductGroupEntity.Info.ICON_LIST, "?");
        item.setString(ProductGroupEntity.Info.GROUP_NAME, "?");
        item.setString(ProductGroupEntity.Info.FLAG, "?");
        item.setString(ProductGroupEntity.Info.UPDATE_TIME, "?");
        rt = m_daoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product group error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    public void cloneData(int aid, int fromAid, Map<Integer, Integer> cloneUnionPriIds) {
        int rt;
        if(cloneUnionPriIds == null || cloneUnionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "cloneUnionPriIds is null;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        FaiList<Integer> fromUnionPriIds = new FaiList<>(cloneUnionPriIds.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, fromAid);
        matcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, fromUnionPriIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        // 根据fromAid设置表名，默认表名是根据aid生成的
        m_daoCtrl.setTableName(fromAid);
        Ref<FaiList<Param>> dataListRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, dataListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException("select clone data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }

        // 根据aid设置表名
        m_daoCtrl.setTableName(aid);
        FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUnionPriIds.values());
        ParamMatcher delMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);
        rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("del old data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        // 组装数据
        for(Param data : dataListRef.value) {
            int fromUnionPriId = data.getInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID);
            int toUnionPriId = cloneUnionPriIds.get(fromUnionPriId);
            data.setInt(ProductGroupEntity.Info.AID, aid);
            data.setInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID, toUnionPriId);
        }
        // 批量插入
        if(!dataListRef.value.isEmpty()) {
            rt = m_daoCtrl.batchInsert(dataListRef.value);
            if(rt != Errno.OK) {
                throw new MgException("batch insert err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
            }
        }
        rt = m_daoCtrl.restoreMaxId(false);
        if(rt != Errno.OK) {
            throw new MgException("restoreMaxId err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, aid, fromAid, cloneUnionPriIds);
        }
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    /*public void incrementalClone(int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        int rt;
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;flow=%d;aid=%d;uid=%d;fromAid=%d;fromUid=%s;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, fromAid);
        searchArg.matcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, fromUnionPriId);

        // 根据fromAid设置表名，默认表名是根据aid生成的
        m_daoCtrl.setTableName(fromAid);
        Ref<FaiList<Param>> fromListRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, fromListRef);
        if(rt != Errno.OK) {
            // not found说明增量为空，直接return
            if(rt == Errno.NOT_FOUND) {
                return;
            }
            throw new MgException("select clone data err;flow=%d;aid=%d;uid=%d;fromAid=%d;fromUid=%s;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }

        // 根据aid设置表名
        m_daoCtrl.setTableName(aid);
        searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException("select old data err;flow=%d;aid=%d;uid=%d;fromAid=%d;fromUid=%s;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }
        // 组装数据
        for(Param data : fromListRef.value) {
            data.setInt(ProductGroupEntity.Info.AID, aid);
            data.setInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        }
        // 批量插入
        if(!fromListRef.value.isEmpty()) {
            rt = m_daoCtrl.batchInsert(fromListRef.value);
            if(rt != Errno.OK) {
                throw new MgException("batch insert err;flow=%d;aid=%d;uid=%d;fromAid=%d;fromUid=%s;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
            }
        }
        rt = m_daoCtrl.restoreMaxId(false);
        if(rt != Errno.OK) {
            throw new MgException("restoreMaxId err;flow=%d;aid=%d;uid=%d;fromAid=%d;fromUid=%s;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }
        m_daoCtrl.clearIdBuilderCache(aid);
    }*/

    public FaiList<Integer> batchAddGroup(int aid, FaiList<Param> groupList) {
        return batchAddGroup(aid, 0, 0, groupList, false);
    }

    /**
     * 批量添加商品分类数据
     * @return 商品分类id集合
     */
    public FaiList<Integer> batchAddGroup(int aid, int unionPriId, int sysType, FaiList<Param> groupList, boolean isCheck) {
        int rt;
        if(groupList == null || groupList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;groupList is null;aid=%d", aid);
        }
        int count = getGroupCountFromDb(aid) + groupList.size();
        if(count > ProductGroupValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupValObj.Limit.COUNT_MAX);
        }
        // 校验名称是否重复
        if (isCheck) {
            List<String> nameList = groupList.stream().map(o -> o.getString(ProductGroupEntity.Info.GROUP_NAME)).collect(Collectors.toList());
            isNameExist(aid, unionPriId, sysType, nameList);
        }
        FaiList<Integer> ids = new FaiList<>();
        for(Param info : groupList) {
            int groupId = creatAndSetId(aid, info);
            ids.add(groupId);
        }
        rt = m_daoCtrl.batchInsert(groupList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group error;flow=%d;aid=%d;", m_flow, aid);
        }

        return ids;
    }

    private int getGroupCountFromDb(int aid) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_daoCtrl.selectCount(searchArg, countRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select group count err;flow=%d,aid=%d", m_flow, aid);
        }
        return countRef.value;
    }

    public void delGroupList(int aid, FaiList<Integer> delIdList, boolean softDel) {
        int rt;
        if(delIdList == null || delIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delIdList);
        }

        ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, delIdList);

        if (softDel) {
            softDelGroupList(aid, matcher);
        } else {
            delGroupList(aid, matcher);
        }
    }

    private void softDelGroupList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "matcher is null;aid=%d;", aid);
        }
        matcher.and(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        ParamUpdater updater = new ParamUpdater(new Param().setInt(ProductGroupEntity.Info.STATUS, ProductGroupValObj.Status.DEL)
                .setCalendar(ProductGroupEntity.Info.UPDATE_TIME, Calendar.getInstance())
        );
        rt = m_daoCtrl.update(updater, matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "softDelGroupList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delGroupList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
    }

    public void delGroupList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "matcher is null;aid=%d;", aid);
        }
        matcher.and(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delGroupList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
    }

    public void backupData(int aid, int backupId, int backupFlag, Set<Integer> bakGroupIds) {
        int rt;
        if(m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
        }
        if(Util.isEmptyList(bakGroupIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "bak groupIds is empty;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, new FaiList<Integer>(bakGroupIds));
        FaiList<Param> fromList = searchFromDb(aid, searchArg);

        Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
        FaiList<Calendar> updateTimeList = new FaiList<>();
        for (Param fromInfo : fromList) {
            fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
            newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
            updateTimeList.add(fromInfo.getCalendar(ProductGroupEntity.Info.UPDATE_TIME));
        }

        // 查出已有的备份数据，通过updateTime确定数据是否已备份
        SearchArg oldBakArg = new SearchArg();
        oldBakArg.matcher = searchArg.matcher.clone();
        oldBakArg.matcher.and(ProductGroupEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
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
            ParamMatcher mergeMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductGroupEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

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
                data.setInt(ProductGroupEntity.Info.AID, aid);
                data.setInt(ProductGroupEntity.Info.GROUP_ID, Integer.valueOf(keys[0]));
                data.setCalendar(ProductGroupEntity.Info.UPDATE_TIME, updateTime);
                // mergeMatcher end

                dataList.add(data);
            }
            rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
            if(rt != Errno.OK) {
                throw new MgException(rt, "merge bak update err;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
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
            Log.logStd("backupData ok, need add bak empty;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
            return;
        }

        // 批量插入备份表
        rt = m_bakDao.batchInsert(fromList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchInsert bak err;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
        }

        Log.logStd("backupData ok;aid=%d;bakGroupIds=%s;backupId=%d;backupFlag=%d;", aid, bakGroupIds, backupId, backupFlag);
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
        ParamMatcher delMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
        rt = m_bakDao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        Log.logStd("delete ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
    }

    public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        delGroupList(aid, delMatcher);

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = searchBakList(aid, bakSearchArg);

        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_daoCtrl.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        m_daoCtrl.restoreMaxId(false);
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public FaiList<Param> searchBakList(int aid, SearchArg searchArg) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bakDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        return listRef.value;
    }

    public FaiList<Param> getGroupList(int aid) {
        return getList(aid);
    }
    
    public int getGroupCount(int aid) {
        FaiList<Param> list = getList(aid);
        if(list == null) {
            return 0;
        }
        return list.size();
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, String ... selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        // 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tablename
        m_daoCtrl.setTableName(aid);
        int rt = m_daoCtrl.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 查完之后恢复最初的tablename
        m_daoCtrl.restoreTableName();
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        return listRef.value;
    }

    private FaiList<Param> getList(int aid) {
        // 从缓存获取数据
        FaiList<Param> list = ProductGroupCache.getCacheList(aid);
        if(!Util.isEmptyList(list)) {
            return list;
        }

        LockUtil.GroupLock.readLock(aid);
        try {
            // check again
            list = ProductGroupCache.getCacheList(aid);
            if(!Util.isEmptyList(list)) {
                return list;
            }

            Ref<FaiList<Param>> listRef = new Ref<>();
            // 从db获取数据
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            int rt = m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "getList error;flow=%d;aid=%d;", m_flow, aid);
            }
            list = listRef.value;
            if(list == null) {
                list = new FaiList<Param>();
            }
            if (list.isEmpty()) {
                rt = Errno.NOT_FOUND;
                Log.logDbg(rt, "not found;aid=%d", aid);
                return list;
            }
            // 添加到缓存
            ProductGroupCache.addCacheList(aid, list);
        }finally {
            LockUtil.GroupLock.readUnLock(aid);
        }

        return list;
    }

    private int creatAndSetId(int aid, Param info) {
        int rt;
        Integer groupId = info.getInt(ProductGroupEntity.Info.GROUP_ID, 0);
        if(groupId <= 0) {
            groupId = m_daoCtrl.buildId(aid, false);
            if (groupId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "groupId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            groupId = m_daoCtrl.updateId(aid, groupId, false);
            if (groupId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "groupId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        info.setInt(ProductGroupEntity.Info.GROUP_ID, groupId);

        return groupId;
    }

    private void init(TransactionCtrl transactionCrtl) {
        if(!transactionCrtl.register(m_daoCtrl)) {
            throw new MgException("registered ProductGroupDao err;");
        }

        if(!transactionCrtl.register(m_bakDao)) {
            throw new MgException("registered ProductGroupBakDaoCtrl err;");
        }
    }

    /**
     * 判读数据库是否含有当前名称
     *
     * @param sysType 系统类型
     * @param name 名称 （可以是单个名称（String），也可以是多个名称（FaiList<String>））
     */
    private void isNameExist(int aid, int unionPriId, int sysType, Object name) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductGroupEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        if (name instanceof List) {
            searchArg.matcher.and(ProductGroupEntity.Info.GROUP_NAME, ParamMatcher.IN, name);
        } else {
            searchArg.matcher.and(ProductGroupEntity.Info.GROUP_NAME, ParamMatcher.EQ, name);
        }
        FaiList<Param> nameList = searchFromDb(aid, searchArg, ProductGroupEntity.Info.GROUP_NAME);
        if(!Utils.isEmptyList(nameList)) {
            throw new MgException(Errno.ALREADY_EXISTED, "group name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, nameList);
        }
    }

    private static String getBakUniqueKey(Param fromInfo) {
        return fromInfo.getInt(ProductGroupEntity.Info.GROUP_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductGroupEntity.Info.UPDATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";

    private int m_flow;
    private ProductGroupDaoCtrl m_daoCtrl;
    private ProductGroupBakDaoCtrl m_bakDao;

}
