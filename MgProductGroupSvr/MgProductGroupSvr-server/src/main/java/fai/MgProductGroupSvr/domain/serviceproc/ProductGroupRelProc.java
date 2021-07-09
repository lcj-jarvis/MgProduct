package fai.MgProductGroupSvr.domain.serviceproc;

import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelValObj;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelCache;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.Map;

public class ProductGroupRelProc {

    public ProductGroupRelProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_relDao = ProductGroupRelDaoCtrl.getInstance(flow, aid);
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

    public void insert4IncrementalClone(int aid, int unionPriId, FaiList<Param> list) {
        if(Util.isEmptyList(list)) {
            Log.logStd("incrementalClone list is empty;aid=%d;uid=%d;", aid, unionPriId);
            return;
        }
        int rt = m_relDao.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        rt = m_relDao.restoreMaxId(unionPriId, false);
        if(rt != Errno.OK) {
            throw new MgException("restoreMaxId err;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        m_relDao.clearIdBuilderCache(aid, unionPriId);
    }

    public FaiList<Integer> batchAddGroupRel(int aid, int unionPriId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "infoList is null;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        int count = getCount(aid, unionPriId);
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

    public void setGroupRelList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> groupUpdaterList) {
        int rt;
        FaiList<Param> oldInfoList = getList(aid, unionPriId);
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int rlGroupId = updateInfo.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
            Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductGroupRelEntity.Info.RL_GROUP_ID, rlGroupId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            int groupId = oldInfo.getInt(ProductGroupRelEntity.Info.GROUP_ID);
            oldInfo = updater.update(oldInfo, true);
            Param data = new Param();

            //只能修改rlFlag和sort
            int sort = oldInfo.getInt(ProductGroupRelEntity.Info.SORT, 0);
            int rlFlag = oldInfo.getInt(ProductGroupRelEntity.Info.RL_FLAG, 0);
            data.setInt(ProductGroupRelEntity.Info.SORT, sort);
            data.setInt(ProductGroupRelEntity.Info.RL_FLAG, rlFlag);
            data.setCalendar(ProductGroupRelEntity.Info.UPDATE_TIME, now);

            data.assign(oldInfo, ProductGroupRelEntity.Info.AID);
            data.assign(oldInfo, ProductGroupRelEntity.Info.UNION_PRI_ID);
            data.assign(oldInfo, ProductGroupRelEntity.Info.RL_GROUP_ID);
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

        Param item = new Param();
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        item.setString(ProductGroupRelEntity.Info.SORT, "?");
        item.setString(ProductGroupRelEntity.Info.RL_FLAG, "?");
        item.setString(ProductGroupRelEntity.Info.UPDATE_TIME, "?");
        rt = m_relDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product group error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    public void delGroupList(int aid, int unionPriId, FaiList<Integer> delRlIdList) {
        int rt;
        if(delRlIdList == null || delRlIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delRlIdList);
        }

        ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.IN, delRlIdList);
        rt = m_relDao.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;delRlIdList=%s", m_flow, aid, delRlIdList);
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
        int rt = m_relDao.select(searchArg, listRef, ProductGroupRelEntity.MANAGE_FIELDS);
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

    public FaiList<Integer> getIdsByRlIds(int aid, int unionPriId, FaiList<Integer> rlIdList) {
        if(rlIdList == null || rlIdList.isEmpty()) {
            return null;
        }
        FaiList<Param> list = getList(aid, unionPriId);
        FaiList<Integer> idList = new FaiList<Integer>();
        if(list.isEmpty()) {
            return idList;
        }

        list = Misc.getList(list, new ParamMatcher(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlIdList));
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
    }

    private int m_flow;
    private ProductGroupRelDaoCtrl m_relDao;
}
