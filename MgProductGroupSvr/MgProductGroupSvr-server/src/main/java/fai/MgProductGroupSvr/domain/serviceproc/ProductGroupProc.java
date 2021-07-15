package fai.MgProductGroupSvr.domain.serviceproc;

import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.common.ProductGroupCheck;
import fai.MgProductGroupSvr.domain.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupValObj;
import fai.MgProductGroupSvr.domain.repository.ProductGroupCache;
import fai.MgProductGroupSvr.domain.repository.ProductGroupDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.Map;

public class ProductGroupProc {
    public ProductGroupProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductGroupDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    /**
     * 添加商品分类数据
     * @return 商品分类id
     */
    public int addGroup(int aid, Param info) {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;info=%s", m_flow, aid, info);
        }

        FaiList<Param> list = getGroupList(aid);
        if(list == null) {
            list = new FaiList<Param>();
        }
        int count = list.size();
        if(count >= ProductGroupValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupValObj.Limit.COUNT_MAX);
        }

        String name = info.getString(ProductGroupEntity.Info.GROUP_NAME);
        Param existInfo = Misc.getFirst(list, ProductGroupEntity.Info.GROUP_NAME, name);
        if(!Str.isEmpty(existInfo)) {
            rt = Errno.ALREADY_EXISTED;
            throw new MgException(rt, "group name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
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
    public void setGroupList(int aid, FaiList<ParamUpdater> updaterList) {
        int rt;
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            String name = updateInfo.getString(ProductGroupEntity.Info.GROUP_NAME);
            if(name != null && !ProductGroupCheck.isNameValid(name)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, name);
            }
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
            data.assign(oldInfo, ProductGroupEntity.Info.PARENT_ID);
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
        item.setString(ProductGroupEntity.Info.PARENT_ID, "?");
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

    /**
     * 批量添加商品分类数据
     * @return 商品分类id集合
     */
    public FaiList<Integer> batchAddGroup(int aid, FaiList<Param> groupList) {
        int rt;
        if(groupList == null || groupList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;groupList is null;aid=%d", aid);
        }
        FaiList<Param> list = getGroupList(aid);
        if(list == null) {
            list = new FaiList<Param>();
        }
        int count = list.size() + groupList.size();
        if(count > ProductGroupValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductGroupValObj.Limit.COUNT_MAX);
        }
        FaiList<Integer> ids = new FaiList<>();
        // 校验参数名是否已经存在
        for(Param info : groupList) {
            String name = info.getString(ProductGroupEntity.Info.GROUP_NAME);
            Param existInfo = Misc.getFirst(list, ProductGroupEntity.Info.GROUP_NAME, name);
            if(!Str.isEmpty(existInfo)) {
                rt = Errno.ALREADY_EXISTED;
                throw new MgException(rt, "group name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
            }
            int groupId = creatAndSetId(aid, info);
            ids.add(groupId);
        }
        rt = m_daoCtrl.batchInsert(groupList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert group error;flow=%d;aid=%d;", m_flow, aid);
        }

        return ids;
    }

    public void delGroupList(int aid, FaiList<Integer> delIdList) {
        int rt;
        if(delIdList == null || delIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delIdList);
        }

        ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, delIdList);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;delIdList=%s", m_flow, aid, delIdList);
        }
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
    }

    private int m_flow;
    private ProductGroupDaoCtrl m_daoCtrl;
}
