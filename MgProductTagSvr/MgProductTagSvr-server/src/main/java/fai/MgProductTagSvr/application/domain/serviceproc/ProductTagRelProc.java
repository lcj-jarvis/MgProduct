package fai.MgProductTagSvr.application.domain.serviceproc;

import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.Map;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:03
 */
public class ProductTagRelProc {
    
    private int m_flow;
    private ProductTagRelDaoCtrl m_relDaoCtrl;

    public ProductTagRelProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        this.m_flow = flow;
        this.m_relDaoCtrl = ProductTagRelDaoCtrl.getInstance(flow, aid);
        init(transactionCtrl);
    }

    private void init(TransactionCtrl transactionCtrl) {
        if (transactionCtrl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductTagDao err;");
        }
        if(!transactionCtrl.register(m_relDaoCtrl)) {
            throw new MgException("registered ProductTagDao err;");

        }
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
    }

    /**
     * 获取最大的排序字段
     */
    public int getMaxSort(int aid, int unionPriId) {
        String sortCache = ProductTagRelCache.SortCache.get(aid, unionPriId);
        if(!Str.isEmpty(sortCache)) {
            return Parser.parseInt(sortCache, ProductTagRelValObj.Default.SORT);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_relDaoCtrl.select(searchArg, listRef, "max(sort) as sort");
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return ProductTagRelValObj.Default.SORT;
        }

        Param info = listRef.value.get(0);
        int sort = info.getInt(ProductTagRelEntity.Info.SORT, ProductTagRelValObj.Default.SORT);
        // 添加到缓存
        ProductTagRelCache.SortCache.set(aid, unionPriId, sort);
        return sort;
    }

    public void addTagRelBatch(int aid, int unionPriId, FaiList<Param> relTagInfoList, FaiList<Integer> relTagIds) {
        int rt;
        if(Util.isEmptyList(relTagInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, relTagInfoList is empty;flow=%d;aid=%d;relTagInfoList=%s", m_flow, aid, relTagInfoList);
        }

        //判断是否超出数量限制
        FaiList<Param> list = getTagRelList(aid, unionPriId,null,true);
        int count = list.size();
        boolean isOverLimit = count + relTagInfoList.size() > ProductTagRelValObj.Limit.COUNT_MAX;
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;addSize=%d;", m_flow, aid, count,
                    ProductTagRelValObj.Limit.COUNT_MAX, relTagInfoList.size());
        }

        int relTagId = 0;
        for (Param relTagInfo:relTagInfoList) {
            //自增标签业务id
            relTagId = createAndSetId(aid, unionPriId, relTagInfo);
            //保存标签业务id
            relTagIds.add(relTagId);
        }

        //批量插入,并且不将relTagInfoList的元素设置为null
        rt = m_relDaoCtrl.batchInsert(relTagInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product tag rel error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public FaiList<Param> getTagRelList(int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) {
        return getListByConditions(aid, unionPriId, searchArg, getFromCache);
    }

    /**
     * 根据标签业务id获取所有的标签id
     */
    public FaiList<Integer> getTagIdsByRlTagIds(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        FaiList<Param> list = getListByConditions(aid, unionPriId, null,true);
        FaiList<Integer> tagIdList = new FaiList<Integer>();
        if(list.isEmpty()) {
            return tagIdList;
        }
        list = Misc.getList(list, new ParamMatcher(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds));
        list.forEach(param -> tagIdList.add(param.getInt(ProductTagRelEntity.Info.TAG_ID)));
        return tagIdList;
    }
    
    /**
     * 按照条件查询数据，默认是查询同一个aid和unionPriId下的全部数据
     * @param searchArg 查询条件
     * @param getFromCache 是否需要从缓存中查询
     */
    private FaiList<Param> getListByConditions(int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) {
        FaiList<Param> list;
        if (getFromCache) {
            // 从缓存获取数据
            list = ProductTagRelCache.InfoCache.getCacheList(aid, unionPriId);
            if(!Util.isEmptyList(list)) {
                return list;
            }
        }

        LockUtil.TagRelLock.readLock(aid);
        try {
            if (getFromCache) {
                // check again
                list = ProductTagRelCache.InfoCache.getCacheList(aid, unionPriId);
                if(!Util.isEmptyList(list)) {
                    return list;
                }
            }

            //无searchArg
            if (searchArg == null) {
                searchArg = new SearchArg();
            }

            //有searchArg，无查询条件
            if (searchArg.matcher == null) {
                searchArg.matcher = new ParamMatcher();
            }

            //避免查询过来的条件已经包含这两个查询条件,就先删除，防止重复添加查询条件
            searchArg.matcher.remove(ProductTagRelEntity.Info.AID);
            searchArg.matcher.remove(ProductTagRelEntity.Info.UNION_PRI_ID);

            //有searchArg，有查询条件，加多两个查询条件
            searchArg.matcher.and(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

            //为了克隆需要,因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tableName（并不影响其他业务）
            m_relDaoCtrl.setTableName(aid);

            Ref<FaiList<Param>> listRef = new Ref<>();
            int rt = m_relDaoCtrl.select(searchArg, listRef);

            //恢复之前的表名
            m_relDaoCtrl.restoreTableName();

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
                return list;
            }

            //添加到缓存（直接查DB的不需要添加缓存）
            if (getFromCache) {
                ProductTagRelCache.InfoCache.addCacheList(aid, unionPriId, list);
            }
        }finally {
            LockUtil.TagRelLock.readUnLock(aid);
        }

        return list;
    }

    /**
     * 根据标签业务id删除标签业务表的数据
     */
    public void delRelTagList(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        int rt;
        if(Util.isEmptyList(rlTagIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, rlTagIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds);
        rt = m_relDaoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delTagList error;flow=%d;aid=%d;delRlIdList=%s", m_flow, aid, rlTagIds);
        }
    }

    /**
     * 创建自增的标签id
     */
    private int createAndSetId(int aid, int unionPriId, Param relTagInfo) {
        Integer rlTagId = relTagInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
        if(rlTagId <= 0) {
            rlTagId = m_relDaoCtrl.buildId(aid, unionPriId, false);
            if (rlTagId == null) {
                throw new MgException(Errno.ERROR, "rlTagId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            rlTagId = m_relDaoCtrl.updateId(aid, unionPriId, rlTagId, false);
            if (rlTagId == null) {
                throw new MgException(Errno.ERROR, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        relTagInfo.setInt(ProductTagRelEntity.Info.RL_TAG_ID, rlTagId);

        return rlTagId;
    }

    /**
     * 修改标签业务表(只修改部分字段)
     */
    public void setTagRelList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> tagUpdaterList) {
        int rt;
        // 保存修改之前标签表的数据
        FaiList<Param> oldInfoList = getListByConditions(aid, unionPriId, null,true);
        // 保存修改的数据
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int rlTagId = updateInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
            //获取到reTagId属于oldInfoList的reTagIds中的数据
            Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductTagRelEntity.Info.RL_TAG_ID, rlTagId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            int tagId = oldInfo.getInt(ProductTagRelEntity.Info.TAG_ID);
            //将updater中保存的更新数据Param保存到oldInfo中
            oldInfo = updater.update(oldInfo, true);

            //保存修改的数据
            Param data = new Param();
            //只能修改rlFlag，sort，和updateTime
            int sort = oldInfo.getInt(ProductTagRelEntity.Info.SORT, 0);
            int rlFlag = oldInfo.getInt(ProductTagRelEntity.Info.RL_FLAG, 0);
            data.setInt(ProductTagRelEntity.Info.SORT, sort);
            data.setInt(ProductTagRelEntity.Info.RL_FLAG, rlFlag);
            data.setCalendar(ProductTagRelEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductTagRelEntity.Info.AID);
            data.assign(oldInfo, ProductTagRelEntity.Info.UNION_PRI_ID);
            data.assign(oldInfo, ProductTagRelEntity.Info.RL_TAG_ID);

            dataList.add(data);
            if(tagUpdaterList != null) {
                //保存关联的tagId
                updateInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagId);
                tagUpdaterList.add(updater);
            }
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList empty;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return;
        }

        //设置更新sql的where条件
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.EQ, "?");

        //设置更新的字段（这里没有修改标签类型）
        Param item = new Param();
        item.setString(ProductTagRelEntity.Info.SORT, "?");
        item.setString(ProductTagRelEntity.Info.RL_FLAG, "?");
        item.setString(ProductTagRelEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);

        //setNullList：sql入参的过程中，入参完成后，清空dataList的数据为null
        rt = m_relDaoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product tag error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    /**
     * 获取数据的状态
     */
    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductTagRelCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductTagRelCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int totalSize = getTagRelList(aid, unionPriId,null,true).size();
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, totalSize);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductTagRelCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    /**
     * 从fromAid、fromUnionPriId中克隆数据到toAid、toUnionPriId下，并且设置toAid、toUnionPriId的自增id
     * @param toAid 克隆到哪个aid下
     * @param fromAid 从哪个aid下克隆
     * @param cloneUnionPriIds key：fromUnionPriId 从哪个uid下克隆
     *                         value: toUnionPriId 克隆到哪个uid下
     *
     */
    public void cloneData(int toAid, int fromAid, Map<Integer, Integer> cloneUnionPriIds) {
        int rt;
        if(cloneUnionPriIds == null || cloneUnionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "cloneUnionPriIds is null;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }
        if(m_relDaoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        //获取到所有被克隆的unionPriIds
        FaiList<Integer> fromUnionPriIds = new FaiList<>(cloneUnionPriIds.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, fromAid);
        matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, fromUnionPriIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        // 根据fromAid设置表名，默认表名是根据aid生成的
        m_relDaoCtrl.setTableName(fromAid);
        Ref<FaiList<Param>> dataListRef = new Ref<>();
        rt = m_relDaoCtrl.select(searchArg, dataListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException("select clone data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        // 根据aid设置表名
        m_relDaoCtrl.setTableName(toAid);
        //克隆之前先删掉已经存在的数据
        FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUnionPriIds.values());
        ParamMatcher delMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, toAid);
        delMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
        rt = m_relDaoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("del old data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        // 组装数据
        for(Param data : dataListRef.value) {
            int fromUnionPriId = data.getInt(ProductTagRelEntity.Info.UNION_PRI_ID);
            int toUnionPriId = cloneUnionPriIds.get(fromUnionPriId);
            data.setInt(ProductTagRelEntity.Info.AID, toAid);
            data.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, toUnionPriId);
        }
        // 批量插入
        if(!dataListRef.value.isEmpty()) {
            rt = m_relDaoCtrl.batchInsert(dataListRef.value);
            if(rt != Errno.OK) {
                throw new MgException("batch insert err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
            }
        }

        //设置toAid和toUnionPriId的自增id
        for(int unionPriId : toUnionPriIds) {
            rt = m_relDaoCtrl.restoreMaxId(unionPriId, false);
            if(rt != Errno.OK) {
                throw new MgException("restoreMaxId err;flow=%d;aid=%d;fromAid=%d;curUid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, unionPriId, cloneUnionPriIds);
            }
            m_relDaoCtrl.clearIdBuilderCache(toAid, unionPriId);
        }
    }

    /**
     * 标签业务表添加增量克隆的数据
     */
    public void addIncrementalClone(int aid, int unionPriId, FaiList<Param> list) {
        if(Util.isEmptyList(list)) {
            Log.logStd("incrementalClone list is empty;aid=%d;uid=%d;", aid, unionPriId);
            return;
        }
        int rt = m_relDaoCtrl.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert tag rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        rt = m_relDaoCtrl.restoreMaxId(unionPriId, false);
        if(rt != Errno.OK) {
            throw new MgException("restoreMaxId err;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
    }
}
