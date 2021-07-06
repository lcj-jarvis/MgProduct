package fai.MgProductLibSvr.domain.serviceproc;

import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelValObj;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibRelCache;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

/**
 * @author LuChaoJi
 * @date 2021-06-23 16:59
 */
public class ProductLibRelProc {
    private int m_flow;
    private ProductLibRelDaoCtrl m_relDaoCtrl;

    public ProductLibRelProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_relDaoCtrl = ProductLibRelDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if (transactionCrtl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductLibDao err;");
        }
        if(!transactionCrtl.register(m_relDaoCtrl)) {
            throw new MgException("registered ProductLibDao err;");

        }
    }

    /**
     * 获取最大的排序字段
     */
    public int getMaxSort(int aid, int unionPriId) {
        String sortCache = ProductLibRelCache.SortCache.get(aid, unionPriId);
        if(!Str.isEmpty(sortCache)) {
            return Parser.parseInt(sortCache, ProductLibRelValObj.Default.SORT);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_relDaoCtrl.select(searchArg, listRef, "max(sort) as sort");
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return ProductLibRelValObj.Default.SORT;
        }

        Param info = listRef.value.get(0);
        int sort = info.getInt(ProductLibRelEntity.Info.SORT, ProductLibRelValObj.Default.SORT);
        // 添加到缓存
        ProductLibRelCache.SortCache.set(aid, unionPriId, sort);
        return sort;
    }

    public void addLibRelBatch(int aid, int unionPriId, FaiList<Param> relLibInfoList, FaiList<Integer> relLibIds) {
        int rt;
        if(Util.isEmptyList(relLibInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, relLibInfoList is empty;flow=%d;aid=%d;relLibInfoList=%s", m_flow, aid, relLibInfoList);
        }

        //判断是否超出数量限制
        FaiList<Param> list = getLibRelList(aid, unionPriId,null,true);
        int count = list.size();
        boolean isOverLimit = (count >= ProductLibValObj.Limit.COUNT_MAX) ||
                (count + relLibInfoList.size() >  ProductLibValObj.Limit.COUNT_MAX);
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;addSize=%d;", m_flow, aid, count,
                    ProductLibValObj.Limit.COUNT_MAX, relLibInfoList.size());
        }

        int relLibId = 0;
        for (Param relLibInfo:relLibInfoList) {
            //自增库业务id
            relLibId = createAndSetId(aid, unionPriId, relLibInfo);
            //保存库业务id
            relLibIds.add(relLibId);
        }

        //批量插入,并且不将relLibInfoList的元素设置为null
        rt = m_relDaoCtrl.batchInsert(relLibInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product lib rel error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

   /* public int addLibRelInfo(int aid, int unionPriId, Param relLibInfo) {
        int rt;
        //获取存在的库的数量
        int count = getLibRelList(aid, unionPriId,null,true).size();
        if(count >= ProductLibRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductLibRelValObj.Limit.COUNT_MAX);
        }
        int rlGroupId = createAndSetId(aid, unionPriId, relLibInfo);
        rt = m_relDaoCtrl.insert(relLibInfo);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert lib rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        return rlGroupId;
    }*/

    private int createAndSetId(int aid, int unionPriId, Param relLibInfo) {
        Integer rlLibId = relLibInfo.getInt(ProductLibRelEntity.Info.RL_LIB_ID, 0);
        if(rlLibId <= 0) {
            rlLibId = m_relDaoCtrl.buildId(aid, unionPriId, false);
            if (rlLibId == null) {
                throw new MgException(Errno.ERROR, "rlLibId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            rlLibId = m_relDaoCtrl.updateId(aid, unionPriId, rlLibId, false);
            if (rlLibId == null) {
                throw new MgException(Errno.ERROR, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        relLibInfo.setInt(ProductLibRelEntity.Info.RL_LIB_ID, rlLibId);

        return rlLibId;
    }

    public FaiList<Param> getLibRelList(int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) {
        return getListByConditions(aid, unionPriId, searchArg, getFromCache);
    }

    /**
     * 按照条件查询数据，默认是查询同一个aid和unionPriId下的全部数据
     * @param searchArg 查询条件
     * @param getFromCache 是否需要从缓存中查询
     * @return
     */
    private FaiList<Param> getListByConditions(int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) {
        FaiList<Param> list;
        if (getFromCache) {
            // 从缓存获取数据
            list = ProductLibRelCache.InfoCache.getCacheList(aid, unionPriId);
            if(!Util.isEmptyList(list)) {
                return list;
            }
        }

        LockUtil.LibRelLock.readLock(aid);
        try {
            if (getFromCache) {
                // check again
                list = ProductLibRelCache.InfoCache.getCacheList(aid, unionPriId);
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
            searchArg.matcher.remove(ProductLibRelEntity.Info.AID);
            searchArg.matcher.remove(ProductLibRelEntity.Info.UNION_PRI_ID);

            //有searchArg，有查询条件，加多两个查询条件
            searchArg.matcher.and(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

            Ref<FaiList<Param>> listRef = new Ref<>();
            int rt = m_relDaoCtrl.select(searchArg, listRef);
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
                ProductLibRelCache.InfoCache.addCacheList(aid, unionPriId, list);
            }
        }finally {
            LockUtil.LibRelLock.readUnLock(aid);
        }

        return list;
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
    }

    /**
     * 根据库业务id获取所有的库id
     */
    public FaiList<Integer> getLibIdsByRlLibIds(int aid, int unionPriId, FaiList<Integer> rlLibIds) {
        FaiList<Param> list = getListByConditions(aid, unionPriId, null,true);
        FaiList<Integer> libIdList = new FaiList<Integer>();
        if(list.isEmpty()) {
            return libIdList;
        }
        list = Misc.getList(list, new ParamMatcher(ProductLibRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, rlLibIds));
        list.forEach(param -> libIdList.add(param.getInt(ProductLibRelEntity.Info.LIB_ID)));
        return libIdList;
    }

    /**
     * 根据库业务id删除库业务表的数据
     */
    public void delRelLibList(int aid, int unionPriId, FaiList<Integer> rlLibIds) {
        int rt;
        if(rlLibIds == null || rlLibIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, rlLibIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductLibRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, rlLibIds);
        rt = m_relDaoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delLibList error;flow=%d;aid=%d;delRlIdList=%s", m_flow, aid, rlLibIds);
        }
    }

    /**
     * 修改库业务表(只修改部分字段)
     */
    public void setLibRelList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> libUpdaterList) {
        int rt;
        // 保存修改之前库表的数据
        FaiList<Param> oldInfoList = getListByConditions(aid, unionPriId, null,true);
        // 保存修改的数据
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int rlLibId = updateInfo.getInt(ProductLibRelEntity.Info.RL_LIB_ID, 0);
            //获取到reLibId属于oldInfoList的reLibIds中的数据
            Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductLibRelEntity.Info.RL_LIB_ID, rlLibId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            int libId = oldInfo.getInt(ProductLibRelEntity.Info.LIB_ID);
            //将updater中保存的更新数据Param保存到oldInfo中
            oldInfo = updater.update(oldInfo, true);

            //保存修改的数据
            Param data = new Param();
            //只能修改rlFlag，sort，和updateTime
            int sort = oldInfo.getInt(ProductLibRelEntity.Info.SORT, 0);
            int rlFlag = oldInfo.getInt(ProductLibRelEntity.Info.RL_FLAG, 0);
            data.setInt(ProductLibRelEntity.Info.SORT, sort);
            data.setInt(ProductLibRelEntity.Info.RL_FLAG, rlFlag);
            data.setCalendar(ProductLibRelEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductLibRelEntity.Info.AID);
            data.assign(oldInfo, ProductLibRelEntity.Info.UNION_PRI_ID);
            data.assign(oldInfo, ProductLibRelEntity.Info.RL_LIB_ID);

            dataList.add(data);
            if(libUpdaterList != null) {
                //保存关联的libId
                updateInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libId);
                libUpdaterList.add(updater);
            }
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList empty;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return;
        }

        //设置更新sql的where条件
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductLibRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, "?");

        //设置更新的字段（这里没有修改库类型）
        Param item = new Param();
        item.setString(ProductLibRelEntity.Info.SORT, "?");
        item.setString(ProductLibRelEntity.Info.RL_FLAG, "?");
        item.setString(ProductLibRelEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);

        //setNullList：sql入参的过程中，入参完成后，清空dataList的数据为null
        rt = m_relDaoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product group error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductLibRelCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductLibRelCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int totalSize = getLibRelList(aid, unionPriId,null,true).size();
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, totalSize);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductLibRelCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

}
