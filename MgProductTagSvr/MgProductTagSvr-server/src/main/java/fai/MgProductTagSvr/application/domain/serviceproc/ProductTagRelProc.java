package fai.MgProductTagSvr.application.domain.serviceproc;

import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:03
 */
public class ProductTagRelProc {
    
    private int m_flow;
    private ProductTagRelDaoCtrl m_relDaoCtrl;

    public ProductTagRelProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_relDaoCtrl = ProductTagRelDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if (transactionCrtl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductTagDao err;");
        }
        if(!transactionCrtl.register(m_relDaoCtrl)) {
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
        boolean isOverLimit = (count >= ProductTagRelValObj.Limit.COUNT_MAX) ||
                (count + relTagInfoList.size() >  ProductTagRelValObj.Limit.COUNT_MAX);
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
            throw new MgException(rt, "batch insert product lib rel error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public FaiList<Param> getTagRelList(int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) {
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
                ProductTagRelCache.InfoCache.addCacheList(aid, unionPriId, list);
            }
        }finally {
            LockUtil.TagRelLock.readUnLock(aid);
        }

        return list;
    }

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

}
