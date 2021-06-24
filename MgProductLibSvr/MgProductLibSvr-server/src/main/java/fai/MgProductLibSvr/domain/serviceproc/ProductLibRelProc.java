package fai.MgProductLibSvr.domain.serviceproc;

import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibRelCache;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibDaoCtrl;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

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

    public int addLibRelInfo(int aid, int unionPriId, Param relLibInfo) {
        int rt;
        int count = getLibRelList(aid, unionPriId) == null ? 0 : getList(aid, unionPriId).size();
        if(count >= ProductLibRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductLibRelValObj.Limit.COUNT_MAX);
        }
        int rlGroupId = creatAndSetId(aid, unionPriId, relLibInfo);
        rt = m_relDaoCtrl.insert(relLibInfo);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert lib rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        return rlGroupId;
    }

    private int creatAndSetId(int aid, int unionPriId, Param relLibInfo) {
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

    public FaiList<Param> getLibRelList(int aid, int unionPriId) {
        return getList(aid, unionPriId);
    }

    private FaiList<Param> getList(int aid, int unionPriId) {
        // 从缓存获取数据
        FaiList<Param> list = ProductLibRelCache.InfoCache.getCacheList(aid, unionPriId);
        if(!Util.isEmptyList(list)) {
            return list;
        }

        LockUtil.LibRelLock.readLock(aid);
        try {
            // check again
            list = ProductLibRelCache.InfoCache.getCacheList(aid, unionPriId);
            if(!Util.isEmptyList(list)) {
                return list;
            }

            // db中获取
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);
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
                return listRef.value;
            }
            // 添加到缓存
            ProductLibRelCache.InfoCache.addCacheList(aid, unionPriId, list);
        }finally {
            LockUtil.LibRelLock.readUnLock(aid);
        }

        return list;
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
    }


}
