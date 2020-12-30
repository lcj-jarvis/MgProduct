package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.ProductCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.ProductDaoCtrl;
import fai.comm.util.*;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductProc {
    public ProductProc(int flow, ProductDaoCtrl dao) {
        this.m_flow = flow;
        this.m_dao = dao;
    }

    public int addProduct(int aid, Param pdData, Ref<Integer> pdIdRef) {
        int rt;
        if(Str.isEmpty(pdData)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err, infoList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdData);
            return rt;
        }
        Ref<Integer> countRef = new Ref<>();
        rt = getPdCount(aid, countRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        int count = countRef.value;
        if(count >= ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            Log.logErr(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
            return rt;
        }
        Integer pdId = pdData.getInt(ProductEntity.Info.PD_ID);
        if(pdId == null) {
            pdId = m_dao.buildId(aid, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "pdId build error;flow=%d;aid=%d;", m_flow, aid);
                return rt;
            }else {
                pdId = m_dao.updateId(aid, pdId, false);
                if (pdId == null) {
                    rt = Errno.ERROR;
                    Log.logErr(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
                    return rt;
                }
            }
            pdData.setInt(ProductEntity.Info.PD_ID, pdId);
        }
        pdIdRef.value = pdId;
        rt = m_dao.insert(aid, pdData, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    public int deleteProductList(int aid, int tid, FaiList<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
            return rt;
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        matcher.and(ProductEntity.Info.SOURCE_TID, ParamMatcher.EQ, tid);
        rt = m_dao.delete(aid, matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "del product list error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
            return rt;
        }
        return rt;
    }

    public int getPdCount(int aid, Ref<Integer> countRef) {
        // 从缓存中获取
        Integer count = ProductCacheCtrl.getCountCache(aid);
        if(count != null) {
            countRef.value = count;
            return Errno.OK;
        }

        // 从db获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        String fields = "count(*) as cnt";
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(aid, searchArg, fields, listRef);
        if(rt != Errno.OK) {
            return rt;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        Param res = listRef.value.get(0);
        count = res.getInt("cnt", 0);
        countRef.value = count;

        // 添加到缓存
        ProductCacheCtrl.setCountCache(aid, count);
        return rt;
    }

    public int getProductInfo(int aid, int pdId, Ref<Param> infoRef) {
        Param info = ProductCacheCtrl.getCacheInfo(aid, pdId);
        if (!Str.isEmpty(info)) {
            infoRef.value = info;
            return Errno.OK;
        }
        HashSet<Integer> pdIds = new HashSet<>();
        pdIds.add(pdId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = getList(aid, pdIds, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        infoRef.value = tmpRef.value.get(0);
        return Errno.OK;
    }

    public int getProductList(int aid, HashSet<Integer> pdIdList, Ref<FaiList<Param>> listRef) {
        int rt = getList(aid, pdIdList, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "get list error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }

        return rt;
    }

    private int getList(int aid, HashSet<Integer> pdIds, Ref<FaiList<Param>> listRef) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("get pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
            return rt;
        }
        List<String> pdIdStrs = pdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductCacheCtrl.getCacheList(aid, pdIdStrs);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == pdIds.size()) {
            listRef.value = list;
            return Errno.OK;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            int pdId = info.getInt(ProductEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        rt = m_dao.select(aid, searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        // 添加到缓存
        ProductCacheCtrl.addCacheList(aid, tmpRef.value);

        listRef.value = list;
        return Errno.OK;
    }

    private int m_flow;
    private ProductDaoCtrl m_dao;
}
