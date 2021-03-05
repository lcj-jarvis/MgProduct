package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.cache.ProductCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindPropDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductProc {
    public ProductProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public int addProduct(int aid, Param pdData) {
        int rt;
        if(Str.isEmpty(pdData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdData);
        }
        int count = getPdCount(aid);
        if(count >= ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        int pdId = creatAndSetId(aid, pdData);
        rt = m_dao.insert(pdData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        return pdId;
    }

    public FaiList<Integer> batchAddProduct(int aid, FaiList<Param> pdDataList) {
        int rt;
        if(pdDataList == null || pdDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, pdDataList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdDataList);
        }
        int count = getPdCount(aid);
        if(count > ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        FaiList<Integer> pdIdList = new FaiList<>();
        for(Param pdData : pdDataList) {
            int pdId = creatAndSetId(aid, pdData);
            pdIdList.add(pdId);
        }

        rt = m_dao.batchInsert(pdDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        return pdIdList;
    }

    private int creatAndSetId(int aid, Param info) {
        int rt;
        Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
        if(pdId == null) {
            pdId = m_dao.buildId(aid, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            pdId = m_dao.updateId(aid, pdId, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        info.setInt(ProductEntity.Info.PD_ID, pdId);
        return pdId;
    }

    public void deleteProductList(int aid, int tid, FaiList<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        matcher.and(ProductEntity.Info.SOURCE_TID, ParamMatcher.EQ, tid);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product list error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
    }

    public int getPdCount(int aid) {
        // 从缓存中获取
        Integer count = ProductCacheCtrl.getCountCache(aid);
        if(count != null) {
            return count;
        }

        // 从db获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        String fields = "count(*) as cnt";
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, fields);
        if(rt != Errno.OK) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            throw new MgException(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        Param res = listRef.value.get(0);
        count = res.getInt("cnt", 0);

        // 添加到缓存
        ProductCacheCtrl.setCountCache(aid, count);
        return count;
    }

    public Param getProductInfo(int aid, int pdId) {
        Param info = ProductCacheCtrl.getCacheInfo(aid, pdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> pdIds = new HashSet<>();
        pdIds.add(pdId);
        FaiList<Param> list = getList(aid, pdIds);
        if(Util.isEmptyList(list)) {
            return new Param();
        }
        info = list.get(0);
        return info;
    }

    public FaiList<Param> getProductList(int aid, HashSet<Integer> pdIdList, Ref<FaiList<Param>> listRef) {
        return getList(aid, pdIdList);
    }

    private FaiList<Param> getList(int aid, HashSet<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
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
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        // 添加到缓存
        ProductCacheCtrl.addCacheList(aid, tmpRef.value);

        return list;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductDaoCtrl m_dao;
}
