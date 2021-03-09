package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelValObj;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.cache.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductRelProc {

    public ProductRelProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductRelDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public int addProductRel(int aid, int unionPriId, Param relData) {
        int rt;
        if(Str.isEmpty(relData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        int count = getPdRelCount(aid, unionPriId);
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductValObj.Limit.COUNT_MAX);
        }
        Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID);
        if(rlPdId == null) {
            rlPdId = m_dao.buildId(aid, unionPriId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }else {
            rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }
        relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
        rt = m_dao.insert(relData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product rel error;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        return rlPdId;
    }

    public FaiList<Integer> batchAddProductRel(int aid, Integer pdId, FaiList<Param> relDataList) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
        }

        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
            if(pdId != null) {
                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
            }
            Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(curPdId == null) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
            }

            int unionPriId = relData.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            int count = getPdRelCount(aid, unionPriId);
            if(count >= ProductRelValObj.Limit.COUNT_MAX) {
                rt = Errno.COUNT_LIMIT;
                throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductValObj.Limit.COUNT_MAX);
            }

            Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID);
            if(rlPdId == null) {
                rlPdId = m_dao.buildId(aid, unionPriId, false);
                if (rlPdId == null) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                }
            }else {
                rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
                if (rlPdId == null) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                }
            }
            relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
            rlPdIds.add(rlPdId);
        }

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        return rlPdIds;
    }

    public int getPdRelCount(int aid, int unionPriId) {
        // 从缓存中获取
        Integer count = ProductRelCacheCtrl.getRelCountCache(aid, unionPriId);
        if(count != null) {
            return count;
        }

        // 从db获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        String fields = "count(*) as cnt";
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, fields);
        if(rt != Errno.OK || listRef.value == null || listRef.value.isEmpty()) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }

        Param res = listRef.value.get(0);
        count = res.getInt("cnt", 0);

        // 添加到缓存
        ProductRelCacheCtrl.setRelCountCache(aid, unionPriId, count);
        return count;
    }

    public void delProductRel(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdIds=%s", m_flow, aid, unionPriId, rlPdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;rlPdIds=%d;", m_flow, aid, unionPriId, rlPdIds);
        }
    }

    /**
     * 根据pdId, 删除所有关联数据
     */
    public void delProductRelByPdId(int aid, FaiList<Integer> pdIds, FaiList<Integer> returnUids) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.UNION_PRI_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get unionPriId error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            return;
        }
        for(Param info : listRef.value) {
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            returnUids.add(unionPriId);
        }
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
    }

    public Param getProductRel(int aid, int unionPriId, int rlPdId) {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdId=%s", m_flow, aid, unionPriId, rlPdId);
        }
        Param info = ProductRelCacheCtrl.getCacheInfo(aid, unionPriId, rlPdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> rlPdIds = new HashSet<>();
        rlPdIds.add(rlPdId);
        FaiList<Param> relList = getList(aid, unionPriId, rlPdIds);
        if(relList == null || relList.isEmpty()) {
            return new Param();
        }

        return relList.get(0);
    }

    public FaiList<Param> getProductRelList(int aid, int unionPriId, FaiList<Integer> rlPdIdList) {
        int rt;
        if(rlPdIdList == null || rlPdIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get rlPdIdList is empty;aid=%d;rlPdIdList=%s;", aid, rlPdIdList);
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>(rlPdIdList);

        return getList(aid, unionPriId, rlPdIds);
    }

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> rlPdIds) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIds is empty;aid=%d;rlPdIds=%s;", aid, rlPdIds);
        }
        List<String> rlPdIdStrs = rlPdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.getCacheList(aid, unionPriId, rlPdIdStrs);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == rlPdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(rlPdIds);
        for(Param info : list) {
            Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
            noCacheIds.remove(rlPdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheIds);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return list;
        }

        // 添加到缓存
        ProductRelCacheCtrl.addCacheList(aid, unionPriId, tmpRef.value);

        return list;
    }

    /** pdId+unionPriId 和 rlPdId的映射 **/
    public FaiList<Param> getRlPdIdList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get error, pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.getRlIdRelCacheList(aid, unionPriId, pdIds);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == pdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<Integer>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        //只查aid+pdId+unionPriId+rlPdId
        rt = m_dao.select(searchArg, tmpRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get product rel info error;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
            // 添加到缓存
            ProductRelCacheCtrl.addRlIdRelCacheList(aid, unionPriId, tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        return list;
    }

    public FaiList<Param> getBoundUniPriIds(int aid, FaiList<Integer> pdIds) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        //只查aid+pdId+unionPriId+rlPdId
        int rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getRlPdInfoByPdIds fail;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);

        }

        return listRef.value;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductRelDaoCtrl m_dao;
}
