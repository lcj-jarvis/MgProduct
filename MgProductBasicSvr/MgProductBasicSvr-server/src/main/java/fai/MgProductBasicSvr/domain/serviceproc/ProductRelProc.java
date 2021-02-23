package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelValObj;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.ProductRelDaoCtrl;
import fai.comm.util.*;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductRelProc {

    public ProductRelProc(int flow, ProductRelDaoCtrl dao) {
        this.m_flow = flow;
        this.m_dao = dao;
    }

    public int addProductRel(int aid, int unionPriId, Param relData, Ref<Integer> rlPdIdRef) {
        int rt;
        if(Str.isEmpty(relData)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err, infoList is empty;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
            return rt;
        }
        Ref<Integer> countRef = new Ref<>();
        rt = getPdRelCount(aid, unionPriId, countRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        int count = countRef.value;
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            Log.logErr(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductValObj.Limit.COUNT_MAX);
            return rt;
        }
        Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID);
        if(rlPdId == null) {
            rlPdId = m_dao.buildId(aid, unionPriId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                return rt;
            }else {
                rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
                if (rlPdId == null) {
                    rt = Errno.ERROR;
                    Log.logErr(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                    return rt;
                }
            }
            relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
        }
        rlPdIdRef.value = rlPdId;
        rt = m_dao.insert(relData);
        if(rt != Errno.OK) {
            Log.logErr(rt, "insert product rel error;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
            return rt;
        }
        return rt;
    }

    public int batchAddProductRel(int aid, Integer pdId, FaiList<Param> relDataList, Ref<FaiList<Integer>> rlPdIdsRef) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
            return rt;
        }

        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
            if(pdId != null) {
                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
            }
            Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(curPdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
                return rt;
            }

            int unionPriId = relData.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            Ref<Integer> countRef = new Ref<>();
            rt = getPdRelCount(aid, unionPriId, countRef);
            if(rt != Errno.OK) {
                Log.logErr(rt, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                return rt;
            }
            int count = countRef.value + relDataList.size();
            if(count >= ProductRelValObj.Limit.COUNT_MAX) {
                rt = Errno.COUNT_LIMIT;
                Log.logErr(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductValObj.Limit.COUNT_MAX);
                return rt;
            }

            Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID);
            if(rlPdId == null) {
                rlPdId = m_dao.buildId(aid, unionPriId, false);
                if (rlPdId == null) {
                    rt = Errno.ERROR;
                    Log.logErr(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                    return rt;
                }else {
                    rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
                    if (rlPdId == null) {
                        rt = Errno.ERROR;
                        Log.logErr(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
                        return rt;
                    }
                }
                relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
            }
            rlPdIds.add(rlPdId);
        }
        rlPdIdsRef.value = rlPdIds;

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    public int getPdRelCount(int aid, int unionPriId, Ref<Integer> countRef) {
        // 从缓存中获取
        Integer count = ProductRelCacheCtrl.getRelCountCache(aid, unionPriId);
        if(count != null) {
            countRef.value = count;
            return Errno.OK;
        }

        // 从db获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        String fields = "count(*) as cnt";
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, fields);
        if(rt != Errno.OK) {
            return rt;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        Param res = listRef.value.get(0);
        count = res.getInt("cnt", 0);
        countRef.value = count;

        // 添加到缓存
        ProductRelCacheCtrl.setRelCountCache(aid, unionPriId, count);
        return rt;
    }

    public int delProductRel(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdIds=%s", m_flow, aid, unionPriId, rlPdIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;rlPdIds=%d;", m_flow, aid, unionPriId, rlPdIds);
            return rt;
        }
        return rt;
    }

    /**
     * 根据pdId, 删除所有关联数据
     */
    public int delProductRelByPdId(int aid, FaiList<Integer> pdIds, FaiList<Integer> returnUids) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.UNION_PRI_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "get unionPriId error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
            return rt;
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            return Errno.OK;
        }
        for(Param info : listRef.value) {
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            returnUids.add(unionPriId);
        }
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "del product rel error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
            return rt;
        }
        return rt;
    }

    public int getProductRel(int aid, int unionPriId, int rlPdId, Ref<Param> infoRef) {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdId=%s", m_flow, aid, unionPriId, rlPdId);
            return rt;
        }
        Param info = ProductRelCacheCtrl.getCacheInfo(aid, unionPriId, rlPdId);
        if (!Str.isEmpty(info)) {
            infoRef.value = info;
            return Errno.OK;
        }
        HashSet<Integer> rlPdIds = new HashSet<>();
        rlPdIds.add(rlPdId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        rt = getList(aid, unionPriId, rlPdIds, tmpRef);
        if (rt != Errno.OK) {
            if (rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "get error;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            }
            return rt;
        }

        FaiList<Param> relList = tmpRef.value;
        if(relList == null || relList.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        infoRef.value = relList.get(0);

        rt = Errno.OK;
        return rt;
    }

    public int getProductRelList(int aid, int unionPriId, FaiList<Integer> rlPdIdList, Ref<FaiList<Param>> listRef) {
        int rt;
        if(rlPdIdList == null || rlPdIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("get rlPdIdList is empty;aid=%d;rlPdIdList=%s;", aid, rlPdIdList);
            return rt;
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>(rlPdIdList);
        rt = getList(aid, unionPriId, rlPdIds, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "get list error;flow=%d;aid=%d;rlPdIdList=%s;", m_flow, aid, rlPdIdList);
            return rt;
        }

        return rt;
    }

    private int getList(int aid, int unionPriId, HashSet<Integer> rlPdIds, Ref<FaiList<Param>> listRef) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("get pdIds is empty;aid=%d;rlPdIds=%s;", aid, rlPdIds);
            return rt;
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
            listRef.value = list;
            return Errno.OK;
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
            return rt;
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        // 添加到缓存
        ProductRelCacheCtrl.addCacheList(aid, unionPriId, tmpRef.value);

        listRef.value = list;
        return Errno.OK;
    }

    /** pdId+unionPriId 和 rlPdId的映射 **/
    public int getRlPdIdList(int aid, int unionPriId, FaiList<Integer> pdIds, Ref<FaiList<Param>> listRef) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("get error, pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
            return rt;
        }
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.getRlIdRelCacheList(aid, unionPriId, pdIds);
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
        FaiList<Integer> noCacheIds = new FaiList<Integer>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
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
        ProductRelCacheCtrl.addRlIdRelCacheList(aid, unionPriId, tmpRef.value);

        listRef.value = list;
        return Errno.OK;
    }

    public int getBoundUniPriIds(int aid, FaiList<Integer> pdIds, Ref<FaiList<Param>> listRef) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        //只查aid+pdId+unionPriId+rlPdId
        int rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
            return rt;
        }

        return Errno.OK;
    }

    private int m_flow;
    private ProductRelDaoCtrl m_dao;
}
