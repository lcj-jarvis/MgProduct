package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelValObj;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.ProductRelDaoCtrl;
import fai.comm.util.*;

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
        rt = m_dao.insert(aid, relData, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "insert product rel error;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
            return rt;
        }
        return rt;
    }

    public int batchAddProductRel(int aid, int pdId, FaiList<Param> relDataList, Ref<FaiList<Integer>> rlPdIdsRef) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
            return rt;
        }

        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
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

            relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
        }
        rlPdIdsRef.value = rlPdIds;

        rt = m_dao.batchInsert(aid, relDataList);
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
        int rt = m_dao.select(aid, searchArg, fields, listRef);
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
        rt = m_dao.delete(aid, matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;rlPdIds=%d;", m_flow, aid, unionPriId, rlPdIds);
            return rt;
        }
        return rt;
    }

    /**
     * 根据pdId, 删除所有关联数据
     */
    public int delProductRelByPdId(int aid, FaiList<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        rt = m_dao.delete(aid, matcher);
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
        rt = getInfo(aid, unionPriId, rlPdId, infoRef);
        if (rt != Errno.OK) {
            if (rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "get error;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            }
            return rt;
        }

        rt = Errno.OK;
        return rt;
    }

    private int getInfo(int aid, int unionPriId, int rlPdId, Ref<Param> infoRef) {
        int rt;
        if(!ProductRelCacheCtrl.exist(aid, unionPriId)) {
            Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
            rt = getList(aid, unionPriId, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
            for (Param p : listRef.value) {
                if (p.getInt(ProductRelEntity.Info.RL_PD_ID) == rlPdId) {
                    infoRef.value = p;
                    return Errno.OK;
                }
            }
        }
        Param info = ProductRelCacheCtrl.getCacheInfo(aid, unionPriId, rlPdId);
        if (info == null || info.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;aid=%d;uid=%d;rlPdId=%d", aid, unionPriId, rlPdId);
            return rt;
        }

        infoRef.value = info;
        return Errno.OK;
    }

    private int getList(int aid, int unionPriId, Ref<FaiList<Param>> listRef) {
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.getCacheList(aid, unionPriId);
        if(list != null && !list.isEmpty()) {
            listRef.value = list;
            return Errno.OK;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        int rt = m_dao.select(aid, searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            ProductRelCacheCtrl.setRelCountCache(aid, unionPriId, 0);
            return rt;
        }

        // 添加到缓存
        ProductRelCacheCtrl.addCacheList(aid, unionPriId, listRef.value);
        return Errno.OK;
    }

    /** rlPdId 和 pdId+unionPriId的映射 **/
    public int getIdRelList(int aid, FaiList<Integer> rlPdIds, Ref<FaiList<Param>> listRef) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("get rlPdIds is empty;aid=%d;rlPdIds=%s;", aid, rlPdIds);
            return rt;
        }
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.getIdRelCacheList(aid, rlPdIds);
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
            int pdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheIds);
        //只查pdId+unionPriId+rlPdId
        String fields = ProductRelEntity.Info.UNION_PRI_ID + "," + ProductRelEntity.Info.PD_ID  + "," + ProductRelEntity.Info.RL_PD_ID;
        rt = m_dao.select(aid, searchArg, fields, tmpRef);
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
        ProductRelCacheCtrl.addIdRelCacheList(aid, tmpRef.value);

        listRef.value = list;
        return Errno.OK;
    }

    private int m_flow;
    private ProductRelDaoCtrl m_dao;
}
