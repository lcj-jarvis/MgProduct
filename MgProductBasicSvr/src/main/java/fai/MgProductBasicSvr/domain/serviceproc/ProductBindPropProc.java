package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.domain.repository.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.repository.ProductBindPropDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;

public class ProductBindPropProc {
    public ProductBindPropProc(int flow, ProductBindPropDaoCtrl dao) {
        this.m_flow = flow;
        this.m_bindPropDao = dao;
    }

    public int getPdBindPropList(int aid, int unionPriId, int rlPdId, Ref<FaiList<Param>> listRef) {
        int rt = getList(aid, unionPriId, rlPdId, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "get error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
            return rt;
        }
        return rt;
    }

    public int addPdBindPropList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;");
            return rt;
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(Param tmpinfo : infoList) {
            Param info = new Param();
            int rlPropId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PROP_ID, 0);
            int propValId = tmpinfo.getInt(ProductBindPropEntity.Info.PROP_VAL_ID, 0);
            if(rlPropId == 0 || propValId == 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, unionPriId, rlPdId, rlPropId, propValId);
                return rt;
            }


            info.setInt(ProductBindPropEntity.Info.AID, aid);
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropId);
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValId);
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, unionPriId);
            info.setInt(ProductBindPropEntity.Info.PD_ID, pdId);
            info.setCalendar(ProductBindPropEntity.Info.CREATE_TIME, now);
            addList.add(info);
        }
        rt = m_bindPropDao.batchInsert(aid, addList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batch insert product prop assoc error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    public int delPdBindPropList(int aid, int rlPdId, FaiList<Param> delList) {
        int rt = Errno.ERROR;
        if(delList == null || delList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;");
            return rt;
        }
        for(Param info : delList) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
            rt = m_bindPropDao.delete(aid, matcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "del info error;flow=%d;aid=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, rlPdId, rlPropId, propValId);
                return rt;
            }
        }
        Log.logStd("delPropAssocList ok;flow=%d;aid=%d;rlPdId=%d;", m_flow, aid, rlPdId);
        rt = Errno.OK;
        return rt;
    }

    /**
     * 根据参数id+参数值id的列表，筛选出商品业务id
     * 目前是直接查db
     */
    public int getRlPdByPropVal(int aid, int unionPriId, FaiList<Param> proIdsAndValIds, Ref<FaiList<Param>> listRef) {
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        for(Param info : proIdsAndValIds) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher tmpMatcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            tmpMatcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
            matcher.and(tmpMatcher);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_bindPropDao.select(aid, searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    private int getList(int aid, int unionPriId, int rlPdId, Ref<FaiList<Param>> listRef) {
        // 缓存中获取
        FaiList<Param> list = ProductBindPropCache.getCacheList(aid, unionPriId, rlPdId);
        if(list != null && !list.isEmpty()) {
            listRef.value = list;
            return Errno.OK;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        int rt = m_bindPropDao.select(aid, searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return rt;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
            return rt;
        }
        // 添加到缓存
        ProductBindPropCache.setCacheList(aid, unionPriId, rlPdId, list);
        return Errno.OK;
    }

    private int m_flow;
    private ProductBindPropDaoCtrl m_bindPropDao;
}
