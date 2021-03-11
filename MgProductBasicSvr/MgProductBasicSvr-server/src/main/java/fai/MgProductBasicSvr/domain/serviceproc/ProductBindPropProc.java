package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindPropDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

public class ProductBindPropProc {

    public ProductBindPropProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public FaiList<Param> getPdBindPropList(int aid, int unionPriId, int rlPdId) {
        return getList(aid, unionPriId, rlPdId);
    }

    public void addPdBindPropList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(Param tmpinfo : infoList) {
            Param info = new Param();
            int rlPropId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PROP_ID, 0);
            int propValId = tmpinfo.getInt(ProductBindPropEntity.Info.PROP_VAL_ID, 0);
            if(rlPropId == 0 || propValId == 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, unionPriId, rlPdId, rlPropId, propValId);
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
        rt = m_bindPropDao.batchInsert(addList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind prop error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public void delPdBindPropList(int aid, int unionPriId, int rlPdId, FaiList<Param> delList) {
        int rt;
        if(delList == null || delList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        for(Param info : delList) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
            rt = m_bindPropDao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, rlPdId, rlPropId, propValId);
            }
        }
        Log.logStd("delPdBindPropList ok;flow=%d;aid=%d;rlPdId=%d;", m_flow, aid, rlPdId);
    }

    /**
     * 根据参数id+参数值id的列表，筛选出商品业务id
     * 目前是直接查db
     */
    public FaiList<Integer> getRlPdByPropVal(int aid, int unionPriId, FaiList<Param> proIdsAndValIds) {
        int rt;
        FaiList<Integer> rlPropIds = new FaiList<Integer>();
        FaiList<Integer> propValIds = new FaiList<Integer>();
        for(Param info : proIdsAndValIds) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            if(!rlPropIds.contains(rlPropId)) {
                rlPropIds.add(rlPropId);
            }
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            if(!propValIds.contains(propValId)) {
                propValIds.add(propValId);
            }
        }
        if(rlPropIds.isEmpty() || propValIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;rlPropIds or propValIds is empty;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        // 先将可能符合条件的数据查出来，再做筛选, 避免循环查db
        ParamMatcher sqlMatcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        sqlMatcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        sqlMatcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlPropIds);
        sqlMatcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, propValIds);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = sqlMatcher;
        rt = m_bindPropDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select error bind prop;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        FaiList<Integer> rlPdIds = new FaiList<>();
        FaiList<Param> list = listRef.value;
        if (list == null || list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rlPdIds;
        }

        for(Param info : proIdsAndValIds) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            searchRlPdByPropVal(aid, unionPriId, rlPropId, propValId, rlPdIds, list);
            if(rlPdIds.isEmpty()) {
                break;
            }
        }

        return rlPdIds;
    }

    private void searchRlPdByPropVal(int aid, int unionPriId, int rlPropId, int propValId, FaiList<Integer> rlPdIds, FaiList<Param> searchList) {
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
        if(!rlPdIds.isEmpty()) {
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        }
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> tmpList = searcher.getParamList(searchList);
        rlPdIds.clear();
        for(Param info : tmpList) {
            int rlPdId = info.getInt(ProductBindPropEntity.Info.RL_PD_ID);
            if(!rlPdIds.contains(rlPdId)) {
                rlPdIds.add(rlPdId);
            }
        }
    }


    private FaiList<Param> getList(int aid, int unionPriId, int rlPdId) {
        // 缓存中获取
        FaiList<Param> list = ProductBindPropCache.getCacheList(aid, unionPriId, rlPdId);
        if(list != null && !list.isEmpty()) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bindPropDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
        }
        list = listRef.value;

        if (list == null){
            list = new FaiList<Param>();
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
            return list;
        }
        // 添加到缓存
        ProductBindPropCache.setCacheList(aid, unionPriId, rlPdId, list);
        return list;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_bindPropDao)) {
            throw new MgException("registered ProductBindPropDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductBindPropDaoCtrl m_bindPropDao;
}
