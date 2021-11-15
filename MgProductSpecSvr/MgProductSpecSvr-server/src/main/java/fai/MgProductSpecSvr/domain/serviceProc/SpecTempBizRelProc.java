package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.SpecTempBizRelEntity;
import fai.MgProductSpecSvr.domain.repository.SpecTempBizRelCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecTempBizRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SpecTempBizRelProc {

    public SpecTempBizRelProc(SpecTempBizRelDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int unionPriId, FaiList<Param> infoList, FaiList<Integer> rtIdList) {
        if(aid <= 0 || unionPriId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("batchAdd arg error;flow=%d;aid=%s;unionPriId=%s;infoList=%s;", m_flow, aid, unionPriId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(SpecTempBizRelEntity.Info.AID, aid);
            data.setInt(SpecTempBizRelEntity.Info.UNION_PRI_ID, unionPriId);
            data.assign( info, SpecTempBizRelEntity.Info.RL_LIB_ID);
            Integer rlTpScId = info.getInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID);
            if(rlTpScId == null){
                rlTpScId = m_daoCtrl.buildId(unionPriId);
            }else{
                rlTpScId = m_daoCtrl.updateId(unionPriId, rlTpScId);
            }
            if(rlTpScId == null){
                Log.logErr("batchAdd arg error;flow=%d;aid=%s;unionPriId=%s;info=%s;", m_flow, aid, unionPriId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(rlTpScId);
            }
            data.setInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID, rlTpScId);
            data.assign( info, SpecTempBizRelEntity.Info.TP_SC_ID);
            data.assign( info, SpecTempBizRelEntity.Info.SORT);
            data.assign( info, SpecTempBizRelEntity.Info.FLAG);
            data.setCalendar(SpecTempBizRelEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(SpecTempBizRelEntity.Info.SYS_UPDATE_TIME, now);
            dataList.add(data);
        }


        int rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd("batchAdd ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, int unionPriId, int sysType, FaiList<Integer> rlTpScIdList) {
        if(aid <= 0 || unionPriId <= 0 || rlTpScIdList == null){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;unionPriId=%s;rlTpScIdList=%s;", m_flow, aid, unionPriId, rlTpScIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpecTempBizRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(SpecTempBizRelEntity.Info.RL_TP_SC_ID, ParamMatcher.IN, rlTpScIdList);
        cacheManage.addNeedDelCachedRlTpScIdList(aid, unionPriId, rlTpScIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;rlTpScIdList=%s;", m_flow, aid, rlTpScIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;rlTpScIdList=%s;", m_flow, aid, rlTpScIdList);
        return rt;
    }

    public int clearAcct(int aid, FaiList<Integer> unionPriIds) {
        if(aid <= 0 || Util.isEmptyList(unionPriIds)){
            Log.logErr("clearAcct arg error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clearAcct error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        // 处理下idBuilder
        for(int unionPriId : unionPriIds) {
            m_daoCtrl.restoreMaxId(unionPriId);
        }
        Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int batchSet(int aid, int unionPriId, int sysType, FaiList<ParamUpdater> specTempBizRelUpdaterList) {
        if(aid <= 0 || unionPriId<= 0 || specTempBizRelUpdaterList == null || specTempBizRelUpdaterList.isEmpty()){
            Log.logErr("batchSet arg error;flow=%d;aid=%s;unionPriId=%s;specTempBizRelUpdaterList=%s;", m_flow, aid, unionPriId, specTempBizRelUpdaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        FaiList<Integer> rlTpScIdList = new FaiList<>(specTempBizRelUpdaterList.size());
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(specTempBizRelUpdaterList, SpecTempBizRelEntity.getValidKeys(), data->{
            rlTpScIdList.add(data.getInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID));
        });
        maxUpdaterKeys.remove(SpecTempBizRelEntity.Info.RL_TP_SC_ID);

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getList(aid, unionPriId, sysType, rlTpScIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        if(rlTpScIdList.size() != listRef.value.size()){
            Log.logStd("batchDel arg err;flow=%d;aid=%s;unionPriId=%s;updaterList=%s;", m_flow, aid, unionPriId, specTempBizRelUpdaterList);
            return rt = Errno.NOT_FOUND;
        }
        Map<Integer, Param> oldDataMap = Utils.getMap(listRef.value, SpecTempBizRelEntity.Info.RL_TP_SC_ID);
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(SpecTempBizRelEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempBizRelEntity.Info.RL_TP_SC_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(specTempBizRelUpdaterList.size());
        for (ParamUpdater specTempBizRelUpdater : specTempBizRelUpdaterList) {
            int rlTpScId = specTempBizRelUpdater.getData().getInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID);

            Param oldData = oldDataMap.remove(rlTpScId); // help gc
            Param updatedData = specTempBizRelUpdater.update(oldData, true);

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(SpecTempBizRelEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(SpecTempBizRelEntity.Info.AID, aid);
                data.setInt(SpecTempBizRelEntity.Info.UNION_PRI_ID, unionPriId);
                data.setInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID, rlTpScId);
            }
            dataList.add(data);
        }
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int getTpScIdByRlTpScId(int aid, int unionPriId, int sysType, int rlTpScId, Ref<Integer> tpScIdRef) {
        int tpScId = SpecTempBizRelCacheCtrl.getRlTpScId(aid, unionPriId, rlTpScId);
        if(tpScId != -1){
            tpScIdRef.value = tpScId;
            return Errno.OK;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpecTempBizRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(SpecTempBizRelEntity.Info.RL_TP_SC_ID, ParamMatcher.EQ, rlTpScId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef, new String[]{SpecTempBizRelEntity.Info.TP_SC_ID});
        if(rt != Errno.OK) {
            Log.logErr(rt, "getTpScIdByRlTpScId err;flow=%d;aid=%s;unionPriId=%s;rlTpScId=%s;", m_flow, aid, unionPriId, rlTpScId);
            return rt;
        }
        tpScId = infoRef.value.getInt(SpecTempBizRelEntity.Info.TP_SC_ID);
        tpScIdRef.value = tpScId;
        SpecTempBizRelCacheCtrl.setRlTpScId(aid, unionPriId, rlTpScId, tpScId);
        Log.logDbg(rt,"getTpScIdByRlTpScId ok;flow=%d;aid=%s;unionPriId=%s;rlTpScId=%s;tpScId=%s;", m_flow, aid, unionPriId, rlTpScId, tpScId);
        return rt;
    }

    public int getList(int aid, int unionPriId, int sysType, Ref<FaiList<Param>> listRef) {
        return getList(aid, unionPriId, sysType, null, listRef);
    }
    public int getList(int aid, int unionPriId, int sysType, FaiList<Integer> rlTpScIdlIst, Ref<FaiList<Param>> listRef) {
        ParamMatcher matcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpecTempBizRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        if(rlTpScIdlIst != null){
            matcher.and(SpecTempBizRelEntity.Info.RL_TP_SC_ID, ParamMatcher.IN, rlTpScIdlIst);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logDbg(rt,"get ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }
    public int getCount(int aid, int unionPriId, int sysType, FaiList<Integer> rlLibIdList, Ref<Integer> countRef) {
        ParamMatcher matcher = new ParamMatcher(SpecTempBizRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempBizRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpecTempBizRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(SpecTempBizRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, rlLibIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.selectCount(searchArg, countRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logDbg(rt,"get ok;flow=%d;aid=%s;unionPriId=%s;countRef.value=%s", m_flow, aid, unionPriId, countRef.value);
        return rt;
    }

    public int clearIdBuilderCache(int aid, int unionPriId){
        int rt = m_daoCtrl.clearIdBuilderCache(aid, unionPriId);
        return rt;
    }

    private int m_flow;
    private SpecTempBizRelDaoCtrl m_daoCtrl;
    private CacheManage cacheManage = new CacheManage();

    public void deleteDirtyCache(int aid, int unionPriId) {
        cacheManage.delNeedDelCache(aid, unionPriId);
    }

    private static class CacheManage{
        public CacheManage() {
            init();
        }

        private Set<Integer> needDelCachedRlTpScIdSet;
        private void addNeedDelCachedRlTpScIdList(int aid, int unionPriId, FaiList<Integer> rlTpScIdList){
            if(rlTpScIdList == null || rlTpScIdList.isEmpty()){
                return;
            }
            SpecTempBizRelCacheCtrl.setRlTpScIdCacheDirty(aid, unionPriId);
            needDelCachedRlTpScIdSet.addAll(rlTpScIdList);
        }
        private void addNeedDelCachedRlTpScId(int aid, int unionPriId, int rlTpScId){
            SpecTempBizRelCacheCtrl.setRlTpScIdCacheDirty(aid, unionPriId);
            needDelCachedRlTpScIdSet.add(rlTpScId);
        }

        private boolean delNeedDelCache(int aid, int unionPriId){
            try {
                boolean boo = SpecTempBizRelCacheCtrl.delRlTpScId(aid, unionPriId, needDelCachedRlTpScIdSet);
                return boo;
            }finally {
                init();
            }
        }

        private void init() {
            needDelCachedRlTpScIdSet = new HashSet<>();
        }
    }
}
