package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.SpecTempEntity;
import fai.MgProductSpecSvr.domain.repository.SpecTempDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;

public class SpecTempProc {
    public SpecTempProc(SpecTempDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, FaiList<Param> infoList, FaiList<Integer> rtIdList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty() || rtIdList == null){
            Log.logErr("batchAdd arg error;flow=%d;aid=%s;infoList=%s;rtIdList=%s;", m_flow, aid, infoList, rtIdList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(SpecTempEntity.Info.AID, aid);
            Integer tpScId = m_daoCtrl.buildId();
            if(tpScId == null){
                Log.logErr("batchAdd buildId error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(tpScId);
            }
            data.setInt(SpecTempEntity.Info.TP_SC_ID, tpScId);
            data.assign( info, SpecTempEntity.Info.NAME);
            data.assign( info, SpecTempEntity.Info.SOURCE_TID);
            data.setCalendar(SpecTempEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(SpecTempEntity.Info.SYS_UPDATE_TIME, now);
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
    public int batchDel(int aid, FaiList<Integer> idList) {
        if(aid <= 0 || idList == null){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;idList=%s;", m_flow, aid, idList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempEntity.Info.TP_SC_ID, ParamMatcher.IN, idList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;idList=%s;", m_flow, aid, idList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;idList=%s;", m_flow, aid, idList);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        if(aid <= 0 || Util.isEmptyList(unionPriIds)){
            Log.logErr("clearData arg error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        // 处理下idBuilder
        m_daoCtrl.restoreMaxId();
        Log.logStd("batchDel ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int batchSet(int aid, FaiList<ParamUpdater> specTempUpdaterList) {
        if(aid <= 0 || specTempUpdaterList == null || specTempUpdaterList.isEmpty()){
            Log.logErr("batchSet arg error;flow=%d;aid=%s;specTempUpdaterList=%s;", m_flow, aid, specTempUpdaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        FaiList<Integer> specTempIdList = new FaiList<>(specTempUpdaterList.size());
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(specTempUpdaterList, SpecTempEntity.getValidKeys(), data->{
            specTempIdList.add(data.getInt(SpecTempEntity.Info.TP_SC_ID));
        });
        maxUpdaterKeys.remove(SpecTempEntity.Info.TP_SC_ID);

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getList(aid, specTempIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Map<Integer, Param> oldDataMap = Utils.getMap(listRef.value, SpecTempEntity.Info.TP_SC_ID);
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(SpecTempEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempEntity.Info.TP_SC_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(specTempUpdaterList.size());
        for (ParamUpdater specTempUpdater : specTempUpdaterList) {
            int tpScId = specTempUpdater.getData().getInt(SpecTempEntity.Info.TP_SC_ID, 0);
            Param oldData = oldDataMap.remove(tpScId); // help gc
            Param updatedData = specTempUpdater.update(oldData, true);

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(SpecTempEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(SpecTempEntity.Info.AID, aid);
                data.setInt(SpecTempEntity.Info.TP_SC_ID, tpScId);
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



    public int get(int aid, int tpScId, Ref<Param> infoRef) {
        ParamMatcher matcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "get error;flow=%d;aid=%s;tpScId=%s;", m_flow, aid, tpScId);
            return rt;
        }
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;tpScId=%s;", m_flow, aid, tpScId);
        return rt;
    }

    public int getList(int aid, FaiList<Integer> idList , Ref<FaiList<Param>> listRef) {
        ParamMatcher matcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempEntity.Info.TP_SC_ID, ParamMatcher.IN, idList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;idList=%s;", m_flow, aid, idList);
            return rt;
        }
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;idList=%s;", m_flow, aid, idList);
        return rt;
    }

    public int getListFromDB(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef, String ... selectFields) {
        if(searchArg == null) {
            Log.logErr("arg error, searchArg is null;flow=%d;aid=%s;", m_flow, aid);
            return Errno.ARGS_ERROR;
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
        int rt =  m_daoCtrl.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getList error;flow=%d;aid=%s;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
            }
            listRef.value = new FaiList<>();
            return rt;
        }
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }

    public int clearIdBuilderCache(int aid){
        int rt = m_daoCtrl.clearIdBuilderCache(aid);
        return rt;
    }
    private int m_flow;
    private SpecTempDaoCtrl m_daoCtrl;
}
