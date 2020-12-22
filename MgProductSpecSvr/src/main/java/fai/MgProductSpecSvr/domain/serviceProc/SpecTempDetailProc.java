package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.Misc2;
import fai.MgProductSpecSvr.domain.entity.SpecTempDetailEntity;
import fai.MgProductSpecSvr.domain.repository.SpecTempDetailDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;

public class SpecTempDetailProc {
    public SpecTempDetailProc(SpecTempDetailDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int tpScId, FaiList<Param> infoList, FaiList<Integer> rtIdList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(SpecTempDetailEntity.Info.AID, aid);
            data.setInt(SpecTempDetailEntity.Info.TP_SC_ID, tpScId);
            data.assign( info, SpecTempDetailEntity.Info.SC_STR_ID);
            Integer tpScDtId = m_daoCtrl.buildId();
            if(tpScDtId == null){
                Log.logErr("batchAdd arg error;flow=%d;aid=%s;tpScId=%s;info=%s;", m_flow, aid, tpScId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(tpScDtId);
            }
            data.setInt(SpecTempDetailEntity.Info.TP_SC_DT_ID, tpScDtId);
            data.assign( info, SpecTempDetailEntity.Info.SORT);
            FaiList<Param> inScValList = info.getList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
            String inScValListStr = inScValList.toJson();
            data.setString(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, inScValListStr);
            data.setCalendar(SpecTempDetailEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, now);
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
    public int batchDel(int aid, int tpScId, FaiList<Integer> tpScDtIdList) {
        if(aid <= 0 || tpScId <=0 || tpScDtIdList == null){
            Log.logStd("batchDel error;flow=%d;aid=%s;tpScId=%tpScDtIdList=%s;", m_flow, aid, tpScId, tpScDtIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
        matcher.and(SpecTempDetailEntity.Info.TP_SC_DT_ID, ParamMatcher.IN, tpScDtIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;tpScId=%sidList=%s;", m_flow, aid, tpScId, tpScDtIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;tpScId=%s;idList=%s;", m_flow, aid, tpScId, tpScDtIdList);
        return rt;
    }


    public int batchSet(int aid, int tpScId, FaiList<ParamUpdater> specTempDetailUpdaterList) {
        if(aid <= 0 || tpScId <= 0 || specTempDetailUpdaterList == null || specTempDetailUpdaterList.isEmpty()){
            Log.logErr("batchSet arg error;flow=%d;aid=%s;tpScId=%s;specTempDetailUpdaterList=%s;", m_flow, aid, tpScId, specTempDetailUpdaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        FaiList<Integer> specTempDtIdList = new FaiList<>(specTempDetailUpdaterList.size());
        Set<String> maxUpdaterKeys = Misc2.validUpdaterList(specTempDetailUpdaterList, SpecTempDetailEntity.getValidKeys(), data->{
            specTempDtIdList.add(data.getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID));
        });

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getList(aid, tpScId, specTempDtIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Map<Integer, Param> oldDataMap = Misc2.getMap(listRef.value, SpecTempDetailEntity.Info.TP_SC_DT_ID);
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempDetailEntity.Info.TP_SC_DT_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(specTempDetailUpdaterList.size());
        for (ParamUpdater specTempDetailUpdater : specTempDetailUpdaterList) {
            int tpScDtId = specTempDetailUpdater.getData().getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID);
            Param oldData = oldDataMap.remove(tpScDtId); // help gc
            Param updatedData = specTempDetailUpdater.update(oldData, true);

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                if(key.equals(SpecTempDetailEntity.Info.IN_SC_VAL_LIST)){
                    FaiList<Object> inScValList = updatedData.getList(key);
                    data.setString(key, inScValList.toJson());
                }else{
                    data.assign(updatedData, key);
                }
            });
            data.setCalendar(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(SpecTempDetailEntity.Info.AID, aid);
                data.setInt(SpecTempDetailEntity.Info.TP_SC_ID, tpScId);
                data.setInt(SpecTempDetailEntity.Info.TP_SC_DT_ID, tpScDtId);
            }
            dataList.add(data);
        }

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;doBatchUpdater.sql=%s;doBatchMatcher.sql=%s;data=%s;", m_flow, aid, doBatchUpdater.getSql(), doBatchMatcher.getSql(), dataList.get(0));
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int get(int aid, int id, Ref<Param> infoRef) {
        ParamMatcher matcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempDetailEntity.Info.TP_SC_DT_ID, ParamMatcher.EQ, id);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "get error;flow=%d;aid=%s;id=%s;", m_flow, aid, id);
            return rt;
        }
        initDBInfo(infoRef.value);
        Log.logDbg("getList ok;flow=%d;aid=%d;id=%s;", m_flow, aid, id);
        return rt;
    }

    public int getList(int aid, int tpScId, FaiList<Integer> tpScDtIdList, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || tpScId <= 0 || listRef == null){
            Log.logErr("batchSet arg error;flow=%d;aid=%s;tpScId=%s;listRef=%s;tpScDtIdList=%s", m_flow, aid, tpScId,listRef, tpScDtIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
        if(tpScDtIdList != null && !tpScDtIdList.isEmpty()){
            matcher.and(SpecTempDetailEntity.Info.TP_SC_DT_ID, ParamMatcher.IN, tpScDtIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(listRef.value);
        Log.logDbg("getList ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    private void initDBInfoList(FaiList<Param> infoList){
        if(infoList == null || infoList.isEmpty()){
            return;
        }
        infoList.forEach(info->{
            initDBInfo(info);
        });
    }

    private void initDBInfo(Param info) {
        if(info == null){
            return;
        }
        String inScValListStr = info.getString(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
        info.setList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, FaiList.parseParamList(inScValListStr, new FaiList<>()));
    }

    private int m_flow;
    private SpecTempDetailDaoCtrl m_daoCtrl;



}
