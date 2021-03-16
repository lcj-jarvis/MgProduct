package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.SpecTempDetailEntity;
import fai.MgProductSpecSvr.domain.repository.SpecTempDetailDaoCtrl;
import fai.comm.util.*;

import java.util.*;

public class SpecTempDetailProc {
    public SpecTempDetailProc(SpecTempDetailDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int tpScId, FaiList<Param> infoList, FaiList<Integer> rtIdList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("batchAdd error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        { // 添加前校验是否有已经存在
            FaiList<Integer> scStrIdList = Utils.getValList(infoList, SpecTempDetailEntity.Info.SC_STR_ID);
            ParamMatcher matcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
            matcher.and(SpecTempDetailEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<Integer> countRef = new Ref<>();
            rt = m_daoCtrl.selectCount(searchArg, countRef);
            if(rt != Errno.OK){
                Log.logErr(rt, "get error;flow=%d;aid=%s;tpScId=%s;scStrIdList=%s;", m_flow, aid, tpScId, scStrIdList);
                return rt;
            }
            if(countRef.value>0){
                rt = Errno.ALREADY_EXISTED;
                Log.logStd(rt, "already existed;flow=%d;aid=%s;tpScId=%s;scStrIdList=%s;", m_flow, aid, tpScId, scStrIdList);
                return rt;
            }
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

        rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd("batchAdd ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, int tpScId, FaiList<Integer> tpScDtIdList) {
        if(aid <= 0 || tpScId <=0 || tpScDtIdList == null){
            Log.logErr("batchDel error;flow=%d;aid=%s;tpScId=%tpScDtIdList=%s;", m_flow, aid, tpScId, tpScDtIdList);
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
        Set<Integer> scStrIdSet = new HashSet<>();
        Set<String> maxUpdaterKeys = Utils.validUpdaterList(specTempDetailUpdaterList, SpecTempDetailEntity.getValidKeys(), data->{
            specTempDtIdList.add(data.getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID));
            Integer scStrId = data.getInt(SpecTempDetailEntity.Info.SC_STR_ID);
            if(scStrId != null){
                scStrIdSet.add(scStrId);
            }
        });
        maxUpdaterKeys.remove(SpecTempDetailEntity.Info.TP_SC_DT_ID);

        // 已经存在的规格字符串id集
        Set<Integer> alreadyExistedScStrIdSet = new HashSet<>();
        if(!scStrIdSet.isEmpty()){
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDao(aid, tpScId, new FaiList<>(scStrIdSet), listRef, SpecTempDetailEntity.Info.SC_STR_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            alreadyExistedScStrIdSet = OptMisc.getValSet(listRef.value, SpecTempDetailEntity.Info.SC_STR_ID);
        }

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDaoByTpScDtIdList(aid, tpScId, specTempDtIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }

        // 用于转化将要改的数据的主键，因为存在：旧数据（id1:规格1，id2:规格2）新数据（id1:规格2，id2:规格1），即同一个规格模板下两个规格名称对调，而规格值集合不变
        FaiList<Integer> needConvertScStrIdList = new FaiList<>();
        Map<Integer, Param> oldDataMap = new HashMap<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            Integer tpScDtId = info.getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID);
            oldDataMap.put(tpScDtId, info);
            Integer oldScStrId = info.getInt(SpecTempDetailEntity.Info.SC_STR_ID);
            if(alreadyExistedScStrIdSet.remove(oldScStrId)){ // 已经存在的规格要改规格名称
                needConvertScStrIdList.add(oldScStrId);
            }
        }

        if(!alreadyExistedScStrIdSet.isEmpty()){ // 要改的规格名称已经存在
            rt = Errno.ALREADY_EXISTED;
            Log.logErr(rt, "already existed;flow=%s;aid=%s;tpScId=%s;alreadyExistedScStrIdSet=%s", m_flow, aid, tpScId, alreadyExistedScStrIdSet);
            return rt;
        }

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(SpecTempDetailEntity.Info.TP_SC_DT_ID, ParamMatcher.EQ, "?");
        Log.logDbg("whalelog  alreadyExistedScStrIdSet=%s", alreadyExistedScStrIdSet);
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(specTempDetailUpdaterList.size());
        for (ParamUpdater specTempDetailUpdater : specTempDetailUpdaterList) {
            int tpScDtId = specTempDetailUpdater.getData().getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID);
            Param oldData = oldDataMap.remove(tpScDtId); // help gc

            Param updatedData = specTempDetailUpdater.update(oldData, true);

            Param data = new Param();
            for (String key : maxUpdaterKeys) {
                if(key.equals(SpecTempDetailEntity.Info.IN_SC_VAL_LIST)){
                    FaiList<Object> inScValList = updatedData.getList(key);
                    data.setString(key, inScValList.toJson());
                }else{
                    data.assign(updatedData, key);
                }
            }

            data.setCalendar(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(SpecTempDetailEntity.Info.AID, aid);
                data.setInt(SpecTempDetailEntity.Info.TP_SC_ID, tpScId);
                data.setInt(SpecTempDetailEntity.Info.TP_SC_DT_ID, tpScDtId);
            }
            dataList.add(data);
        }

        if(!needConvertScStrIdList.isEmpty()){ // 同一个规格模板存在多个规格名称对调的情况，需要把这些规格名称（scStrId） 变化为负数，避免更新时主键冲突
            ParamMatcher convertMatcher = new ParamMatcher();
            convertMatcher.and(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
            convertMatcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
            convertMatcher.and(SpecTempDetailEntity.Info.SC_STR_ID, ParamMatcher.IN, needConvertScStrIdList);
            ParamUpdater convertUpdater = new ParamUpdater();
            convertUpdater.add(SpecTempDetailEntity.Info.SC_STR_ID, ParamUpdater.DEC, Integer.MAX_VALUE);
            rt = m_daoCtrl.update(convertUpdater, convertMatcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "update error;flow=%d;aid=%s;convertUpdater.sql=%s;convertMatcher.sql=%s;data=%s;", m_flow, aid, convertUpdater.getSql(), convertMatcher.getSql());
                return rt;
            }
        }

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchUpdate error;flow=%d;aid=%s;doBatchUpdater.sql=%s;doBatchMatcher.sql=%s;data=%s;", m_flow, aid, doBatchUpdater.getSql(), doBatchMatcher.getSql(), dataList.get(0));
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

    public int getListFromDao(int aid, int tpScId, FaiList<Integer> scStrIdList, Ref<FaiList<Param>> listRef, String ... fields) {
        if(aid <= 0 || tpScId <= 0 || scStrIdList == null || listRef == null){
            Log.logErr("batchSet arg error;flow=%d;aid=%s;tpScId=%s;listRef=%s;scStrIdList=%s;fields=%s;", m_flow, aid, tpScId,listRef, scStrIdList, fields);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpecTempDetailEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecTempDetailEntity.Info.TP_SC_ID, ParamMatcher.EQ, tpScId);
        matcher.and(SpecTempDetailEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logDbg(rt,"getListFromDao ok;flow=%d;aid=%d;tpScId=%s;scStrIdList=%s;", m_flow, aid, tpScId, scStrIdList);
        return rt;
    }

    public int getListFromDaoByTpScDtIdList(int aid, int tpScId, FaiList<Integer> tpScDtIdList, Ref<FaiList<Param>> listRef) {
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
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(listRef.value);
        Log.logDbg(rt,"getListFromDaoByTpScDtIdList ok;flow=%d;aid=%d;tpScId=%s;tpScDtIdList=%s;", m_flow, aid, tpScId, tpScDtIdList);
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

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    private int m_flow;
    private SpecTempDetailDaoCtrl m_daoCtrl;

}
