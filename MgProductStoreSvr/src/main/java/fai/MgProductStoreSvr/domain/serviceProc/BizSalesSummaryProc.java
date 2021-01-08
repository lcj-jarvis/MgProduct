package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.BizSalesSummaryEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.domain.repository.BizSalesSummaryDaoCtrl;
import fai.comm.util.*;

import java.util.*;

public class BizSalesSummaryProc {
    public BizSalesSummaryProc(BizSalesSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int report(int aid, int pdId, FaiList<Param> infoList) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        Set<String> needMaxFields = new HashSet<>();
        Map<Integer, Param> unionPriIdInfoMap = new HashMap<>(infoList.size()*4/3+1);
        FaiList<Integer> unionPriIdList = new FaiList<>();
        for (Param info : infoList) {
            int unionPriId = info.getInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, 0);
            int rlPdId = info.getInt(BizSalesSummaryEntity.Info.RL_PD_ID, 0);
            if(unionPriId <= 0 || rlPdId <= 0){
                Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;unionPriId=%s;rlPdId=%s", m_flow, aid, pdId, unionPriId, rlPdId);
                return Errno.ARGS_ERROR;
            }
            needMaxFields.addAll(info.keySet());
            unionPriIdInfoMap.put(unionPriId, info);
            unionPriIdList.add(unionPriId);
        }
        { // 移除字段
            needMaxFields.remove(BizSalesSummaryEntity.Info.AID);
            needMaxFields.remove(BizSalesSummaryEntity.Info.PD_ID);
        }

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getInfoListFromDao(aid, pdId, unionPriIdList, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        // 移除unionPriId
        needMaxFields.remove(BizSalesSummaryEntity.Info.UNION_PRI_ID);

        Calendar now = Calendar.getInstance();
        FaiList<Param> addDataList = new FaiList<>();
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldFields : listRef.value) {
            int oldUnionPriId = oldFields.getInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, 0);
            Param newFields = unionPriIdInfoMap.remove(oldUnionPriId); // 移除
            if(newFields == null){
                Log.logErr("db data err;flow=%s;aid=%s;pdId=%s;oldFields=%s", m_flow, aid, pdId, oldFields);
                return Errno.ERROR;
            }

            new ParamUpdater(newFields).update(oldFields, false);// 更新旧数据
            Param updateData = new Param();
            { // updater
                for (String field : needMaxFields) {
                    updateData.assign(oldFields, field);
                }
                updateData.setCalendar(BizSalesSummaryEntity.Info.SYS_UPDATE_TIME, now);
            }
            { // matcher
                updateData.setInt(BizSalesSummaryEntity.Info.AID, aid);
                updateData.setInt(BizSalesSummaryEntity.Info.PD_ID, pdId);
                updateData.setInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, oldUnionPriId);
            }
            batchUpdateDataList.add(updateData);
        }
        for (Param info : unionPriIdInfoMap.values()) {
            Param addData = new Param();
            addData.setInt(BizSalesSummaryEntity.Info.AID, aid);
            addData.setInt(BizSalesSummaryEntity.Info.PD_ID, pdId);
            int unionPriId = info.getInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, 0);
            if(unionPriId <= 0){
                Log.logErr("add info unionPriId err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return Errno.ERROR;
            }
            int rlPdId = info.getInt(BizSalesSummaryEntity.Info.RL_PD_ID, 0);
            if(rlPdId <= 0){
                Log.logErr("add info rlPdId err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return Errno.ERROR;
            }
            addData.setInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            addData.setInt(BizSalesSummaryEntity.Info.RL_PD_ID, rlPdId);
            addData.setInt(BizSalesSummaryEntity.Info.PRICE_TYPE, info.getInt(BizSalesSummaryEntity.Info.PRICE_TYPE, 0));
            addData.setInt(BizSalesSummaryEntity.Info.MODE_TYPE, info.getInt(BizSalesSummaryEntity.Info.MODE_TYPE, 0));
            addData.setLong(BizSalesSummaryEntity.Info.MARKET_PRICE, info.getLong(BizSalesSummaryEntity.Info.MARKET_PRICE, 0L));
            addData.setLong(BizSalesSummaryEntity.Info.MIN_PRICE, info.getLong(BizSalesSummaryEntity.Info.MIN_PRICE, 0L));
            addData.setLong(BizSalesSummaryEntity.Info.MAX_PRICE, info.getLong(BizSalesSummaryEntity.Info.MAX_PRICE, 0L));
            addData.setInt(BizSalesSummaryEntity.Info.VIRTUAL_SALES, info.getInt(BizSalesSummaryEntity.Info.VIRTUAL_SALES, 0));
            addData.setInt(BizSalesSummaryEntity.Info.SALES, info.getInt(BizSalesSummaryEntity.Info.SALES, 0));
            addData.setInt(BizSalesSummaryEntity.Info.COUNT, info.getInt(BizSalesSummaryEntity.Info.COUNT, 0));
            addData.setInt(BizSalesSummaryEntity.Info.REMAIN_COUNT, info.getInt(BizSalesSummaryEntity.Info.REMAIN_COUNT, 0));
            addData.setInt(BizSalesSummaryEntity.Info.HOLDING_COUNT, info.getInt(BizSalesSummaryEntity.Info.HOLDING_COUNT, 0));
            addData.setInt(BizSalesSummaryEntity.Info.FLAG, info.getInt(BizSalesSummaryEntity.Info.FLAG, 0));
            addData.setString(BizSalesSummaryEntity.Info.DISTRIBUTE_LIST, info.getString(BizSalesSummaryEntity.Info.DISTRIBUTE_LIST, ""));
            addData.setString(BizSalesSummaryEntity.Info.KEEP_PROP1, info.getString(BizSalesSummaryEntity.Info.KEEP_PROP1, ""));
            addData.setInt(BizSalesSummaryEntity.Info.KEEP_INT_PROP1, info.getInt(BizSalesSummaryEntity.Info.KEEP_INT_PROP1, 0));
            addData.setCalendar(BizSalesSummaryEntity.Info.SYS_UPDATE_TIME, now);
            addData.setCalendar(BizSalesSummaryEntity.Info.SYS_CREATE_TIME, now);
            addDataList.add(addData);
        }
        if(!addDataList.isEmpty()){
            rt = m_daoCtrl.batchInsert(addDataList, null, true);
            if(rt != Errno.OK){
                Log.logErr("batchInsert err;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
                return rt;
            }
        }
        if(!batchUpdateDataList.isEmpty()){
            ParamUpdater doBatchUpdater = new ParamUpdater();
            for (String field : needMaxFields) {
                doBatchUpdater.getData().setString(field, "?");
            }
            doBatchUpdater.getData().setString(BizSalesSummaryEntity.Info.SYS_UPDATE_TIME, "?");
            ParamMatcher doBatchMatcher = new ParamMatcher();
            doBatchMatcher.and(BizSalesSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            doBatchMatcher.and(BizSalesSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            doBatchMatcher.and(BizSalesSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logErr("batchUpdate err;flow=%s;aid=%s;pdId=%s;doBatchUpdater.json=%s;batchUpdateDataList=%s;", m_flow, aid, pdId, doBatchUpdater.toJson(), batchUpdateDataList);
                return rt;
            }
        }
        Log.logStd("ok!;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int getReportInfo(int aid, int pdId, Ref<Param> infoRef){
        if(aid <= 0 || pdId <= 0 || infoRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;infoRef=%s;", m_flow, aid, pdId, infoRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = COMM_REPORT_FIELDS;
        selectArg.searchArg.matcher = new ParamMatcher(BizSalesSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(BizSalesSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        if(listRef.value.isEmpty()){
            infoRef.value = new Param();
        }else{
            infoRef.value = listRef.value.get(0);
        }
        Log.logDbg(rt,"getReportList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    private static final String COMM_REPORT_FIELDS =
            "sum(" + BizSalesSummaryEntity.Info.COUNT + ") as " + BizSalesSummaryEntity.ReportInfo.SUM_COUNT + ", " +
                    "sum(" + BizSalesSummaryEntity.Info.REMAIN_COUNT + ") as " + BizSalesSummaryEntity.ReportInfo.SUM_REMAIN_COUNT + ", " +
                    "sum(" + BizSalesSummaryEntity.Info.HOLDING_COUNT + ") as "+BizSalesSummaryEntity.ReportInfo.SUM_HOLDING_COUNT+", " +
                    "min(" + BizSalesSummaryEntity.Info.MIN_PRICE + ") as "+BizSalesSummaryEntity.ReportInfo.MIN_PRICE+", " +
                    "max(" + BizSalesSummaryEntity.Info.MAX_PRICE + ") as "+BizSalesSummaryEntity.ReportInfo.MAX_PRICE+" ";


    public int getInfoListFromDao(int aid, int pdId, FaiList<Integer> unionPriIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdId <= 0 || (unionPriIdList != null && unionPriIdList.isEmpty())|| listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;unionPriIdList=%s;listRef=%s;", m_flow, aid, pdId, unionPriIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(BizSalesSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(BizSalesSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(unionPriIdList != null){
            matcher.and(BizSalesSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logDbg("ok!;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    private int m_flow;
    private BizSalesSummaryDaoCtrl m_daoCtrl;
}
