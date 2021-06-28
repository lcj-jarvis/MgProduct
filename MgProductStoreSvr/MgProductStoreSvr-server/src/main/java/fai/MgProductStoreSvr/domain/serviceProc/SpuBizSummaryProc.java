package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryValObj;
import fai.MgProductStoreSvr.domain.repository.DataType;
import fai.MgProductStoreSvr.domain.repository.SpuBizSummaryCacheCtrl;
import fai.MgProductStoreSvr.domain.repository.SpuBizSummaryDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class SpuBizSummaryProc {
    public SpuBizSummaryProc(SpuBizSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public SpuBizSummaryProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null){
            throw new RuntimeException(String.format("SpuBizSummaryDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int report4synSPU2SKU(int aid, Map<Integer, Map<Integer, Param>> unionPriId_pdId_bizSalesSummaryInfoMapMap) {
        if(aid <= 0 || unionPriId_pdId_bizSalesSummaryInfoMapMap == null || unionPriId_pdId_bizSalesSummaryInfoMapMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId_pdId_bizSalesSummaryInfoMapMap=%s;", m_flow, aid, unionPriId_pdId_bizSalesSummaryInfoMapMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Calendar now = Calendar.getInstance();
        for (Map.Entry<Integer, Map<Integer, Param>> unionPriId_pdId_bizSalesSummaryInfoMapEntry : unionPriId_pdId_bizSalesSummaryInfoMapMap.entrySet()) {
            int unionPriId = unionPriId_pdId_bizSalesSummaryInfoMapEntry.getKey();
            Map<Integer, Param> pdId_bizSalesSummaryInfoMap = unionPriId_pdId_bizSalesSummaryInfoMapEntry.getValue();
            FaiList<Integer> pdIdList = new FaiList<>(pdId_bizSalesSummaryInfoMap.keySet());
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getInfoListByPdIdListFromDao(aid, unionPriId, pdIdList, listRef, SpuBizSummaryEntity.Info.PD_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Param> batchUpdateDataList = new FaiList<>();
            FaiList<Param> oldInfoList = listRef.value;
            for (Param oldInfo : oldInfoList) {
                Integer pdId = oldInfo.getInt(SpuBizSummaryEntity.Info.PD_ID);
                Param newInfo = pdId_bizSalesSummaryInfoMap.remove(pdId);
                Param data = new Param();
                data.assign(newInfo, SpuBizSummaryEntity.Info.MAX_PRICE);
                data.assign(newInfo, SpuBizSummaryEntity.Info.MIN_PRICE);
                data.assign(newInfo, SpuBizSummaryEntity.Info.COUNT);
                data.assign(newInfo, SpuBizSummaryEntity.Info.REMAIN_COUNT);
                data.assign(newInfo, SpuBizSummaryEntity.Info.HOLDING_COUNT);
                data.assign(newInfo, SpuBizSummaryEntity.Info.SALES);
                data.assign(newInfo, SpuBizSummaryEntity.Info.FLAG);
                data.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);

                data.setInt(SpuBizSummaryEntity.Info.AID, aid);
                data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                data.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
                batchUpdateDataList.add(data);
            }
            if(!batchUpdateDataList.isEmpty()){
                ParamUpdater batchUpdater = new ParamUpdater();
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.MAX_PRICE, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.MIN_PRICE, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.COUNT, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.REMAIN_COUNT, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.HOLDING_COUNT, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.SALES, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.FLAG, "?");
                batchUpdater.getData().setString(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, "?");

                ParamMatcher batchMatcher = new ParamMatcher();
                batchMatcher.and(SpuBizSummaryEntity.Info.AID,ParamMatcher.EQ, "?");
                batchMatcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID,ParamMatcher.EQ, "?");
                batchMatcher.and(SpuBizSummaryEntity.Info.PD_ID,ParamMatcher.EQ, "?");
                rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"m_daoCtrl.batchUpdate err;flow=%s;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
                    return rt;
                }
            }
            if(!pdId_bizSalesSummaryInfoMap.isEmpty()){
                FaiList<Param> addDataList = new FaiList<>();
                for (Map.Entry<Integer, Param> pdId_bizSalesSummaryInfoEntry : pdId_bizSalesSummaryInfoMap.entrySet()) {
                    Integer pdId = pdId_bizSalesSummaryInfoEntry.getKey();
                    Param bizSalesSummaryInfo = pdId_bizSalesSummaryInfoEntry.getValue();
                    Param data = new Param();
                    data.setInt(SpuBizSummaryEntity.Info.AID, aid);
                    data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                    data.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
                    data.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);
                    data.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, now);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.RL_PD_ID);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.MAX_PRICE);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.MIN_PRICE);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.SALES);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.COUNT);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.FLAG);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.REMAIN_COUNT);
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.HOLDING_COUNT);
                    addDataList.add(data);
                }
                // 标记访客数据为脏
                cacheManage.addDataTypeDirtyCacheKey(DataType.Visitor, unionPriId);
                // 标记管理数据为脏
                cacheManage.addDataTypeDirtyCacheKey(DataType.Manage, unionPriId);
                rt = m_daoCtrl.batchInsert(addDataList, null, true);
                if(rt != Errno.OK){
                    Log.logErr(rt,"m_daoCtrl.batchInsert err;flow=%s;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
                    return rt;
                }
            }
            Log.logStd("doing;flow=%s;aid=%s;unionPruId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
        }

        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int report(int aid, int pdId, FaiList<Param> infoList) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        Set<String> needMaxFields = new HashSet<>();
        Map<Integer, Param> unionPriIdInfoMap = new HashMap<>(infoList.size()*4/3+1);
        FaiList<Integer> unionPriIdList = new FaiList<>();
        for (Param info : infoList) {
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, 0);
            int rlPdId = info.getInt(SpuBizSummaryEntity.Info.RL_PD_ID, 0);
            if(unionPriId <= 0 || rlPdId <= 0){
                Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;unionPriId=%s;rlPdId=%s", m_flow, aid, pdId, unionPriId, rlPdId);
                return Errno.ARGS_ERROR;
            }
            needMaxFields.addAll(info.keySet());
            unionPriIdInfoMap.put(unionPriId, info);
            unionPriIdList.add(unionPriId);
        }
        { // 移除字段
            needMaxFields.remove(SpuBizSummaryEntity.Info.AID);
            needMaxFields.remove(SpuBizSummaryEntity.Info.PD_ID);
        }

        // 标记访客数据为脏
        cacheManage.addDataTypeDirtyCacheKey(DataType.Visitor, unionPriIdInfoMap.keySet());

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getInfoListByUnionPriIdListFromDao(aid, unionPriIdList, pdId, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        // 移除unionPriId
        needMaxFields.remove(SpuBizSummaryEntity.Info.UNION_PRI_ID);
        boolean containFlag = needMaxFields.contains(SpuBizSummaryEntity.Info.FLAG);

        Calendar now = Calendar.getInstance();
        FaiList<Param> addDataList = new FaiList<>();
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        Map<Integer, FaiList<Integer>> unionPirIdPdIdListMap = new HashMap<>();
        for (Param oldFields : listRef.value) {
            int oldUnionPriId = oldFields.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, 0);
            Param newFields = unionPriIdInfoMap.remove(oldUnionPriId); // 移除
            if(newFields == null){
                Log.logErr("db data err;flow=%s;aid=%s;pdId=%s;oldFields=%s", m_flow, aid, pdId, oldFields);
                return Errno.ERROR;
            }

            int newFlagOrBit = newFields.getInt(SpuBizSummaryEntity.Info.FLAG, 0);
            int oldFlag = oldFields.getInt(SpuBizSummaryEntity.Info.FLAG, 0);

            new ParamUpdater(newFields).update(oldFields, false);// 更新旧数据
            if(containFlag){
                oldFields.setInt(SpuBizSummaryEntity.Info.FLAG, oldFlag|newFlagOrBit);
            }

            Param updateData = new Param();
            { // updater
                for (String field : needMaxFields) {
                    updateData.assign(oldFields, field);
                }
                updateData.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);
            }
            { // matcher
                updateData.setInt(SpuBizSummaryEntity.Info.AID, aid);
                updateData.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
                updateData.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, oldUnionPriId);
            }
            batchUpdateDataList.add(updateData);
            FaiList<Integer> pdIdList = unionPirIdPdIdListMap.get(oldUnionPriId);
            if(pdIdList == null){
                pdIdList = new FaiList<>();
                unionPirIdPdIdListMap.put(oldUnionPriId, pdIdList);
            }
            pdIdList.add(pdId);
        }

        for (Param info : unionPriIdInfoMap.values()) {
            Param addData = new Param();
            addData.setInt(SpuBizSummaryEntity.Info.AID, aid);
            addData.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, 0);
            // 标记管理数据为脏
            cacheManage.addDataTypeDirtyCacheKey(DataType.Manage, unionPriId);

            if(unionPriId <= 0){
                Log.logErr("add info unionPriId err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return Errno.ERROR;
            }
            int rlPdId = info.getInt(SpuBizSummaryEntity.Info.RL_PD_ID, 0);
            if(rlPdId <= 0){
                Log.logErr("add info rlPdId err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return Errno.ERROR;
            }
            addData.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            addData.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
            addData.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, info.getInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, 0));
            addData.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, info.getInt(SpuBizSummaryEntity.Info.PRICE_TYPE, 0));
            addData.setInt(SpuBizSummaryEntity.Info.MODE_TYPE, info.getInt(SpuBizSummaryEntity.Info.MODE_TYPE, 0));
            addData.setLong(SpuBizSummaryEntity.Info.MARKET_PRICE, info.getLong(SpuBizSummaryEntity.Info.MARKET_PRICE, 0L));
            addData.setLong(SpuBizSummaryEntity.Info.MIN_PRICE, info.getLong(SpuBizSummaryEntity.Info.MIN_PRICE, 0L));
            addData.setLong(SpuBizSummaryEntity.Info.MAX_PRICE, info.getLong(SpuBizSummaryEntity.Info.MAX_PRICE, 0L));
            addData.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, info.getInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 0));
            addData.setInt(SpuBizSummaryEntity.Info.SALES, info.getInt(SpuBizSummaryEntity.Info.SALES, 0));
            addData.setInt(SpuBizSummaryEntity.Info.COUNT, info.getInt(SpuBizSummaryEntity.Info.COUNT, 0));
            addData.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, info.getInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 0));
            addData.setInt(SpuBizSummaryEntity.Info.HOLDING_COUNT, info.getInt(SpuBizSummaryEntity.Info.HOLDING_COUNT, 0));
            addData.setInt(SpuBizSummaryEntity.Info.FLAG, info.getInt(SpuBizSummaryEntity.Info.FLAG, 0));
            addData.setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, info.getString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, ""));
            addData.setString(SpuBizSummaryEntity.Info.KEEP_PROP1, info.getString(SpuBizSummaryEntity.Info.KEEP_PROP1, ""));
            addData.setInt(SpuBizSummaryEntity.Info.KEEP_INT_PROP1, info.getInt(SpuBizSummaryEntity.Info.KEEP_INT_PROP1, 0));
            addData.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);
            addData.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, now);
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
            doBatchUpdater.getData().setString(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, "?");
            ParamMatcher doBatchMatcher = new ParamMatcher();
            doBatchMatcher.and(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            doBatchMatcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            doBatchMatcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
            cacheManage.addDirtyCacheKey(aid, unionPirIdPdIdListMap);
            rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logErr("batchUpdate err;flow=%s;aid=%s;pdId=%s;doBatchUpdater.json=%s;batchUpdateDataList=%s;", m_flow, aid, pdId, doBatchUpdater.toJson(), batchUpdateDataList);
                return rt;
            }
        }
        Log.logStd("ok!;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int getReportList(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;listRef=%s;", m_flow, aid, pdIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = SpuBizSummaryEntity.Info.PD_ID + ", "
                +COMM_REPORT_FIELDS;
        selectArg.group = SpuBizSummaryEntity.Info.PD_ID;
        selectArg.searchArg.matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        for (Param reportInfo : listRef.value) {
            initReportInfo(reportInfo);
        }
        Log.logStd(rt,"getReportList ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int getReportInfo(int aid, int pdId, Ref<Param> infoRef){
        if(aid <= 0 || pdId <= 0 || infoRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;infoRef=%s;", m_flow, aid, pdId, infoRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = COMM_REPORT_FIELDS;
        selectArg.searchArg.matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
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
        initReportInfo(infoRef.value);
        Log.logStd(rt,"getReportList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    private void initReportInfo(Param info){
        if(info.getLong(SpuBizSummaryEntity.ReportInfo.MIN_PRICE, Long.MAX_VALUE) == Long.MAX_VALUE){
            info.setLong(SpuBizSummaryEntity.ReportInfo.MIN_PRICE, 0L);
        }
    }
    /**
     * min(if(flag&0x1=0x1, minPrice, 0x7fffffffffffffffL)) 设置过价格的才参与计算最小值。
     */
    private static final String COMM_REPORT_FIELDS = SpuBizSummaryEntity.ReportInfo.SOURCE_UNION_PRI_ID+", "+
            "sum(" + SpuBizSummaryEntity.Info.COUNT + ") as " + SpuBizSummaryEntity.ReportInfo.SUM_COUNT + ", " +
                    "sum(" + SpuBizSummaryEntity.Info.REMAIN_COUNT + ") as " + SpuBizSummaryEntity.ReportInfo.SUM_REMAIN_COUNT + ", " +
                    "sum(" + SpuBizSummaryEntity.Info.HOLDING_COUNT + ") as "+ SpuBizSummaryEntity.ReportInfo.SUM_HOLDING_COUNT+", " +
                    "min( if("+SpuBizSummaryEntity.Info.FLAG+"&"+ SpuBizSummaryValObj.FLag.SETED_PRICE+"="+ SpuBizSummaryValObj.FLag.SETED_PRICE+","+ SpuBizSummaryEntity.Info.MIN_PRICE+"," + Long.MAX_VALUE + ") ) as "+ SpuBizSummaryEntity.ReportInfo.MIN_PRICE+", " +
                    "max(" + SpuBizSummaryEntity.Info.MAX_PRICE + ") as "+ SpuBizSummaryEntity.ReportInfo.MAX_PRICE+" ";


    public int batchAdd(int aid, int unionPriId, FaiList<Param> infoList){
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0 || unionPriId <= 0 || Util.isEmptyList(infoList)){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;unionPriId=%s;infoList=%s;", m_flow, aid, unionPriId, infoList);
            return rt;
        }
        FaiList<Param> addList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(SpuBizSummaryEntity.Info.AID, aid);
            data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            data.assign(info, SpuBizSummaryEntity.Info.PD_ID);
            data.assign(info, SpuBizSummaryEntity.Info.RL_PD_ID);
            data.assign(info, SpuBizSummaryEntity.Info.PRICE_TYPE);
            data.assign(info, SpuBizSummaryEntity.Info.MODE_TYPE);
            data.assign(info, SpuBizSummaryEntity.Info.MARKET_PRICE);
            data.assign(info, SpuBizSummaryEntity.Info.MIN_PRICE);
            data.assign(info, SpuBizSummaryEntity.Info.MAX_PRICE);
            data.assign(info, SpuBizSummaryEntity.Info.VIRTUAL_SALES);
            data.assign(info, SpuBizSummaryEntity.Info.SALES);
            data.assign(info, SpuBizSummaryEntity.Info.COUNT);
            data.assign(info, SpuBizSummaryEntity.Info.REMAIN_COUNT);
            data.assign(info, SpuBizSummaryEntity.Info.HOLDING_COUNT);
            data.assign(info, SpuBizSummaryEntity.Info.DISTRIBUTE_LIST);
            data.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);
            addList.add(data);
        }
        rt = m_daoCtrl.batchInsert(addList);
        if(rt != Errno.OK){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;unionPriId=%s;addList=%s", m_flow, aid, unionPriId, addList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int batchSet(int aid, int unionPriId, FaiList<ParamUpdater> updaterList){
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0 || unionPriId <= 0 || Util.isEmptyList(updaterList)){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;unionPriId=%s;updaterList=%s;", m_flow, aid, unionPriId, updaterList);
            return rt;
        }

        //TODO

        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef, SpuBizSummaryEntity.Info.UNION_PRI_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd(rt, "select err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Map<Integer, FaiList<Integer>> unionPirIdPdIdListMap = new HashMap<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            unionPirIdPdIdListMap.put(info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID), pdIdList);
        }
        // 标记访客数据为脏
        cacheManage.addDataTypeDirtyCacheKey(DataType.Visitor, unionPirIdPdIdListMap.keySet());
        // 标记管理数据为脏
        cacheManage.addDataTypeDirtyCacheKey(DataType.Manage, unionPirIdPdIdListMap.keySet());
        cacheManage.addDirtyCacheKey(aid, unionPirIdPdIdListMap);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int clearData(int aid, int unionPriId) {
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriId;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int getInfoListByUnionPriIdListFromDao(int aid, FaiList<Integer> unionPriIdList, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty() || (unionPriIdList != null && unionPriIdList.isEmpty())|| listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriIdList=%s;pdIdList=%s;listRef=%s;", m_flow, aid, unionPriIdList, pdIdList, listRef);
            return Errno.ARGS_ERROR;
        }

        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if(unionPriIdList != null){
            matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int getInfoListByUnionPriIdListFromDao(int aid, FaiList<Integer> unionPriIdList, int pdId, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdId <= 0 || (unionPriIdList != null && unionPriIdList.isEmpty())|| listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriIdList=%s;pdId=%s;listRef=%s;", m_flow, aid, unionPriIdList, pdId, listRef);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(unionPriIdList != null){
            matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int getInfoListByPdIdListFromDao(int aid, int unionPriId, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
        return rt;
    }

    public int getAllDataFromDao(int aid, int unionPriId, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int searchAllDataFromDao(int aid, int unionPriId, SearchArg searchArg, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(searchArg.matcher);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int getInfoListByPdIdList(int aid, int unionPriId, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> resultList = new FaiList<>();
        listRef.value = resultList;
        Set<Integer> pdIdSet = new HashSet<>(pdIdList);
        FaiList<Param> cacheList = SpuBizSummaryCacheCtrl.getCacheList(aid, unionPriId, pdIdList);
        int rt = Errno.ERROR;
        if(cacheList != null){
            for (Param info : cacheList) {
                if(pdIdSet.remove(info.getInt(SpuBizSummaryEntity.Info.PD_ID))){
                    resultList.add(info);
                }
            }
        }
        if(pdIdSet.isEmpty()){
            return rt = Errno.OK;
        }
        rt = getInfoListByPdIdListFromDao(aid, unionPriId, pdIdList, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> list = listRef.value;
        SpuBizSummaryCacheCtrl.setCacheList(aid, unionPriId, list);
        resultList.addAll(list);
        return rt;
    }

    public long getLastUpdateTime(DataType dataType, int aid, int unionPriId){
        Long lastUpdateTime = SpuBizSummaryCacheCtrl.LastUpdateCache.getLastUpdateTime(dataType, aid, unionPriId);
        if(lastUpdateTime != null){
            return lastUpdateTime;
        }
        LockUtil.lock(aid);
        try {
            lastUpdateTime = SpuBizSummaryCacheCtrl.LastUpdateCache.getLastUpdateTime(dataType, aid, unionPriId);
            if(lastUpdateTime != null){
                return lastUpdateTime;
            }
            lastUpdateTime = System.currentTimeMillis();
            SpuBizSummaryCacheCtrl.LastUpdateCache.setLastUpdateTime(dataType, aid, unionPriId, lastUpdateTime);
            return lastUpdateTime;
        }finally {
            LockUtil.unlock(aid);
        }
    }

    public int getTotal(int aid, int unionPriId, Ref<Integer> totalRef) {
        int rt = Errno.ERROR;
        int total = SpuBizSummaryCacheCtrl.getTotal(aid, unionPriId);
        if(total > 0){
            totalRef.value = total;
            return Errno.OK;
        }
        LockUtil.lock(aid);
        try {
            total = SpuBizSummaryCacheCtrl.getTotal(aid, unionPriId);
            if(total > 0){
                totalRef.value = total;
                return Errno.OK;
            }
            ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            rt = m_daoCtrl.selectCount(searchArg, totalRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "selectCount err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
                return rt;
            }
            SpuBizSummaryCacheCtrl.setTotal(aid, unionPriId, totalRef.value);
        }finally {
            LockUtil.unlock(aid);
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    /**
     * 设置缓存过期
     */
    public boolean setDirtyCacheEx(int aid){
        return cacheManage.setDirtyCacheEx(aid);
    }
    public void deleteDirtyCache(int aid){
        cacheManage.deleteDirtyCache(aid);
    }

    private int m_flow;
    private SpuBizSummaryDaoCtrl m_daoCtrl;

    private CacheManage cacheManage = new CacheManage();


    private static class CacheManage{
        private Map<DataType, Set<Integer>> dirtyDataTypeMap;
        private Map<Integer, FaiList<Integer>> dirtyCacheKeyMap;
        public CacheManage() {
            init();
        }
        private void init() {
            dirtyCacheKeyMap = new HashMap<>();
            dirtyDataTypeMap = new HashMap<>();
        }
        public boolean setDirtyCacheEx(int aid) {
            boolean boo = SpuBizSummaryCacheCtrl.setCacheDirty(aid, dirtyCacheKeyMap.keySet());
            boo &= SpuBizSummaryCacheCtrl.setTotalCacheDirty(aid, dirtyCacheKeyMap.keySet());
            return boo;
        }
        public void deleteDirtyCache(int aid){
            try {
                SpuBizSummaryCacheCtrl.delCache(aid, dirtyCacheKeyMap);
                if(!dirtyDataTypeMap.isEmpty()){
                    for (Map.Entry<DataType, Set<Integer>> unionPriIdDataTypeEntry : dirtyDataTypeMap.entrySet()) {
                        DataType dataType = unionPriIdDataTypeEntry.getKey();
                        Set<Integer> unionPriIdSet = unionPriIdDataTypeEntry.getValue();
                        for (Integer unionPriId : unionPriIdSet) {
                            SpuBizSummaryCacheCtrl.LastUpdateCache.setLastUpdateTime(dataType, aid, unionPriId, System.currentTimeMillis());
                        }
                    }
                }
            }finally {
                init();
            }
        }
        
        private void addDirtyCacheKey(int aid, Map<Integer, FaiList<Integer>> unionPirIdPdIdListMap){
            if(unionPirIdPdIdListMap == null){
                return;
            }
            for (Map.Entry<Integer, FaiList<Integer>> dataEntry : unionPirIdPdIdListMap.entrySet()) {
                int unionPriId = dataEntry.getKey();
                FaiList<Integer> pdIds = dirtyCacheKeyMap.get(unionPriId);
                if(pdIds == null) {
                    pdIds = new FaiList<>();
                    dirtyCacheKeyMap.put(unionPriId, pdIds);
                }
                pdIds.addAll(dataEntry.getValue());
            }
            //dirtyCacheKeyMap.putAll(unionPirIdPdIdListMap);
        }
        private void addDataTypeDirtyCacheKey(DataType dataType, Set<Integer> unionPriIdSet){
            if(unionPriIdSet.isEmpty()){
                return;
            }
            Set<Integer> set = getSet(dataType);
            set.addAll(unionPriIdSet);
        }

        private Set<Integer> getSet(DataType dataType) {
            Set<Integer> set = dirtyDataTypeMap.get(dataType);
            if(set == null){
                set = new HashSet<>();
                dirtyDataTypeMap.put(dataType, set);
            }
            return set;
        }

        private void addDataTypeDirtyCacheKey(DataType dataType, int unionPriId){
            Set<Integer> set = getSet(dataType);
            set.add(unionPriId);
        }
    }
}
