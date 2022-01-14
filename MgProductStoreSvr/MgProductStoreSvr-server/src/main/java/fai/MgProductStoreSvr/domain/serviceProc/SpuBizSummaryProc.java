package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.DataType;
import fai.MgProductStoreSvr.domain.repository.SpuBizSummaryCacheCtrl;
import fai.MgProductStoreSvr.domain.repository.SpuBizSummaryDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.SpuBizSummarySagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.exception.MgException;

import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class SpuBizSummaryProc {
    public SpuBizSummaryProc(SpuBizSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public SpuBizSummaryProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        m_sagaDaoCtrl = SpuBizSummarySagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null || m_sagaDaoCtrl == null){
            throw new RuntimeException(String.format("SpuBizSummaryDaoCtrl or SpuBizSummarySagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public int migrate(int aid, FaiList<Param> list) {
        if(Utils.isEmptyList(list)) {
            Log.logErr("arg error;flow=%d;aid=%s;", m_flow, aid);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, FaiList<Integer>> unionPriId_pdIds = new HashMap<>();
        for(Param info : list) {
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(SpuBizSummaryEntity.Info.PD_ID);
            FaiList<Integer> pdIds = unionPriId_pdIds.get(unionPriId);
            if(pdIds == null) {
                pdIds = new FaiList<>();
                unionPriId_pdIds.put(unionPriId, pdIds);
            }

            if(!pdIds.contains(pdId)) {
                pdIds.add(pdId);
            }
        }
        int rt;
        // 先查一遍数据
        for(int unionPriId : unionPriId_pdIds.keySet()) {
            FaiList<Integer> pdIds = unionPriId_pdIds.get(unionPriId);
            Ref<FaiList<Param>> ref = new Ref<>();
            rt = getInfoListByPdIdListFromDao(aid, unionPriId, pdIds, ref, SpuBizSummaryEntity.Info.PD_ID);
            if(rt != Errno.OK) {
                Log.logErr(rt, "check old info err;aid=%d;uid=%d;pdIds=%s", aid, unionPriId, pdIds);
                return rt;
            }
            if(pdIds.size() != ref.value.size()) {
                rt = Errno.ERROR;
                FaiList<Integer> dbPdIds = Utils.getValList(ref.value, SpuBizSummaryEntity.Info.PD_ID);
                // 打印没查到的pdId
                pdIds.removeAll(dbPdIds);
                Log.logErr(rt, "old data err;aid=%d;unionPriId=%d;pdIds=%s;", aid, unionPriId, pdIds);
                return rt;
            }
        }
        FaiList<Param> dataList = new FaiList<>();
        for(Param info : list) {
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(SpuBizSummaryEntity.Info.PD_ID);
            String distributeList = info.getString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST);
            int priceType = info.getInt(SpuBizSummaryEntity.Info.PRICE_TYPE);
            Param data = new Param();
            // for updater
            data.setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, distributeList);
            data.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);

            // for matcher
            data.setInt(SpuBizSummaryEntity.Info.AID, aid);
            data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);

            dataList.add(data);
        }

        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");

        ParamUpdater updater = new ParamUpdater();
        updater.getData().setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, "?");
        updater.getData().setString(SpuBizSummaryEntity.Info.PRICE_TYPE, "?");

        rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "spu batchUpdate err;aid=%d;", aid);
            return rt;
        }
        cacheManage.addDirtyCacheKey(aid, unionPriId_pdIds);
        return rt;
    }
    public int setSingle(int aid, int unionPriId, int pdId, ParamUpdater updater, boolean isSaga) {
        if(updater == null || updater.isEmpty()) {
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;uid=%s;", m_flow, aid, pdId, unionPriId);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, FaiList<Integer>> unionPriId_pdIds = new HashMap<>();
        unionPriId_pdIds.put(unionPriId, Utils.asFaiList(pdId));

        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);

        int rt = doUpdate(aid, matcher, updater, isSaga);
        if(rt != Errno.OK) {
            Log.logErr(rt, "spu batchUpdate err;aid=%d;", aid);
            return rt;
        }
        cacheManage.addDirtyCacheKey(aid, unionPriId_pdIds);
        return rt;
    }

    public int cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId) {
        ParamMatcher delMatcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, toUnionPriId);

        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clear old list error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }

        return copyBizBind(aid, fromUnionPriId, toUnionPriId, null, false);
    }

    public int copyBizBind(int aid, int fromUnionPriId, int toUnionPriId, FaiList<Integer> pdIds, boolean isSaga) {
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, fromUnionPriId);
        if(!Utils.isEmptyList(pdIds)) {
            matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "copyBizBind error;flow=%d;aid=%d;fromUid=%s;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }

        FaiList<Param> list = listRef.value;

        Calendar now = Calendar.getInstance();
        for(Param info : list) {
            // 业务场景如此，库存相关数据不复制过来
            info.remove(SpuBizSummaryEntity.Info.VIRTUAL_SALES);
            info.remove(SpuBizSummaryEntity.Info.SALES);
            info.remove(SpuBizSummaryEntity.Info.COUNT);
            info.remove(SpuBizSummaryEntity.Info.REMAIN_COUNT);
            info.remove(SpuBizSummaryEntity.Info.HOLDING_COUNT);

            info.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, now);
            info.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, now);
            info.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, toUnionPriId);
        }
        // 分布式事务需要添加 Saga 记录
        if (isSaga) {
            rt = addInsOp4Saga(aid, list);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        if (!Utils.isEmptyList(list)) {
            rt = m_daoCtrl.batchInsert(list, null, true);
            if(rt != Errno.OK) {
                Log.logErr(rt, "copyBizBind error;flow=%d;aid=%d;fromUid=%s;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
                return rt;
            }
        } else {
            return Errno.OK;
        }

        Log.logStd("copyBizBind ok;flow=%d;aid=%d;fromUid=%s;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        return rt;
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
                    data.assign(bizSalesSummaryInfo, SpuBizSummaryEntity.Info.SYS_TYPE);
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
        return report(aid, pdId, infoList, false);
    }

    public int report(int aid, int pdId, FaiList<Param> infoList, boolean isSaga) {
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
        // Saga 模式下需要记录下原始数据
        if (isSaga) {
            if (!listRef.value.isEmpty()) {
                preAddUpdateSaga(aid, listRef.value);
            }
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

        // 在 unionPriIdInfoMap 剩下的就是应该添加的数据
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
            int sysType = info.getInt(SpuBizSummaryEntity.Info.SYS_TYPE, 0);
            addData.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            addData.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
            addData.setInt(SpuBizSummaryEntity.Info.SYS_TYPE, sysType);
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
            rt = m_daoCtrl.batchInsert(addDataList, null, !isSaga);
            if(rt != Errno.OK){
                Log.logErr("batchInsert err;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
                return rt;
            }
            // 分布式事务需要添加 Saga 记录
            if (isSaga) {
                rt = addInsOp4Saga(aid, addDataList);
                if (rt != Errno.OK) {
                    return rt;
                }
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

    /**
     * 获取补偿信息
     *
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param spuBizSummarySagaListRef 接收返回的list
     * @return {@link Errno}
     */
    public int getSagaList(String xid, Long branchId, Ref<FaiList<Param>> spuBizSummarySagaListRef) {
        int rt;
        if (Str.isEmpty(xid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;xid is empty;flow=%d", m_flow);
            return rt;
        }
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        searchArg.matcher = matcher;
        rt = m_sagaDaoCtrl.select(searchArg, spuBizSummarySagaListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
            return rt;
        }
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


    public int batchAdd(int aid, FaiList<Param> infoList, boolean isSaga){
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0 || Utils.isEmptyList(infoList)){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return rt;
        }
        FaiList<Param> addList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            Integer unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
            if(unionPriId == null || unionPriId <= 0) {
                Log.logErr(rt, "arg error, unionPriId err;flow=%s;aid=%s;infoList=%s;", m_flow, aid, infoList);
                return rt;
            }
            int sourceUnionPriId = info.getInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            data.setInt(SpuBizSummaryEntity.Info.AID, aid);
            data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
            data.assign(info, SpuBizSummaryEntity.Info.PD_ID);
            data.assign(info, SpuBizSummaryEntity.Info.RL_PD_ID);
            data.assign(info, SpuBizSummaryEntity.Info.SYS_TYPE);
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
        rt = m_daoCtrl.batchInsert(addList, null, !isSaga);
        if(rt != Errno.OK){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;addList=%s", m_flow, aid, addList);
            return rt;
        }
        if(isSaga) {
            rt = addInsOp4Saga(aid, addList);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int batchSet(int aid, FaiList<Integer> unionPriIds, FaiList<Integer> pdIds, ParamUpdater updater, boolean isSaga){
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0 || Utils.isEmptyList(pdIds) || Utils.isEmptyList(unionPriIds) || updater == null || updater.isEmpty()){
            Log.logErr(rt, "arg error;flow=%s;aid=%s;pdIds=%s;unionPriId=%s;updater=%s;", m_flow, aid, pdIds, unionPriIds, updater.toJson());
            return rt;
        }

        Map<Integer, FaiList<Integer>> unionPriId_pdIds = new HashMap<>();
        for(int unionPriId : unionPriIds) {
            unionPriId_pdIds.put(unionPriId, pdIds);
        }

        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        rt = doUpdate(aid, matcher, updater, isSaga);
        if(rt != Errno.OK) {
            Log.logErr(rt, "spu batchUpdate err;aid=%d;", aid);
            return rt;
        }
        cacheManage.addDirtyCacheKey(aid, unionPriId_pdIds);

        Log.logStd("batchSet ok;flow=%s;aid=%s;pdIds=%s;unionPriId=%s;update=%s;", m_flow, aid, pdIds, unionPriIds, updater.toJson());
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        // 如果不是分布式事务只需要查询，单个字段
        if (isSaga) {
            rt = m_daoCtrl.select(searchArg, listRef);
        } else {
            rt = m_daoCtrl.select(searchArg, listRef, SpuBizSummaryEntity.Info.UNION_PRI_ID);
        }
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
        if (isSaga) {
            rt = addDelOp4Saga(aid, listRef.value);
            if(rt != Errno.OK){
                return rt;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }

        Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int clearData(int aid, Integer unionPriId) {
        return clearData(aid, new FaiList<>(Arrays.asList(unionPriId)));
    }
    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
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
    private SpuBizSummarySagaDaoCtrl m_sagaDaoCtrl;

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

    private int doUpdate(int aid, ParamMatcher matcher, ParamUpdater updater, boolean isSaga) {
        int rt;
        if(matcher == null || updater == null || updater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "update args err;aid=%d;matcher=%s;updater=%s", aid, matcher.toJson(), updater.toJson());
            return rt;
        }
        matcher.and(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);

        // Saga 模式下需要记录下原始数据
        if (isSaga) {
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = searchFromDB(aid, matcher, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
            preAddUpdateSaga(aid, listRef.value);
        }

        rt = m_daoCtrl.update(updater, matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "spu batchUpdate err;aid=%d;", aid);
            return rt;
        }

        return rt;
    }

    private int searchFromDB(int aid, ParamMatcher matcher, Ref<FaiList<Param>> listRef) {
        matcher.and(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt == Errno.NOT_FOUND) {
            Log.logDbg(rt, "not found;aid=%d;matcher=%s;", aid, matcher.toJson());
           return rt;
        }
        if(rt != Errno.OK) {
            Log.logErr(rt, "select error;aid=%d;matcher=%s;", aid, matcher.toJson());
            return rt;
        }
        return rt;
    }

    // 记录删除前的原数据
    private int addDelOp4Saga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            return Errno.OK;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        // 构建数据
        list.forEach(sagaInfo -> {
            sagaInfo.setString(SagaEntity.Common.XID, xid);
            sagaInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
            sagaInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
        });
        // 添加 Saga 操作记录
        int rt = m_sagaDaoCtrl.batchInsert(list, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchInsert SagaOperation error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, list);
            return rt;
        }
        return rt;
    }

    // 记录添加操作
    private int addInsOp4Saga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            Log.logStd("addInsOp4Saga list is empty;flow=%d;aid=%d", m_flow, aid);
            return Errno.OK;
        }
        FaiList<Param> sagaOpList = new FaiList<>();
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        list.forEach(addData -> {
            // 添加数据其实只需要记录主键就可以了
            Param sagaOpInfo = new Param();
            sagaOpInfo.assign(addData, SpuBizSummaryEntity.Info.AID);
            sagaOpInfo.assign(addData, SpuBizSummaryEntity.Info.PD_ID);
            sagaOpInfo.assign(addData, SpuBizSummaryEntity.Info.UNION_PRI_ID);
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaOpList.add(sagaOpInfo);
        });
        int rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addInsOp4Saga err;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    // 预记录修改操作数据
    private void preAddUpdateSaga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            Log.logStd("preAddUpdateSaga list is empty;flow=%d;aid=%d", m_flow, aid);
            return;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        String[] keys = SpuBizSummaryEntity.getMaxUpdateAndPriKeys();
        for (Param info : list) {
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(SpuBizSummaryEntity.Info.PD_ID);
            PrimaryKey primaryKey = new PrimaryKey(aid, pdId, unionPriId);
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            Param sagaOpInfo = new Param();
            for (String key : keys) {
                sagaOpInfo.assign(info, key);
            }
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, sagaOpInfo);
        }
    }

    // 将 sagaMap 中的数据持久化到 db
    public int addUpdateSaga2Db(int aid) {
        int rt;
        if (sagaMap.isEmpty()) {
            return Errno.OK;
        }
        rt = m_sagaDaoCtrl.batchInsert(new FaiList<>(sagaMap.values()), null, false);
        if (rt != Errno.OK) {
            Log.logErr("insert sagaMap error;flow=%d;aid=%d;sagaList=%s", m_flow, aid, sagaMap.values().toString());
            return rt;
        }
        return rt;
    }

    /**
     * SpuBizSummaryProc 回滚
     * @param aid aid
     * @param xid 全局事务id
     * @param branchId 分支事务id
     */
    public void rollback4Saga(int aid, String xid, Long branchId) {
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getSagaList(xid, branchId, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get sagaOpList err;flow=%d;aid=%;xid=%s;branchId=%s", m_flow, aid, xid, branchId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            Log.logStd("SpuBizSummaryProc sagaOpList is empty");
            return;
        }

        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = listRef.value.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚删除
        rollback4Del(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));

        // 回滚新增
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚修改
        rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE));
    }

    // 回滚删除
    private void rollback4Del(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        // 去除 Saga 字段
        FaiList<Param> infoList = Util.removeSpecificColumn(new FaiList<>(list), SagaEntity.Common.XID, SagaEntity.Common.BRANCH_ID, SagaEntity.Common.SAGA_OP, SagaEntity.Common.SAGA_TIME);
        int rt = m_daoCtrl.batchInsert(infoList, null, false);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch insert err;flow=%d;aid=%d;infoList=%s", m_flow, aid, infoList);
        }
        Log.logStd("rollback del ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚修改
    private void rollback4Update(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        String[] updateKeys = SpuBizSummaryEntity.getMaxUpdateAndPriKeys();
        FaiList<String> keys = new FaiList<>(Arrays.asList(updateKeys));
        // 去除主键
        keys.remove(SpuBizSummaryEntity.Info.AID);
        keys.remove(SpuBizSummaryEntity.Info.UNION_PRI_ID);
        keys.remove(SpuBizSummaryEntity.Info.PD_ID);

        FaiList<Param> dataList = new FaiList<>();
        for (Param info : list) {
            int pdId = info.getInt(SpuBizSummaryEntity.Info.PD_ID);
            int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
            Param data = new Param();
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(SpuBizSummaryEntity.Info.AID, aid);
            data.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
            data.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            dataList.add(data);
        }

        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch update err;flow=%d;aid=%d;dataList=%d", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚新增
    private void rollback4Add(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        // 根据 unionPriId 分组，减少循环里操作 db 的次数
        Map<Integer, List<Param>> groupByUidMap = list.stream().collect(Collectors.groupingBy(x -> x.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID)));
        for (Map.Entry<Integer, List<Param>> entry : groupByUidMap.entrySet()) {
            int unionPriId = entry.getKey();
            List<Param> entryValue = entry.getValue();
            FaiList<Integer> pdIdList = Utils.getValList(entryValue, SpuBizSummaryEntity.Info.PD_ID);
            ParamMatcher matcher = new ParamMatcher(SpuBizSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpuBizSummaryEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(SpuBizSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
            int rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                throw new MgException(rt, "delete err;flow=%d;aid=%d;unionPriId=%d;pdIdList=%s", m_flow, aid, unionPriId, pdIdList);
            }
        }
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    private HashMap<PrimaryKey, Param> sagaMap;

    private static class PrimaryKey {
        int aid;
        int unionPriId;
        int pdId;

        public PrimaryKey(int aid, int unionPriId, int pdId) {
            this.aid = aid;
            this.unionPriId = unionPriId;
            this.pdId = pdId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    unionPriId == that.unionPriId &&
                    pdId == that.pdId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, unionPriId, pdId);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", unionPriId=" + unionPriId +
                    ", pdId=" + pdId +
                    '}';
        }
    }
}
