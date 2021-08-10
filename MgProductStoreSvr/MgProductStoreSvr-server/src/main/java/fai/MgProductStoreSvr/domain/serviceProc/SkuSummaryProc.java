package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.SkuSummaryDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.SkuSummarySagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class SkuSummaryProc {
    public SkuSummaryProc(SkuSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public SkuSummaryProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        m_sagaDaoCtrl = SkuSummarySagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null || m_sagaDaoCtrl == null){
            throw new RuntimeException(String.format("SkuSummaryDaoCtrl or m_sagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int report(int aid, long skuId, Param info) {
        if(aid <= 0 || info == null || info.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"selectFirst err;flow=%s;aid=%s;skuId=%s;", m_flow, aid, skuId);
            return rt;
        }
        Param oldInfo = infoRef.value;

        Calendar now = Calendar.getInstance();
        info.setCalendar(SkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
        if(oldInfo.isEmpty()){
            info.setInt(SkuSummaryEntity.Info.AID, aid);
            info.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
            info.setCalendar(SkuSummaryEntity.Info.SYS_CREATE_TIME, now);
            rt = m_daoCtrl.insert(info);
            if(rt != Errno.OK){
                Log.logErr(rt,"insert err;flow=%s;aid=%s;skuId=%s;info=%s", m_flow, aid, skuId, info);
                return rt;
            }
        }else {
            ParamUpdater updater = new ParamUpdater(info);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logErr(rt,"update err;flow=%s;aid=%s;skuId=%s;info=%s", m_flow, aid, skuId, info);
                return rt;
            }
        }

        Log.logStd("ok!;flow=%s;aid=%s;skuId=%s;", m_flow, aid, skuId);
        return rt;
    }
    public int report4synSPU2SKU(int aid, Map<Long, Param> skuIdStoreSkuSummaryInfoMap) {
        if(aid <= 0 || skuIdStoreSkuSummaryInfoMap == null || skuIdStoreSkuSummaryInfoMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;skuIdStoreSkuSummaryInfoMap=%s;", m_flow, aid, skuIdStoreSkuSummaryInfoMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Ref<FaiList<Param>> listRef = new Ref<>();
        Calendar now = Calendar.getInstance();
        FaiList<Long> skuIdList = new FaiList<>(skuIdStoreSkuSummaryInfoMap.keySet());
        rt = getListFromDao(aid, skuIdList, listRef, SkuSummaryEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> oldInfoList = listRef.value;
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : oldInfoList) {
            Long skuId = oldInfo.getLong(SkuSummaryEntity.Info.SKU_ID);
            Param newInfo = skuIdStoreSkuSummaryInfoMap.remove(skuId);
            Param data = new Param();
            { // for update
                data.assign(newInfo, SkuSummaryEntity.Info.COUNT);
                data.assign(newInfo, SkuSummaryEntity.Info.REMAIN_COUNT);
                data.assign(newInfo, SkuSummaryEntity.Info.HOLDING_COUNT);
                data.assign(newInfo, SkuSummaryEntity.Info.MIN_PRICE);
                data.assign(newInfo, SkuSummaryEntity.Info.MAX_PRICE);
                data.setCalendar(SkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
            }
            { // for matcher
                data.setInt(SkuSummaryEntity.Info.AID, aid);
                data.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
            }
            batchUpdateDataList.add(data);
        }
        if(!batchUpdateDataList.isEmpty()){
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData()
                    .setString(SkuSummaryEntity.Info.COUNT, "?")
                    .setString(SkuSummaryEntity.Info.REMAIN_COUNT, "?")
                    .setString(SkuSummaryEntity.Info.HOLDING_COUNT, "?")
                    .setString(SkuSummaryEntity.Info.MIN_PRICE, "?")
                    .setString(SkuSummaryEntity.Info.MAX_PRICE, "?")
                    .setString(SkuSummaryEntity.Info.SYS_UPDATE_TIME, "?")
            ;
            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logStd("dao.batchUpdate error;flow=%d;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
            }
        }
        if(!skuIdStoreSkuSummaryInfoMap.isEmpty()){
            FaiList<Param> addDataList = new FaiList<>(skuIdStoreSkuSummaryInfoMap.size());
            for (Map.Entry<Long, Param> skuIdStoreSkuSummaryInfoEntry : skuIdStoreSkuSummaryInfoMap.entrySet()) {
                Long skuId = skuIdStoreSkuSummaryInfoEntry.getKey();
                Param info = skuIdStoreSkuSummaryInfoEntry.getValue();
                Param data = new Param();
                data.setCalendar(SkuSummaryEntity.Info.SYS_CREATE_TIME, now);
                data.setCalendar(SkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
                data.setInt(SkuSummaryEntity.Info.AID, aid);
                data.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
                data.assign(info, SkuSummaryEntity.Info.PD_ID);
                data.assign(info, SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign(info, SkuSummaryEntity.Info.MIN_PRICE);
                data.assign(info, SkuSummaryEntity.Info.MAX_PRICE);
                data.assign(info, SkuSummaryEntity.Info.COUNT);
                data.assign(info, SkuSummaryEntity.Info.REMAIN_COUNT);
                data.assign(info, SkuSummaryEntity.Info.HOLDING_COUNT);
                addDataList.add(data);
            }
            rt = m_daoCtrl.batchInsert(addDataList, null, true);
            if(rt != Errno.OK){
                Log.logStd("dao.batchInsert error;flow=%d;aid=%s;addDataList=%s;", m_flow, aid, addDataList);
            }
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int report(int aid, FaiList<Param> infoList, boolean isSaga) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        Calendar now = Calendar.getInstance();
        FaiList<Long> skuIdList = new FaiList<>(infoList.size());
        Map<Long, Param> skuIdInfoMap = new HashMap<>(infoList.size()*4/3+1);
        for (Param info : infoList) {
            Long skuId = info.getLong(SkuSummaryEntity.Info.SKU_ID);
            skuIdList.add(skuId);
            info.setInt(SkuSummaryEntity.Info.AID, aid);
            info.setCalendar(SkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
            skuIdInfoMap.put(skuId, info);
        }
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDao(aid, skuIdList, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> oldInfoList = listRef.value;
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : oldInfoList) {
            Long skuId = oldInfo.getLong(SkuSummaryEntity.Info.SKU_ID);
            Param newData = skuIdInfoMap.remove(skuId);
            ParamUpdater updater = new ParamUpdater(newData);
            updater.update(oldInfo, false);
            Param data = new Param();
            { // for update
                data.assign(oldInfo, SkuSummaryEntity.Info.MIN_PRICE);
                data.assign(oldInfo, SkuSummaryEntity.Info.MAX_PRICE);
                data.assign(oldInfo, SkuSummaryEntity.Info.COUNT);
                data.assign(oldInfo, SkuSummaryEntity.Info.REMAIN_COUNT);
                data.assign(oldInfo, SkuSummaryEntity.Info.HOLDING_COUNT);
                data.assign(oldInfo, SkuSummaryEntity.Info.FIFO_TOTAL_COST);
                data.assign(oldInfo, SkuSummaryEntity.Info.MW_TOTAL_COST);
                data.assign(oldInfo, SkuSummaryEntity.Info.SYS_UPDATE_TIME);
            }
            { // for matcher
                data.setInt(SkuSummaryEntity.Info.AID, aid);
                data.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
            }
            batchUpdateDataList.add(data);
        }
        FaiList<Param> addList = null;
        if(skuIdInfoMap.size() > 0){ // 批量添加
            addList = new FaiList<>(skuIdInfoMap.size());
            for (Param info : skuIdInfoMap.values()) {
                info.setCalendar(SkuSummaryEntity.Info.SYS_CREATE_TIME, now);
                addList.add(info);
            }
            rt = m_daoCtrl.batchInsert(addList, null, false);
            if(rt != Errno.OK){
                Log.logStd("dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
            }
        }
        // 分布式事务 需要记录 Saga 记录
        if (isSaga && !Util.isEmptyList(addList)) {
            FaiList<Param> sagaList = new FaiList<>();
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            addList.forEach(addInfo -> {
                Param saga = new Param();
                saga.assign(addInfo, SkuSummaryEntity.Info.AID);
                saga.assign(addInfo, SkuSummaryEntity.Info.SKU_ID);
                saga.setString(StoreSagaEntity.Info.XID, xid);
                saga.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
                saga.setInt(StoreSagaEntity.Info.SAGA_OP, StoreSagaValObj.SagaOp.ADD);
                sagaList.add(saga);
            });
            rt = m_sagaDaoCtrl.batchInsert(sagaList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "insert saga error;flow=%d;aid=%d;sagaList=%s", m_flow, aid, sagaList);
                return rt;
            }
        }
        if(batchUpdateDataList.size() > 0){ // 批量更新
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData()
                    .setString(SkuSummaryEntity.Info.MIN_PRICE, "?")
                    .setString(SkuSummaryEntity.Info.MAX_PRICE, "?")
                    .setString(SkuSummaryEntity.Info.COUNT, "?")
                    .setString(SkuSummaryEntity.Info.REMAIN_COUNT, "?")
                    .setString(SkuSummaryEntity.Info.HOLDING_COUNT, "?")
                    .setString(SkuSummaryEntity.Info.FIFO_TOTAL_COST, "?")
                    .setString(SkuSummaryEntity.Info.MW_TOTAL_COST, "?")
                    .setString(SkuSummaryEntity.Info.SYS_UPDATE_TIME, "?")
            ;
            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logStd("dao.batchUpdate error;flow=%d;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
            }
        }


        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if (isSaga) {
            // 分布事务 需要记录 Saga 操作记录
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK) {
                if (rt == Errno.NOT_FOUND) {
                    return Errno.OK;
                }
                Log.logErr(rt, "select error;flow=%d;aid=%d;pdIdList=%s", m_flow, aid, pdIdList);
                return rt;
            }
            FaiList<Param> sagaOpList = listRef.value;
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            // 构建数据
            sagaOpList.forEach(sagaInfo -> {
                sagaInfo.setString(StoreSagaEntity.Info.XID, xid);
                sagaInfo.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
                sagaInfo.setInt(StoreSagaEntity.Info.SAGA_OP, StoreSagaValObj.SagaOp.DEL);
            });
            // 添加 Saga 操作记录
            rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchInsert SagaOperation error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
                return rt;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }

        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }

    public int batchDel(int aid, int pdId, FaiList<Long> skuIdList, boolean isSaga) {
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        int rt;
        if (isSaga) {
            // 分布式事务，需要记录老的数据 录入 Saga 操作记录表中
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK) {
                if (rt == Errno.NOT_FOUND) {
                    return Errno.OK;
                }
                Log.logErr(rt, "select error;flow=%d;aid=%d;pdId=%d;skuIdList=%s", m_flow, aid, pdId, skuIdList);
                return rt;
            }
            FaiList<Param> sagaOpList = listRef.value;
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            // 构建数据
            sagaOpList.forEach(sagaInfo -> {
                sagaInfo.setString(StoreSagaEntity.Info.XID, xid);
                sagaInfo.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
                sagaInfo.setInt(StoreSagaEntity.Info.SAGA_OP, StoreSagaValObj.SagaOp.DEL);
            });
            // 添加 Saga 操作记录
            rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchInsert SagaOperation error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
                return rt;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdId=%s;skuIdList=%s;", m_flow, aid, pdId, skuIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdId=%s;skuIdList=%s;", m_flow, aid, pdId, skuIdList);
        return rt;
    }

    /**
     * Saga 模式 补偿 batchDel 方法
     *
     * @param aid aid
     * @param sagaList Saga 操作记录
     * @return {@link Errno}
     */
    public int batchDelRollback(int aid, FaiList<Param> sagaList) {
        int rt;
        if (Util.isEmptyList(sagaList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;addList is empty");
            return rt;
        }
        Util.removeSagaColumn(sagaList);
        rt = m_daoCtrl.batchInsert(sagaList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchDelRollback err;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        Log.logStd("skuSummary batchDelRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    /**
     * Saga 模式 补偿删除 根据 aid + skuIds
     *
     * @param aid aid
     * @param sagaSkuSumList 含有 sku 汇总表 主键集合
     * @return {@link Errno}
     */
    public int batchAddRollback(int aid, FaiList<Param> sagaSkuSumList) {
        FaiList<Long> skuIds = new FaiList<>(sagaSkuSumList.size());
        for (Param sagaSkuSum : sagaSkuSumList) {
            Long skuId = sagaSkuSum.getLong(SkuSummaryEntity.Info.SKU_ID);
            skuIds.add(skuId);
        }
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.IN, skuIds);
        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchAddRollback err;flow=%d;aid=%d;skuIds=%s", m_flow, aid, skuIds);
            return rt;
        }
        Log.logStd(rt, "batchAddRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    private int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields) {
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        }
        Log.logStd(rt, "ok;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }

    /**
     * 获取补偿记录
     *
     * @param xid xid
     * @param branchId branchId
     * @param skuSummarySagaListRef 接收返回的list
     * @return {@link Errno}
     */
    public int getSagaList(String xid, Long branchId, Ref<FaiList<Param>> skuSummarySagaListRef) {
        int rt;
        if (Str.isEmpty(xid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;xid is empty;flow=%d", m_flow);
            return rt;
        }
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(StoreSagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(StoreSagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        searchArg.matcher = matcher;
        rt = m_sagaDaoCtrl.select(searchArg, skuSummarySagaListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }


    public int searchListFromDao(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        int rt = Errno.ERROR;
        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;searchArg=%s;", m_flow, aid, searchArg);
            return rt;
        }
        Log.logStd(rt, "ok;flow=%d;aid=%s;match=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }


    private int m_flow;
    private SkuSummaryDaoCtrl m_daoCtrl;
    private SkuSummarySagaDaoCtrl m_sagaDaoCtrl;
}
