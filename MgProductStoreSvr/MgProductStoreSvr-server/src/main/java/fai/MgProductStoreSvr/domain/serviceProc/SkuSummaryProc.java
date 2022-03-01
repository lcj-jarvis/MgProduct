package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.dao.SkuSummaryDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.dao.saga.SkuSummarySagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class SkuSummaryProc {
    public SkuSummaryProc(SkuSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public SkuSummaryProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        m_sagaDaoCtrl = SkuSummarySagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null || m_sagaDaoCtrl == null){
            throw new RuntimeException(String.format("SkuSummaryDaoCtrl or m_sagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
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
                data.assign(info, SkuSummaryEntity.Info.SYS_TYPE);
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
        // 预记录修改操作
        if (isSaga) {
            if (!listRef.value.isEmpty()) {
                preAddUpdateSaga(aid, listRef.value);
            }
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
            rt = m_daoCtrl.batchInsert(addList, null, !isSaga);
            if(rt != Errno.OK){
                Log.logStd("dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
            }
        }
        // 分布式事务 需要记录 Saga 记录
        if (isSaga) {
            rt = addInsOp4Saga(aid, addList);
            if (rt != Errno.OK) {
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
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean softDel, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if(softDel) {
            if (isSaga) {
                Ref<FaiList<Param>> listRef = new Ref<>();
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher.clone();
                rt = searchListFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }

                preAddUpdateSaga(aid, listRef.value);
            }

            ParamUpdater updater = new ParamUpdater();
            updater.getData().setInt(SkuSummaryEntity.Info.STATUS, SpuBizSummaryValObj.Status.DEL);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logStd(rt, "update err;flow=%s;aid=%s;matcher=%s;", m_flow, aid, matcher.toJson());
                return rt;
            }
        }else {
            if (isSaga) {
                rt = addDelOp4Saga(aid, matcher);
                if (rt != Errno.OK) {
                    return rt;
                }
            }

            rt = m_daoCtrl.delete(matcher);
            if(rt != Errno.OK){
                Log.logStd(rt, "delete err;flow=%s;aid=%s;matcher=%s;;", m_flow, aid, matcher.toJson());
                return rt;
            }
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
            rt = addDelOp4Saga(aid, matcher);
            if (rt != Errno.OK) {
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
        ParamMatcher matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
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

    // 记录删除操作前的原数据
    private int addDelOp4Saga(int aid, ParamMatcher matcher) {
        int rt;
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef);
        if (rt != Errno.OK) {
            if (rt == Errno.NOT_FOUND) {
                return Errno.OK;
            }
            Log.logErr(rt, "select error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
            return rt;
        }
        FaiList<Param> sagaOpList = listRef.value;
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        // 构建数据
        sagaOpList.forEach(sagaInfo -> {
            sagaInfo.setString(SagaEntity.Common.XID, xid);
            sagaInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
            sagaInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
        });
        // 添加 Saga 操作记录
        rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchInsert SagaOperation error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
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
            sagaOpInfo.assign(addData, SkuSummaryEntity.Info.AID);
            sagaOpInfo.assign(addData, SkuSummaryEntity.Info.SKU_ID);
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
        for (Param info : list) {
            long skuId = info.getLong(SkuSummaryEntity.Info.SKU_ID);
            PrimaryKey primaryKey = new PrimaryKey(aid, skuId);
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            String[] keys = SkuSummaryEntity.getMaxUpdateAndPriKeys();
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
     * SkuSummaryProc 回滚
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
            Log.logStd("SkuSummaryProc sagaOpList is empty");
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

    // 回滚新增
    private void rollback4Add(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        FaiList<Long> skuIdList = Utils.getValList(list, SkuSummaryEntity.Info.SKU_ID);
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "delete err;flow=%d;aid=%d;skuIdList=%s", m_flow, aid, skuIdList);
        }
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚修改
    private void rollback4Update(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        String[] updateKeys = SkuSummaryEntity.getMaxUpdateAndPriKeys();
        FaiList<String> keys = new FaiList<>(Arrays.asList(updateKeys));
        keys.remove(SkuSummaryEntity.Info.AID);
        keys.remove(SkuSummaryEntity.Info.SKU_ID);

        FaiList<Param> dataList = new FaiList<>();
        for (Param info : list) {
            Param data = new Param();
            long skuId = info.getLong(SkuSummaryEntity.Info.SKU_ID);
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(SkuSummaryEntity.Info.AID, aid);
            data.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
            dataList.add(data);
        }

        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(SkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch update err;flow=%d;aid=%d;dataList=%d", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    private HashMap<PrimaryKey, Param> sagaMap;

    public void migrateYKDel(int aid, FaiList<Integer> pdIds) {
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "dao.migrateYKDel error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
        }
    }

    public void restoreData(int aid, FaiList<Integer> pdIds, boolean isSaga) {
        int rt;
        if (Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;pdIds is empty;flow=%d;aid=%d;", m_flow, aid);
        }
        if (isSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "dao.get restore data error;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
            }
            preAddUpdateSaga(aid, listRef.value);
        }

        ParamUpdater updater = new ParamUpdater(new Param().setInt(SkuSummaryEntity.Info.STATUS, SkuSummaryValObj.Status.DEFAULT));
        ParamMatcher matcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        rt = m_daoCtrl.update(updater, matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "dao.restore data error;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }
    }

    private static class PrimaryKey {
        int aid;
        long skuId;

        public PrimaryKey(int aid, long skuId) {
            this.aid = aid;
            this.skuId = skuId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    skuId == that.skuId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, skuId);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", skuId=" + skuId +
                    '}';
        }
    }

    private int m_flow;
    private SkuSummaryDaoCtrl m_daoCtrl;
    private SkuSummarySagaDaoCtrl m_sagaDaoCtrl;
}
