package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.SpuSummaryEntity;
import fai.MgProductStoreSvr.domain.repository.SpuSummaryCacheCtrl;
import fai.MgProductStoreSvr.domain.repository.SpuSummaryDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.SpuSummarySagaDaoCtrl;
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

public class SpuSummaryProc {
    public SpuSummaryProc(SpuSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public SpuSummaryProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        m_sagaDaoCtrl = SpuSummarySagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null || m_sagaDaoCtrl == null){
            throw new RuntimeException(String.format("SpuSummaryDaoCtrl or SpuSummarySagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public int report4synSPU2SKU(int aid, Map<Integer, Param> pdIdSalesSummaryInfoMap) {
        if(aid <= 0 || pdIdSalesSummaryInfoMap == null || pdIdSalesSummaryInfoMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdSalesSummaryInfoMap=%s;", m_flow, aid, pdIdSalesSummaryInfoMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Integer> pdIdList = new FaiList<>(pdIdSalesSummaryInfoMap.keySet());
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDao(aid, pdIdList, listRef, SpuSummaryEntity.Info.PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> oldInfoList = listRef.value;
        Calendar now = Calendar.getInstance();
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : oldInfoList) {
            Integer pdId = oldInfo.getInt(SpuSummaryEntity.Info.PD_ID);
            Param newInfo = pdIdSalesSummaryInfoMap.remove(pdId);
            Param data = new Param();
            data.assign(newInfo, SpuSummaryEntity.Info.MAX_PRICE);
            data.assign(newInfo, SpuSummaryEntity.Info.MIN_PRICE);
            data.assign(newInfo, SpuSummaryEntity.Info.COUNT);
            data.assign(newInfo, SpuSummaryEntity.Info.REMAIN_COUNT);
            data.assign(newInfo, SpuSummaryEntity.Info.HOLDING_COUNT);
            data.setCalendar(SpuSummaryEntity.Info.SYS_UPDATE_TIME, now);

            {
                data.setInt(SpuSummaryEntity.Info.AID, aid);
                data.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
            }
            batchUpdateDataList.add(data);
        }
        cacheManage.addDirtyCacheKey(pdIdList);
        if(!batchUpdateDataList.isEmpty()){
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData().setString(SpuSummaryEntity.Info.MAX_PRICE, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.MIN_PRICE, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.REMAIN_COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.HOLDING_COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.SYS_UPDATE_TIME, "?");

            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logErr(rt, "m_daoCtrl.batchUpdate err;flow=%s;aid=%s;batchUpdateDataList=%s", m_flow, aid, batchUpdateDataList);
                return rt;
            }
        }
        if(!pdIdSalesSummaryInfoMap.isEmpty()){
            FaiList<Param> addDataList = new FaiList<>(pdIdSalesSummaryInfoMap.size());
            for (Map.Entry<Integer, Param> pdIdSalesSummaryInfoEntry : pdIdSalesSummaryInfoMap.entrySet()) {
                Integer pdId = pdIdSalesSummaryInfoEntry.getKey();
                Param salesSummaryInfo = pdIdSalesSummaryInfoEntry.getValue();
                Param data = new Param();
                data.setInt(SpuSummaryEntity.Info.AID, aid);
                data.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
                data.setCalendar(SpuSummaryEntity.Info.SYS_UPDATE_TIME, now);
                data.setCalendar(SpuSummaryEntity.Info.SYS_CREATE_TIME, now);

                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.MAX_PRICE);
                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.MIN_PRICE);
                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.COUNT);
                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.REMAIN_COUNT);
                data.assign(salesSummaryInfo, SpuSummaryEntity.Info.HOLDING_COUNT);
                addDataList.add(data);
            }
            rt = m_daoCtrl.batchInsert(addDataList, null, true);
            if(rt != Errno.OK){
                Log.logErr(rt, "m_daoCtrl.batchInsert err;flow=%s;aid=%s;addDataList=%s", m_flow, aid, addDataList);
                return rt;
            }
            Log.logStd("doing;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int batchReport(int aid, Map<Integer, Param> pdIdInfoMap, boolean isSaga) {
        if(aid <= 0 || pdIdInfoMap == null || pdIdInfoMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdInfoMap=%s;", m_flow, aid, pdIdInfoMap);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        FaiList<Integer> pdIdList = new FaiList<>(pdIdInfoMap.keySet());
        int rt;
        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"select err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        if (isSaga) {
            if (!listRef.value.isEmpty()) {
                FaiList<Param> cloneList = listRef.value.clone();
                preAddUpdateSaga(aid, cloneList);
            }
        }

        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : listRef.value) {
            Integer pdId = oldInfo.getInt(SpuSummaryEntity.Info.PD_ID);
            Param newInfo = pdIdInfoMap.remove(pdId);

            ParamUpdater updater = new ParamUpdater(newInfo);
            updater.update(oldInfo, false);
            Param data = new Param();
            data.assign(oldInfo, SpuSummaryEntity.Info.MIN_PRICE);
            data.assign(oldInfo, SpuSummaryEntity.Info.MAX_PRICE);
            data.assign(oldInfo, SpuSummaryEntity.Info.COUNT);
            data.assign(oldInfo, SpuSummaryEntity.Info.HOLDING_COUNT);
            data.assign(oldInfo, SpuSummaryEntity.Info.REMAIN_COUNT);
            data.setCalendar(SpuSummaryEntity.Info.SYS_UPDATE_TIME, now);

            data.setInt(SpuSummaryEntity.Info.AID, aid);
            data.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
            batchUpdateDataList.add(data);
        }
        cacheManage.addDirtyCacheKey(pdIdList);
        if(!batchUpdateDataList.isEmpty()){
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData().setString(SpuSummaryEntity.Info.MIN_PRICE, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.MAX_PRICE, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.HOLDING_COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.REMAIN_COUNT, "?");
            batchUpdater.getData().setString(SpuSummaryEntity.Info.SYS_UPDATE_TIME, "?");

            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logErr(rt,"update err;flow=%s;aid=%s;batchUpdateDataList=%s", m_flow, aid, batchUpdateDataList);
                return rt;
            }
        }
        FaiList<Param> addInfoList = new FaiList<>();
        for (Map.Entry<Integer, Param> pdIdInfoEntry : pdIdInfoMap.entrySet()) {
            Integer pdId = pdIdInfoEntry.getKey();
            Param info = pdIdInfoEntry.getValue();
            info.setInt(SpuSummaryEntity.Info.AID, aid);
            info.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
            info.setCalendar(SpuSummaryEntity.Info.SYS_CREATE_TIME, now);
            info.setCalendar(SpuSummaryEntity.Info.SYS_UPDATE_TIME, now);
            addInfoList.add(info);
        }
        if(!addInfoList.isEmpty()){
            rt = m_daoCtrl.batchInsert(addInfoList, null, !isSaga);
            if(rt != Errno.OK){
                Log.logErr(rt,"batchInsert err;flow=%s;aid=%s;addInfoList=%s", m_flow, aid, addInfoList);
                return rt;
            }
            // 分布式事务 需要记录 Saga 记录
            if (isSaga) {
                rt = addInsOp4Saga(aid, addInfoList);
                if (rt != Errno.OK) {
                    return rt;
                }
            }
        }

        Log.logStd("ok;flow=%s;aid=%s;pdIdInfoMap=%s;", m_flow, aid, pdIdInfoMap);
        return rt;
    }

    public int report(int aid, int pdId, Param info) {
        if(aid <= 0 || pdId <= 0 || info == null || info.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;info=%s;", m_flow, aid, pdId, info);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"selectFirst err;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Param oldInfo = infoRef.value;
        cacheManage.addDirtyCacheKey(pdId);
        Calendar now = Calendar.getInstance();
        info.setCalendar(SpuSummaryEntity.Info.SYS_UPDATE_TIME, now);
        if(oldInfo.isEmpty()){
            info.setInt(SpuSummaryEntity.Info.AID, aid);
            info.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
            info.setCalendar(SpuSummaryEntity.Info.SYS_CREATE_TIME, now);
            rt = m_daoCtrl.insert(info);
            if(rt != Errno.OK){
                Log.logErr(rt,"insert err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return rt;
            }
        }else {
            ParamUpdater updater = new ParamUpdater(info);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logErr(rt,"update err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return rt;
            }
        }

        Log.logStd("ok!;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        cacheManage.addDirtyCacheKey(pdIdList);
        if (isSaga) {
            // 分布式事务，需要记录老的数据 录入 Saga 操作记录表中
            rt = addDelOp4Saga(aid, matcher);
            if(rt != Errno.OK){
                return rt;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }

        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int getListFromDao(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef, String ... fields) {
        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logDbg(rt, "select err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd(rt,"ok!;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int getList(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;listRef=%s;", m_flow, aid, pdIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> resultList = new FaiList<>();
        listRef.value = resultList;
        Set<Integer> pdIdSet = new HashSet<>(pdIdList);
        FaiList<Param> cacheList = SpuSummaryCacheCtrl.getCacheList(aid, pdIdSet);
        if(cacheList != null){
            for (Param info : cacheList) {
                if(pdIdSet.remove(info.getInt(SpuSummaryEntity.Info.PD_ID))){
                    resultList.add(info);
                }
            }
        }
        int rt = Errno.ERROR;
        if(pdIdSet.isEmpty()){
            return rt = Errno.OK;
        }
        Ref<FaiList<Param>> tmpListRef = new Ref<>();
        rt = getListFromDao(aid, new FaiList<>(pdIdSet), tmpListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> list = tmpListRef.value;
        SpuSummaryCacheCtrl.setCacheList(aid, list);
        resultList.addAll(list);
        return rt = Errno.OK;
    }

    /**
     * 获取补偿记录
     *
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param spuSummarySagaListRef 接收返回的list
     * @return {@link Errno}
     */
    public int getSagaList(String xid, Long branchId, Ref<FaiList<Param>> spuSummarySagaListRef) {
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
        rt = m_sagaDaoCtrl.select(searchArg, spuSummarySagaListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    /**
     * 设置缓存过期
     */
    public boolean setDirtyCacheEx(int aid){
        return cacheManage.setDirtyCacheEx(aid);
    }
    public void deleteDirtyCache(int aid) {
        cacheManage.deleteDirtyCache(aid);
    }

    private int m_flow;
    private SpuSummaryDaoCtrl m_daoCtrl;
    private SpuSummarySagaDaoCtrl m_sagaDaoCtrl;

    // 用于记录当前请求中需要操作到缓存key
    private CacheManage cacheManage = new CacheManage();

    private static class CacheManage{
        private Set<Integer> pdIdSet;
        public CacheManage() {
            init();
        }
        private void init() {
            pdIdSet = new HashSet<>();
        }

        public void addDirtyCacheKey(FaiList<Integer> pdIdList){
            if(pdIdList == null){
                return;
            }
            pdIdSet.addAll(pdIdList);
        }
        public void addDirtyCacheKey(int pdId){
            pdIdSet.add(pdId);
        }

        private void deleteDirtyCache(int aid){
            try {
                SpuSummaryCacheCtrl.delCacheList(aid, pdIdSet);
            }finally {
                init();
            }
        }

        public boolean setDirtyCacheEx(int aid) {
            return SpuSummaryCacheCtrl.setCacheDirty(aid);
        }
    }

    // 记录删除前的原数据
    private int addDelOp4Saga(int aid, ParamMatcher matcher) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);
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
        rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, true);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addDelOp4Saga err;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    // 记录添加的数据
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
            sagaOpInfo.assign(addData, SpuSummaryEntity.Info.AID);
            sagaOpInfo.assign(addData, SpuSummaryEntity.Info.PD_ID);
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
            int pdId = info.getInt(SpuSummaryEntity.Info.PD_ID);
            PrimaryKey primaryKey = new PrimaryKey(aid, pdId);
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            String[] keys = SpuSummaryEntity.getMaxUpdateAndPriKeys();
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
     * SpuSummaryProc 回滚
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
        if (listRef.value.isEmpty()) {
            Log.logStd("SpuSummaryProc sagaOpList is empty");
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
        String[] updateKeys = SpuSummaryEntity.getMaxUpdateAndPriKeys();
        FaiList<String> keys = new FaiList<>(Arrays.asList(updateKeys));
        // 移除主键
        keys.remove(SpuSummaryEntity.Info.AID);
        keys.remove(SpuSummaryEntity.Info.PD_ID);
        FaiList<Param> dataList = new FaiList<>();
        for (Param info : list) {
            int pdId = info.getInt(SpuSummaryEntity.Info.PD_ID);
            Param data = new Param();
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(SpuSummaryEntity.Info.AID, aid);
            data.setInt(SpuSummaryEntity.Info.PD_ID, pdId);
            dataList.add(data);
        }

        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, "?");

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
        FaiList<Integer> pdIdList = Utils.getValList(list, SpuSummaryEntity.Info.PD_ID);
        ParamMatcher matcher = new ParamMatcher(SpuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "delete err;flow=%d;aid=%d;pdIdList=%s", m_flow, aid, pdIdList);
        }
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    private HashMap<PrimaryKey, Param> sagaMap;

    private static class PrimaryKey {
        int aid;
        int pdId;

        public PrimaryKey(int aid, int pdId) {
            this.aid = aid;
            this.pdId = pdId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    pdId == that.pdId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, pdId);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", pdId=" + pdId +
                    '}';
        }
    }
}
