package fai.MgProductStoreSvr.domain.serviceProc;


import fai.MgProductStoreSvr.application.MgProductStoreSvr;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.PdKey;
import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.StoreSalesSkuCacheCtrl;
import fai.MgProductStoreSvr.domain.repository.StoreSalesSkuDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.StoreSalesSkuSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;


public class StoreSalesSkuProc {
    public StoreSalesSkuProc(StoreSalesSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public StoreSalesSkuProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        m_sagaDaoCtrl = StoreSalesSkuSagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null || m_sagaDaoCtrl == null){
            throw new RuntimeException(String.format("StoreSalesSkuDaoCtrl or StoreSalesSkuSagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int batchAdd(int aid, Integer argPdId, FaiList<Param> infoList) {
        return batchAdd(aid, argPdId, infoList, false);
    }

    /**
     * 添加销售库存sku
     *
     * @param isSaga 是否属于分布式事务
     * @return {@link Errno}
     */
    public int batchAdd(int aid, Integer argPdId, FaiList<Param> infoList, boolean isSaga) {
        if(aid <= 0 || (argPdId != null && argPdId <= 0) || infoList == null || infoList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;argPdId=%s;infoList=%s;", m_flow, aid, argPdId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        Set<Integer> addPdIdList = new HashSet<>();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(StoreSalesSkuEntity.Info.AID, aid);
            data.assign(info, StoreSalesSkuEntity.Info.UNION_PRI_ID);
            data.assign(info, StoreSalesSkuEntity.Info.RL_PD_ID);
            data.assign(info, StoreSalesSkuEntity.Info.SYS_TYPE);
            data.assign(info, StoreSalesSkuEntity.Info.SKU_ID);
            Integer pdId = info.getInt(StoreSalesSkuEntity.Info.PD_ID);
            pdId = pdId == null ? argPdId : pdId;
            if(pdId == null){
                Log.logStd("pdId arg error;flow=%d;aid=%s;pdId=%s;info=%s;", m_flow, aid, pdId, info);
                return Errno.ARGS_ERROR;
            }
            addPdIdList.add(pdId);
            data.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
            data.assign(info, StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID);
            data.assign(info, StoreSalesSkuEntity.Info.SKU_TYPE);
            data.assign(info, StoreSalesSkuEntity.Info.SORT);
            data.assign(info, StoreSalesSkuEntity.Info.COUNT);
            data.assign(info, StoreSalesSkuEntity.Info.REMAIN_COUNT);
            data.assign(info, StoreSalesSkuEntity.Info.HOLDING_COUNT);
            Long price = info.getLong(StoreSalesSkuEntity.Info.PRICE);
            int flag = info.getInt(StoreSalesSkuEntity.Info.FLAG, 0);
            if(price != null){
                flag |= StoreSalesSkuValObj.FLag.SETED_PRICE;
            }else{
                price = 0L;
            }
            data.setLong(StoreSalesSkuEntity.Info.PRICE, price);
            data.setInt(StoreSalesSkuEntity.Info.FLAG, flag);
            data.setLong(StoreSalesSkuEntity.Info.ORIGIN_PRICE, 0L); // 给默认值
            data.assign(info, StoreSalesSkuEntity.Info.ORIGIN_PRICE); // 有就覆盖
            data.assign(info, StoreSalesSkuEntity.Info.MIN_AMOUNT);
            data.assign(info, StoreSalesSkuEntity.Info.MAX_AMOUNT);
            data.assign(info, StoreSalesSkuEntity.Info.DURATION);
            data.assign(info, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
            data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
            data.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null, !isSaga);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchAdd dao.error;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }

        // 如果开启了分布式事务 需要向当前表的Saga表 插入Saga记录
        if (isSaga) {
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            FaiList<Param> sagaList = new FaiList<>();
            for (Param info : dataList) {
                Param sagaInfo = new Param();
                sagaInfo.assign(info, StoreSalesSkuEntity.Info.AID);
                sagaInfo.assign(info, StoreSalesSkuEntity.Info.UNION_PRI_ID);
                sagaInfo.assign(info, StoreSalesSkuEntity.Info.SKU_ID);
                // 记录 pdId 用于上报补偿
                sagaInfo.assign(info, StoreSalesSkuEntity.Info.PD_ID);
                sagaInfo.setString(StoreSagaEntity.Info.XID, xid);
                sagaInfo.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
                sagaInfo.setInt(StoreSagaEntity.Info.SAGA_OP, StoreSagaValObj.SagaOp.ADD);
                sagaList.add(sagaInfo);
            }
            rt = m_sagaDaoCtrl.batchInsert(sagaList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchAddSaga dao.error;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
        }
        Log.logStd("ok;flow=%d;aid=%d;addPdIdList=%s;", m_flow, aid, addPdIdList);
        return rt;
    }

    /**
     * batchAdd 的补偿方法
     */
    public int batchAddRollback(int aid, FaiList<Param> delList) {
        int rt = Errno.ERROR;
        if (Util.isEmptyList(delList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;delList is empty;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }
        for (Param delInfo : delList) {
            ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, delInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID));
            matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, delInfo.getInt(StoreSalesSkuEntity.Info.PD_ID));
            matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, delInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID));
            rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchAddRollback err;flow=%d;aid=%d;", m_flow, aid);
                return rt;
            }
        }
        Log.logStd("batchAddRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }
    public int cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId) {
        ParamMatcher delMatcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, toUnionPriId);

        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clear old list error;flow=%d;aid=%d;fuid=%s;tuid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }
        return copyBizBind(aid, fromUnionPriId, toUnionPriId, null);
    }

    public int copyBizBind(int aid, int fromUnionPriId, int toUnionPriId, FaiList<Integer> pdIds) {
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, fromUnionPriId);
        if(!Utils.isEmptyList(pdIds)) {
            matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "cloneBizBind error;flow=%d;aid=%d;fromUid=%s;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }

        FaiList<Param> list = listRef.value;

        Calendar now = Calendar.getInstance();
        for(Param info : list) {
            info.remove(StoreSalesSkuEntity.Info.COUNT);
            info.remove(StoreSalesSkuEntity.Info.REMAIN_COUNT);
            info.remove(StoreSalesSkuEntity.Info.HOLDING_COUNT);
            info.remove(StoreSalesSkuEntity.Info.MW_COST);
            info.remove(StoreSalesSkuEntity.Info.MW_TOTAL_COST);
            info.remove(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST);
            info.remove(StoreSalesSkuEntity.Info.VIRTUAL_COUNT);

            info.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, now);
            info.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);

            info.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, toUnionPriId);

        }
        rt = m_daoCtrl.batchInsert(list, null, true);
        if(rt != Errno.OK) {
            Log.logErr(rt, "cloneBizBind error;flow=%d;aid=%d;fromUid=%s;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }

        Log.logStd("copyBizBind ok;flow=%d;aid=%d;fromUid=%s;toUid=%s;pdIds=%s;", m_flow, aid, fromUnionPriId, toUnionPriId, pdIds);
        return rt;
    }

    public int batchSynchronousSPU2SKU(int aid, Map<Integer, Map<Long, Param>> unionPriId_skuId_salesStoreDataMapMap, Set<String> maxUpdateFieldSet) {
        if(aid <= 0 || unionPriId_skuId_salesStoreDataMapMap == null || unionPriId_skuId_salesStoreDataMapMap.isEmpty() || maxUpdateFieldSet == null || maxUpdateFieldSet.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId_skuId_salesStoreDataMapMap=%s;maxUpdateFieldSet=%s;", m_flow, aid, unionPriId_skuId_salesStoreDataMapMap, maxUpdateFieldSet);
            return Errno.ARGS_ERROR;
        }
        Set<String> maxGetFieldSet = new HashSet<>(maxUpdateFieldSet);
        maxGetFieldSet.add(StoreSalesSkuEntity.Info.SKU_ID); // 添加需要额外获取的字段

        int rt = Errno.ERROR;
        Calendar now = Calendar.getInstance();
        for (Map.Entry<Integer, Map<Long, Param>> unionPriId_skuId_salesStoreDataMapEntry : unionPriId_skuId_salesStoreDataMapMap.entrySet()) {
            int unionPriId = unionPriId_skuId_salesStoreDataMapEntry.getKey();
            Map<Long, Param> skuId_salesStoreDataMap = unionPriId_skuId_salesStoreDataMapEntry.getValue();
            FaiList<Long> skuIdList = new FaiList<>(skuId_salesStoreDataMap.keySet());
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef, maxGetFieldSet.toArray(new String[]{}));
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Param> oldInfoList = listRef.value;
            FaiList<Param> batchUpdateDataList = new FaiList<>(oldInfoList.size());
            for (Param oldInfo : oldInfoList) {
                Long skuId = oldInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                Param newInfo = skuId_salesStoreDataMap.remove(skuId); // 移除，剩余的就是要新增的
                Param data = new Param();
                for (String field : maxUpdateFieldSet) {
                    data.assign(oldInfo, field); // 先赋值旧数据
                    data.assign(newInfo, field); // 再覆盖
                }
                data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
                { // matcher
                    data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                    data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                    data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                }
                batchUpdateDataList.add(data);
            }
            if(!batchUpdateDataList.isEmpty()){
                ParamUpdater batchUpdater = new ParamUpdater();
                for (String field : maxUpdateFieldSet) {
                    batchUpdater.getData().setString(field, "?");
                }
                batchUpdater.getData().setString(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, "?");

                ParamMatcher batchMatcher = new ParamMatcher();
                batchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
                batchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                batchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
                rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
                if(rt != Errno.OK){
                    Log.logStd(rt, "dao.batchUpdate err;flow=%s;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
                    return rt;
                }
            }
            if(!skuId_salesStoreDataMap.isEmpty()){
                FaiList<Param> addDataList = new FaiList<>(skuId_salesStoreDataMap.size());
                for (Map.Entry<Long, Param> skuId_salesStoreDataEntry : skuId_salesStoreDataMap.entrySet()) {
                    long skuId = skuId_salesStoreDataEntry.getKey();
                    Param data = new Param();
                    data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                    data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                    data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                    data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
                    data.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, now);
                    Param info = skuId_salesStoreDataEntry.getValue();
                    data.assign(info, StoreSalesSkuEntity.Info.PD_ID);
                    data.assign(info, StoreSalesSkuEntity.Info.RL_PD_ID);
                    data.assign(info, StoreSalesSkuEntity.Info.SYS_TYPE);
                    for (String field : maxUpdateFieldSet) {
                        data.assign(info, field);
                    }
                    addDataList.add(data);
                }
                rt = m_daoCtrl.batchInsert(addDataList, null, true);
                if(rt != Errno.OK){
                    Log.logStd(rt, "dao.batchInsert err;flow=%s;aid=%s;addDataList=%s;", m_flow, aid, addDataList);
                    return rt;
                }
            }

            Log.logStd("doing;flow=%s;aid=%s;unionPruId=%s;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, int pdId, FaiList<Long> delSkuIdList, boolean isSaga) {
        if(aid <= 0 || pdId <= 0 || delSkuIdList == null || delSkuIdList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);

        if (isSaga) {
            // 分布式事务，需要先记录之前的数据 再删除
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK) {
                if (rt == Errno.NOT_FOUND) {
                    return Errno.OK;
                }
                Log.logErr(rt, "select error;flow=%d;aid=%d;pdId=%d;skuIdList=%s", m_flow, aid, pdId, delSkuIdList);
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
        // 删除业务表数据
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "dao.delete err;flow=%s;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return rt;
        }

        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if (isSaga) {
            // 分布式事务，需要先记录之前的数据 再删除
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
            Log.logStd(rt, "dao.delete err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
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
            Log.logErr(rt, "arg err;sagaList is empty;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }
        // 去除分布式事务相关字段
        Util.removeSagaColumn(sagaList);
        rt = m_daoCtrl.batchInsert(sagaList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchDelRollback error;flow=%d;aid=%d;sagaList=%s", m_flow, aid, sagaList);
            return rt;
        }
        Log.logStd("storeSalesSku batchDelRollback ok;flow=%s;aid=%s;", m_flow, aid);
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
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "dao.delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int batchSet(int aid, List<Integer> unionPriIdList, int pdId, FaiList<ParamUpdater> updaterList, boolean isSaga) {
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        // 需要更新的最多key集
        Set<String> maxUpdaterKeys = Utils.validUpdaterList(updaterList, Utils.asFaiList(StoreSalesSkuEntity.getValidKeys()), data->{
            skuIdList.add(data.getLong(StoreSalesSkuEntity.Info.SKU_ID));
        });
        if(maxUpdaterKeys.contains(StoreSalesSkuEntity.Info.PRICE)){
            maxUpdaterKeys.add(StoreSalesSkuEntity.Info.FLAG);
        }
        maxUpdaterKeys.add(StoreSalesSkuEntity.Info.UNION_PRI_ID);

        Ref<FaiList<Param>> listRef = new Ref<>();
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        // 查询老的更新时间
        maxUpdaterKeys.add(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME);
        // 查询老数据
        rt = m_daoCtrl.select(searchArg, listRef, maxUpdaterKeys.toArray(new String[]{}));
        if(rt != Errno.OK){
            Log.logErr(rt,"dao.select error;flow=%d;aid=%s;unionPriIdList=%s;skuIdList=%s;", m_flow, aid, unionPriIdList, skuIdList);
            return rt;
        }
        // 分布式事务，需要插入 Saga 记录
        if (isSaga) {
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            if (!Util.isEmptyList(listRef.value)) {
                FaiList<Param> sagaOldList = listRef.value.clone();
                // 设置 pdId , 方便上报补偿
                sagaOldList.forEach(oldInfo -> {
                    oldInfo.setInt(StoreSalesSkuEntity.Info.AID, aid);
                    oldInfo.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                    oldInfo.setString(StoreSagaEntity.Info.XID, xid);
                    oldInfo.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
                    oldInfo.setInt(StoreSagaEntity.Info.SAGA_OP, StoreSagaValObj.SagaOp.MODIFY);
                });
                // 添加补偿记录
                rt = m_sagaDaoCtrl.batchInsert(sagaOldList, null, false);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "insert saga error;flow=%d;aid=%s;sagaOldList=%s;", m_flow, aid, sagaOldList);
                    return rt;
                }
            }
        }
        Map<SkuBizKey, Param> oldDataMap = new HashMap<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            int unionPriId = info.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
            long skuId = info.getInt(StoreSalesSkuEntity.Info.SKU_ID);
            oldDataMap.put(new SkuBizKey(unionPriId, skuId), info);
        }

        // 移除主键
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.AID);
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.SKU_ID);
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.UNION_PRI_ID);

        // prepare updater
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });

        listRef.value = null; // help gc
        // prepare matcher
        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        // 组成批量更新的数据集
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        for (int uid : unionPriIdList) {
            updaterList.forEach(updater -> {
                long skuId = updater.getData().getLong(StoreSalesSkuEntity.Info.SKU_ID);
                Param oldData = oldDataMap.remove(new SkuBizKey(uid, skuId)); // help gc
                Param updatedData = updater.update(oldData, true);
                Param data = new Param();
                { // for prepare updater
                    maxUpdaterKeys.forEach(key->{
                        data.assign(updatedData, key);
                    });
                    if(data.containsKey(StoreSalesSkuEntity.Info.PRICE)){
                        int flag = data.getInt(StoreSalesSkuEntity.Info.FLAG, 0);
                        flag |= StoreSalesSkuValObj.FLag.SETED_PRICE;
                        data.setInt(StoreSalesSkuEntity.Info.FLAG, flag);
                    }
                    data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
                }
                { // for prepare matcher
                    data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                    data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, uid);
                    data.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                    data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                }
                dataList.add(data);
            });
        }

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchUpdate error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logDbg("flow=%d;aid=%d;pdId=%s;doBatchUpdater.json=%s;dataList=%s;",  m_flow, aid, pdId, doBatchUpdater, dataList);
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    /**
     * batchSet 的补偿方法
     */
    public int batchSetRollback(int aid, FaiList<Param> storeSalesSkuSagaList) {
        int rt;
        if (Util.isEmptyList(storeSalesSkuSagaList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;storeSalesSkuSagaList is empty;");
            return rt;
        }
        FaiList<ParamUpdater> updaterList = new FaiList<>();
        storeSalesSkuSagaList.clone().forEach(data -> updaterList.add(new ParamUpdater(data)));
        // 获取更新最大key
        Set<String> maxUpdaterKeys = Utils.validUpdaterList(updaterList, Utils.asFaiList(StoreSalesSkuEntity.getValidKeys()), null);
        if(maxUpdaterKeys.contains(StoreSalesSkuEntity.Info.PRICE)){
            maxUpdaterKeys.add(StoreSalesSkuEntity.Info.FLAG);
        }
        maxUpdaterKeys.add(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME);

        // 移除主键
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.AID);
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.SKU_ID);
        // 同时要移除 pdId
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.PD_ID);

        // prepare updater
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key -> doBatchUpdater.getData().setString(key, "?"));

        // prepare matcher
        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        // 组成批量更新的数据集，主要是为了对齐 doBatchUpdater
        FaiList<Param> dataList = new FaiList<>(storeSalesSkuSagaList.size());
        storeSalesSkuSagaList.forEach(oldData -> {
            Param data = new Param();
            { // for prepare updater
                maxUpdaterKeys.forEach(key -> data.assign(oldData, key));
            }
            { // for prepare matcher
                data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                data.assign(oldData, StoreSalesSkuEntity.Info.UNION_PRI_ID);
                data.assign(oldData, StoreSalesSkuEntity.Info.PD_ID);
                data.assign(oldData, StoreSalesSkuEntity.Info.SKU_ID);
            }
            dataList.add(data);
        });

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchSetRollback err;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }
        return rt;
    }

    public int batchSet(int aid, FaiList<ParamUpdater> updaterList, Map<SkuBizKey, Param> existMap) {
        if(aid <= 0 || existMap == null || existMap.isEmpty() || updaterList == null || updaterList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;existMap=%s;updaterList=%s;", m_flow, aid, existMap, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Set<String> maxUpdaterKeys = new HashSet<>(Arrays.asList(StoreSalesSkuEntity.getValidKeys()));
        // 移除主键
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.AID);
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.SKU_ID);
        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.UNION_PRI_ID);

        FaiList<Param> dataList = new FaiList<>();

        // 组成批量更新的数据集
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList) {
            Param info = updater.getData();
            int unionPriId = info.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
            long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
            Param oldData = existMap.remove(new SkuBizKey(unionPriId, skuId)); // help gc
            Param updatedData = updater.update(oldData, true);
            Param data = new Param();
            { // for prepare updater
                maxUpdaterKeys.forEach(key->{
                    data.assign(updatedData, key);
                });
                if(data.containsKey(StoreSalesSkuEntity.Info.PRICE)){
                    int flag = data.getInt(StoreSalesSkuEntity.Info.FLAG, 0);
                    flag |= StoreSalesSkuValObj.FLag.SETED_PRICE;
                    data.setInt(StoreSalesSkuEntity.Info.FLAG, flag);
                }
                data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
            }
            { // for prepare matcher
                data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
            }
            dataList.add(data);
        }

        // prepare updater
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, "?");
        // prepare matcher
        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchUpdate error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logDbg("flow=%d;aid=%d;doBatchUpdater.json=%s;dataList=%s;",  m_flow, aid, doBatchUpdater, dataList);
        Log.logStd("ok;flow=%d;aid=%d;updaterList=%s;", m_flow, aid, updaterList);
        return rt;
    }

    int random = Sys.random();
    /**
     * 扣减库存
     * @return
     */
    public int batchReduceStore(int aid, int unionPriId, TreeMap<Long, Integer> skuIdCountMap, boolean holdingMode, boolean reduceHoldingCount) {
        if(skuIdCountMap == null || skuIdCountMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdCountMap=%s;", m_flow, aid, unionPriId, skuIdCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Map<Long, Integer> alreadyChangeSkuIdCountMap = new HashMap<>();
        if(!reduceHoldingCount){
            // 扣除缓存库存
            rt = batchReduceStoreCache(aid, unionPriId, skuIdCountMap, alreadyChangeSkuIdCountMap);
            if(rt != Errno.OK){
                //扣除失败补偿缓存库存
                batchRollbackReduceStoreCache(aid, unionPriId, alreadyChangeSkuIdCountMap);
                return rt;
            }
        }

        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            int count = skuIdCountEntry.getValue();
            // 扣除db库存  之所以不做成批量操作，原因是批量操作获取不到更新的数量
            rt = reduceStore(aid, unionPriId, skuId, count, holdingMode, reduceHoldingCount);
            if(rt != Errno.OK){
                break;
            }
        }
        if(rt !=Errno.OK){
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }
    /**
     * 批量回滚 扣减的缓存
     */
    private void batchRollbackReduceStoreCache(int aid, int unionPriId, Map<Long, Integer> alreadyChangeSkuIdCountMap) {
        for (Map.Entry<Long, Integer> skuIdCountEntry : alreadyChangeSkuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            int count = skuIdCountEntry.getValue();
            int result = StoreSalesSkuCacheCtrl.makeupRemainCount(aid, unionPriId, skuId, count);
            if(result>=0){
                cacheManage.removeRemainCountDirtyCacheKey(aid, unionPriId, skuId);
            }
            Log.logDbg("whalelog xxx aid=%s;unionPriId=%s;skuId=%s;result=%s;", aid, unionPriId, skuId, result);
            if(result < 0 && result != StoreSalesSkuCacheCtrl.CacheErrno.NO_CACHE){
                boolean boo = StoreSalesSkuCacheCtrl.delRemainCount(aid, unionPriId, skuId);
                Log.logErr("makeupRemainCount err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;count=%s;boo=%s;result=%s;", m_flow, aid, unionPriId, skuId, count, boo, result);
            }
        }
    }

    /**
     * 批量 扣减缓存中的剩余库存数量
     */
    private int batchReduceStoreCache(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, Map<Long, Integer> alreadyChangeSkuIdCountMap) {
        int rt = Errno.OK;
        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            cacheManage.collectionRemainCountDirtyCacheKey(aid, unionPriId, skuId);
            int count = skuIdCountEntry.getValue();
            int result = StoreSalesSkuCacheCtrl.reduceRemainCount(aid, unionPriId, skuId, count, zeroCountExpireTime);
            Log.logDbg("whalelog aid=%s;unionPriId=%s;skuId=%s;result=%s;random=%s;", aid, unionPriId, skuId, result, random);
            if(result < 0){
                if(result == StoreSalesSkuCacheCtrl.CacheErrno.NO_CACHE){
                    rt = initRemainCountCache(aid, unionPriId, skuId);
                    if(rt != Errno.OK){
                        break;
                    }
                    result = StoreSalesSkuCacheCtrl.reduceRemainCount(aid, unionPriId, skuId, count, zeroCountExpireTime);
                    Log.logDbg("whalelog 111 aid=%s;unionPriId=%s;skuId=%s;result=%s;random=%s;", aid, unionPriId, skuId, result, random);
                }
                if(result == StoreSalesSkuCacheCtrl.CacheErrno.SHORTAGE){
                    cacheManage.removeRemainCountDirtyCacheKey(aid, unionPriId, skuId);
                    rt = MgProductErrno.Store.SHORTAGE;
                    break;
                }
            }
            alreadyChangeSkuIdCountMap.put(skuId, count);
        }
        return rt;
    }

    /**
     * 初始化剩余库存的缓存
     * 加锁
     */
    private int initRemainCountCache(int aid, int unionPriId, long skuId) {
        int rt = Errno.ERROR;
        LockUtil.lock(aid);
        try {
            if(StoreSalesSkuCacheCtrl.existsRemainCount(aid, unionPriId, skuId)){
                return Errno.OK;
            }
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher();
            searchArg.matcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
            Ref<Param> infoRef = new Ref<>();
            rt = m_daoCtrl.selectFirst(searchArg, infoRef, new String[]{StoreSalesSkuEntity.Info.REMAIN_COUNT});
            if(rt != Errno.OK){
                Log.logErr(rt, "selectFirst err;flow=%s;aid=%s;unionPriId=%s;skuId=%s", m_flow, aid, unionPriId, skuId);
                return rt;
            }
            Param info = infoRef.value;
            int remainCount = info.getInt(StoreSalesSkuEntity.Info.REMAIN_COUNT, 0);
            rt = StoreSalesSkuCacheCtrl.initRemainCount(aid, unionPriId, skuId, remainCount);
            if(rt != Errno.OK){
                Log.logErr(rt, "initRemainCount err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;remainCount=%s;", m_flow, aid, unionPriId, skuId, remainCount);
                return rt;
            }
            Log.logDbg("whalelog  init aid=%s;unionPriId=%s;skuId=%s;random=%s;", aid, unionPriId, skuId, random);
        }finally {
            LockUtil.unlock(aid);
        }
        return rt;
    }

    /**
     * 扣减库存
     * @return
     */
    public int reduceStore(int aid, int unionPriId, long skuId, int count, boolean holdingMode, boolean reduceHoldingCount) {
        if(aid <= 0 || unionPriId <= 0 || skuId <=0 || count <= 0){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Ref<Integer> refRowCount = new Ref<>(0);
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        ParamUpdater updater = new ParamUpdater();
        if(!reduceHoldingCount){ // 扣减 剩余库存
            // remainCount>=count;
            matcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, count);

            updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, count);
            if(holdingMode){ // 预扣模式
                updater.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamUpdater.INC, count);
            }
        }else{ // 扣减 预扣库存
            // holdingCount>=count;
            matcher.and(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamMatcher.GE, count);

            updater.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamUpdater.DEC, count);
        }
        rt = m_daoCtrl.update(updater, matcher, refRowCount);
        if(rt != Errno.OK){
            Log.logStd(rt,"dao update err;matcher=%s", matcher.toJson());
            return rt;
        }
        if(refRowCount.value <= 0){ // 库存不足
            Log.logStd("store shortage;matcher=%s", matcher.toJson());
            return MgProductErrno.Store.SHORTAGE;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;holdingMode=%s;reduceHoldingCount=%s;", m_flow, aid, unionPriId, skuId, count, holdingMode, reduceHoldingCount);
        return rt;
    }

    /**
     * 补偿库存
     */
    public int batchMakeUpStore(int aid, int unionPriId, TreeMap<Long, Integer> skuIdCountMap, boolean holdingMode) {
        if(skuIdCountMap == null || skuIdCountMap.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdCountMap=%s;", m_flow, aid, unionPriId, skuIdCountMap);
            return Errno.ARGS_ERROR;
        }
        Map<Long, Integer> alreadyChangeSkuIdCountMap = new HashMap<>();
        int rt = Errno.ERROR;
        // 批量补偿缓存库存
        batchMakeupStoreCache(aid, unionPriId, skuIdCountMap, alreadyChangeSkuIdCountMap);

        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            int count = skuIdCountEntry.getValue();
            // 补偿db库存
            rt = makeUpStore(aid, unionPriId, skuId, count, holdingMode);
            if(rt != Errno.OK){
                break;
            }
        }
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    /**
     * 批量补偿缓存中的剩余库存数量
     */
    private void batchMakeupStoreCache(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, Map<Long, Integer> alreadyChangeSkuIdCountMap) {
        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            cacheManage.collectionRemainCountDirtyCacheKey(aid, unionPriId, skuId);
            int count = skuIdCountEntry.getValue();
            int result = StoreSalesSkuCacheCtrl.makeupRemainCount(aid, unionPriId, skuId, count);
            if(result < 0 && result != StoreSalesSkuCacheCtrl.CacheErrno.NO_CACHE){
                boolean boo = StoreSalesSkuCacheCtrl.delRemainCount(aid, unionPriId, skuId);
                Log.logErr("makeupRemainCount err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;count=%s;boo=%s;result=%s;", m_flow, aid, unionPriId, skuId, count, boo, result);
            }
            if(result >= count){
                alreadyChangeSkuIdCountMap.put(skuId, count);
            }
        }
    }

    /**
     * 补偿库存
     */
    public int makeUpStore(int aid, int unionPriId, long skuId, int count, boolean holdingMode) {
        if(aid <= 0 || unionPriId <= 0 || skuId <=0 || count <= 0){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Ref<Integer> refRowCount = new Ref<>(0);
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        ParamUpdater updater = new ParamUpdater();
        updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.INC, count);
        if(holdingMode){
            matcher.and(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamMatcher.GE, count);
            updater.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamUpdater.DEC, count);
        }
        rt = m_daoCtrl.update(updater, matcher, refRowCount);
        if(rt != Errno.OK){
            Log.logStd(rt,"dao update err;matcher=%s", matcher.toJson());
            return rt;
        }
        if(refRowCount.value <= 0){ // 库存异常
            Log.logStd("store err;matcher=%s", matcher.toJson());
            return Errno.ERROR;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
        return rt;
    }

    /**
     * 检查是否存在sku, 没有则生成
     * @param aid
     * @param ownerUnionPriId
     * @param needCheckSkuStoreKeyPdKeyMap
     * @return
     */
    public int checkAndAdd(int aid, int ownerUnionPriId, Map<SkuBizKey, PdKey> needCheckSkuStoreKeyPdKeyMap) {
        if(needCheckSkuStoreKeyPdKeyMap == null || needCheckSkuStoreKeyPdKeyMap.isEmpty()){
            Log.logErr("arg error;flow=%s;aid=%s;ownerUnionPriId=%s;needCheckSkuStoreKeyPdKeyMap=%s;", m_flow, aid, ownerUnionPriId, needCheckSkuStoreKeyPdKeyMap);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, Set<Long>> uidSkuIdSetMap = new HashMap<>();
        for (SkuBizKey skuBizKey : needCheckSkuStoreKeyPdKeyMap.keySet()) {
            Set<Long> skuIdSet = uidSkuIdSetMap.get(skuBizKey.unionPriId);
            if(skuIdSet == null){
                skuIdSet = new HashSet<>();
                uidSkuIdSetMap.put(skuBizKey.unionPriId, skuIdSet);
            }
            skuIdSet.add(skuBizKey.skuId);
        }
        int rt = Errno.ERROR;
        Set<Long> needAddedSkuIdSet = new HashSet<>();
        for (Map.Entry<Integer, Set<Long>> uidSkuIdSetEntry : uidSkuIdSetMap.entrySet()) {
            int uid = uidSkuIdSetEntry.getKey();
            Set<Long> skuIdSet = uidSkuIdSetEntry.getValue();
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDaoBySkuIdList(aid, uid, new FaiList<>(skuIdSet), listRef, StoreSalesSkuEntity.Info.SKU_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            for (Param info : listRef.value) {
                skuIdSet.remove(info.getLong(StoreSalesSkuEntity.Info.SKU_ID));
            }
            needAddedSkuIdSet.addAll(skuIdSet);
        }
        if(needAddedSkuIdSet.isEmpty()){
            return Errno.OK;
        }
        Ref<FaiList<Param>> sourceListRef = new Ref<>();
        rt = getListFromDaoBySkuIdList(aid, ownerUnionPriId, new FaiList<>(needAddedSkuIdSet), sourceListRef,
                StoreSalesSkuEntity.Info.SKU_ID, StoreSalesSkuEntity.Info.PRICE, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "get sourceList err;flow=%s;aid=%s;ownerUnionPriId=%s;skuSet=%s;", m_flow, aid, ownerUnionPriId, needAddedSkuIdSet);
            return rt;
        }
        FaiList<Param> sourceList = sourceListRef.value;
        if(sourceList == null) {
            sourceList = new FaiList<>();
        }
        Map<Long, Param> map = sourceList.stream().collect(Collectors.toMap(info -> info.getLong(StoreSalesSkuEntity.Info.SKU_ID), info -> info));
        // 生关联的sku
        FaiList<Param> addInfoList = new FaiList<>(needAddedSkuIdSet.size());
        for (Map.Entry<Integer, Set<Long>> uidSkuIdSetEntry : uidSkuIdSetMap.entrySet()) {
            int uid = uidSkuIdSetEntry.getKey();
            Set<Long> skuIdSet = uidSkuIdSetEntry.getValue();
            for (Long skuId : skuIdSet) {
                Param sourceInfo = map.get(skuId);
                int flag = StoreSalesSkuValObj.FLag.SETED_PRICE;
                if(sourceInfo == null) {
                    sourceInfo = new Param();
                    flag = 0;
                }
                long price = sourceInfo.getLong(StoreSalesSkuEntity.Info.PRICE, 0L);
                long originPrice = sourceInfo.getLong(StoreSalesSkuEntity.Info.ORIGIN_PRICE, 0L);
                PdKey pdKey = needCheckSkuStoreKeyPdKeyMap.get(new SkuBizKey(uid, skuId));
                addInfoList.add(
                        new Param()
                                .setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, uid)
                                .setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId)
                                .setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId)
                                .setInt(StoreSalesSkuEntity.Info.PD_ID, pdKey.pdId)
                                .setInt(StoreSalesSkuEntity.Info.RL_PD_ID, pdKey.rlPdId)
                                .setInt(StoreSalesSkuEntity.Info.SYS_TYPE, pdKey.sysType)
                                .setLong(StoreSalesSkuEntity.Info.PRICE, price)
                                .setLong(StoreSalesSkuEntity.Info.ORIGIN_PRICE, originPrice)
                                .setInt(StoreSalesSkuEntity.Info.FLAG, flag)
                );
            }
        }
        rt = batchAdd(aid, null, addInfoList);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logDbg("flow=%s;aid=%s;ownerUnionPriId=%s;addInfoList=%s;", m_flow, aid, ownerUnionPriId, addInfoList);
        Log.logStd("ok!;flow=%s;aid=%s;ownerUnionPriId=%s;", m_flow, aid, ownerUnionPriId);
        return rt;
    }

    /**
     * 只会改到count 和 remainCount
     */
    public int batchChangeStore(int aid, TreeMap<SkuBizKey, Pair<Integer, Integer>>  skuBizChangeCountMap) {
        if(aid <= 0 || skuBizChangeCountMap == null || skuBizChangeCountMap.isEmpty()){
            Log.logErr("arg error;flow=%s;aid=%s;skuBizChangeCountMap=%s;", m_flow, aid, skuBizChangeCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.OK;
        Calendar now = Calendar.getInstance();
        for (Map.Entry<SkuBizKey, Pair<Integer, Integer>> skuBizKeyChangeCountEntry : skuBizChangeCountMap.entrySet()) {
            /*
              changeCount 和 changeRemainCount 取相反数，便于更新
               ... set `remainCount` = `remainCount` - changeCount, `changeCount` = `changeCount` - changeCount  where ... and `remainCount` >= changeCount and `changeCount` >= changeCount
               乐观锁 不考虑aba问题
             */
            Pair<Integer, Integer> changePair = skuBizKeyChangeCountEntry.getValue();
            Integer changeRemainCount = -changePair.first; // 取相反数
            if(changeRemainCount == 0){
                continue;
            }
            Integer changeCount = -changePair.second; // 取相反数


            ParamUpdater updater = new ParamUpdater();
            if(changeCount != 0){
                updater.add(StoreSalesSkuEntity.Info.COUNT, ParamUpdater.DEC, changeCount);
            }
            updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, changeRemainCount);
            updater.getData().setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);

            SkuBizKey skuBizKey = skuBizKeyChangeCountEntry.getKey();
            cacheManage.collectionRemainCountDirtyCacheKey(aid, skuBizKey.unionPriId, skuBizKey.skuId);
            ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, skuBizKey.unionPriId);
            matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuBizKey.skuId);
            if(changeCount != 0){
                matcher.and(StoreSalesSkuEntity.Info.COUNT, ParamMatcher.GE, changeCount);
            }
            matcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, changeRemainCount);

            Ref<Integer> refRowCount = new Ref<>(0);
            rt = m_daoCtrl.update(updater, matcher, refRowCount);
            if(rt != Errno.OK){
                Log.logStd(rt,"dao update err;matcher=%s", matcher.toJson());
                return rt;
            }
            if(refRowCount.value <= 0){ // 库存异常
                Log.logStd("store err;matcher=%s", matcher.toJson());
                return MgProductErrno.Store.SHORTAGE;
            }
        }
        /* 批量更新不能根据影响的行数来判断更新是否成功。
        FaiList<Param> dataList = new FaiList<>(skuBizChangeCountMap.size());
        for (Map.Entry<SkuStoreKey, Integer> skuStoreKeyCountEntry : skuBizChangeCountMap.entrySet()) {
            // count 取相反数，用于批量更新  where ... and remainCount >= count  乐观锁
            Integer count = -skuStoreKeyCountEntry.getValue();
            Param data = new Param();
            { // updater
                data.setInt(StoreSalesSkuEntity.Info.REMAIN_COUNT, count);
                data.setInt(StoreSalesSkuEntity.Info.COUNT, count);
            }
            SkuStoreKey skuStoreKey = skuStoreKeyCountEntry.getKey();
            { // matcher
                data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, skuStoreKey.unionPriId);
                data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuStoreKey.skuId);
                // 与updater字段冲突，set的时候加个前缀（啥前缀都可）。for matcher
                data.setInt("matcher_"+StoreSalesSkuEntity.Info.REMAIN_COUNT, count);
                data.setInt("matcher_"+StoreSalesSkuEntity.Info.COUNT, count);
            }
            dataList.add(data);
        }
        ParamUpdater doBatchUpdater = new ParamUpdater();
        doBatchUpdater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, "?");
        doBatchUpdater.add(StoreSalesSkuEntity.Info.COUNT, ParamUpdater.DEC, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.COUNT, ParamMatcher.GE, "?");
        int rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK){
            Log.logStd(rt, "batchChangeStore;flow=%s;doBatchMatcher=%s", m_flow, doBatchMatcher.toJson());
            return rt;
        }*/
        Log.logStd("ok!flow=%s;aid=%s;skuBizChangeCountMap=%s;", m_flow, aid, skuBizChangeCountMap);
        return rt;
    }

    /**
     * 只会改到 总成相关的字段
     */
    public int batchUpdateTotalCost(int aid, Map<SkuBizKey, Param> changeCountAfterSkuBizCountAndTotalCostMap) {
        int rt;
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>();
        for (Map.Entry<SkuBizKey, Param> skuBizKeyInfoEntry : changeCountAfterSkuBizCountAndTotalCostMap.entrySet()) {
            SkuBizKey skuBizKey = skuBizKeyInfoEntry.getKey();
            int unionPriId = skuBizKey.unionPriId;
            long skuId = skuBizKey.skuId;
            Param totalCostInfo = skuBizKeyInfoEntry.getValue();
            long fifoTotalCost = totalCostInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST);
            long mwTotalCost = totalCostInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST);
            long mwCost = totalCostInfo.getLong(StoreSalesSkuEntity.Info.MW_COST);
            Param data = new Param();
            // updater field
            data.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost);
            data.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost);
            data.setLong(StoreSalesSkuEntity.Info.MW_COST, mwCost);
            data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
            // matcher field
            data.setInt(StoreSalesSkuEntity.Info.AID, aid);
            data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
            dataList.add(data);
        }
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setString(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, "?");
        updater.getData().setString(StoreSalesSkuEntity.Info.MW_TOTAL_COST, "?");
        updater.getData().setString(StoreSalesSkuEntity.Info.MW_COST, "?");
        updater.getData().setString(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher matcher = new ParamMatcher();
        matcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
        ParamComparator comparator = new ParamComparator();
        comparator.addKey(StoreSalesSkuEntity.Info.UNION_PRI_ID);
        comparator.addKey(StoreSalesSkuEntity.Info.SKU_ID);
        // 排序下，避免乱序导致mysql死锁
        Collections.sort(dataList, comparator);
        rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK){
            Log.logErr(rt, "dao.batchUpdate error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int getListFromDao(int aid, FaiList<Integer> uidList, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(aid <= 0 || Util.isEmptyList(skuIdList) || Util.isEmptyList(uidList) || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;uidList=%s;uidList=%s;listRef=%s;fields=%s", m_flow, aid, uidList, uidList, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher();
        searchArg.matcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, uidList);
        searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuIdList=%s;uidList=%s;", m_flow, aid, skuIdList, uidList);
            return rt;
        }
        Log.logStd(rt,"ok;flow=%d;aid=%d;skuIdList=%s;uidList=%s;", m_flow, aid, skuIdList, uidList);
        return rt;
    }

    public int getListFromDao(int aid, int unionPriId, int pdId, Ref<FaiList<Param>> listRef, String ... fields){
        return getListFromDaoByPdIdListAndUidList(aid, new FaiList<>(Arrays.asList(pdId)), new FaiList<>(Arrays.asList(unionPriId)), listRef, fields);
    }

    public int getListFromDaoByPdIdListAndUidList(int aid, FaiList<Integer> pdIdList, FaiList<Integer> uidList, Ref<FaiList<Param>> listRef, String ... fields){
        if(aid <= 0 || Util.isEmptyList(pdIdList) || (uidList != null && uidList.isEmpty()) || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;uidList=%s;listRef=%s;fields=%s", m_flow, aid, pdIdList, uidList, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if(uidList != null){
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, uidList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdIdList=%s;uidList=%s;", m_flow, aid, pdIdList, uidList);
            return rt;
        }

        Log.logStd(rt,"ok;flow=%d;aid=%d;pdIdList=%s;uidList=%s;", m_flow, aid, pdIdList, uidList);
        return rt;
    }

    public int getListFromDaoBySkuIdList(int aid, int unionPriId, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdList=%s;listRef=%s;fields=%s", m_flow, aid, unionPriId, skuIdList, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        return getListFromDao(aid, new FaiList<>(Arrays.asList(unionPriId)), skuIdList, listRef, fields);
    }

    /**
     * 获取库存销售Sku表的补偿信息
     *
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param storeSalesSagaListRef 接收返回的list
     * @return {@link Errno}
     */
    public int getSagaList(String xid, Long branchId, Ref<FaiList<Param>> storeSalesSagaListRef) {
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
        rt = m_sagaDaoCtrl.select(searchArg, storeSalesSagaListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    public int getInfoMap4OutRecordFromDao(int aid, Set<SkuBizKey> skuBizKeySet, Map<SkuBizKey, Param> skuCountAndTotalCostMap) {
        if(skuBizKeySet == null || skuBizKeySet.isEmpty() || skuCountAndTotalCostMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;skuStoreKeySet=%s;skuCountAndTotalCostMap=%s;", m_flow, aid, skuBizKeySet, skuCountAndTotalCostMap);
            return Errno.ARGS_ERROR;
        }
        // unionPriId - skuList
        Map<Integer, FaiList<Long>> unionPriIdSkuIdListMap = new HashMap<>();
        for (SkuBizKey skuBizKey : skuBizKeySet) {
            int unionPriId = skuBizKey.unionPriId;
            FaiList<Long> skuIdList = unionPriIdSkuIdListMap.get(unionPriId);
            if(skuIdList == null){
                skuIdList = new FaiList<>();
                unionPriIdSkuIdListMap.put(unionPriId, skuIdList);
            }
            skuIdList.add(skuBizKey.skuId);
        }
        int rt = Errno.ERROR;
        for (Map.Entry<Integer, FaiList<Long>> unionPriIdSkuIdListEntry : unionPriIdSkuIdListMap.entrySet()) {
            Integer unionPriId = unionPriIdSkuIdListEntry.getKey();
            FaiList<Long> skuIdList = unionPriIdSkuIdListEntry.getValue();
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef,
                    StoreSalesSkuEntity.Info.SKU_ID,
                    StoreSalesSkuEntity.Info.RL_PD_ID,
                    StoreSalesSkuEntity.Info.SYS_TYPE,
                    StoreSalesSkuEntity.Info.PD_ID,
                    StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID,
                    StoreSalesSkuEntity.Info.REMAIN_COUNT,
                    StoreSalesSkuEntity.Info.HOLDING_COUNT,
                    StoreSalesSkuEntity.Info.FIFO_TOTAL_COST,
                    StoreSalesSkuEntity.Info.MW_TOTAL_COST,
                    StoreSalesSkuEntity.Info.MW_COST);
            if(rt != Errno.OK){
                return rt;
            }
            for (Param info : listRef.value) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                skuCountAndTotalCostMap.put(new SkuBizKey(unionPriId, skuId), info);
            }
        }

        return Errno.OK;
    }

    /**
     * 查询skuBiz汇总数据
     */
    public int searchSkuBizSummaryFromDao(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || searchArg == null || searchArg.matcher == null || searchArg.matcher.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;searchArg=%s;listRef=%s;", m_flow, aid, searchArg, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.getSkuBizSummaryFields();
        selectArg.searchArg = searchArg;
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;searchArg.matcher.toJson=%s;", m_flow, aid, searchArg.matcher.toJson());
            return rt;
        }
        Log.logDbg(rt,"ok;flow=%d;aid=%d;searchArg.matcher.toJson=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }


    //========================== 用于汇总 ↓↓↓ ===================================//
    public int getReportListBySkuIdList(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;skuIdList=%s;listRef=%s;", m_flow, aid, skuIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.PD_ID + ", " +
                StoreSalesSkuEntity.Info.SKU_ID + ", " +
                COMM_REPORT_FIELDS;
        selectArg.group = StoreSalesSkuEntity.Info.SKU_ID;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        initReportInfoList(listRef.value);
        Log.logDbg(rt,"ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }
    public int getReportList4synSPU2SKU(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;skuIdList=%s;listRef=%s;", m_flow, aid, skuIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.PD_ID + ", " +
                StoreSalesSkuEntity.Info.RL_PD_ID + ", " +
                StoreSalesSkuEntity.Info.SYS_TYPE + ", " +
                StoreSalesSkuEntity.Info.SKU_ID + ", " +
                StoreSalesSkuEntity.Info.UNION_PRI_ID + ", " +
                COMM_REPORT_FIELDS;
        selectArg.group = StoreSalesSkuEntity.Info.UNION_PRI_ID + "," + StoreSalesSkuEntity.Info.SKU_ID;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        initReportInfoList(listRef.value);
        Log.logDbg(rt,"ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }
    public int getReportListByPdIdList(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;listRef=%s;", m_flow, aid, pdIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.UNION_PRI_ID + ", "
                + StoreSalesSkuEntity.Info.PD_ID + ", "
                + StoreSalesSkuEntity.Info.RL_PD_ID + ", "
                + StoreSalesSkuEntity.Info.SYS_TYPE + ", "
                +COMM_REPORT_FIELDS;
        selectArg.group = StoreSalesSkuEntity.Info.PD_ID+ "," + StoreSalesSkuEntity.Info.UNION_PRI_ID;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        initReportInfoList(listRef.value);
        Log.logDbg(rt,"ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }
    public int getReportInfo(int aid, int pdId, int unionPriId, Ref<Param> infoRef){
        if(aid <= 0 || pdId <=0 || unionPriId <= 0|| infoRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;unionPriId=%s;listRef=%s;", m_flow, aid, pdId, unionPriId, infoRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.RL_PD_ID + ", " + StoreSalesSkuEntity.Info.SYS_TYPE + ", " +
                COMM_REPORT_FIELDS;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
        Log.logDbg(rt,"ok;flow=%d;aid=%d;pdId=%s;unionPriId=%s;", m_flow, aid, pdId, unionPriId);
        return rt;
    }
    public int getReportInfo(int aid, long skuId, Ref<Param> infoRef) {
        int rt = Errno.ERROR;
        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = COMM_REPORT_FIELDS;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
            return rt;
        }
        if(listRef.value.isEmpty()){
            infoRef.value = new Param();
        }else{
            infoRef.value = listRef.value.get(0);
        }
        initReportInfo(infoRef.value);
        Log.logDbg(rt,"ok;flow=%d;aid=%d;skuId=%s;", m_flow, aid, skuId);
        return rt;
    }

    public int getAllSkuIdAndPdId(int aid, int unionPriId, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;uid=%s;listRef=%s;", m_flow, aid, unionPriId, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = "distinct " + StoreSalesSkuEntity.Info.SKU_ID + ", " + StoreSalesSkuEntity.Info.PD_ID;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;uid=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logDbg(rt,"ok;flow=%d;aid=%d;uid=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    private void initReportInfoList(FaiList<Param> list){
        for (Param info : list) {
            initReportInfo(info);
        }
    }
    private void initReportInfo(Param info){
        if(info.getLong(StoreSalesSkuEntity.ReportInfo.MIN_PRICE, Long.MAX_VALUE) == Long.MAX_VALUE){
            info.setLong(StoreSalesSkuEntity.ReportInfo.MIN_PRICE, 0L);
        }
    }
    /**
     * min(if(flag&0x1=0x1, price, 0x7fffffffffffffffL)) 设置过价格的才参与计算最小值。
     */
    private static final String COMM_REPORT_FIELDS = StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID
            + ", sum(" + StoreSalesSkuEntity.Info.COUNT + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_COUNT
            + ", sum(" + StoreSalesSkuEntity.Info.REMAIN_COUNT + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT
            + ", sum(" + StoreSalesSkuEntity.Info.HOLDING_COUNT + ") as "+StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT
            + ", min( if(" + StoreSalesSkuEntity.Info.FLAG+"&" +StoreSalesSkuValObj.FLag.SETED_PRICE +"=" +StoreSalesSkuValObj.FLag.SETED_PRICE+"," + StoreSalesSkuEntity.Info.PRICE +"," + Long.MAX_VALUE + ") ) as "+StoreSalesSkuEntity.ReportInfo.MIN_PRICE
            + ", max(" + StoreSalesSkuEntity.Info.PRICE + ") as "+StoreSalesSkuEntity.ReportInfo.MAX_PRICE
            + ", sum(" + StoreSalesSkuEntity.Info.FIFO_TOTAL_COST + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_FIFO_TOTAL_COST
            + ", sum(" + StoreSalesSkuEntity.Info.MW_TOTAL_COST + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_MW_TOTAL_COST
            + ", bit_or(" + StoreSalesSkuEntity.Info.FLAG + ") as " + StoreSalesSkuEntity.ReportInfo.BIT_OR_FLAG
            ;
    //========================== 用于汇总 ↑↑↑ ===================================//

    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }

    public boolean deleteRemainCountDirtyCache(int aid) {
        return cacheManage.deleteRemainCountDirtyCache(aid);
    }

    private int zeroCountExpireTime = MgProductStoreSvr.SVR_OPTION.getZeroCountExpireTime(); // 扣减后库存为0时缓存的过期时间

    private int m_flow;
    private StoreSalesSkuDaoCtrl m_daoCtrl;
    private StoreSalesSkuSagaDaoCtrl m_sagaDaoCtrl;

    //用于记录当前请求中需要操作到缓存key
    private CacheManage cacheManage = new CacheManage();
    private static class CacheManage{

        public CacheManage() {
            init();
        }

        private Set<SkuBizKey> remainCountDirtyCacheKeySet;

        private void collectionRemainCountDirtyCacheKey(int aid, int unionPriId, long skuId){
            remainCountDirtyCacheKeySet.add(new SkuBizKey(unionPriId, skuId));
        }
        public void removeRemainCountDirtyCacheKey(int aid, int unionPriId, long skuId) {
            remainCountDirtyCacheKeySet.remove(new SkuBizKey(unionPriId, skuId));
        }
        private boolean deleteRemainCountDirtyCache(int aid){
            try {
                return StoreSalesSkuCacheCtrl.delRemainCount(aid, remainCountDirtyCacheKeySet);
            }finally {
                initRemainCountDirtyCacheKeySet();
            }
        }
        private boolean deleteDirtyCache(int aid){
            boolean boo = deleteRemainCountDirtyCache(aid);
            return boo;
        }

        private void init() {
            initRemainCountDirtyCacheKeySet();
        }
        private void initRemainCountDirtyCacheKeySet() {
            remainCountDirtyCacheKeySet = new HashSet<>();
        }
    }
}
