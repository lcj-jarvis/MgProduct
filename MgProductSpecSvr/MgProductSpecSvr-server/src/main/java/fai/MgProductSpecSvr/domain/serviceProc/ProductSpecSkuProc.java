package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.*;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class ProductSpecSkuProc {
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, ProductSpecSkuSagaDaoCtrl sagaDaoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_sagaDaoCtrl = sagaDaoCtrl;
        m_flow = flow;
    }
    public ProductSpecSkuProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
        m_sagaDaoCtrl = ProductSpecSkuSagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        if (m_sagaDaoCtrl == null) {
            throw new RuntimeException(String.format("ProductSpecSkuDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList) {
        return batchAdd(aid, Collections.singletonMap(pdId, infoList), null);
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, boolean isSaga) {
        return batchAdd(aid, Collections.singletonMap(pdId, infoList), null, isSaga);
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap) {
        return batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap, null);
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap, boolean isSaga) {
        return batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap, null, isSaga);
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap,
                        Ref<FaiList<Long>> skuIdListRef) {
        return batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap, skuIdListRef, false);
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap,
                        Ref<FaiList<Long>> skuIdListRef, boolean isSaga) {
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0){
            Log.logErr("error;flow=%d;aid=%s;", m_flow, aid);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>();
        Calendar now = Calendar.getInstance();
        FaiList<Long> skuIdList = new FaiList();
        Long skuId = m_daoCtrl.getId();
        if(skuId == null){
            Log.logErr("skuId err;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
            return Errno.ERROR;
        }
        for (Map.Entry<Integer, FaiList<Param>> pdIdInfoListEntry : pdIdPdScSkuListMap.entrySet()) {
            Integer pdId = pdIdInfoListEntry.getKey();
            List<Param> infoList = pdIdInfoListEntry.getValue();
            if(pdId == null || pdId <= 0 || Util.isEmptyList(infoList)){
                Log.logErr(rt, "arg error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
                return rt;
            }
            Map<String, Long> inPdScStrIdListJsonSkuIdMap = null;
            if(pdIdInPdScStrIdListJsonSkuIdMap != null){
               inPdScStrIdListJsonSkuIdMap = pdIdInPdScStrIdListJsonSkuIdMap.get(pdId);
               if(inPdScStrIdListJsonSkuIdMap == null){
                   inPdScStrIdListJsonSkuIdMap = new HashMap<>();
               }
                pdIdInPdScStrIdListJsonSkuIdMap.put(pdId, inPdScStrIdListJsonSkuIdMap);
            }

            for (Param info : infoList) {
                Param data = new Param();
                data.setInt(ProductSpecSkuEntity.Info.AID, aid);
                data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                skuId++;
                skuIdList.add(skuId);
                data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
                data.assign(info, ProductSpecSkuEntity.Info.SORT);
                data.assign(info, ProductSpecSkuEntity.Info.SOURCE_TID);
                data.assign(info, ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign(info, ProductSpecSkuEntity.Info.SKU_CODE); // TODO
                int flag = info.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                String inPdScStrIdListJson = null;
                if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){
                    inPdScStrIdListJson = ""; // spu 的 ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST 为 ""
                }else{
                    FaiList<Integer> inPdScStrIdList = info.getListNullIsEmpty(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                    inPdScStrIdListJson = inPdScStrIdList.toJson();
                    if(inPdScStrIdList.isEmpty()){
                        flag |= ProductSpecSkuValObj.FLag.ALLOW_EMPTY;
                    }
                }
                data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdListJson);
                data.setInt(ProductSpecSkuEntity.Info.FLAG, flag);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
                dataList.add(data);
                if(inPdScStrIdListJsonSkuIdMap != null){
                    inPdScStrIdListJsonSkuIdMap.put(inPdScStrIdListJson, skuId);
                }
            }

        }
        cacheManage.addNeedDelCachedPdIdSet(aid, pdIdPdScSkuListMap.keySet());
        cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
        if(skuIdListRef != null){
            skuIdListRef.value = skuIdList;
        }
        if(m_daoCtrl.updateId(skuId) == null){
            rt = Errno.ERROR;
            Log.logErr(rt, "dao.updateId error;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
            return rt;
        }
        rt = m_daoCtrl.batchInsert(dataList, null, false);
        if (isSaga) {
            // 记录 Saga 操作
            FaiList<Param> sagaOpList = new FaiList<>();
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            dataList.forEach(data -> {
                // 记录主键 + Saga 字段
                Param sagaOpInfo = new Param();
                sagaOpInfo.assign(data, ProductSpecSkuEntity.Info.AID);
                sagaOpInfo.assign(data, ProductSpecSkuEntity.Info.SKU_ID);
                sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaOpList.add(sagaOpInfo);
            });
            if (sagaOpList.isEmpty()) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;sagaOpList is empty;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
            rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
            if (rt != Errno.OK) {
                Log.logErr(rt, "productSpecSkuProc sagaOpList batch insert error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
                return rt;
            }
        }
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchInsert error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIdPdScSkuListMap.keySet());
        return rt;
    }

    /**
     * batchAdd 的补偿方法
     *
     * @param aid aid
     * @param sagaOpList Saga 操作记录
     * @return {@link Errno}
     */
    public int batchAddRollback(int aid, FaiList<Param> sagaOpList) {
        int rt;
        if (Util.isEmptyList(sagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;sagaOpList is empty");
            return rt;
        }
        // 获取 skuIdList
        FaiList<Long> skuIdList = new FaiList<>();
        sagaOpList.forEach(sagaOpInfo -> skuIdList.add(sagaOpInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID)));
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchAddRollback dao.delete error;flow=%d;aid=%d;skuIdList=%s", m_flow, aid, skuIdList);
            return rt;
        }
        Log.logStd("batchAddRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    /**
     * 批量同步，没有就添加，有就跳过
     * @param aid
     * @param pdId_pdScSkuInfoMap
     * @param pdIdSkuIdMap
     * @return
     */
    public int batchSynchronousSPU2SKU(int aid, Map<Integer, Param> pdId_pdScSkuInfoMap, Map<Integer, Long> pdIdSkuIdMap) {
        if(aid <= 0 || pdId_pdScSkuInfoMap == null || pdIdSkuIdMap == null){
            Log.logErr(" arg error;flow=%d;aid=%s;pdId_pdScSkuInfoMap=%s;pdIdSkuIdMap=%s;", m_flow, aid, pdId_pdScSkuInfoMap, pdIdSkuIdMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Integer> pdIdList = new FaiList<>(pdId_pdScSkuInfoMap.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND_NE, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao select error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Calendar now = Calendar.getInstance();
        FaiList<Param> oldInfoList = listRef.value;
        for (Param oldInfo : oldInfoList) {
            int pdId = oldInfo.getInt(ProductSpecSkuEntity.Info.PD_ID);
            long skuId = oldInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            Param info = pdId_pdScSkuInfoMap.remove(pdId);
            if(info == null){
                rt = Errno.ERROR;
                Log.logErr(rt,"data err;aid=%s;pdId=%s;skuId=%s;", aid, pdId, skuId);
                return rt;
            }
            pdIdSkuIdMap.put(pdId, skuId);
        }
        if(!pdId_pdScSkuInfoMap.isEmpty()){
            FaiList<Param> batchAddDataList = new FaiList<>();
            for (Map.Entry<Integer, Param> pdId_pdScSkuInfoEntry : pdId_pdScSkuInfoMap.entrySet()) {
                int pdId = pdId_pdScSkuInfoEntry.getKey();
                Param pdScSkuInfo = pdId_pdScSkuInfoEntry.getValue();
                Long skuId = m_daoCtrl.buildId();
                if(skuId == null){
                    rt = Errno.ERROR;
                    Log.logStd(rt,"arg error 2;flow=%d;aid=%s;skuId=%s;pdScSkuInfo=%s;", m_flow, aid, skuId, pdScSkuInfo);
                    return rt;
                }
                pdIdSkuIdMap.put(pdId, skuId);
                Param data = new Param();
                data.setInt(ProductSpecSkuEntity.Info.AID, aid);
                data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SORT);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SOURCE_TID);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SKU_CODE);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.FLAG);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);

                batchAddDataList.add(data);
            }
            rt = m_daoCtrl.batchInsert(batchAddDataList, null);
            if(rt != Errno.OK) {
                Log.logErr(rt, "dao batchInsert error;flow=%d;aid=%s;batchAddDataList=%s;", m_flow, aid, batchAddDataList);
                return rt;
            }
        }

        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int batchGenSkuRepresentSpuInfo(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, Map<Integer/*pdId*/, Long/*skuId*/> pdIdSkuIdMap){
        return batchGenSkuRepresentSpuInfo(aid, tid, unionPriId, pdIdList, pdIdSkuIdMap, false);
    }
    /**
     * 批量生成代表spu的sku
     * @param aid
     * @param tid
     * @param unionPriId
     * @return
     */
    public int batchGenSkuRepresentSpuInfo(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, Map<Integer/*pdId*/, Long/*skuId*/> pdIdSkuIdMap, boolean isSaga){
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao.select err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Set<Integer> needAddPdIdSet = new HashSet<>(pdIdList);
        for (Param info : listRef.value) { // 已经存在的不需要添加
            int pdId = info.getInt(ProductSpecSkuEntity.Info.PD_ID);
            long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            if(pdIdSkuIdMap != null){
                pdIdSkuIdMap.put(pdId, skuId);
            }
            needAddPdIdSet.remove(pdId);
        }
        if(!needAddPdIdSet.isEmpty()){
            Calendar now = Calendar.getInstance();
            FaiList<Param> addList = new FaiList<>(needAddPdIdSet.size());
            for (int pdId : needAddPdIdSet) {
                cacheManage.addNeedDelCachedPdId(aid, pdId);
                Long skuId = m_daoCtrl.buildId();
                if(skuId == null){
                    rt = Errno.ERROR;
                    Log.logErr(rt,"genSpuInfo arg error;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
                    return rt;
                }
                if(pdIdSkuIdMap != null){
                    pdIdSkuIdMap.put(pdId, skuId);
                }
                Param data = new Param();
                data.setInt(ProductSpecSkuEntity.Info.AID, aid);
                data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
                data.setInt(ProductSpecSkuEntity.Info.SORT, 0);
                data.setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid);
                data.setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                data.setInt(ProductSpecSkuEntity.Info.FLAG, ProductSpecSkuValObj.FLag.SPU);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
                data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
                addList.add(data);
            }
            rt = m_daoCtrl.batchInsert(addList, null, false);
            if(rt != Errno.OK){
                Log.logErr(rt,"dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
                return rt;
            }
            // 添加 Saga 操作记录
            if (isSaga) {
                FaiList<Param> sagaOpList = new FaiList<>();
                String xid = RootContext.getXID();
                Long branchId = RootContext.getBranchId();
                addList.forEach(info -> {
                    Param sagaOpInfo = new Param();
                    sagaOpInfo.setInt(SpecStrEntity.Info.AID, aid);
                    sagaOpInfo.assign(info, ProductSpecSkuEntity.Info.SKU_ID);
                    sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                    sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                    sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                    sagaOpList.add(sagaOpInfo);
                });
                if (!Util.isEmptyList(sagaOpList)) {
                    rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
                    if (rt != Errno.OK) {
                        Log.logErr(rt, "dao.insert sagaOpList error;flow=%d;aid=%d;sagaOpList=%s",m_flow, aid, sagaOpList);
                    }
                }
            }
        }else{
            rt = Errno.OK;
        }
        Log.logStd("ok;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    /**
     * genSkuRepresentSpuInfo
     */
    public int genSkuRepresentSpuInfo(int aid, int tid, int unionPriId, int pdId, Ref<Long> skuIdRef, boolean isSaga){
        int rt;
        // 查一下
        rt = getSkuIdRepresentSpu(aid, pdId, skuIdRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        if(skuIdRef.value != null){
            return rt = Errno.OK;
        }
        // 没有就添加
        HashMap<Integer, Long> pdIdSkuIdMap = new HashMap<>();
        FaiList<Integer> pdIdList = new FaiList<>(Arrays.asList(pdId));
        rt = batchGenSkuRepresentSpuInfo(aid, tid, unionPriId, pdIdList, pdIdSkuIdMap, isSaga);
        if(rt != Errno.OK){
            return rt;
        }
        skuIdRef.value = pdIdSkuIdMap.get(pdId);
        Log.logStd("ok;flow=%d;aid=%d;tid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, tid, unionPriId, pdId);
        return rt;
    }

    /**
     * genSkuRepresentSpuInfo 的补偿方法
     * @param aid aid
     * @param addSpecSkuSagaOpList saga 操作记录
     * @return {@link Errno}
     */
    public int genSpuRollback(int aid, FaiList<Param> addSpecSkuSagaOpList) {
        int rt;
        if (Util.isEmptyList(addSpecSkuSagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("arg err;addSpecSkuSagaOpList is empty");
            return rt;
        }
        FaiList<Long> skuIdList = new FaiList<>();
        // 找出 skuIdList，通过 aid + skuIdList 删除之前添加的 spu
        addSpecSkuSagaOpList.forEach(sagaOpInfo -> skuIdList.add(sagaOpInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID)));
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "genSpuRollback error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher);
            return rt;
        }
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean softDel, boolean isSaga) {
        int rt;
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        { // 获取skuIdList
            Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
            rt = getListFromDaoByPdIdList(aid, pdIdList, true, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Long> skuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
            cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
            deletedSkuIdSet.addAll(skuIdList);
            cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        }
        if (isSaga) {
            FaiList<Param> sagaOpList = new FaiList<>();
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            Calendar now = Calendar.getInstance();
            // 根据 是否软删除 记录不同的 Saga 操作
            if (softDel) {
                // 记录 aid + pdIdList
                pdIdList.forEach(pdId -> {
                    Param sagaOpInfo = new Param();
                    sagaOpInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
                    sagaOpInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                    sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                    sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                    sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                    sagaOpList.add(sagaOpInfo);
                });
            } else {
                // 记录整条旧数据
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = delMatcher;
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = m_daoCtrl.select(searchArg, listRef);
                if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    Log.logErr(rt, "productSpecSkuProc select oldList err;flow=%d;aid=%d", m_flow, aid);
                    return rt;
                }
                if (!Util.isEmptyList(listRef.value)) {
                    listRef.value.forEach(info -> {
                        info.setString(SagaEntity.Common.XID, xid);
                        info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                        info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                        info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                        sagaOpList.add(info);
                    });
                }
            }
            // 添加 Saga 操作记录
            if (!Util.isEmptyList(sagaOpList)) {
                rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "productSpecSkuProc insert sagaOpList error;flow=%d;aid=%d", m_flow, aid);
                    return rt;
                }
            }
        }
        if (softDel) {
            ParamUpdater updater = new ParamUpdater();
            updater.getData().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEL);
            rt = m_daoCtrl.update(updater, delMatcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "dao.update error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
                return rt;
            }
        } else {
            rt = m_daoCtrl.delete(delMatcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "dao.delete error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
                return rt;
            }
        }

        Log.logStd("ok;flow=%d;aid=%d;pdIdList=%s;softDel=%s;", m_flow, aid, pdIdList, softDel);
        return rt;
    }

    /**
     * batchDel 的补偿方法
     */
    public int batchDelRollback(int aid, FaiList<Param> sagaOpList) {
        int rt;
        if (Util.isEmptyList(sagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;sagaOpList is empty;flow=%d;aid=%", m_flow, aid);
            return rt;
        }
        // 获取操作的类型，因为要么之前都软删除，要么都删除
        Integer sagaOp = sagaOpList.get(0).getInt(SagaEntity.Common.SAGA_OP);
        if (sagaOp == SagaValObj.SagaOp.DEL) {
            // 去除 Saga 字段
            Util.removeSpecificColumn(sagaOpList, SagaEntity.Common.XID, SagaEntity.Common.BRANCH_ID, SagaEntity.Common.SAGA_OP, SagaEntity.Common.SAGA_TIME);
            rt = m_daoCtrl.batchInsert(sagaOpList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "productSpecSkuProc dao.batchRollback error;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
        } else {
            // 软删除
            // 获取 pdIdList 因为之前是根据 aid + pdIdList 下进行软删除的
            FaiList<Integer> pdIdList = new FaiList<>();
            sagaOpList.forEach(sagaOpInfo -> pdIdList.add(sagaOpInfo.getInt(ProductSpecSkuEntity.Info.PD_ID)));
            if (Util.isEmptyList(pdIdList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;pdIdList is empty");
                return rt;
            }
            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
            ParamUpdater updater = new ParamUpdater(new Param().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEFAULT));
            rt = m_daoCtrl.update(updater, matcher);
            if (rt != Errno.OK) {
                Log.logErr(rt, "productSpecSkuProc dao.batchRollback(soft) error;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int batchSoftDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
        return batchSoftDel(aid, pdId, delSkuIdList, false);
    }

    /**
     * 软删
     */
    public int batchSoftDel(int aid, int pdId, FaiList<Long> delSkuIdList, boolean isSaga) {
        if(aid <= 0 || pdId <= 0){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        delMatcher.and(ProductSpecSkuEntity.Info.FLAG,  ParamMatcher.LAND_NE, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        if(delSkuIdList != null){
            if(delSkuIdList.isEmpty()){
                return Errno.OK;
            }
            delMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);
        }else{
            { // 获取skuIdList
                Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = delMatcher;
                int rt = m_daoCtrl.select(searchArg, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    Log.logErr(rt, "select error;flow=%d;aid=%s;pdId=%s;delMatcher=%s;", m_flow, aid, pdId, delMatcher);
                    return rt;
                }
                delSkuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
            }
        }
        deletedSkuIdSet.addAll(delSkuIdList);
        cacheManage.addNeedDelCachedSkuIdList(aid, delSkuIdList);
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        Log.logDbg("delMatcher.sql=%s;delMatcher.json=%s;", delMatcher.getSql(), delMatcher.toJson());
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEL);
        // 记录 Saga 操作
        if (isSaga) {
            FaiList<Param> sagaOpList = new FaiList<>();
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            Calendar now = Calendar.getInstance();
            delSkuIdList.forEach(skuId -> {
                Param sagaOpInfo = new Param();
                sagaOpInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
                sagaOpInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                sagaOpInfo.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
                sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaOpList.add(sagaOpInfo);
            });
            if (!fai.middleground.svrutil.misc.Utils.isEmptyList(sagaOpList)) {
               int rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
               if (rt != Errno.OK) {
                   Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d;", m_flow, aid);
                   return rt;
               }
            }
        }
        int rt = m_daoCtrl.update(updater, delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.update error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }

    public int batchSoftDelRollback(int aid, FaiList<Param> delSagaOpList) {
        int rt;
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(delSagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;delSagaOpList is empty");
            return rt;
        }
        FaiList<Long> skuIdList = new FaiList<>();
        delSagaOpList.forEach(sgagOpInfo -> skuIdList.add(sgagOpInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID)));
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        ParamUpdater updater = new ParamUpdater(new Param().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEFAULT));
        rt = m_daoCtrl.update(updater, matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchSoftDelRollback err;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
            return rt;
        }
        Log.logStd("batchSoftDelRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        if(aid <= 0 || Util.isEmptyList(unionPriIds)){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.delete error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        // 处理下idBuilder
        m_daoCtrl.restoreMaxId();
        Log.logStd("ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int refreshSku(int aid, int tid, int unionPriId, int pdId, FaiList<FaiList<Integer>> skuList, boolean isSaga) {
        if(aid <= 0 || pdId <= 0 || skuList == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;skuList=%s;", m_flow, aid, pdId, skuList);
            return Errno.ARGS_ERROR;
        }
        int rt = batchSoftDel(aid, pdId, null, isSaga);
        if(rt != Errno.OK) {
            return rt;
        }
        if(skuList.isEmpty()){
            Log.logStd("refreshSku ok;empty sku;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
            return Errno.OK;
        }
        FaiList<Param> infoList = new FaiList<>(skuList.size());
        skuList.forEach(inPdScStrIdList->{
            infoList.add(
                    new Param()
                    .setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList)
                    .setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid)
                    .setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId)
            );
        });
        rt = batchAdd(aid, pdId, infoList, isSaga);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList, boolean isSaga){
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(updaterList, ProductSpecSkuEntity.getValidKeys(), data->{
            skuIdList.add(data.getLong(ProductSpecSkuEntity.Info.SKU_ID));
        });

        maxUpdaterKeys.remove(ProductSpecSkuEntity.Info.SKU_ID);
        if(maxUpdaterKeys.isEmpty()){
            return Errno.OK;
        }

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDao(aid, pdId, skuIdList, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }

        Map<Integer, Param> oldDataMap = Utils.getMap(listRef.value, ProductSpecSkuEntity.Info.SKU_ID);

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, "?");

        // 记录 Saga 操作
        if (isSaga) {
            if (!Util.isEmptyList(listRef.value)) {
                FaiList<Param> sagaOpList = new FaiList<>();
                maxUpdaterKeys.add(ProductSpecSkuEntity.Info.SKU_ID);
                maxUpdaterKeys.add(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME);
                String xid = RootContext.getXID();
                Long branchId = RootContext.getBranchId();
                Calendar now = Calendar.getInstance();
                listRef.value.forEach(info -> {
                    Param sagaOpInfo = new Param();
                    sagaOpInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
                    sagaOpInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                    // 记录下要被修改数据行的源数据
                    maxUpdaterKeys.forEach(key -> {
                        if (key.equals(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST)) {
                            sagaOpInfo.setString(key, info.getList(key).toJson());
                        } else {
                            sagaOpInfo.assign(info, key);
                        }
                    });
                    // 记录 Saga 字段
                    sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                    sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                    sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                    sagaOpList.add(sagaOpInfo);
                });
                // 记得去除掉字段，避免影响后面的操作
                maxUpdaterKeys.remove(ProductSpecSkuEntity.Info.SKU_ID);
                maxUpdaterKeys.remove(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME);
                if (!Util.isEmptyList(sagaOpList)) {
                    rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
                    if (rt != Errno.OK) {
                        Log.logErr(rt, "dao.insert sagaOpList error;flow=%d;aid=%d;sagaOpList=%s",m_flow, aid, sagaOpList);
                        return rt;
                    }
                }
            }
        }
        listRef.value = null; // help gc
        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        updaterList.forEach(updater -> {
            long skuId = updater.getData().getLong(ProductSpecSkuEntity.Info.SKU_ID);
            Param oldData = oldDataMap.remove(skuId); // help gc
            if(oldData == null){
                return;
            }
            Param updatedData = updater.update(oldData, true);
            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
            { // matcher
                data.setInt(ProductSpecSkuEntity.Info.AID, aid);
                data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            }

            dataList.add(data);
        });
        cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchUpdate error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    /**
     * batchSet 的回滚方法
     */
    public int batchSetRollback(int aid, FaiList<Param> modifySpecSkuSagaOpList) {
        int rt;
        if (Util.isEmptyList(modifySpecSkuSagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("batchSet rollback error;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }
        FaiList<ParamUpdater> updaterList = new FaiList<>();
        modifySpecSkuSagaOpList.clone().forEach(info -> updaterList.add(new ParamUpdater(info)));
        // 获取更新最大key
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(updaterList, ProductSpecSkuEntity.getValidKeys(), null);
        maxUpdaterKeys.remove(ProductSpecSkuEntity.Info.SKU_ID);
        maxUpdaterKeys.add(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME);

        // updater
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key -> doBatchUpdater.getData().setString(key, "?"));

        // matcher
        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        FaiList<Param> dataList = new FaiList<>();
        modifySpecSkuSagaOpList.forEach(updateInfo -> {
            Param data = new Param();
            maxUpdaterKeys.forEach(key -> data.assign(updateInfo, key));
            // matcher
            data.setInt(ProductSpecSkuEntity.Info.AID, aid);
            data.assign(updateInfo, ProductSpecSkuEntity.Info.PD_ID);
            data.assign(updateInfo, ProductSpecSkuEntity.Info.SKU_ID);
            dataList.add(data);
        });
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "dao.batchSetRollback error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int updateAllowEmptySku(int aid, int tid, int unionPriId, int pdId, FaiList<Integer> inPdScStrIdList, boolean isSaga) {
        int rt;
        Ref<FaiList<Param>> listRef = new Ref<>();
        {
            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductSpecSkuEntity.Info.STATUS, ParamMatcher.EQ, ProductSpecSkuValObj.Status.DEFAULT);
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;

            rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.FLAG);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "select error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
                return rt;
            }
        }
        Long allowEmptySkuId = null;
        FaiList<Long> delSkuIdList = new FaiList<>(listRef.value.size());
        for (Param info : listRef.value) {
            int flag = info.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
            long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.ALLOW_EMPTY)){
                allowEmptySkuId = skuId;
            }else{
                delSkuIdList.add(skuId);
            }
        }
        rt = batchSoftDel(aid, pdId, delSkuIdList, isSaga);
        if(rt != Errno.OK){
            return rt;
        }
        Calendar now = Calendar.getInstance();
        if(allowEmptySkuId == null){
            Param addInfo = new Param();
            addInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
            addInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
            addInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid);
            addInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            addInfo.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
            addInfo.setInt(ProductSpecSkuEntity.Info.FLAG, ProductSpecSkuValObj.FLag.ALLOW_EMPTY);
            addInfo.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
            addInfo.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
            Long skuId = m_daoCtrl.buildId();
            if(skuId == null){
                Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;skuId=%s;", m_flow, aid, pdId, skuId);
                return Errno.ERROR;
            }
            addInfo.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            // 记录 Saga 操作
            if (isSaga) {
                String xid = RootContext.getXID();
                Long branchId = RootContext.getBranchId();
                Param sagaOpInfo = new Param();
                sagaOpInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
                sagaOpInfo.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
                sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                m_sagaDaoCtrl.insert(sagaOpInfo);
            }
            rt = m_daoCtrl.insert(addInfo);
            if(rt != Errno.OK){
                Log.logErr(rt, "dao.insert err;flow=%d;aid=%s;pdId=%s;skuId=%s;", m_flow, aid, pdId, skuId);
                return rt;
            }
        }else {
            ParamUpdater updater = new ParamUpdater();
            updater.getData()
                    .setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson())
                    .setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);

            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.EQ, allowEmptySkuId);

            // 记录 Saga 操作
            if (isSaga) {
                // 先查询一遍旧数据
                Ref<FaiList<Param>> oldListRef = new Ref<>();
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                rt = m_daoCtrl.select(searchArg, oldListRef, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, ProductSpecSkuEntity.Info.SYS_UPDATE_TIME);
                if (rt != Errno.OK && rt != Errno.ERROR) {
                    Log.logErr(rt, "select oldList err;flow=%d;aid=%d", m_flow, aid);
                    return rt;
                }
                if (!fai.middleground.svrutil.misc.Utils.isEmptyList(oldListRef.value)) {
                    String xid = RootContext.getXID();
                    Long branchId = RootContext.getBranchId();
                    for (Param info : oldListRef.value) {
                        info.setInt(ProductSpecSkuEntity.Info.AID, aid);
                        info.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                        info.setLong(ProductSpecSkuEntity.Info.SKU_ID, allowEmptySkuId);
                        info.setString(SagaEntity.Common.XID, xid);
                        info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                        info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                        info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                    }
                    rt = m_sagaDaoCtrl.batchInsert(oldListRef.value);
                    if (rt != Errno.OK) {
                        Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d", m_flow, aid);
                        return rt;
                    }
                }
            }
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logErr(rt, "dao.update err;flow=%d;aid=%s;pdId=%s;skuId=%s;", m_flow, aid, pdId, allowEmptySkuId);
                return rt;
            }
            cacheManage.addNeedDelCachedSkuIdSet(aid, new HashSet<>(Arrays.asList(allowEmptySkuId)));
        }
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        Log.logStd("ok;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int getListFromDaoByPdIdList(int aid, FaiList<Integer> pdIdList, boolean withSpuInfo, Ref<FaiList<Param>> pdScSkuInfoListRef, String... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        if(!withSpuInfo){
            matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND_NE, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        }
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(pdScSkuInfoListRef.value, onlyNeedFields);
        Log.logStd(rt,"ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return Errno.OK;
    }
    public int getListFromDao(int aid, int pdId, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        return getListFromDao(aid, pdId, null, pdScSkuInfoListRef, onlyNeedFields);
    }
    public int getListFromDao(int aid, int pdId, FaiList<Long> skuIdList, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        return getListFromDao(aid, pdId, false, skuIdList, pdScSkuInfoListRef, onlyNeedFields);
    }

    public int getListFromDao(int aid, int pdId, boolean withSpuInfo, FaiList<Long> skuIdList, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(skuIdList != null){
            matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        }else{
            if(!withSpuInfo){
                matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND_NE, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
            }
        }
        matcher.and(ProductSpecSkuEntity.Info.STATUS, ParamMatcher.EQ, ProductSpecSkuValObj.Status.DEFAULT);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(pdScSkuInfoListRef.value, onlyNeedFields);
        Log.logStd(rt,"ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return Errno.OK;
    }

    /**
     * 明确skuId时可以获取到已删除的sku
     */
    public int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(listRef.value, onlyNeedFields);
        Log.logStd(rt,"ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return Errno.OK;
    }
    public int getListByScStrIdListFromDao(int aid, int pdId, FaiList<String> inPdScStrIdLists, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND_NE, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        matcher.and(ProductSpecSkuEntity.Info.STATUS, ParamMatcher.EQ, ProductSpecSkuValObj.Status.DEFAULT);
        matcher.and(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, ParamMatcher.IN, inPdScStrIdLists);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd(rt,"ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return Errno.OK;
    }
    private void initDBInfoList(FaiList<Param> infoList, String ... onlyNeedFields){
        if(infoList == null || infoList.isEmpty()){
            return;
        }
        if(onlyNeedFields != null && onlyNeedFields.length > 0){
            boolean needInit = false;
            for (String onlyNeedField : onlyNeedFields) {
                if(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST.equals(onlyNeedField)){
                    needInit = true;
                    break;
                }
            }
            if(!needInit){
                return;
            }
        }
        infoList.forEach(info->{
            String inPdScStrIdListStr = info.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            info.setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, FaiList.parseIntList(inPdScStrIdListStr, new FaiList<>()));
        });
    }

    /**
     * 获取spu信息从dao
     */
    public int getSpuInfoFromDao(int aid, int pdId, Ref<Param> infoRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getSpuInfoFromDao error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd(rt, "getSpuInfoFromDao ok;aid=%s;pdId=%s;",aid, pdId);
        return rt;
    }

    /**
     * 获取代表spu的sku数据
     */
    public int getSkuIdRepresentSpu(int aid, int pdId, Ref<Long> skuIdRef) {
        Long skuId = ProductSpecSkuCacheCtrl.getSkuIdRepresentSpu(aid, pdId);
        if(skuId != null){
            skuIdRef.value = skuId;
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        Ref<Param> infoRef = new Ref<>();
        rt = getSpuInfoFromDao(aid, pdId, infoRef, ProductSpecSkuEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        skuId = infoRef.value.getLong(ProductSpecSkuEntity.Info.SKU_ID);
        if(skuId != null){
            ProductSpecSkuCacheCtrl.setSkuIdRepresentSpu(aid, pdId, skuId);
        }
        skuIdRef.value = skuId;
        return rt;
    }


    public int getList(int aid, int pdId, boolean withSpuInfo, Ref<FaiList<Param>> listRef){
        if(pdId <= 0 || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return Errno.ARGS_ERROR;
        }
        boolean fromCache = true;
        Integer getSize = null;
        Integer returnSize = null;
        int rt = Errno.ERROR;
        try {
            FaiList<Param> cacheList = ProductSpecSkuCacheCtrl.getListByPdId(aid, pdId);
            if(cacheList != null && !cacheList.isEmpty()){
                listRef.value = cacheList;
                return Errno.OK;
            }
            try {
                LockUtil.readLock(aid);
                // double check
                cacheList = ProductSpecSkuCacheCtrl.getListByPdId(aid, pdId);
                if(cacheList != null && !cacheList.isEmpty()){
                    listRef.value = cacheList;
                    return Errno.OK;
                }
                fromCache = false;
                rt = getListFromDao(aid, pdId, true, null, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                ProductSpecSkuCacheCtrl.initInfoWithRelCache(aid, pdId, listRef.value);
            }finally {
                LockUtil.unReadLock(aid);
            }
        }finally {
            if(listRef.value != null) {
                getSize = listRef.value.size();
            }
            if(!withSpuInfo){ // 不需要获取spu数据就移除掉
                for(Iterator<Param> iterator = listRef.value.iterator(); iterator.hasNext();){
                    Param info = iterator.next();
                    int flag = info.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                    if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){
                        iterator.remove();
                    }
                }
            }
            if(listRef.value != null) {
                returnSize = listRef.value.size();
            }
            Log.logStd("get sku;flow=%d;aid=%d;fromCache=%s;getSize=%s;returnSize=%s;", m_flow, aid, fromCache, getSize, returnSize);
        }

        return rt;
    }
    public int getList(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logErr("arg error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return Errno.ARGS_ERROR;
        }
        Set<Long> skuIdSet = new HashSet<>(skuIdList);
        FaiList<Param> resultList = new FaiList<>();
        int rt = Errno.ERROR;
        try {
            getListFromCache(aid, skuIdSet, resultList);
            if(skuIdSet.isEmpty()){
                listRef.value = resultList;
                return rt = Errno.OK;
            }
            try {
                LockUtil.readLock(aid);
                // double check
                getListFromCache(aid, skuIdSet, resultList);
                if(skuIdSet.isEmpty()){
                    listRef.value = resultList;
                    return rt = Errno.OK;
                }

                rt = getListFromDao(aid, new FaiList<>(skuIdSet), listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Param> infoList = listRef.value;
                for (Param info : infoList) {
                    resultList.add(info);
                    skuIdSet.remove(info.getLong(ProductSpecSkuEntity.Info.SKU_ID));
                }
                for (Long skuId : skuIdSet) {
                    infoList.add(genEmptyCacheInfo(skuId));
                }
                ProductSpecSkuCacheCtrl.initInfoCache(aid, infoList);
            }finally {
                LockUtil.unReadLock(aid);
            }

            listRef.value = resultList;
            return rt;
        }finally {
            if(fields != null && fields.length > 0){
                Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
                for (Param info : listRef.value) {
                    for (String field : info.keySet()) {
                        if(fieldSet.contains(field)){
                            continue;
                        }
                        info.remove(field);
                    }
                }
            }
        }
    }

    /**
     * 仅获取spu信息
     */
    public int getSpuInfoList(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> pdScSkuInfoListRef) {
        FaiList<Long> skuIdList = new FaiList<>();
        Set<Integer> noCachePdIds = new HashSet<>(pdIdList);
        getSpuInfoListFromCache(aid, pdIdList, skuIdList, noCachePdIds);
        if(!noCachePdIds.isEmpty()){
            try {
                LockUtil.readLock(aid);
                getSpuInfoListFromCache(aid, pdIdList, skuIdList, noCachePdIds);
                if(!noCachePdIds.isEmpty()){
                    ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
                    matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, new FaiList<>(noCachePdIds));
                    matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = matcher;
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID);
                    if(rt != Errno.OK &&rt != Errno.NOT_FOUND){
                        Log.logErr(rt, "error;flow=%d;aid=%s;pdIdSet=%s;", m_flow, aid, noCachePdIds);
                        return rt;
                    }
                    Map<Integer, Long> map = new HashMap<>(listRef.value.size()*4/3+1);
                    for (Param info : listRef.value) {
                        int pdId = info.getInt(ProductSpecSkuEntity.Info.PD_ID);
                        long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                        map.put(pdId, skuId);
                        skuIdList.add(skuId);
                        noCachePdIds.remove(pdId);
                    }
                    for (Integer pdId : noCachePdIds) {
                        map.put(pdId, 0L);
                    }
                    ProductSpecSkuCacheCtrl.batchSetSkuIdRepresentSpu(aid, map);
                }
            }finally {
                LockUtil.unReadLock(aid);
            }
        }
        if(skuIdList.isEmpty()){
            pdScSkuInfoListRef.value = new FaiList<>();
            Log.logStd("not found flow=%s;aid=%s,pdIdList=%s", m_flow, aid, pdIdList);
            return Errno.NOT_FOUND;
        }
        return getList(aid, skuIdList, pdScSkuInfoListRef);
    }

    private void getSpuInfoListFromCache(int aid, FaiList<Integer> pdIdList, FaiList<Long> skuIdList, Set<Integer> noCachePdIds) {
        Map<Integer, Long> pdIdSkuIdMap = ProductSpecSkuCacheCtrl.batchGetSkuIdRepresentSpu(aid, pdIdList, true);
        pdIdSkuIdMap.forEach((pdId, skuId)->{
            if(skuId != 0){
                skuIdList.add(skuId);
            }
            noCachePdIds.remove(pdId);
            Log.logDbg("aid=%s;pdId=%s;skuId=%s", aid, pdId, skuId);
        });
    }

    private void getListFromCache(int aid, Set<Long> skuIdSet, FaiList<Param> resultList) {
        FaiList<Param> cacheList = ProductSpecSkuCacheCtrl.getList(aid, new FaiList<>(skuIdSet));
        if(cacheList != null){
            for (Param cacheInfo : cacheList) {
                if(!isEmptyCacheInfo(cacheInfo)){
                    resultList.add(cacheInfo);
                }
                skuIdSet.remove(cacheInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID));
            }
        }
    }

    /**
     * 获取 Saga 操作记录
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param sagaOpListRef 接收返回的集合
     * @return {@link Errno}
     */
    public int getSagaOpList(String xid, Long branchId, Ref<FaiList<Param>> sagaOpListRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        int rt = m_sagaDaoCtrl.select(searchArg, sagaOpListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "productSpecSkuProc dao.getSagaOpList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    private Param genEmptyCacheInfo(long skuId){
        return new Param()
                .setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId)
                .setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, Calendar.getInstance());
    }
    private boolean isEmptyCacheInfo(Param cacheInfo){
        Set<String> keySet = cacheInfo.keySet();
        return keySet.size() <= 2;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }
    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }
    public void restoreMaxId(int aid, boolean needLock) {
        m_daoCtrl.restoreMaxId(needLock);
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    /**
     * 获取被删除的skuIdList
     */
    public FaiList<Long> getDeletedSkuIdList(){
        return new FaiList<>(deletedSkuIdSet);
    }

    private int m_flow;
    private CacheManage cacheManage = new CacheManage();
    private ProductSpecSkuDaoCtrl m_daoCtrl;
    private ProductSpecSkuSagaDaoCtrl m_sagaDaoCtrl;
    private Set<Long> deletedSkuIdSet = new HashSet<>();


    private static class CacheManage{

        public CacheManage() {
            init();
        }

        private Set<Long> needDelCachedSkuIdSet;
        private Set<Integer> needDelPdIdSet;
        private void addNeedDelCachedSkuIdSet(int aid, Set<Long> skuIdSet){
            if(skuIdSet == null || skuIdSet.isEmpty()){
                return;
            }
            ProductSpecSkuCacheCtrl.setInfoCacheDirty(aid);
            needDelCachedSkuIdSet.addAll(skuIdSet);
        }
        private void addNeedDelCachedSkuIdList(int aid, FaiList<Long> skuIdList){
            if(skuIdList == null || skuIdList.isEmpty()){
                return;
            }
            HashSet<Long> skuIdSet = new HashSet<>(skuIdList);
            addNeedDelCachedSkuIdSet(aid, skuIdSet);
        }
        private void addNeedDelCachedPdId(int aid, int pdId){
            addNeedDelCachedPdIdSet(aid, new HashSet<>(Arrays.asList(pdId)));
        }
        private void addNeedDelCachedPdIdList(int aid, FaiList<Integer> pdIdList){
            if(pdIdList == null || pdIdList.isEmpty()){
                return;
            }
            addNeedDelCachedPdIdSet(aid, new HashSet<>(pdIdList));
        }
        private void addNeedDelCachedPdIdSet(int aid, Set<Integer> pdIdSet){
            if(pdIdSet == null || pdIdSet.isEmpty()){
                return;
            }
            ProductSpecSkuCacheCtrl.setRelCacheDirty(aid);
            needDelPdIdSet.addAll(pdIdSet);
        }

        private boolean deleteDirtyCache(int aid){
            try {
                boolean boo = ProductSpecSkuCacheCtrl.delRelCache(aid, needDelPdIdSet);
                boo &= ProductSpecSkuCacheCtrl.delSkuIdRepresentSpu(aid, needDelPdIdSet);
                boo &= ProductSpecSkuCacheCtrl.delInfoCache(aid, needDelCachedSkuIdSet);
                return boo;
            }finally {
                init();
            }
        }

        private void init() {
            needDelCachedSkuIdSet = new HashSet<>();
            needDelPdIdSet = new HashSet<>();
        }
    }

}
