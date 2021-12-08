package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.entity.*;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductSpecSkuProc {
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
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
        sagaMap = new HashMap<>();
    }

    public int batchAdd(ProductSpecProc specProc, int aid, int pdId, FaiList<Param> infoList, boolean isSaga) {
        return batchAdd(specProc, aid, Collections.singletonMap(pdId, infoList), null, null, isSaga);
    }
    public int batchAdd(ProductSpecProc specProc, int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap,
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
            if(pdId == null || pdId <= 0 || Utils.isEmptyList(infoList)){
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
                data.assign(info, ProductSpecSkuEntity.Info.SKU_CODE); // TODO 没有这个值，所以在db中也没有设置这个值
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
        // 若设置了inPdScStrIdList，需同步设置inPdScList
        rt = assemblyInPdScList4Insert(specProc, aid, dataList);
        if(rt != Errno.OK) {
            Log.logErr("assemblyInPdScList err;aid=%s;dataList=%s;", aid, dataList);
            return rt;
        }
        rt = m_daoCtrl.batchInsert(dataList, null, false);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchInsert error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        // 添加 Saga 操作记录
        if (isSaga) {
            rt = addInsOp4Saga(aid, skuIdList);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        Log.logStd("ok;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIdPdScSkuListMap.keySet());
        return rt;
    }

    /**
     * 批量同步，没有就添加，有就跳过
     * @param aid
     * @param pdId_pdScSkuInfoMap
     * @param pdIdSkuIdMap
     * @return
     */
    public int batchSynchronousSPU2SKU(ProductSpecProc specProc, int aid, Map<Integer, Param> pdId_pdScSkuInfoMap, Map<Integer, Long> pdIdSkuIdMap) {
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
            // 若设置了inPdScStrIdList，需同步设置inPdScList
            rt = assemblyInPdScList4Insert(specProc, aid, batchAddDataList);
            if(rt != Errno.OK) {
                Log.logErr("assemblyInPdScList err;aid=%s;dataList=%s;", aid, batchAddDataList);
                return rt;
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

    public int batchGenSkuRepresentSpuInfo(ProductSpecProc specProc, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, Map<Integer/*pdId*/, Long/*skuId*/> pdIdSkuIdMap){
        return batchGenSkuRepresentSpuInfo(specProc, aid, tid, unionPriId, pdIdList, pdIdSkuIdMap, false);
    }
    /**
     * 批量生成代表spu的sku
     * @param aid
     * @param tid
     * @param unionPriId
     * @return
     */
    public int batchGenSkuRepresentSpuInfo(ProductSpecProc specProc, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, Map<Integer/*pdId*/, Long/*skuId*/> pdIdSkuIdMap, boolean isSaga){
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
            // 若设置了inPdScStrIdList，需同步设置inPdScList
            rt = assemblyInPdScList4Insert(specProc, aid, addList);
            if(rt != Errno.OK) {
                Log.logErr("assemblyInPdScList err;aid=%s;dataList=%s;", aid, addList);
                return rt;
            }
            rt = m_daoCtrl.batchInsert(addList, null, !isSaga);
            if(rt != Errno.OK){
                Log.logErr(rt,"dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
                return rt;
            }
            // 添加 Saga 操作记录
            if (isSaga) {
                FaiList<Long> skuList = Utils.getValList(addList, ProductSpecSkuEntity.Info.SKU_ID);
                rt = addInsOp4Saga(aid, skuList);
                if (rt != Errno.OK) {
                    return rt;
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
    public int genSkuRepresentSpuInfo(ProductSpecProc specProc, int aid, int tid, int unionPriId, int pdId, Ref<Long> skuIdRef, boolean isSaga){
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
        rt = batchGenSkuRepresentSpuInfo(specProc,aid, tid, unionPriId, pdIdList, pdIdSkuIdMap, isSaga);
        if(rt != Errno.OK){
            return rt;
        }
        skuIdRef.value = pdIdSkuIdMap.get(pdId);
        Log.logStd("ok;flow=%d;aid=%d;tid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, tid, unionPriId, pdId);
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
            // 查询旧数据
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = delMatcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "productSpecSkuProc select oldList err;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
            // 根据 是否软删除 记录不同的 Saga 操作
            if (softDel) {
                // 预记录修改操作
                preAddUpdateSaga(aid, listRef.value);
            } else {
                // 记录删除操作
                rt = addDelOp4Saga(aid, listRef.value);
                if (rt != Errno.OK) {
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
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = delMatcher;
        int rt;
        Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
        if(delSkuIdList != null){
            if(delSkuIdList.isEmpty()){
                return Errno.OK;
            }
            delMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);
            // Saga 模式需要获取所有一次所有能被修改的字段 + 主键
            if (isSaga) {
                rt = m_daoCtrl.select(searchArg, skuIdInfoListRef, ProductSpecSkuEntity.getSagaKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    Log.logErr(rt, "select error;flow=%d;aid=%s;pdId=%s;delMatcher=%s;", m_flow, aid, pdId, delMatcher);
                    return rt;
                }
            }
        }else{
            if (isSaga) {
                rt = m_daoCtrl.select(searchArg, skuIdInfoListRef, ProductSpecSkuEntity.getSagaKeys());
            } else {
                rt = m_daoCtrl.select(searchArg, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
            }
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "select error;flow=%d;aid=%s;pdId=%s;delMatcher=%s;", m_flow, aid, pdId, delMatcher);
                return rt;
            }
            delSkuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
        }
        deletedSkuIdSet.addAll(delSkuIdList);
        cacheManage.addNeedDelCachedSkuIdList(aid, delSkuIdList);
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        Log.logDbg("delMatcher.sql=%s;delMatcher.json=%s;", delMatcher.getSql(), delMatcher.toJson());
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEL);
        // 记录 Saga 操作
        if (isSaga) {
            preAddUpdateSaga(aid, skuIdInfoListRef.value);
        }

        rt = m_daoCtrl.update(updater, delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.update error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        if(aid <= 0 || Utils.isEmptyList(unionPriIds)){
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

    public int refreshSku(ProductSpecProc specProc, int aid, int tid, int unionPriId, int pdId, FaiList<FaiList<Integer>> skuList, boolean isSaga) {
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
        rt = batchAdd(specProc, aid, pdId, infoList, isSaga);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int batchSet(ProductSpecProc specProc, int aid, int pdId, FaiList<ParamUpdater> updaterList, boolean isSaga){
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        // 若设置了inPdScStrIdList，需同步设置inPdScList
        rt = assemblyInPdScList4Update(specProc, aid, pdId, updaterList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "assemblyInPdScList err;aid=%d;pdId=%s;updates=%s;", aid, pdId, updaterList.toJson());
            return rt;
        }

        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Utils.validUpdaterList(updaterList, Utils.asFaiList(ProductSpecSkuEntity.getValidKeys()), data->{
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

        // 预记录修改数据
        if (isSaga) {
            // 要将 inPdScStrIdList 转成 String 再调用 preAddUpdateSaga
            // 因为 preAddUpdateSaga 被多个地方调用，而有些传入的 list 中的 inPdScStrIdList 是 String 类型
            for (Param info : listRef.value) {
                info.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, info.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST).toJson());
                info.setString(ProductSpecSkuEntity.Info.IN_PD_SC_LIST, info.getList(ProductSpecSkuEntity.Info.IN_PD_SC_LIST).toJson());
            }
            preAddUpdateSaga(aid, listRef.value);
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

    public int updateAllowEmptySku(ProductSpecProc specProc, int aid, int tid, int unionPriId, int pdId, FaiList<Integer> inPdScStrIdList, boolean isSaga) {
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
                addInsOp4Saga(aid, new FaiList<>(Collections.singletonList(skuId)));
            }
            // 若设置了inPdScStrIdList，需同步设置inPdScList
            rt = assemblyInPdScList4Insert(specProc, aid, addInfo);
            if(rt != Errno.OK) {
                Log.logErr("assemblyInPdScList err;aid=%s;addInfo=%s;", aid, addInfo);
                return rt;
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

            // 若设置了inPdScStrIdList，需同步设置inPdScList
            rt = assemblyInPdScList4Update(specProc, aid, pdId, Utils.asFaiList(updater));
            if(rt != Errno.OK) {
                Log.logErr(rt, "assemblyInPdScList err;aid=%d;pdId=%s;updater=%s;", aid, pdId, updater.toJson());
                return rt;
            }

            // 预记录修改数据
            if (isSaga) {
                // 先查询一遍旧数据
                Ref<FaiList<Param>> oldListRef = new Ref<>();
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                rt = m_daoCtrl.select(searchArg, oldListRef, ProductSpecSkuEntity.getSagaKeys());
                if (rt != Errno.OK && rt != Errno.ERROR) {
                    Log.logErr(rt, "select oldList err;flow=%d;aid=%d", m_flow, aid);
                    return rt;
                }
                preAddUpdateSaga(aid, oldListRef.value);
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
        // 是否需要spu的信息
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
                if(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST.equals(onlyNeedField) || ProductSpecSkuEntity.Info.IN_PD_SC_LIST.equals(onlyNeedField)){
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
            String inPdScListStr = info.getString(ProductSpecSkuEntity.Info.IN_PD_SC_LIST);
            info.setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, FaiList.parseIntList(inPdScStrIdListStr, new FaiList<>()));
            info.setList(ProductSpecSkuEntity.Info.IN_PD_SC_LIST, FaiList.parseStringList(inPdScListStr, new FaiList<>()));
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

    // 修改数据时，若修改了inPdScStrIdList字段，则需同步修改 inPdScList字段
    private int assemblyInPdScList4Update(ProductSpecProc specProc, int aid, int pdId, FaiList<ParamUpdater> updaters) {
        int rt;
        if(Utils.isEmptyList(updaters)) {
            return Errno.OK;
        }
        Map<Integer, FaiList<Integer>> pdScIdMap = new HashMap<>();
        rt = getPdScIds(specProc, aid, Utils.asFaiList(pdId), pdScIdMap);
        if(rt != Errno.OK) {
            return rt;
        }
        for(ParamUpdater updater : updaters) {
            Param data = updater.getData();
            if(Str.isEmpty(data)) {
                continue;
            }
            String inPdScStrIdListJson = data.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            FaiList<Integer> inPdScStrIdList = FaiList.parseIntList(inPdScStrIdListJson);
            // 没有inPdScStrIdList的就不用组装inPdScList了
            if(Utils.isEmptyList(inPdScStrIdList)) {
                continue;
            }

            FaiList<Integer> pdScIds = pdScIdMap.get(pdId);
            if(pdScIds == null || pdScIds.size() != inPdScStrIdList.size()) {
                rt = Errno.ERROR;
                Log.logErr("sku not match spec err;aid=%d;pdId=%s;inPdScStrIdList=%s;pdScIds=%s;", aid, pdId, inPdScStrIdList, pdScIds);
                return rt;
            }
            FaiList<String> inPdScInfoList = new FaiList<>();
            for(int i = 0; i < inPdScStrIdList.size(); i++) {
                int pdScId = pdScIds.get(i);
                int si = inPdScStrIdList.get(i);
                inPdScInfoList.add(pdScId + JOINER + si);
            }
            data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_LIST, inPdScInfoList.toJson());
        }
        return Errno.OK;
    }

    // 新增数据时，若设置了inPdScStrIdList字段，则需同步设置 inPdScList字段
    private int assemblyInPdScList4Insert(ProductSpecProc specProc, int aid, Param data) {
        return assemblyInPdScList4Insert(specProc, aid, Utils.asFaiList(data));
    }
    private int assemblyInPdScList4Insert(ProductSpecProc specProc, int aid, FaiList<Param> dataList) {
        int rt;
        if(Utils.isEmptyList(dataList)) {
            return Errno.OK;
        }
        FaiList<Integer> pdIds = Utils.getValList(dataList, ProductSpecSkuEntity.Info.PD_ID);
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ERROR;
            Log.logErr(rt, "assemblyInPdScList err;aid=%s;dataList=%s;", aid, dataList);
            return rt;
        }
        Map<Integer, FaiList<Integer>> pdScIdMap = new HashMap<>();
        rt = getPdScIds(specProc, aid, new FaiList<>(pdIds), pdScIdMap);
        if(rt != Errno.OK) {
            return rt;
        }
        for(Param data : dataList) {
            String inPdScStrIdListJson = data.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            FaiList<Integer> inPdScStrIdList = FaiList.parseIntList(inPdScStrIdListJson);
            // 没有inPdScStrIdList的就不用组装inPdScList了
            if(Utils.isEmptyList(inPdScStrIdList)) {
                data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_LIST, "");
                continue;
            }
            int pdId = data.getInt(ProductSpecSkuEntity.Info.PD_ID);

            FaiList<Integer> pdScIds = pdScIdMap.get(pdId);
            if(pdScIds == null || pdScIds.size() != inPdScStrIdList.size()) {
                rt = Errno.ERROR;
                Log.logErr("sku not match spec err;aid=%d;pdId=%s;inPdScStrIdList=%s;pdScIds=%s;", aid, pdId, inPdScStrIdList, pdScIds);
                return rt;
            }
            FaiList<String> inPdScInfoList = new FaiList<>();
            for(int i = 0; i < inPdScStrIdList.size(); i++) {
                int pdScId = pdScIds.get(i);
                int si = inPdScStrIdList.get(i);
                inPdScInfoList.add(pdScId + JOINER + si);
            }
            data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_LIST, inPdScInfoList.toJson());
        }
        return Errno.OK;
    }

    private int getPdScIds(ProductSpecProc specProc, int aid, FaiList<Integer> pdIds, Map<Integer, FaiList<Integer>> pdScIdMap) {
        if(Utils.isEmptyList(pdIds)) {
            return Errno.OK;
        }
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = specProc.getListFromDao(aid, pdIds, listRef, ProductSpecEntity.Info.PD_ID, ProductSpecEntity.Info.PD_SC_ID, ProductSpecEntity.Info.SORT);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPdScIds err;aid=%s;pdIds=%s;", aid, pdIds);
            return rt;
        }
        ParamComparator comparator = new ParamComparator(ProductSpecEntity.Info.SORT);
        comparator.addKey(ProductSpecEntity.Info.PD_SC_ID);
        comparator.sort(listRef.value);

        for(Param info : listRef.value) {
            int pdId = info.getInt(ProductSpecEntity.Info.PD_ID);
            int pdScId = info.getInt(ProductSpecEntity.Info.PD_SC_ID);
            FaiList<Integer> pdScIds = pdScIdMap.get(pdId);
            if(pdScIds == null) {
                pdScIds = new FaiList<>();
                pdScIdMap.put(pdId, pdScIds);
            }
            pdScIds.add(pdScId);
        }

        return Errno.OK;
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

    // 记录删除操作
    private int addDelOp4Saga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            Log.logStd("addDelOp4Saga list is empty");
            return Errno.OK;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        list.forEach(info -> {
            info.setString(SagaEntity.Common.XID, xid);
            info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
            info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
        });
        int rt = m_sagaDaoCtrl.batchInsert(list, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d;list=%s", m_flow, aid, list);
            return rt;
        }
        return rt;
    }

    // 记录添加操作
    private int addInsOp4Saga(int aid, FaiList<Long> skuIdList) {
        int rt;
        if (skuIdList.isEmpty()) {
            Log.logStd("addInsOp4Saga list is empty;flow=%d;aid=%d", m_flow, aid);
            return Errno.OK;
        }
        FaiList<Param> sagaOpList = new FaiList<>();
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        for (Long skuId : skuIdList) {
            Param sagaOpInfo = new Param();
            sagaOpInfo.setInt(ProductSpecSkuEntity.Info.AID, aid);
            sagaOpInfo.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaOpList.add(sagaOpInfo);
        }
        rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "ProductSpecSkuProc sagaOpList batch insert error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    /**
     * 预记录修改数据
     * @param aid aid
     * @param list 旧数据
     */
    private void preAddUpdateSaga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            Log.logStd("preAddUpdateSaga list is empty;flow=%d;aid=%d;", m_flow, aid);
            return;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        for (Param info : list) {
            int curAid = info.getInt(ProductSpecSkuEntity.Info.AID);
            long curSkuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            PrimaryKey primaryKey = new PrimaryKey(curAid, curSkuId);
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            Param sagaOpInfo = new Param();
            String[] sagaKeys = ProductSpecSkuEntity.getSagaKeys();
            // 记录下要被修改数据行的源数据
            for (String sagaKey : sagaKeys) {
                sagaOpInfo.assign(info, sagaKey);
            }
            // 记录 Saga 字段
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, sagaOpInfo);
        }
    }

    // 添加 Saga 操作记录
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
     * specSku 回滚
     * @param aid aid
     * @param xid 全局事务id
     * @param branchId 分支事务id
     */
    public void rollback4Saga(int aid, String xid, Long branchId) {
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getSagaOpList(xid, branchId, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get sagaOpList err;flow=%d;aid=%;xid=%s;branchId=%s", m_flow, aid, xid, branchId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            Log.logStd("specSkuProc sagaOpList is empty");
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

    // 门店迁移服务使用，之后废弃
    public FaiList<Param> migrateYkService(int aid, FaiList<Param> skuList) {
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0){
            throw new MgException(rt, "error;flow=%d;aid=%s;", m_flow, aid);
        }

        Long skuId = m_daoCtrl.getId();
        if(skuId == null){
            rt = Errno.ERROR;
            throw new MgException(rt, "skuId err;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
        }

        if (skuList == null || skuList.isEmpty()) {
            throw new MgException(rt, "skuList is empty;flow=%d;aid=%s;", m_flow, aid);
        }

        FaiList<Param> returnList = new FaiList<>();
        FaiList<Integer> pdIdList = new FaiList<>();
        FaiList<Long> skuIdList = new FaiList<>();
        for (Param sku : skuList) {
            Param returnParam = new Param();
            Integer pdId = sku.getInt(ProductSpecSkuEntity.Info.PD_ID);
            skuId++;
            sku.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            returnParam.assign(sku, ProductSpecSkuEntity.Info.AID);
            returnParam.assign(sku, ProductSpecSkuEntity.Info.PD_ID);
            returnParam.assign(sku, ProductSpecSkuEntity.Info.SOURCE_TID);
            returnParam.assign(sku, ProductSpecSkuEntity.Info.SKU_ID);
            pdIdList.add(pdId);
            skuIdList.add(skuId);
            returnList.add(returnParam);
        }

        cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);

        if (m_daoCtrl.updateId(skuId) == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao.updateId error;flow=%d;aid=%s;skuId=%s;", m_flow, aid, skuId);
        }

        Log.logDbg("joke add skuList=%s", skuList);
        rt = m_daoCtrl.batchInsert(skuList, null, false);
        if (rt != Errno.OK) {
            throw new MgException(rt, "dao.addSkuList error;flow=%d;aid=%d;skuList=%s", m_flow, aid, skuList);
        }

        Log.logStd("ok;flow=%d;aid=%d;skuIds=%s", m_flow, aid, skuIdList);

        return returnList;
    }

    // 回滚修改
    private void rollback4Update(int aid, List<Param> list) {
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(list)) {
            return;
        }
        String[] sagaKeys = ProductSpecSkuEntity.getSagaKeys();
        FaiList<String> keys = new FaiList<>(Arrays.asList(sagaKeys));
        // 去除主键
        keys.remove(ProductSpecSkuEntity.Info.AID);
        keys.remove(ProductSpecSkuEntity.Info.SKU_ID);
        FaiList<Param> dataList = new FaiList<>();
        for (Param info : list) {
            Param data = new Param();
            long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(ProductSpecSkuEntity.Info.AID, aid);
            data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);

            dataList.add(data);
        }

        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch update err;flow=%d;aid=%d;dataList=%d", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚添加
    private void rollback4Add(int aid, List<Param> list) {
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(list)) {
            return;
        }
        FaiList<Long> skuIdList = Utils.getValList(new FaiList<>(list), ProductSpecSkuEntity.Info.SKU_ID);
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);

        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "delete err;flow=%d;aid=%d;skuIdList=%s", m_flow, aid, skuIdList);
        }
        restoreMaxId(aid, false);
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚删除
    private void rollback4Del(int aid, List<Param> list) {
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(list)) {
            return;
        }
        // 去除 Saga 字段
        for (Param info : list) {
            info.remove(SagaEntity.Common.XID);
            info.remove(SagaEntity.Common.BRANCH_ID);
            info.remove(SagaEntity.Common.SAGA_OP);
            info.remove(SagaEntity.Common.SAGA_TIME);
        }

        int rt = m_daoCtrl.batchInsert(new FaiList<>(list), null, false);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch insert err;flow=%d;aid=%d;list=%s", m_flow, aid, list);
        }
        Log.logStd("rollback del ok;flow=%;aid=%d", m_flow, aid);
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

    private Map<PrimaryKey, Param> sagaMap;

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

    private static final String JOINER = "-";
}
