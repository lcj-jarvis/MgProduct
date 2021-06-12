package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuValObj;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class ProductSpecSkuProc {
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public ProductSpecSkuProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        m_flow = flow;
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap) {
        return batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap, null);
    }
    public int batchAdd(int aid, Map<Integer, FaiList<Param>> pdIdPdScSkuListMap, Map<Integer, Map<String, Long>> pdIdInPdScStrIdListJsonSkuIdMap, Ref<FaiList<Long>> skuIdListRef) {
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
        rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchInsert error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIdPdScSkuListMap.keySet());
        return rt;
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList) {
        return batchAdd(aid, Collections.singletonMap(pdId, infoList), null);
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

    /**
     * 批量生成代表spu的sku
     * @param aid
     * @param tid
     * @param unionPriId
     * @return
     */
    public int batchGenSkuRepresentSpuInfo(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, Map<Integer/*pdId*/, Long/*skuId*/> pdIdSkuIdMap){
        int rt = Errno.ERROR;
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
            rt = m_daoCtrl.batchInsert(addList);
            if(rt != Errno.OK){
                Log.logErr(rt,"dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
                return rt;
            }
        }else{
            rt = Errno.OK;
        }
        Log.logStd("ok;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }
    public int genSkuRepresentSpuInfo(int aid, int tid, int unionPriId, int pdId, Ref<Long> skuIdRef){
        int rt = Errno.ERROR;
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
        rt = batchGenSkuRepresentSpuInfo(aid, tid, unionPriId, pdIdList, pdIdSkuIdMap);
        if(rt != Errno.OK){
            return rt;
        }
        skuIdRef.value = pdIdSkuIdMap.get(pdId);
        Log.logStd("ok;flow=%d;aid=%d;tid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, tid, unionPriId, pdId);
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean softDel) {
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        { // 获取skuIdList
            Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
            int rt = getListFromDaoByPdIdList(aid, pdIdList, true, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Long> skuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
            cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
            deletedSkuIdSet.addAll(skuIdList);
            cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        }
        int rt = Errno.ERROR;
        if(softDel){
            ParamUpdater updater = new ParamUpdater();
            updater.getData().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEL);
            rt = m_daoCtrl.update(updater, delMatcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "dao.update error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
                return rt;
            }
        }else{
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
    public int batchSoftDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
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
                delSkuIdList =  Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
            }
        }
        deletedSkuIdSet.addAll(delSkuIdList);
        cacheManage.addNeedDelCachedSkuIdList(aid, delSkuIdList);
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        Log.logDbg("delMatcher.sql=%s;delMatcher.json=%s;", delMatcher.getSql(), delMatcher.toJson());
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setInt(ProductSpecSkuEntity.Info.STATUS, ProductSpecSkuValObj.Status.DEL);
        int rt = m_daoCtrl.update(updater, delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.update error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }

    public int refreshSku(int aid, int tid, int unionPriId, int pdId, FaiList<FaiList<Integer>> skuList) {
        if(aid <= 0 || pdId <= 0 || skuList == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%s;skuList=%s;", m_flow, aid, pdId, skuList);
            return Errno.ARGS_ERROR;
        }
        int rt = batchSoftDel(aid, pdId, null);
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
        rt = batchAdd(aid, pdId, infoList);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList){
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("arg error;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
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
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, "?");

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

    public int updateAllowEmptySku(int aid, int tid, int unionPriId, int pdId, FaiList<Integer> inPdScStrIdList) {
        int rt = Errno.ERROR;
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
        rt = batchSoftDel(aid, pdId, delSkuIdList);
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
        //matcher.and(ProductSpecSkuEntity.Info.STATUS, ParamMatcher.EQ, ProductSpecSkuValObj.Status.DEFAULT);
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
            if(cacheList != null){
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
        Set<Integer> pdIdSet = new HashSet<>(pdIdList);
        getSpuInfoListFromCache(aid, pdIdList, skuIdList, pdIdSet);
        if(!pdIdSet.isEmpty()){
            try {
                LockUtil.readLock(aid);
                getSpuInfoListFromCache(aid, pdIdList, skuIdList, pdIdSet);
                if(!pdIdSet.isEmpty()){
                    ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
                    matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, new FaiList<>(pdIdSet));
                    matcher.and(ProductSpecSkuEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecSkuValObj.FLag.SPU, ProductSpecSkuValObj.FLag.SPU);
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = matcher;
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID);
                    if(rt != Errno.OK &&rt != Errno.NOT_FOUND){
                        Log.logErr(rt, "error;flow=%d;aid=%s;pdIdSet=%s;", m_flow, aid, pdIdSet);
                        return rt;
                    }
                    Map<Integer, Long> map = new HashMap<>(listRef.value.size()*4/3+1);
                    for (Param info : listRef.value) {
                        int pdId = info.getInt(ProductSpecSkuEntity.Info.PD_ID);
                        long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                        map.put(pdId, skuId);
                        skuIdList.add(skuId);
                        pdIdSet.remove(pdId);
                    }
                    for (Integer pdId : pdIdSet) {
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

    private void getSpuInfoListFromCache(int aid, FaiList<Integer> pdIdList, FaiList<Long> skuIdList, Set<Integer> pdIdSet) {
        Map<Integer, Long> pdIdSkuIdMap = ProductSpecSkuCacheCtrl.batchGetSkuIdRepresentSpu(aid, pdIdList, true);
        pdIdSkuIdMap.forEach((pdId, skuId)->{
            if(skuId != 0){
                skuIdList.add(skuId);
            }
            pdIdSet.remove(pdId);
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

    /**
     * 获取被删除的skuIdList
     */
    public FaiList<Long> getDeletedSkuIdList(){
        return new FaiList<>(deletedSkuIdSet);
    }

    private int m_flow;
    private CacheManage cacheManage = new CacheManage();
    private ProductSpecSkuDaoCtrl m_daoCtrl;
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
