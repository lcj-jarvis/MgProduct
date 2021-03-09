package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuValObj;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.comm.util.*;

import java.util.*;

public class ProductSpecSkuProc {
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, FaiList<Long> rtIdList) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("batchAdd error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(ProductSpecSkuEntity.Info.AID, aid);
            data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
            Long skuId = m_daoCtrl.buildId();
            if(skuId == null){
                Log.logErr("batchAdd arg error;flow=%d;aid=%s;skuId=%s;info=%s;", m_flow, aid, skuId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(skuId);
            }
            data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            data.assign(info, ProductSpecSkuEntity.Info.SORT);
            data.assign(info, ProductSpecSkuEntity.Info.SOURCE_TID);
            data.assign(info, ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID);
            data.assign(info, ProductSpecSkuEntity.Info.SKU_NUM); // TODO
            FaiList<Integer> inPdScStrIdList = info.getListNullIsEmpty(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
            int flag = info.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
            if(inPdScStrIdList.isEmpty()){
                flag |= ProductSpecSkuValObj.FLag.EMPTY;
            }
            info.setInt(ProductSpecSkuEntity.Info.FLAG, flag);
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
            dataList.add(data);
        }
        cacheManage.addNeedDelCachedSkuIdList(aid, rtIdList);
        cacheManage.addNeedCachedPdId(aid, pdId);
        int rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("batchAdd ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
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
                    Log.logStd(rt,"batchReplace4SpuToSku arg error;flow=%d;aid=%s;skuId=%s;pdScSkuInfo=%s;", m_flow, aid, skuId, pdScSkuInfo);
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
                data.assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SKU_NUM);
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

    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty()){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        { // 获取skuIdList
            Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
            int rt = getListFromDaoByPdIdList(aid, pdIdList, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Long> skuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
            cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
            cacheManage.addNeedCachedPdIdList(aid, pdIdList);
        }
        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }
    public int batchDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
        if(aid <= 0 || pdId <= 0){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(delSkuIdList != null){
            if(delSkuIdList.isEmpty()){
                return Errno.OK;
            }
            delMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);
        }else{
            { // 获取skuIdList
                Ref<FaiList<Param>> skuIdInfoListRef = new Ref<>();
                int rt = getListFromDao(aid, pdId, skuIdInfoListRef, ProductSpecSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Long> skuIdList = Utils.getValList(skuIdInfoListRef.value, ProductSpecSkuEntity.Info.SKU_ID);
                cacheManage.addNeedDelCachedSkuIdList(aid, skuIdList);
            }
        }
        cacheManage.addNeedDelCachedSkuIdList(aid, delSkuIdList);
        cacheManage.addNeedCachedPdId(aid, pdId);
        Log.logDbg("delMatcher.sql=%s;delMatcher.json=%s;", delMatcher.getSql(), delMatcher.toJson());
        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }

    public int refreshSku(int aid, int tid, int unionPriId, int pdId, FaiList<FaiList<Integer>> skuList, FaiList<Long> rtIdList) {
        if(aid <= 0 || pdId <= 0 || skuList == null){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdId=%s;skuList=%s;", m_flow, aid, pdId, skuList);
            return Errno.ARGS_ERROR;
        }
        int rt = batchDel(aid, pdId, null);
        if(rt != Errno.OK) {
            return rt;
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
        rt = batchAdd(aid, pdId, infoList, rtIdList);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("refreshSku ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList){
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("batchDel error;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Utils.validUpdaterList(updaterList, ProductSpecSkuEntity.getValidKeys(), data->{
            skuIdList.add(data.getLong(ProductSpecSkuEntity.Info.SKU_ID));
        });

        maxUpdaterKeys.remove(ProductSpecSkuEntity.Info.SKU_ID);

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
        cacheManage.addNeedCachedPdId(aid, pdId);
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int getListFromDaoByPdIdList(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getListFromDao error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(pdScSkuInfoListRef.value, onlyNeedFields);
        Log.logStd(rt,"getListFromDao ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return Errno.OK;
    }
    public int getListFromDao(int aid, int pdId, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        return getListFromDao(aid, pdId, null, pdScSkuInfoListRef, onlyNeedFields);
    }

    public int getListFromDao(int aid, int pdId, FaiList<Long> skuIdList, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(skuIdList != null){
            matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        }
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getListFromDao error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(pdScSkuInfoListRef.value, onlyNeedFields);
        Log.logStd(rt,"getListFromDao ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return Errno.OK;
    }
    public int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getListFromDao error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(listRef.value, onlyNeedFields);
        Log.logStd(rt,"getListFromDao ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return Errno.OK;
    }
    public int getListByScStrIdListFromDao(int aid, int pdId, FaiList<String> inPdScStrIdLists, Ref<FaiList<Param>> pdScSkuInfoListRef, String ... onlyNeedFields) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, ParamMatcher.IN, inPdScStrIdLists);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef, onlyNeedFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getListByScStrIdListFromDao error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd(rt,"getListByScStrIdListFromDao ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
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

    public int getList(int aid, int pdId, Ref<FaiList<Param>> listRef){
        if(pdId <= 0 || listRef == null){
            Log.logErr("getList arg error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> cacheList = ProductSpecSkuCacheCtrl.getListByPdId(aid, pdId);
        if(cacheList != null){
            listRef.value = cacheList;
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        rt = getListFromDao(aid, pdId, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        ProductSpecSkuCacheCtrl.initInfoWithRelCache(aid, pdId, listRef.value);
        return rt;
    }
    public int getList(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logErr("getList arg error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return Errno.ARGS_ERROR;
        }
        Set<Long> skuIdSet = new HashSet<>(skuIdList);
        FaiList<Param> cacheList = ProductSpecSkuCacheCtrl.getList(aid, skuIdList);
        FaiList<Param> resultList = new FaiList<>();
        if(cacheList != null){
            for (Param cacheInfo : cacheList) {
                if(!isEmptyCacheInfo(cacheInfo)){
                    resultList.add(cacheInfo);
                }
                skuIdSet.remove(cacheInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID));
            }
        }
        int rt = Errno.ERROR;
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
        listRef.value = resultList;
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

    private int m_flow;
    private CacheManage cacheManage = new CacheManage();
    private ProductSpecSkuDaoCtrl m_daoCtrl;

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
        private void addNeedCachedPdId(int aid, int pdId){
            addNeedCachedPdIdSet(aid, new HashSet<>(Arrays.asList(pdId)));
        }
        private void addNeedCachedPdIdList(int aid, FaiList<Integer> pdIdList){
            if(pdIdList == null || pdIdList.isEmpty()){
                return;
            }
            addNeedCachedPdIdSet(aid, new HashSet<>(pdIdList));
        }
        private void addNeedCachedPdIdSet(int aid, Set<Integer> pdIdSet){
            if(pdIdSet == null || pdIdSet.isEmpty()){
                return;
            }
            ProductSpecSkuCacheCtrl.setRelCacheDirty(aid);
            needDelPdIdSet.addAll(pdIdSet);
        }

        private boolean deleteDirtyCache(int aid){
            try {
                boolean boo = ProductSpecSkuCacheCtrl.delRelCache(aid, needDelPdIdSet);
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
