package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuNumEntity;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuNumCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuNumDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class ProductSpecSkuNumProc {
    public ProductSpecSkuNumProc(ProductSpecSkuNumDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public ProductSpecSkuNumProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        m_flow = flow;
    }
    public int batchAdd(int aid, int unionPriId, FaiList<Param> skuNumInfoList) {
        int rt = Errno.ARGS_ERROR;
        for (Param skuNumInfo : skuNumInfoList) {
            skuNumInfo.setInt(ProductSpecSkuNumEntity.Info.AID, aid);
            skuNumInfo.setInt(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, unionPriId);
            cacheManage.setSkuIdDirty(aid, skuNumInfo.getLong(ProductSpecSkuNumEntity.Info.SKU_ID));
        }
        cacheManage.setDataStatusDirty(aid, unionPriId);
        rt = m_daoCtrl.batchInsert(skuNumInfoList);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;skuNumInfoList=%s;", skuNumInfoList);
            return rt;
        }
        Log.logStd("insert ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int refresh(int aid, int unionPriId, int pdId, Map<String, Long> newSkuNumSkuIdMap, FaiList<Long> needDelSkuNumSkuIdList, FaiList<Param> skuNumSortList, HashSet<Long> changeSkuNumSkuIdSet) {
        if(newSkuNumSkuIdMap.isEmpty() && needDelSkuNumSkuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.OK;
        cacheManage.setSkuIdListDirty(aid, changeSkuNumSkuIdSet);
        if(!needDelSkuNumSkuIdList.isEmpty()){ // 删除skuNum
            cacheManage.setDataStatusDirty(aid, unionPriId);

            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuNumEntity.Info.SKU_ID, ParamMatcher.IN, needDelSkuNumSkuIdList);
            rt = m_daoCtrl.delete(matcher);
            if(rt != Errno.OK){
                Log.logErr(rt, "delete err;flow=%s;unionPriId=%s;needDelSkuNumSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuNumSkuIdList);
                return rt;
            }
        }
        if(!newSkuNumSkuIdMap.isEmpty()){
            HashMap<Long, String> newSkuIdSkuNumMap = new HashMap<>(newSkuNumSkuIdMap.size()*4/3+1);
            Set<String> skuIdStrSet = new HashSet<>(newSkuNumSkuIdMap.size()*4/3+1);
            for (Map.Entry<String, Long> skuNumSkuIdEntry : newSkuNumSkuIdMap.entrySet()) {
                String skuNum = skuNumSkuIdEntry.getKey();
                Long skuId = skuNumSkuIdEntry.getValue();
                newSkuIdSkuNumMap.put(skuId, skuNum);
                skuIdStrSet.add(String.valueOf(skuId));
            }
            String querySql = "select * from " + m_daoCtrl.getTableName()
                    + " where "+ProductSpecSkuNumEntity.Info.AID+" = " + aid
                    + " and " +ProductSpecSkuNumEntity.Info.UNION_PRI_ID+ " = " + unionPriId
                    + " and " + ProductSpecSkuNumEntity.Info.SKU_NUM + " in ('" + Str.join("','", new FaiList<>(newSkuNumSkuIdMap.keySet())) + "')"
                    + " union all "
                    + " select * from " + m_daoCtrl.getTableName()
                    + " where "+ProductSpecSkuNumEntity.Info.AID+" = " + aid
                    + " and " +ProductSpecSkuNumEntity.Info.UNION_PRI_ID+ " = " + unionPriId
                    + " and " + ProductSpecSkuNumEntity.Info.SKU_ID + " in ('" + Str.join("','", new FaiList<>(skuIdStrSet)) + "')"
                    + ";" ;
            Log.logDbg("flow=%s;aid=%s;querySql=%s", m_flow, aid, querySql);
            Dao dao = m_daoCtrl.getDao();
            FaiList<Param> list = dao.executeQuery(querySql);

            // 已经存在的映射关系
            Map<String, Long> oldSkuNumSkuIdMap = new HashMap<>(list.size());
            for (Param info : list) {
                String skuNum = info.getString(ProductSpecSkuNumEntity.Info.SKU_NUM);
                long skuId = info.getLong(ProductSpecSkuNumEntity.Info.SKU_ID);
                oldSkuNumSkuIdMap.put(skuNum, skuId);
            }
            FaiList<Param> addDataList = new FaiList<>();
            Iterator<Map.Entry<String, Long>> itr = newSkuNumSkuIdMap.entrySet().iterator();
            while (itr.hasNext()){
                Map.Entry<String, Long> newSkuNumSkuIdEntry = itr.next();
                String skuNum = newSkuNumSkuIdEntry.getKey();
                long newSkuId = newSkuNumSkuIdEntry.getValue();
                Long oldSkuId = oldSkuNumSkuIdMap.remove(skuNum);
                if(oldSkuId != null){
                    if(newSkuId == oldSkuId){
                        itr.remove();
                    }else{
                        // 判断是否是替换，从要更新的里面能拿出来，如果有 就说明是要替换
                        String newSkuNum = newSkuIdSkuNumMap.get(oldSkuId);
                        if(newSkuNum == null){
                            rt = Errno.ALREADY_EXISTED;
                            Log.logErr(rt, "skuNum already;flow=%s;aid=%s;unionPriId=%s;skuNum=%s;oldSkuId=%s;newSkuId=%s;", m_flow, aid, unionPriId, skuNum, oldSkuId, newSkuId);
                            return rt;
                        }
                    }
                }else{
                    itr.remove();
                    addDataList.add(
                            new Param()
                            .setInt(ProductSpecSkuNumEntity.Info.AID, aid)
                            .setInt(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, unionPriId)
                            .setString(ProductSpecSkuNumEntity.Info.SKU_NUM, skuNum)
                            .setLong(ProductSpecSkuNumEntity.Info.SKU_ID, newSkuId)
                            .setInt(ProductSpecSkuNumEntity.Info.PD_ID, pdId)
                    );
                }
            }
            FaiList<Param> updateDataList = new FaiList<>(newSkuNumSkuIdMap.size());
            for (Map.Entry<String, Long> newSkuNumSkuIdEntry : newSkuNumSkuIdMap.entrySet()) {
                String skuNum = newSkuNumSkuIdEntry.getKey();
                long skuId = newSkuNumSkuIdEntry.getValue();
                updateDataList.add(
                        new Param()
                                .setLong(ProductSpecSkuNumEntity.Info.SKU_ID, skuId)

                                .setInt(ProductSpecSkuNumEntity.Info.AID, aid)
                                .setInt(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, unionPriId)
                                .setString(ProductSpecSkuNumEntity.Info.SKU_NUM, skuNum)
                );
            }
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;updateDataList=%s;", aid, unionPriId, updateDataList);
            FaiList<String> needDelList = new FaiList<>(oldSkuNumSkuIdMap.keySet());
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;needDelList=%s;", aid, unionPriId, needDelList);
            if(!needDelList.isEmpty()){
                cacheManage.setDataStatusDirty(aid, unionPriId);


                ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
                matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(ProductSpecSkuNumEntity.Info.SKU_NUM, ParamMatcher.IN, needDelList);

                rt = m_daoCtrl.delete(matcher);
                if(rt != Errno.OK){
                    Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;needDelList=%s;", needDelList);
                    return rt;
                }
                Log.logStd("delete ok!;flow=%s;aid=%s;unionPriId=%s;needDelList=%s;", m_flow, aid, unionPriId, needDelList);
            }
            if(!updateDataList.isEmpty()){
                cacheManage.setManageDirty(aid, unionPriId);

                ParamUpdater updater = new ParamUpdater();
                updater.getData().setString(ProductSpecSkuNumEntity.Info.SKU_ID, "?");

                ParamMatcher matcher = new ParamMatcher();
                matcher.and(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuNumEntity.Info.SKU_NUM, ParamMatcher.EQ, "?");
                rt = m_daoCtrl.batchUpdate(updater, matcher, updateDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt, "batchUpdate err;flow=%s;aid=%s;unionPriId=%s;updateDataList=%s;", updateDataList);
                    return rt;
                }
                Log.logStd("update ok!;flow=%s;aid=%s;unionPriId=%s;updateDataList=%s;", m_flow, aid, unionPriId, updateDataList);
            }
            if(!addDataList.isEmpty()){
                cacheManage.setDataStatusDirty(aid, unionPriId);

                rt = m_daoCtrl.batchInsert(addDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;addDataList=%s;", addDataList);
                    return rt;
                }
                Log.logStd("insert ok!;flow=%s;aid=%s;unionPriId=%s;addDataList=%s;", m_flow, aid, unionPriId, addDataList);
            }
        }
        if(!skuNumSortList.isEmpty()){ // 设置排序
            cacheManage.setManageDirty(aid, unionPriId);

            ParamUpdater updater = new ParamUpdater();
            updater.getData().setString(ProductSpecSkuNumEntity.Info.SORT, "?");

            ParamMatcher matcher = new ParamMatcher();
            matcher.and(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuNumEntity.Info.SKU_NUM, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(updater, matcher, skuNumSortList);
            if(rt != Errno.OK){
                Log.logErr(rt, "batchUpdate err;flow=%s;aid=%s;unionPriId=%s;skuNumSortList=%s;", m_flow, aid, unionPriId, skuNumSortList);
                return rt;
            }
            Log.logStd("set sort ok!;flow=%s;aid=%s;unionPriId=%s;skuNumSortList=%s;", m_flow, aid, unionPriId, skuNumSortList);
        }
        Log.logStd("ok!;flow=%s;aid=%s;unionPriId=%s;needDelSkuNumSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuNumSkuIdList);
        return rt;
    }

    public int batchDel(int aid, Integer unionPriId, FaiList<Long> skuIdList){
        if(skuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        cacheManage.setSkuIdListDirty(aid, skuIdList);
        if(unionPriId != null){
            cacheManage.setDataStatusDirty(aid, unionPriId);
        }else{
            Dao.SelectArg selectArg = new Dao.SelectArg();
            selectArg.field = " distinct " + ProductSpecSkuNumEntity.Info.UNION_PRI_ID ;
            selectArg.searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(selectArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "select err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
                return rt;
            }
            for (Param info : listRef.value) {
                Integer _unionPriId = info.getInt(ProductSpecSkuNumEntity.Info.UNION_PRI_ID);
                cacheManage.setDataStatusDirty(aid, _unionPriId);
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }



    public int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        int rt = Errno.ERROR;

        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        Log.logStd(rt,"ok;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }

    public int getSkuNumListFromDao(int aid, int unionPriId, FaiList<String> skuNumList, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuNumEntity.Info.SKU_NUM, ParamMatcher.IN, skuNumList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuNumEntity.Info.SKU_NUM);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuNumList=%s;", m_flow, aid, unionPriId, skuNumList);
            return rt;
        }
        Log.logDbg(rt,"ok;flow=%s;aid=%s;unionPriId=%s;skuNumList=%s;", m_flow, aid, unionPriId, skuNumList);
        return rt = Errno.OK;
    }

    public int searchByLikeSkuNum(int aid, int unionPriId, String skuNum, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuNumEntity.Info.SKU_NUM, ParamMatcher.LK, skuNum);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuNumEntity.Info.SKU_ID, ProductSpecSkuNumEntity.Info.SKU_NUM);
        if(rt != Errno.OK){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuNum=%s;", m_flow, aid, unionPriId, skuNum);
            return rt;
        }
        return rt;
    }

    public int getDataStatus(int aid, int unionPriId, Ref<Param> dataStatusRef){
        Param info = ProductSpecSkuNumCacheCtrl.DataStatusCache.get(aid, unionPriId);
        boolean needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
        boolean needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);
        int rt = Errno.ERROR;
        if(needInitTotalSize || needInitManage){
            try {
                LockUtil.readLock(aid);
                info = ProductSpecSkuNumCacheCtrl.DataStatusCache.get(aid, unionPriId);
                needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
                needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
                if(needInitTotalSize){
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
                    searchArg.matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    Ref<Integer> countRef = new Ref<>();
                    rt = m_daoCtrl.selectCount(searchArg, countRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "selectCount err;flow=%s;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
                        return rt;
                    }
                    info.setInt(DataStatus.Info.TOTAL_SIZE, countRef.value);
                }
                if(needInitManage){
                    info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
                }
                ProductSpecSkuNumCacheCtrl.DataStatusCache.set(aid, unionPriId, info);
            }finally {
                LockUtil.unReadLock(aid);
            }
        }else{
            rt = Errno.OK;
        }
        dataStatusRef.value = info;
        return rt;
    }

    public int getAllDataFromDao(int aid, int unionPriId, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuNumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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

    public int getList(int aid, FaiList<Long> skuIdList, Ref<Map<Long, FaiList<String>>> skuIdSkuNumListMapRef){
        if(skuIdList == null || skuIdList.isEmpty()){
            Log.logErr("arg err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return Errno.ARGS_ERROR;
        }
        HashMap<Long, FaiList<String>> resultMap = new HashMap<>();
        skuIdSkuNumListMapRef.value = resultMap;
        HashSet<Long> skuIdSet = new HashSet<>(skuIdList);
        ParamComparator comparator = new ParamComparator(ProductSpecSkuNumEntity.Info.SORT);

        getListFromCache(aid, resultMap, skuIdSet, comparator);
        if(skuIdSet.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        Map<Long, FaiList<Param>> map = null;
        try {
            LockUtil.readLock(aid);
            // double check
            getListFromCache(aid, resultMap, skuIdSet, comparator);
            if(skuIdSet.isEmpty()){
                return Errno.OK;
            }

            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDao(aid, new FaiList<>(skuIdSet), listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            map = new HashMap<>(listRef.value.size()*4/3+1);
            for (Param info : listRef.value) {
                Long skuId = info.getLong(ProductSpecSkuNumEntity.Info.SKU_ID);
                skuIdSet.remove(skuId);
                FaiList<Param> list = map.getOrDefault(skuId, new FaiList<>());
                list.add(info);
                if(list.size() == 1){
                    map.put(skuId, list);
                }
            }
            for (Long skuId : skuIdSet) {
                map.put(skuId, new FaiList<>());
            }
            ProductSpecSkuNumCacheCtrl.setInfoCache(aid, map);
        }finally {
            LockUtil.unReadLock(aid);
        }
        map.forEach((skuId, skuNumInfoList)->{
            comparator.sort(skuNumInfoList);
            resultMap.put(skuId, Utils.getValList(skuNumInfoList, ProductSpecSkuNumEntity.Info.SKU_NUM));
        });
        return rt;
    }

    private ParamComparator getListFromCache(int aid, HashMap<Long, FaiList<String>> resultMap, HashSet<Long> skuIdSet, ParamComparator comparator) {
        Map<Long, FaiList<Param>> cacheMap = ProductSpecSkuNumCacheCtrl.getInfoCache(aid, skuIdSet);
        cacheMap.forEach((skuId, skuNumInfoList)->{
            comparator.sort(skuNumInfoList);
            resultMap.put(skuId, Utils.getValList(skuNumInfoList, ProductSpecSkuNumEntity.Info.SKU_NUM));
            skuIdSet.remove(skuId);
        });
        return comparator;
    }

    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }

    private int m_flow;
    private ProductSpecSkuNumDaoCtrl m_daoCtrl;

    private CacheManage cacheManage = new CacheManage();

    private static class CacheManage{

        public CacheManage() {
            init();
        }

        Map<Integer, Integer> unionPriIdDataFlagMap;
        Set<Long> skuIdSet;

        public boolean deleteDirtyCache(int aid){
            try {
                unionPriIdDataFlagMap.forEach((unionPriId, dataFlag)->{
                    ProductSpecSkuNumCacheCtrl.DataStatusCache.clearDirty(aid, unionPriId, dataFlag);
                });
                ProductSpecSkuNumCacheCtrl.delInfoCache(aid, skuIdSet);
            }finally {
                init();
            }
            return false;
        }

        public void setDataStatusDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            dataFlag |= ProductSpecSkuNumCacheCtrl.DataStatusCache.DataFlag.TOTAL_SIZE;
            dataFlag |= ProductSpecSkuNumCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME;
            unionPriIdDataFlagMap.put(unionPriId, dataFlag);
        }

        public void setManageDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            unionPriIdDataFlagMap.put(unionPriId, dataFlag|ProductSpecSkuNumCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME);
        }

        private void init() {
            unionPriIdDataFlagMap = new HashMap<>();
            skuIdSet = new HashSet<>();
        }

        public void setSkuIdListDirty(int aid, Collection<Long> skuIdList) {
            skuIdSet.addAll(skuIdList);
        }
        public void setSkuIdDirty(int aid, Long skuId) {
            skuIdSet.add(skuId);
        }


    }
}
