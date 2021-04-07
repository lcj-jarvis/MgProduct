package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuCodeEntity;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCodeCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCodeDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class ProductSpecSkuCodeProc {
    public ProductSpecSkuCodeProc(ProductSpecSkuCodeDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public ProductSpecSkuCodeProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        m_flow = flow;
    }
    public int batchAdd(int aid, int unionPriId, FaiList<Param> skuCodeInfoList) {
        int rt = Errno.ARGS_ERROR;
        for (Param skuCodeInfo : skuCodeInfoList) {
            skuCodeInfo.setInt(ProductSpecSkuCodeEntity.Info.AID, aid);
            skuCodeInfo.setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId);
            cacheManage.setSkuIdDirty(aid, skuCodeInfo.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID));
        }
        cacheManage.setDataStatusDirty(aid, unionPriId);
        rt = m_daoCtrl.batchInsert(skuCodeInfoList);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;skuCodeInfoList=%s;", m_flow, aid, unionPriId, skuCodeInfoList);
            return rt;
        }
        Log.logStd("insert ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int refresh(int aid, int unionPriId, int pdId, Map<String, Long> newSkuCodeSkuIdMap, FaiList<Long> needDelSkuCodeSkuIdList, FaiList<Param> skuCodeSortList, HashSet<Long> changeSkuCodeSkuIdSet) {
        if(newSkuCodeSkuIdMap.isEmpty() && needDelSkuCodeSkuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.OK;
        cacheManage.setSkuIdListDirty(aid, changeSkuCodeSkuIdSet);
        if(!needDelSkuCodeSkuIdList.isEmpty()){ // 删除skuCode
            cacheManage.setDataStatusDirty(aid, unionPriId);

            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, needDelSkuCodeSkuIdList);
            rt = m_daoCtrl.delete(matcher);
            if(rt != Errno.OK){
                Log.logErr(rt, "delete err;flow=%s;unionPriId=%s;needDelSkuCodeSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuCodeSkuIdList);
                return rt;
            }
        }
        if(!newSkuCodeSkuIdMap.isEmpty()){
            HashMap<Long, String> newSkuIdSkuCodeMap = new HashMap<>(newSkuCodeSkuIdMap.size()*4/3+1);
            for (Map.Entry<String, Long> skuCodeSkuIdEntry : newSkuCodeSkuIdMap.entrySet()) {
                String skuCode = skuCodeSkuIdEntry.getKey();
                Long skuId = skuCodeSkuIdEntry.getValue();
                newSkuIdSkuCodeMap.put(skuId, skuCode);
            }
            // 查询 本次修改到所关联的所有旧数据
            StringBuilder skuCodeSetPlaceholder = new StringBuilder();
            StringBuilder skuIdSetPlaceholder = new StringBuilder();

            genPlaceholder4refresh(newSkuCodeSkuIdMap, newSkuIdSkuCodeMap, skuCodeSetPlaceholder, skuIdSetPlaceholder);

            String prepareSql = "select * from " + m_daoCtrl.getTableName()
                    + " where "+ ProductSpecSkuCodeEntity.Info.AID+" = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID+ " = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.SKU_CODE + " in " + skuCodeSetPlaceholder.toString()
                    + " union all "
                    + " select * from " + m_daoCtrl.getTableName()
                    + " where "+ ProductSpecSkuCodeEntity.Info.AID+" = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID+ " = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.SKU_ID + " in " + skuIdSetPlaceholder.toString()
                    + ";" ;

            Dao dao = m_daoCtrl.getDao();
            PreparedStatement preparedStatement = dao.prepareStatement(prepareSql);

            rt = setPrepareStatement4refresh(preparedStatement, aid, unionPriId, newSkuCodeSkuIdMap.keySet(), newSkuIdSkuCodeMap.keySet());
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Param> list = dao.executeQuery(preparedStatement, null);

            // 已经存在的映射关系
            Map<String/*oldSkuCode*/, Long /*skuId*/> oldSkuCodeSkuIdMap = new HashMap<>(list.size());
            for (Param info : list) {
                String skuCode = info.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE);
                long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
                oldSkuCodeSkuIdMap.put(skuCode, skuId);
            }
            FaiList<Param> addDataList = new FaiList<>();
            Iterator<Map.Entry<String, Long>> itr = newSkuCodeSkuIdMap.entrySet().iterator();
            while (itr.hasNext()){
                Map.Entry<String, Long> newSkuCodeSkuIdEntry = itr.next();
                String skuCode = newSkuCodeSkuIdEntry.getKey();
                long newSkuId = newSkuCodeSkuIdEntry.getValue();
                Long oldSkuId = oldSkuCodeSkuIdMap.remove(skuCode);
                if(oldSkuId != null){
                    if(newSkuId == oldSkuId){
                        itr.remove();
                    }else{
                        // 判断是否是替换，从要更新的里面能拿出来，如果有 就说明是要替换
                        String newSkuCode = newSkuIdSkuCodeMap.get(oldSkuId);
                        if(newSkuCode == null){
                            rt = Errno.ALREADY_EXISTED;
                            Log.logErr(rt, "skuCode already;flow=%s;aid=%s;unionPriId=%s;skuCode=%s;oldSkuId=%s;newSkuId=%s;", m_flow, aid, unionPriId, skuCode, oldSkuId, newSkuId);
                            return rt;
                        }
                    }
                }else{
                    itr.remove();
                    addDataList.add(
                            new Param()
                            .setInt(ProductSpecSkuCodeEntity.Info.AID, aid)
                            .setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId)
                            .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                            .setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, newSkuId)
                            .setInt(ProductSpecSkuCodeEntity.Info.PD_ID, pdId)
                    );
                }
            }
            FaiList<Param> updateDataList = new FaiList<>(newSkuCodeSkuIdMap.size());
            for (Map.Entry<String, Long> newSkuCodeSkuIdEntry : newSkuCodeSkuIdMap.entrySet()) {
                String skuCode = newSkuCodeSkuIdEntry.getKey();
                long skuId = newSkuCodeSkuIdEntry.getValue();
                updateDataList.add(
                        new Param()
                                .setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, skuId)

                                .setInt(ProductSpecSkuCodeEntity.Info.AID, aid)
                                .setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId)
                                .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                );
            }
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;updateDataList=%s;", aid, unionPriId, updateDataList);
            FaiList<String> needDelList = new FaiList<>(oldSkuCodeSkuIdMap.keySet());
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;needDelList=%s;", aid, unionPriId, needDelList);
            if(!needDelList.isEmpty()){
                cacheManage.setDataStatusDirty(aid, unionPriId);


                ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, needDelList);

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
                updater.getData().setString(ProductSpecSkuCodeEntity.Info.SKU_ID, "?");

                ParamMatcher matcher = new ParamMatcher();
                matcher.and(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, "?");
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
        if(!skuCodeSortList.isEmpty()){ // 设置排序
            cacheManage.setManageDirty(aid, unionPriId);

            ParamUpdater updater = new ParamUpdater();
            updater.getData().setString(ProductSpecSkuCodeEntity.Info.SORT, "?");

            ParamMatcher matcher = new ParamMatcher();
            matcher.and(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.doBatchUpdate(updater, matcher, skuCodeSortList, false);
            if(rt != Errno.OK){
                Log.logErr(rt, "doBatchUpdate err;flow=%s;aid=%s;unionPriId=%s;skuCodeSortList=%s;", m_flow, aid, unionPriId, skuCodeSortList);
                return rt;
            }
            Log.logStd("set sort ok!;flow=%s;aid=%s;unionPriId=%s;skuCodeSortList=%s;", m_flow, aid, unionPriId, skuCodeSortList);
        }
        Log.logStd("ok!;flow=%s;aid=%s;unionPriId=%s;needDelSkuCodeSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuCodeSkuIdList);
        return rt;
    }

    /**
     * 填充占位符
     */
    private int setPrepareStatement4refresh(PreparedStatement preparedStatement, int aid, int unionPriId, Set<String> skuCodeSet, Set<Long> skuIdSet){
        int rt = Errno.OK;
        try {
            int start = 1;
            preparedStatement.setInt(start++, aid);
            preparedStatement.setInt(start++, unionPriId);
            for (String skuCode : skuCodeSet) {
                preparedStatement.setString(start++, skuCode);
            }
            preparedStatement.setInt(start++, aid);
            preparedStatement.setInt(start++, unionPriId);
            for (Long skuId : skuIdSet) {
                preparedStatement.setLong(start++, skuId);
            }
        } catch (SQLException exp) {
            rt = Errno.DAO_SQL_ERROR;
            Log.logErr(rt, exp, "code=" + exp.getErrorCode());
            return rt;
        } catch (Exception exp) {
            rt = Errno.DAO_SQL_ERROR;
            Log.logErr(rt, exp);
            return rt;
        }
        return rt;
    }

    /**
     * 生成占位符
     */
    private void genPlaceholder4refresh(Map<String, Long> newSkuCodeSkuIdMap, HashMap<Long, String> newSkuIdSkuCodeMap, StringBuilder skuCodeSetPlaceholder, StringBuilder skuIdSetPlaceholder) {
        skuCodeSetPlaceholder.append("(");
        int len = newSkuCodeSkuIdMap.keySet().size();
        for (int i = 0; i < len; i++) {
            if(i != 0){
                skuCodeSetPlaceholder.append(", ");
            }
            skuCodeSetPlaceholder.append("?");
        }
        skuCodeSetPlaceholder.append(")");

        skuIdSetPlaceholder.append("(");
        len = newSkuIdSkuCodeMap.keySet().size();
        for (int i = 0; i < len; i++) {
            if(i != 0){
                skuIdSetPlaceholder.append(", ");
            }
            skuIdSetPlaceholder.append("?");
        }
        skuIdSetPlaceholder.append(")");
    }

    public int batchDel(int aid, Integer unionPriId, FaiList<Long> skuIdList){
        if(skuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        cacheManage.setSkuIdListDirty(aid, skuIdList);
        if(unionPriId != null){
            cacheManage.setDataStatusDirty(aid, unionPriId);
        }else{
            Dao.SelectArg selectArg = new Dao.SelectArg();
            selectArg.field = " distinct " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID ;
            selectArg.searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(selectArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "select err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
                return rt;
            }
            for (Param info : listRef.value) {
                Integer _unionPriId = info.getInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
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

        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
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

    public int getSkuCodeListFromDao(int aid, int unionPriId, FaiList<String> skuCodeList, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, skuCodeList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_CODE);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuCodeList=%s;", m_flow, aid, unionPriId, skuCodeList);
            return rt;
        }
        Log.logDbg(rt,"ok;flow=%s;aid=%s;unionPriId=%s;skuCodeList=%s;", m_flow, aid, unionPriId, skuCodeList);
        return rt = Errno.OK;
    }

    public int searchBySkuCode(int aid, int unionPriId, String skuCode, boolean isFuzzySearch, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        if(isFuzzySearch){
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.LK, skuCode);
        }else{
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, skuCode);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_ID, ProductSpecSkuCodeEntity.Info.SKU_CODE);
        if(rt != Errno.OK){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuCode=%s;", m_flow, aid, unionPriId, skuCode);
            return rt;
        }
        return rt;
    }

    public int getDataStatus(int aid, int unionPriId, Ref<Param> dataStatusRef){
        Param info = ProductSpecSkuCodeCacheCtrl.DataStatusCache.get(aid, unionPriId);
        boolean needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
        boolean needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);
        int rt = Errno.ERROR;
        if(needInitTotalSize || needInitManage){
            try {
                LockUtil.readLock(aid);
                info = ProductSpecSkuCodeCacheCtrl.DataStatusCache.get(aid, unionPriId);
                needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
                needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
                if(needInitTotalSize){
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                    searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
                ProductSpecSkuCodeCacheCtrl.DataStatusCache.set(aid, unionPriId, info);
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
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
    public int getSkuIdListFromDao(int aid, int unionPriId, FaiList<Integer> pdIdList, Ref<FaiList<Long>> skuIdListRef) {
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuCodeEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        FaiList<Long> skuIdList = new FaiList<>(listRef.value.size());
        for (Param skuIdInfo : listRef.value) {
            skuIdList.add(skuIdInfo.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID));
        }
        skuIdListRef.value = skuIdList;
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt = Errno.OK;
    }

    public int getList(int aid, FaiList<Long> skuIdList, Ref<Map<Long, FaiList<String>>> skuIdSkuCodeListMapRef){
        if(skuIdList == null || skuIdList.isEmpty()){
            Log.logErr("arg err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return Errno.ARGS_ERROR;
        }
        HashMap<Long, FaiList<String>> resultMap = new HashMap<>();
        skuIdSkuCodeListMapRef.value = resultMap;
        HashSet<Long> skuIdSet = new HashSet<>(skuIdList);
        ParamComparator comparator = new ParamComparator(ProductSpecSkuCodeEntity.Info.SORT);

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
                Long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
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
            ProductSpecSkuCodeCacheCtrl.setInfoCache(aid, map);
        }finally {
            LockUtil.unReadLock(aid);
        }
        map.forEach((skuId, skuCodeInfoList)->{
            comparator.sort(skuCodeInfoList);
            resultMap.put(skuId, Utils.getValList(skuCodeInfoList, ProductSpecSkuCodeEntity.Info.SKU_CODE));
        });
        return rt;
    }

    private ParamComparator getListFromCache(int aid, HashMap<Long, FaiList<String>> resultMap, HashSet<Long> skuIdSet, ParamComparator comparator) {
        Map<Long, FaiList<Param>> cacheMap = ProductSpecSkuCodeCacheCtrl.getInfoCache(aid, skuIdSet);
        cacheMap.forEach((skuId, skuCodeInfoList)->{
            comparator.sort(skuCodeInfoList);
            resultMap.put(skuId, Utils.getValList(skuCodeInfoList, ProductSpecSkuCodeEntity.Info.SKU_CODE));
            skuIdSet.remove(skuId);
        });
        return comparator;
    }

    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }

    private int m_flow;
    private ProductSpecSkuCodeDaoCtrl m_daoCtrl;

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
                    ProductSpecSkuCodeCacheCtrl.DataStatusCache.clearDirty(aid, unionPriId, dataFlag);
                });
                ProductSpecSkuCodeCacheCtrl.delInfoCache(aid, skuIdSet);
            }finally {
                init();
            }
            return false;
        }

        public void setDataStatusDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            dataFlag |= ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.TOTAL_SIZE;
            dataFlag |= ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME;
            unionPriIdDataFlagMap.put(unionPriId, dataFlag);
        }

        public void setManageDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            unionPriIdDataFlagMap.put(unionPriId, dataFlag| ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME);
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
