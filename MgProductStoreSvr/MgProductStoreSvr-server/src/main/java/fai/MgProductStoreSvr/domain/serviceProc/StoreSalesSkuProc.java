package fai.MgProductStoreSvr.domain.serviceProc;


import fai.MgProductStoreSvr.domain.comm.Misc2;
import fai.MgProductStoreSvr.domain.comm.PdKey;
import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuValObj;
import fai.MgProductStoreSvr.domain.repository.StoreSalesSkuDaoCtrl;
import fai.comm.util.*;

import java.util.*;


public class StoreSalesSkuProc {
    public StoreSalesSkuProc(StoreSalesSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public int batchAdd(int aid, Integer pdId, FaiList<Param> infoList) {
        if(aid <= 0 || (pdId != null && pdId <= 0) || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd arg error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(StoreSalesSkuEntity.Info.AID, aid);
            data.assign(info, StoreSalesSkuEntity.Info.UNION_PRI_ID);
            data.assign(info, StoreSalesSkuEntity.Info.RL_PD_ID);
            data.assign(info, StoreSalesSkuEntity.Info.SKU_ID);
            if(pdId == null){
                pdId = info.getInt(StoreSalesSkuEntity.Info.PD_ID);
                if(pdId == null || pdId <= 0){
                    Log.logStd("batchAdd pdId arg error;flow=%d;aid=%s;pdId=%s;info=%s;", m_flow, aid, pdId, info);
                    return Errno.ARGS_ERROR;
                }
            }
            data.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
            data.assign(info, StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID);
            data.assign(info, StoreSalesSkuEntity.Info.SKU_TYPE);
            data.assign(info, StoreSalesSkuEntity.Info.SORT);
            data.assign(info, StoreSalesSkuEntity.Info.COUNT);
            data.assign(info, StoreSalesSkuEntity.Info.REMAIN_COUNT);
            data.assign(info, StoreSalesSkuEntity.Info.HOLDING_COUNT);
            Long price = info.getLong(StoreSalesSkuEntity.Info.PRICE);
            if(price != null){
                info.setInt(StoreSalesSkuEntity.Info.FLAG, info.getInt(StoreSalesSkuEntity.Info.FLAG, 0) | StoreSalesSkuValObj.FLag.SETED_PRICE);
            }
            data.assign(info, StoreSalesSkuEntity.Info.PRICE);
            data.assign(info, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
            data.assign(info, StoreSalesSkuEntity.Info.MIN_AMOUNT);
            data.assign(info, StoreSalesSkuEntity.Info.MAX_AMOUNT);
            data.assign(info, StoreSalesSkuEntity.Info.DURATION);
            data.assign(info, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
            data.assign(info, StoreSalesSkuEntity.Info.FLAG);
            data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);
            data.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        Log.logStd("batchAdd ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int batchSynchronousSPU2SKU(int aid, Map<Integer, Map<Long, Param>> unionPriId_skuId_salesStoreDataMapMap, Set<String> maxUpdateFieldSet) {
        if(aid <= 0 || unionPriId_skuId_salesStoreDataMapMap == null || unionPriId_skuId_salesStoreDataMapMap.isEmpty() || maxUpdateFieldSet == null || maxUpdateFieldSet.isEmpty()){
            Log.logStd("batchSynchronous arg error;flow=%d;aid=%s;unionPriId_skuId_salesStoreDataMapMap=%s;maxUpdateFieldSet=%s;", m_flow, aid, unionPriId_skuId_salesStoreDataMapMap, maxUpdateFieldSet);
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
                    Log.logStd(rt, "m_daoCtrl batchUpdate err;flow=%s;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
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
                    for (String field : maxUpdateFieldSet) {
                        data.assign(info, field);
                    }
                    addDataList.add(data);
                }
                rt = m_daoCtrl.batchInsert(addDataList, null, true);
                if(rt != Errno.OK){
                    Log.logStd(rt, "m_daoCtrl batchInsert err;flow=%s;aid=%s;addDataList=%s;", m_flow, aid, addDataList);
                    return rt;
                }
            }

            Log.logStd("doing;flow=%s;aid=%s;unionPruId=%s;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
        if(aid <= 0 || pdId <= 0 || delSkuIdList == null || delSkuIdList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }


    public int batchSet(int aid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList) {
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logStd("batchSet error;flow=%d;aid=%s;pdId=%s;updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Misc2.validUpdaterList(updaterList, StoreSalesSkuEntity.getValidKeys(), data->{
            skuIdList.add(data.getLong(StoreSalesSkuEntity.Info.SKU_ID));
        });
        if(maxUpdaterKeys.contains(StoreSalesSkuEntity.Info.PRICE)){
            maxUpdaterKeys.add(StoreSalesSkuEntity.Info.FLAG);
        }
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDaoByPdIdAndSkuIdList(aid, pdId, skuIdList, listRef, maxUpdaterKeys.toArray(new String[]{}));
        if(rt != Errno.OK){
            return rt;
        }
        rt = Errno.OK;
        Map<Integer, Param> oldDataMap = Misc2.getMap(listRef.value, StoreSalesSkuEntity.Info.SKU_ID);
        listRef.value = null; // help gc

        maxUpdaterKeys.remove(StoreSalesSkuEntity.Info.SKU_ID);
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        updaterList.forEach(updater -> {
            long skuId = updater.getData().getLong(StoreSalesSkuEntity.Info.SKU_ID);
            Param oldData = oldDataMap.remove(skuId); // help gc
            Param updatedData = updater.update(oldData, true);
            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            if(data.containsKey(StoreSalesSkuEntity.Info.PRICE)){
                int flag = data.getInt(StoreSalesSkuEntity.Info.FLAG, 0);
                flag |= StoreSalesSkuValObj.FLag.SETED_PRICE;
                data.setInt(StoreSalesSkuEntity.Info.FLAG, flag);
                Log.logDbg("whalelog data=%s", data);
            }

            data.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(StoreSalesSkuEntity.Info.AID, aid);
                data.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                data.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                data.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
            }

            dataList.add(data);
        });

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logDbg("whalelog flow=%d;aid=%d;pdId=%s;doBatchUpdater.json=%s;dataList=%s;",  m_flow, aid, pdId, doBatchUpdater, dataList);
        Log.logStd("batchSet ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    /**
     * 扣减库存
     * @return
     */
    public int batchReduceStore(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, boolean holdingMode, boolean reduceHoldingCount) {
        if(skuIdCountMap == null || skuIdCountMap.isEmpty()){
            Log.logStd("batchReduceStore arg;flow=%d;aid=%s;unionPriId=%s;skuIdCountMap=%s;", m_flow, aid, unionPriId, skuIdCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            int count = skuIdCountEntry.getValue();
            rt = reduceStore(aid, unionPriId, skuId, count, holdingMode, reduceHoldingCount);
            if(rt != Errno.OK){
                return rt;
            }
        }
        Log.logStd("batchReduceStore ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    /**
     * 扣减库存
     * @return
     */
    public int reduceStore(int aid, int unionPriId, long skuId, int count, boolean holdingMode, boolean reduceHoldingCount) {
        if(aid <= 0 || unionPriId <= 0 || skuId <=0 || count <= 0){
            Log.logStd("reduceStore arg;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Ref<Integer> refRowCount = new Ref<>(0);
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        ParamUpdater updater = new ParamUpdater();
        if(!reduceHoldingCount){ // 扣减 真实库存
            matcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, count);
            updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, count);
            if(holdingMode){ // 预扣模式
                updater.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, ParamUpdater.INC, count);
            }
        }else{ // 扣减 预扣库存
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
            return Errno.ERROR;
        }
        Log.logStd("reduceStore ok;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
        return rt;
    }

    /**
     * 补偿库存
     */
    public int batchMakeUpStore(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, boolean holdingMode) {
        if(skuIdCountMap == null || skuIdCountMap.isEmpty()){
            Log.logStd("batchMakeUpStore arg;flow=%d;aid=%s;unionPriId=%s;skuIdCountMap=%s;", m_flow, aid, unionPriId, skuIdCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        for (Map.Entry<Long, Integer> skuIdCountEntry : skuIdCountMap.entrySet()) {
            long skuId = skuIdCountEntry.getKey();
            int count = skuIdCountEntry.getValue();
            rt = makeUpStore(aid, unionPriId, skuId, count, holdingMode);
            if(rt != Errno.OK){
                return rt;
            }
        }
        Log.logStd("batchMakeUpStore ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }
    /**
     * 补偿库存
     */
    public int makeUpStore(int aid, int unionPriId, long skuId, int count, boolean holdingMode) {
        if(aid <= 0 || unionPriId <= 0 || skuId <=0 || count <= 0){
            Log.logStd("makeUpStore arg;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
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
        Log.logStd("makeUpStore ok;flow=%d;aid=%s;unionPriId=%s;skuId=%s;count=%s;", m_flow, aid, unionPriId, skuId, count);
        return rt;
    }

    /**
     * 检查是否存在sku, 没有则生成
     * @param aid
     * @param ownerUnionPriId
     * @param needCheckSkuStoreKeyPdKeyMap
     * @return
     */
    public int checkAndAdd(int aid, int ownerUnionPriId, Map<SkuStoreKey, PdKey> needCheckSkuStoreKeyPdKeyMap) {
        if(needCheckSkuStoreKeyPdKeyMap == null || needCheckSkuStoreKeyPdKeyMap.isEmpty()){
            Log.logStd("checkAndAdd arg error;flow=%s;aid=%s;ownerUnionPriId=%s;needCheckSkuStoreKeyPdKeyMap=%s;", m_flow, aid, ownerUnionPriId, needCheckSkuStoreKeyPdKeyMap);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, Set<Long>> uidSkuIdSetMap = new HashMap<>();
        for (SkuStoreKey skuStoreKey : needCheckSkuStoreKeyPdKeyMap.keySet()) {
            Set<Long> skuIdSet = uidSkuIdSetMap.get(skuStoreKey.unionPriId);
            if(skuIdSet == null){
                skuIdSet = new HashSet<>();
                uidSkuIdSetMap.put(skuStoreKey.unionPriId, skuIdSet);
            }
            skuIdSet.add(skuStoreKey.skuId);
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
        // 生关联的sku
        FaiList<Param> addInfoList = new FaiList<>(needAddedSkuIdSet.size());
        for (Map.Entry<Integer, Set<Long>> uidSkuIdSetEntry : uidSkuIdSetMap.entrySet()) {
            int uid = uidSkuIdSetEntry.getKey();
            Set<Long> skuIdSet = uidSkuIdSetEntry.getValue();
            for (Long skuId : skuIdSet) {
                PdKey pdKey = needCheckSkuStoreKeyPdKeyMap.get(new SkuStoreKey(uid, skuId));
                addInfoList.add(
                        new Param()
                                .setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, uid)
                                .setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId)
                                .setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId)
                                .setInt(StoreSalesSkuEntity.Info.PD_ID, pdKey.pdId)
                                .setInt(StoreSalesSkuEntity.Info.RL_PD_ID, pdKey.rlPdId)
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
     * 悲观锁锁住数据，一定要命中索引！不然升级为表锁就GG
     */
    @Deprecated
    public int selectForUpdate(int aid, Map<SkuStoreKey, Integer> skuStoreChangeCountMap, Map<SkuStoreKey, Integer> changeBeforeSkuStoreCountMap){
        if(aid <= 0 || skuStoreChangeCountMap == null || skuStoreChangeCountMap.isEmpty() || changeBeforeSkuStoreCountMap == null){
            Log.logStd("selectForUpdate arg error;flow=%s;aid=%s;skuStoreChangeCountMap=%s;changeBeforeSkuStoreCountMap=%s;", m_flow, aid, skuStoreChangeCountMap, changeBeforeSkuStoreCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Map<Integer, FaiList<Long>> unionPriIdSkuIdListMap = new HashMap<>();
        for (SkuStoreKey skuStoreKey : skuStoreChangeCountMap.keySet()) {
            int unionPriId = skuStoreKey.unionPriId;
            FaiList<Long> skuIdList = unionPriIdSkuIdListMap.get(unionPriId);
            if(skuIdList == null){
                skuIdList = new FaiList<>();
                unionPriIdSkuIdListMap.put(unionPriId, skuIdList);
            }
            skuIdList.add(skuStoreKey.skuId);
        }
        for (Map.Entry<Integer, FaiList<Long>> unionPriIdSkuIdListEntry : unionPriIdSkuIdListMap.entrySet()) {
            Integer unionPriId = unionPriIdSkuIdListEntry.getKey();
            FaiList<Long> skuIdList = unionPriIdSkuIdListEntry.getValue();
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < skuIdList.size(); i++) {
                Long skuId = skuIdList.get(i);
                if(i != 0){
                    sb.append(",");
                }
                sb.append(skuId);
            }
            sb.append(")");

            String selectForUpdateSql = "SELECT " + StoreSalesSkuEntity.Info.SKU_ID + "," + StoreSalesSkuEntity.Info.REMAIN_COUNT + ","+ StoreSalesSkuEntity.Info.HOLDING_COUNT
                    + " FROM " + m_daoCtrl.getTableName()
                    + " WHERE " + StoreSalesSkuEntity.Info.AID +"=" + aid
                    + " AND " + StoreSalesSkuEntity.Info.UNION_PRI_ID + "=" + unionPriId
                    + " AND " + StoreSalesSkuEntity.Info.SKU_ID + " IN " + sb.toString()
                    + " FOR UPDATE "
                    + " ;";
            Log.logDbg("whalelog sql=%s;", selectForUpdateSql);
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.executeQuery(selectForUpdateSql, listRef);
            if(rt != Errno.OK){
                Log.logErr(rt,"selectForUpdate err;flow=%s;aid=%s;sql=%s;", m_flow, aid, selectForUpdateSql);
                return rt;
            }
            if(listRef.value.size() != skuIdList.size()){
                rt = Errno.NOT_FOUND;
                Log.logErr(rt,"size no match;flow=%s;aid=%s;sql=%s;", m_flow, aid, selectForUpdateSql);
                return rt;
            }
            for (Param info : listRef.value) {
                Long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.REMAIN_COUNT) + info.getInt(StoreSalesSkuEntity.Info.HOLDING_COUNT);
                changeBeforeSkuStoreCountMap.put(new SkuStoreKey(unionPriId, skuId), count);
            }
        }
        return rt;
    }
    /**
     * 只会改到count 和 remainCount
     */
    public int batchChangeStore(int aid, Map<SkuStoreKey, Integer> skuStoreChangeCountMap) {
        if(aid <= 0 || skuStoreChangeCountMap == null || skuStoreChangeCountMap.isEmpty()){
            Log.logStd("batchChangeStore arg error;flow=%s;aid=%s;skuStoreChangeCountMap=%s;", m_flow, aid, skuStoreChangeCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.OK;
        Calendar now = Calendar.getInstance();
        for (Map.Entry<SkuStoreKey, Integer> skuStoreKeyChangeCountEntry : skuStoreChangeCountMap.entrySet()) {
            /*
              changeCount 取相反数，便于更新
               ... set `remainCount` = `remainCount` - changeCount, `changeCount` = `changeCount` - changeCount  where ... and `remainCount` >= changeCount and `changeCount` >= changeCount
               乐观锁 不考虑aba问题
             */
            Integer changeCount = -skuStoreKeyChangeCountEntry.getValue();

            ParamUpdater updater = new ParamUpdater();
            updater.add(StoreSalesSkuEntity.Info.COUNT, ParamUpdater.DEC, changeCount);
            updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, changeCount);
            updater.getData().setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, now);

            SkuStoreKey skuStoreKey = skuStoreKeyChangeCountEntry.getKey();
            ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, skuStoreKey.unionPriId);
            matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuStoreKey.skuId);
            matcher.and(StoreSalesSkuEntity.Info.COUNT, ParamMatcher.GE, changeCount);
            matcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, changeCount);

            Ref<Integer> refRowCount = new Ref<>(0);
            rt = m_daoCtrl.update(updater, matcher, refRowCount);
            if(rt != Errno.OK){
                Log.logStd(rt,"dao update err;matcher=%s", matcher.toJson());
                return rt;
            }
            if(refRowCount.value <= 0){ // 库存异常
                Log.logStd("store err;matcher=%s", matcher.toJson());
                return Errno.ERROR;
            }
        }
        /* 批量更新不能根据影响的行数来判断更新是否成功。
        FaiList<Param> dataList = new FaiList<>(skuStoreChangeCountMap.size());
        for (Map.Entry<SkuStoreKey, Integer> skuStoreKeyCountEntry : skuStoreChangeCountMap.entrySet()) {
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
        Log.logStd("ok!flow=%s;aid=%s;skuStoreChangeCountMap=%s;", m_flow, aid, skuStoreChangeCountMap);
        return rt;
    }

    /**
     * 只会改到 总成相关的字段
     */
    public int batchUpdateTotalCost(int aid, Map<SkuStoreKey, Param> changeCountAfterSkuStoreCountAndTotalCostMap) {
        int rt = Errno.OK;
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>();
        for (Map.Entry<SkuStoreKey, Param> skuStoreKeyInfoEntry : changeCountAfterSkuStoreCountAndTotalCostMap.entrySet()) {
            SkuStoreKey skuStoreKey = skuStoreKeyInfoEntry.getKey();
            int unionPriId = skuStoreKey.unionPriId;
            long skuId = skuStoreKey.skuId;
            Param totalCostInfo = skuStoreKeyInfoEntry.getValue();
            long fifoTotalCost = totalCostInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST);
            long mwTotalCost = totalCostInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST);
            Param data = new Param();
            // updater field
            data.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost);
            data.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost);
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
        updater.getData().setString(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher matcher = new ParamMatcher();
        matcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
        rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if(rt != Errno.OK){
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int getListFromDaoByPdIdAndSkuIdList(int aid, int pdId, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(aid <= 0 || pdId <=0 || skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;skuIdList=%s;listRef=%s;fields=%s", m_flow, aid, pdId, skuIdList, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }

        Log.logDbg(rt,"getListFromDao ok;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int getListFromDao(int aid, int unionPriId, int pdId, Ref<FaiList<Param>> listRef, String ... fields){
        if(aid <= 0 || unionPriId <=0 || pdId <=0 || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;pdId=%s;listRef=%s;fields=%s", m_flow, aid, unionPriId, pdId, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
            return rt;
        }

        Log.logDbg(rt,"getListFromDao ok;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
        return rt;
    }

    public int getCountAndTotalCostFromDao(int aid, Set<SkuStoreKey> skuStoreKeySet, Map<SkuStoreKey, Param> skuCountAndTotalCostMap) {
        if(skuStoreKeySet == null || skuStoreKeySet.isEmpty() || skuCountAndTotalCostMap == null){
            Log.logStd("arg error;flow=%d;aid=%s;skuStoreKeySet=%s;skuCountAndTotalCostMap=%s;", m_flow, aid, skuStoreKeySet, skuCountAndTotalCostMap);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, FaiList<Long>> unionPriIdSkuIdListMap = new HashMap<>();
        for (SkuStoreKey skuStoreKey : skuStoreKeySet) {
            int unionPriId = skuStoreKey.unionPriId;
            FaiList<Long> skuIdList = unionPriIdSkuIdListMap.get(unionPriId);
            if(skuIdList == null){
                skuIdList = new FaiList<>();
                unionPriIdSkuIdListMap.put(unionPriId, skuIdList);
            }
            skuIdList.add(skuStoreKey.skuId);
        }
        int rt = Errno.ERROR;
        for (Map.Entry<Integer, FaiList<Long>> unionPriIdSkuIdListEntry : unionPriIdSkuIdListMap.entrySet()) {
            Integer unionPriId = unionPriIdSkuIdListEntry.getKey();
            FaiList<Long> skuIdList = unionPriIdSkuIdListEntry.getValue();
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef,
                    StoreSalesSkuEntity.Info.SKU_ID,
                    StoreSalesSkuEntity.Info.REMAIN_COUNT,
                    StoreSalesSkuEntity.Info.HOLDING_COUNT,
                    StoreSalesSkuEntity.Info.FIFO_TOTAL_COST,
                    StoreSalesSkuEntity.Info.MW_TOTAL_COST);
            if(rt != Errno.OK){
                return rt;
            }
            for (Param info : listRef.value) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                skuCountAndTotalCostMap.put(new SkuStoreKey(unionPriId, skuId), info);
            }
        }

        return Errno.OK;
    }

    public int getStoreCountFromDao(int aid, Set<SkuStoreKey> skuStoreKeySet, Map<SkuStoreKey, Integer> skuStoreCountMap) {
        if(skuStoreKeySet == null || skuStoreKeySet.isEmpty() || skuStoreCountMap == null){
            Log.logStd("arg error;flow=%d;aid=%s;skuStoreKeySet=%s;skuStoreCountMap=%s;", m_flow, aid, skuStoreKeySet, skuStoreCountMap);
            return Errno.ARGS_ERROR;
        }
        Map<Integer, FaiList<Long>> unionPriIdSkuIdListMap = new HashMap<>();
        for (SkuStoreKey skuStoreKey : skuStoreKeySet) {
            int unionPriId = skuStoreKey.unionPriId;
            FaiList<Long> skuIdList = unionPriIdSkuIdListMap.get(unionPriId);
            if(skuIdList == null){
                skuIdList = new FaiList<>();
                unionPriIdSkuIdListMap.put(unionPriId, skuIdList);
            }
            skuIdList.add(skuStoreKey.skuId);
        }
        int rt = Errno.ERROR;
        for (Map.Entry<Integer, FaiList<Long>> unionPriIdSkuIdListEntry : unionPriIdSkuIdListMap.entrySet()) {
            Integer unionPriId = unionPriIdSkuIdListEntry.getKey();
            FaiList<Long> skuIdList = unionPriIdSkuIdListEntry.getValue();
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef, StoreSalesSkuEntity.Info.SKU_ID, StoreSalesSkuEntity.Info.REMAIN_COUNT, StoreSalesSkuEntity.Info.HOLDING_COUNT);
            if(rt != Errno.OK){
                return rt;
            }
            for (Param info : listRef.value) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int remainCount = info.getInt(StoreSalesSkuEntity.Info.REMAIN_COUNT);
                int holdingCount = info.getInt(StoreSalesSkuEntity.Info.HOLDING_COUNT);
                skuStoreCountMap.put(new SkuStoreKey(unionPriId, skuId), remainCount+holdingCount);
            }
        }

        return Errno.OK;
    }
    public int getListFromDaoBySkuIdList(int aid, int unionPriId, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
        if(skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdList=%s;listRef=%s;fields=%s", m_flow, aid, unionPriId, skuIdList, listRef, fields);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;unionPriId=%s;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
            return rt;
        }

        Log.logDbg(rt,"getInfoListFromDao ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
        return rt;
    }
    private int getListFromDaoBySkuIdAndUnionPriIdList(int aid, long skuId, FaiList<Integer> unionPriIdList, Ref<FaiList<Param>> listRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher();
        searchArg.matcher.and(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        if(unionPriIdList != null){
            searchArg.matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        }
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuId=%s;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
            return rt;
        }
        return rt;
    }

    /**
     * 查询库存
     */
    public int searchStoreSkuSummaryFromDao(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || searchArg == null || searchArg.matcher == null || searchArg.matcher.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;searchArg=%s;listRef=%s;", m_flow, aid, searchArg, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.getStoreSkuSummaryFields();
        selectArg.searchArg = searchArg;
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;searchArg.matcher.toJson=%s;", m_flow, aid, searchArg.matcher.toJson());
            return rt;
        }
        Log.logDbg(rt,"getReportList ok;flow=%d;aid=%d;searchArg.matcher.toJson=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }
    public int getReportList(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;skuIdList=%s;listRef=%s;", m_flow, aid, skuIdList, listRef);
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
        Log.logDbg(rt,"getReportList ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }
    public int getReportList4synSPU2SKU(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || skuIdList == null || skuIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;skuIdList=%s;listRef=%s;", m_flow, aid, skuIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.PD_ID + ", " +
                StoreSalesSkuEntity.Info.RL_PD_ID + ", " +
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
        Log.logDbg(rt,"getReportList ok;flow=%d;aid=%d;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }
    public int getReportList(int aid, int pdId, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || pdId <=0 || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;listRef=%s;", m_flow, aid, pdId, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.UNION_PRI_ID + ", " +
                StoreSalesSkuEntity.Info.RL_PD_ID + ", " +
                COMM_REPORT_FIELDS;
        selectArg.group = StoreSalesSkuEntity.Info.UNION_PRI_ID;
        selectArg.searchArg.matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        selectArg.searchArg.matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        rt = m_daoCtrl.select(selectArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        initReportInfoList(listRef.value);
        Log.logDbg(rt,"getReportList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int getReportInfo(int aid, int pdId, int unionPriId, Ref<Param> infoRef){
        if(aid <= 0 || pdId <=0 || unionPriId <= 0|| infoRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;unionPriId=%s;listRef=%s;", m_flow, aid, pdId, unionPriId, infoRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Dao.SelectArg selectArg = new Dao.SelectArg();
        selectArg.field = StoreSalesSkuEntity.Info.RL_PD_ID + ", " +
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
        Log.logDbg(rt,"getReportInfo ok;flow=%d;aid=%d;pdId=%s;unionPriId=%s;", m_flow, aid, pdId, unionPriId);
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
        Log.logDbg(rt,"getReportInfo ok;flow=%d;aid=%d;skuId=%s;", m_flow, aid, skuId);
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
    private static final String COMM_REPORT_FIELDS = StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID+", "+
            "sum(" + StoreSalesSkuEntity.Info.COUNT + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_COUNT + ", " +
            "sum(" + StoreSalesSkuEntity.Info.REMAIN_COUNT + ") as " + StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT + ", " +
            "sum(" + StoreSalesSkuEntity.Info.HOLDING_COUNT + ") as "+StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT+", " +
            "min( if(" + StoreSalesSkuEntity.Info.FLAG+"&" +StoreSalesSkuValObj.FLag.SETED_PRICE +"=" +StoreSalesSkuValObj.FLag.SETED_PRICE+"," + StoreSalesSkuEntity.Info.PRICE +"," + Long.MAX_VALUE + ") ) as "+StoreSalesSkuEntity.ReportInfo.MIN_PRICE+", " +
            "max(" + StoreSalesSkuEntity.Info.PRICE + ") as "+StoreSalesSkuEntity.ReportInfo.MAX_PRICE+" ";

    public int getList(int aid, int unionPriId, int pdId, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <=0 || pdId <= 0 || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;listRef=%s;", m_flow, aid, pdId, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        /*FaiList<Param> cacheList = StoreSalesSkuCacheCtrl.getCacheList(aid, unionPriId, pdId);
        if(cacheList != null){
            listRef.value = cacheList;
            return Errno.OK;
        }*/
        rt = getListFromDao(aid, unionPriId, pdId, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        //StoreSalesSkuCacheCtrl.setCacheList(aid, unionPriId, pdId, listRef.value);

        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
        return rt = Errno.OK;
    }
    public int getListBySkuIdAndUnionPriIdList(int aid, long skuId, FaiList<Integer> unionPriIdList, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || skuId <=0 || (unionPriIdList != null && unionPriIdList.isEmpty()) || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;skuId=%s;unionPriIdList=%s;;listRef=%s;", m_flow, aid, skuId, unionPriIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        rt = getListFromDaoBySkuIdAndUnionPriIdList(aid, skuId, unionPriIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logDbg(rt,"getListBySkuIdAndUnionPriId ok;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
        return rt = Errno.OK;
    }




    private int m_flow;
    private StoreSalesSkuDaoCtrl m_daoCtrl;



}
