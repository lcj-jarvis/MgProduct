package fai.MgProductStoreSvr.domain.serviceProc;


import fai.MgProductStoreSvr.domain.comm.Misc2;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuValObj;
import fai.MgProductStoreSvr.domain.repository.StoreSalesSkuDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;


public class StoreSalesSkuProc {
    public StoreSalesSkuProc(StoreSalesSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int pdId, FaiList<Param> infoList) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
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
            data.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
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
    public int batchDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
        if(aid <= 0 || pdId <= 0 || delSkuIdList == null || delSkuIdList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
            return Errno.ARGS_ERROR;
        }
        // TODO
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);

        rt = m_daoCtrl.delete(matcher);
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }
    /*public int batchDel(int aid, int pdId, FaiList<Integer> unionPriIdList) {
        if(aid <= 0 || pdId <=0 || (unionPriIdList != null && unionPriIdList.isEmpty())){
            Log.logStd("batchDel error;flow=%d;aid=%s;pdId=%s;tpScDtIdList=%s;", m_flow, aid, pdId, unionPriIdList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(unionPriIdList != null){
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIdList);
        }
        rt = m_daoCtrl.delete(matcher);
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;idList=%s;", m_flow, aid, pdId, unionPriIdList);
        return rt;
    }*/

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
        rt = getListFromDao(aid, pdId, skuIdList, listRef, maxUpdaterKeys.toArray(new String[]{}));
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
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;", m_flow, aid);
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
     * 会改到count 和 remainCount
     */
    public int batchChangeStore(int aid, Map<SkuStoreKey, Integer> skuStoreCountMap) {
        if(aid <= 0 || skuStoreCountMap == null || skuStoreCountMap.isEmpty()){
            Log.logStd("batchChangeStore error;aid=%s;skuStoreCountMap=%s;",  aid, skuStoreCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        for (Map.Entry<SkuStoreKey, Integer> skuStoreKeyCountEntry : skuStoreCountMap.entrySet()) {
            /*
              count 取相反数，便于更新
               ... set `remainCount` = `remainCount` - count, `count` = `count` - count  where ... and `remainCount` >= count and `count` >= count
               乐观锁 不考虑aba问题
             */
            Integer count = -skuStoreKeyCountEntry.getValue();

            ParamUpdater updater = new ParamUpdater();
            updater.add(StoreSalesSkuEntity.Info.COUNT, ParamUpdater.DEC, count);
            updater.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamUpdater.DEC, count);

            SkuStoreKey skuStoreKey = skuStoreKeyCountEntry.getKey();
            ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, skuStoreKey.unionPriId);
            matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuStoreKey.skuId);
            matcher.and(StoreSalesSkuEntity.Info.COUNT, ParamMatcher.GE, count);
            matcher.and(StoreSalesSkuEntity.Info.REMAIN_COUNT, ParamMatcher.GE, count);

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
        FaiList<Param> dataList = new FaiList<>(skuStoreCountMap.size());
        for (Map.Entry<SkuStoreKey, Integer> skuStoreKeyCountEntry : skuStoreCountMap.entrySet()) {
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
        Log.logStd("ok!flow=%s;aid=%s;skuStoreCountMap=%s;", m_flow, aid, skuStoreCountMap);
        return rt;
    }


    public int getListFromDao(int aid, int pdId, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef, String ... fields){
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

    public int getInfoFromDao(int aid, int unionPriId, long skuId, Ref<Param> infoRef, String ... fields){
        if(aid <= 0 || unionPriId <=0 || skuId <=0 || infoRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;skuId=%s;infoRef=%s;fields=%s", m_flow, aid, unionPriId, skuId, infoRef, fields);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreSalesSkuEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.selectFirst(searchArg, infoRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;unionPriId=%s;skuId=%s;", m_flow, aid, unionPriId, skuId);
            return rt;
        }

        Log.logDbg(rt,"getListFromDao ok;flow=%d;aid=%d;unionPriId=%s;skuId=%s;", m_flow, aid, unionPriId, skuId);
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
        Log.logDbg(rt,"getReportInfo ok;flow=%d;aid=%d;pdId=%s;unionPriId=%s;", m_flow, aid, pdId, unionPriId);
        return rt;
    }

    /**
     * min(if(flag&0x1=0x1, price, 0x7fffffffffffffffL)) 设置过价格的才参与计算最小值。
     */
    private static final String COMM_REPORT_FIELDS =
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


        rt = getListFromDao(aid, unionPriId, pdId, listRef);


        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
        return rt = Errno.OK;
    }



    private int m_flow;
    private StoreSalesSkuDaoCtrl m_daoCtrl;



}
