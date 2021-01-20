package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.StoreSkuSummaryEntity;
import fai.MgProductStoreSvr.domain.repository.StoreSkuSummaryDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class StoreSkuSummaryProc {
    public StoreSkuSummaryProc(StoreSkuSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int report(int aid, long skuId, Param info) {
        if(aid <= 0 || info == null || info.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"selectFirst err;flow=%s;aid=%s;skuId=%s;", m_flow, aid, skuId);
            return rt;
        }
        Param oldInfo = infoRef.value;

        Calendar now = Calendar.getInstance();
        info.setCalendar(StoreSkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
        if(oldInfo.isEmpty()){
            info.setInt(StoreSkuSummaryEntity.Info.AID, aid);
            info.setLong(StoreSkuSummaryEntity.Info.SKU_ID, skuId);
            info.setCalendar(StoreSkuSummaryEntity.Info.SYS_CREATE_TIME, now);
            rt = m_daoCtrl.insert(info);
            if(rt != Errno.OK){
                Log.logErr(rt,"insert err;flow=%s;aid=%s;skuId=%s;info=%s", m_flow, aid, skuId, info);
                return rt;
            }
        }else {
            ParamUpdater updater = new ParamUpdater(info);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logErr(rt,"update err;flow=%s;aid=%s;skuId=%s;info=%s", m_flow, aid, skuId, info);
                return rt;
            }
        }

        Log.logStd("ok!;flow=%s;aid=%s;skuId=%s;", m_flow, aid, skuId);
        return rt;
    }
    public int report(int aid, FaiList<Param> infoList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Calendar now = Calendar.getInstance();
        FaiList<Long> skuIdList = new FaiList<>(infoList.size());
        Map<Long, Param> skuIdInfoMap = new HashMap<>(infoList.size()*4/3+1);
        for (Param info : infoList) {
            Long skuId = info.getLong(StoreSkuSummaryEntity.Info.SKU_ID);
            skuIdList.add(skuId);
            info.setInt(StoreSkuSummaryEntity.Info.AID, aid);
            info.setCalendar(StoreSkuSummaryEntity.Info.SYS_UPDATE_TIME, now);
            skuIdInfoMap.put(skuId, info);
        }
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFromDao(aid, skuIdList, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        FaiList<Param> oldInfoList = listRef.value;
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : oldInfoList) {
            Long skuId = oldInfo.getLong(StoreSkuSummaryEntity.Info.SKU_ID);
            Param newData = skuIdInfoMap.remove(skuId);
            ParamUpdater updater = new ParamUpdater(newData);
            updater.update(oldInfo, false);
            Param data = new Param();
            { // for update
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.COUNT);
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.REMAIN_COUNT);
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.HOLDING_COUNT);
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.FIFO_TOTAL_COST);
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.MW_TOTAL_COST);
                data.assign(oldInfo, StoreSkuSummaryEntity.Info.SYS_UPDATE_TIME);
            }
            { // for matcher
                data.setInt(StoreSkuSummaryEntity.Info.AID, aid);
                data.setLong(StoreSkuSummaryEntity.Info.SKU_ID, skuId);
            }
            batchUpdateDataList.add(data);
        }

        if(skuIdInfoMap.size() > 0){
            FaiList<Param> addList = new FaiList<>(skuIdInfoMap.size());
            for (Param info : skuIdInfoMap.values()) {
                info.setCalendar(StoreSkuSummaryEntity.Info.SYS_CREATE_TIME, now);
                addList.add(info);
            }
            rt = m_daoCtrl.batchInsert(addList, null, true);
            if(rt != Errno.OK){
                Log.logStd("dao.batchInsert error;flow=%d;aid=%s;addList=%s;", m_flow, aid, addList);
            }
        }
        if(batchUpdateDataList.size() > 0){
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData()
                    .setString(StoreSkuSummaryEntity.Info.COUNT, "?")
                    .setString(StoreSkuSummaryEntity.Info.REMAIN_COUNT, "?")
                    .setString(StoreSkuSummaryEntity.Info.HOLDING_COUNT, "?")
                    .setString(StoreSkuSummaryEntity.Info.FIFO_TOTAL_COST, "?")
                    .setString(StoreSkuSummaryEntity.Info.MW_TOTAL_COST, "?")
                    .setString(StoreSkuSummaryEntity.Info.SYS_UPDATE_TIME, "?")
            ;
            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(StoreSkuSummaryEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(StoreSkuSummaryEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logStd("dao.batchUpdate error;flow=%d;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
            }
        }


        Log.logStd("ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher(StoreSkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSkuSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }

    private int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef) {
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(StoreSkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreSkuSummaryEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        }
        Log.logDbg(rt, "flow=%d;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }


    public int searchListFromDao(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        int rt = Errno.ERROR;
        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logStd("dao.select error;flow=%d;aid=%s;searchArg=%s;", m_flow, aid, searchArg);
        }
        Log.logDbg(rt, "flow=%d;aid=%s;searchArg=%s;", m_flow, aid, searchArg);
        return rt;
    }


    private int m_flow;
    private StoreSkuSummaryDaoCtrl m_daoCtrl;

}
