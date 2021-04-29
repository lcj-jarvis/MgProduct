package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.RecordKey;
import fai.MgProductStoreSvr.domain.comm.Utils;
import fai.MgProductStoreSvr.domain.entity.HoldingRecordEntity;
import fai.MgProductStoreSvr.domain.repository.HoldingRecordDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class HoldingRecordProc {
    public HoldingRecordProc(HoldingRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public HoldingRecordProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null){
            throw new RuntimeException(String.format("HoldingRecordDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int batchSynchronous(int aid, FaiList<Param> holdingRecordList) {
        if(holdingRecordList.isEmpty()){
            return Errno.OK;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"dao selectFirst error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        Set<String/*unionPirId-skuId-rlOrderCode*/> keySet = new HashSet<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            int unionPriId = info.getInt(HoldingRecordEntity.Info.UNION_PRI_ID);
            long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
            String rlOrderCode = info.getString(HoldingRecordEntity.Info.RL_ORDER_CODE);
            keySet.add(unionPriId+"-"+skuId+"-"+rlOrderCode);
        }
        Log.logDbg("whalelog  keySet=%s;", keySet);
        FaiList<Param> addInfoList = new FaiList<>();
        Calendar now = Calendar.getInstance();
        Calendar expireTime = Calendar.getInstance();
        expireTime.set(Calendar.YEAR, 9999); // 不过期
        for (Param info : holdingRecordList) {
            int unionPriId = info.getInt(HoldingRecordEntity.Info.UNION_PRI_ID);
            long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
            String rlOrderCode = info.getString(HoldingRecordEntity.Info.RL_ORDER_CODE);
            if(keySet.contains(unionPriId+"-"+skuId+"-"+rlOrderCode)){
                continue;
            }

            info.setCalendar(HoldingRecordEntity.Info.EXPIRE_TIME, expireTime);
            info.setCalendar(HoldingRecordEntity.Info.SYS_CREATE_TIME, now);
            addInfoList.add(info);
        }
        Log.logDbg("whalelog  addInfoList=%s;", addInfoList);

        rt = m_daoCtrl.batchInsert(addInfoList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"batchAdd error;flow=%d;aid=%d;addInfoList=%s", m_flow, aid, addInfoList);
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int batchAdd(int aid, int unionPriId, TreeMap<RecordKey, Integer> recordCountMap, String rlOrderCode, int expireTimeSeconds) {
        if(aid <= 0 || unionPriId <= 0 || recordCountMap == null || Str.isEmpty(rlOrderCode) || expireTimeSeconds < 0){
            Log.logErr("add error;flow=%d;aid=%d;unionPriId=%s;recordCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, recordCountMap, rlOrderCode, expireTimeSeconds);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        Calendar expireTime = Utils.addSecond(now, expireTimeSeconds);
        if(expireTimeSeconds == 0){
            expireTime.set(Calendar.YEAR, 9999); // 不过期
        }
        FaiList<Param> dataList = new FaiList<>(recordCountMap.size());
        recordCountMap.forEach((recordKey, count)->{
            Param data = new Param();
            data.setInt(HoldingRecordEntity.Info.AID, aid);
            data.setInt(HoldingRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(HoldingRecordEntity.Info.SKU_ID, recordKey.skuId);
            data.setString(HoldingRecordEntity.Info.RL_ORDER_CODE, rlOrderCode);
            data.setInt(HoldingRecordEntity.Info.ITEM_ID, recordKey.itemId);
            data.setInt(HoldingRecordEntity.Info.COUNT, count);
            data.setCalendar(HoldingRecordEntity.Info.EXPIRE_TIME, expireTime);
            data.setCalendar(HoldingRecordEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        });

        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"batchAdd error;flow=%d;aid=%d;unionPriId=%s;recordCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, recordCountMap, rlOrderCode, expireTimeSeconds);
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;recordCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, recordCountMap, rlOrderCode, expireTimeSeconds);
        return rt;
    }

    public int batchDel(int aid, int unionPriId, Set<RecordKey> recordSet, String rlOrderCode){
        if(aid <= 0 || unionPriId <= 0 || Util.isEmptyList(recordSet) || Str.isEmpty(rlOrderCode)){
            Log.logErr("batchLogicDel error;flow=%d;aid=%d;unionPriId=%s;recordSet=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, recordSet, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher commMatcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        commMatcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        commMatcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);

        for (RecordKey recordKey : recordSet) {
            ParamMatcher matcher = commMatcher.clone();
            matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.EQ, recordKey.skuId);
            matcher.and(HoldingRecordEntity.Info.ITEM_ID, ParamMatcher.EQ, recordKey.itemId);
            Ref<Integer> countRef = new Ref<>();
            int rt = m_daoCtrl.delete(matcher, countRef);
            if(rt != Errno.OK){
                Log.logErr(rt,"dao delete error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher);
                return rt;
            }
            if(countRef.value != 1){
                Log.logStd("may be abnormal;flow=%d;aid=%d;unionPriId=%s;matcher=%s;countRef.value=%s", m_flow, aid, unionPriId, matcher.toJson(), countRef.value);
            }
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;rlOrderCode=%s;recordSet=%s", m_flow, aid, unionPriId,rlOrderCode , recordSet);
        return Errno.OK;
    }

    public int batchLogicDel(int aid, int unionPriId, Set<RecordKey> recordSet, String rlOrderCode){
        if(aid <= 0 || unionPriId <= 0 || Util.isEmptyList(recordSet) || Str.isEmpty(rlOrderCode)){
            Log.logErr("batchLogicDel error;flow=%d;aid=%d;unionPriId=%s;recordSet=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, recordSet, rlOrderCode);
            return Errno.ARGS_ERROR;
        }

        FaiList<Param> batchDataList = new FaiList<>();
        for (RecordKey recordKey : recordSet) {
            Param data = new Param();
            // updater field
            data.setBoolean(HoldingRecordEntity.Info.ALREADY_DEL, true);
            // matcher field
            data.setInt(HoldingRecordEntity.Info.AID, aid);
            data.setInt(HoldingRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(HoldingRecordEntity.Info.SKU_ID, recordKey.skuId);
            data.setString(HoldingRecordEntity.Info.RL_ORDER_CODE, rlOrderCode);
            data.setInt(HoldingRecordEntity.Info.ITEM_ID, recordKey.itemId);

            batchDataList.add(data);
        }
        ParamUpdater batchUpdater = new ParamUpdater();
        batchUpdater.getData().setString(HoldingRecordEntity.Info.ALREADY_DEL, "?");

        ParamMatcher batchMatcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, "?");
        batchMatcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        batchMatcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
        batchMatcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, "?");
        batchMatcher.and(HoldingRecordEntity.Info.ITEM_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchDataList);
        if(rt != Errno.OK){
            Log.logErr(rt,"dao update error;flow=%d;aid=%d;unionPriId=%s;batchDataList=%s;", m_flow, aid, unionPriId, batchDataList);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;recordSet=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, recordSet, rlOrderCode);
        return rt;
    }
    // 逻辑删除
    public int batchLogicDel(int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null || skuIdList.isEmpty() || Str.isEmpty(rlOrderCode)){
            Log.logErr("batchLogicDel error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setBoolean(HoldingRecordEntity.Info.ALREADY_DEL, true);
        Ref<Integer> rowRef = new Ref<>();
        int rt = m_daoCtrl.update(updater, matcher, rowRef);
        if(rt != Errno.OK){
            Log.logErr(rt,"dao update error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
            return rt;
        }
        if(rowRef.value != skuIdList.size()){ // 匹配
            //rt = Errno.ERROR;
            Log.logStd("size no match;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;rowRef.value=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode, rowRef.value);
            //return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;rowRef.value=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode, rowRef.value);
        return rt;
    }

    public int batchSet(int aid, int unionPriId, String rlOrderCode, Map<RecordKey, Integer> delRecord) {

        return -1;
    }

    public int getListFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null || Str.isEmpty(rlOrderCode)){
            Log.logErr("get error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
        }
        Log.logStd(rt,"ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
        return rt;
    }

    public int getNotDelListFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null){
            Log.logErr("get error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        if(rlOrderCode != null){
            matcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        }
        matcher.and(HoldingRecordEntity.Info.ALREADY_DEL, ParamMatcher.EQ, false);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s", m_flow, aid, unionPriId, skuIdList);
        }
        Log.logStd(rt,"ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
        return rt;
    }

    private int m_flow;
    private HoldingRecordDaoCtrl m_daoCtrl;
}
