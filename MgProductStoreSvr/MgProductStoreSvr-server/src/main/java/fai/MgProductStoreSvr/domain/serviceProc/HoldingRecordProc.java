package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.Utils;
import fai.MgProductStoreSvr.domain.entity.HoldingRecordEntity;
import fai.MgProductStoreSvr.domain.repository.HoldingRecordDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.Map;

public class HoldingRecordProc {
    public HoldingRecordProc(HoldingRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, String rlOrderCode, int expireTimeSeconds) {
        if(aid <= 0 || unionPriId <= 0 || skuIdCountMap == null || Str.isEmpty(rlOrderCode) || expireTimeSeconds < 0){
            Log.logErr("add error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        Calendar expireTime = Utils.addSecond(now, expireTimeSeconds);
        if(expireTimeSeconds == 0){
            expireTime.set(Calendar.YEAR, 9999); // 不过期
        }
        FaiList<Param> dataList = new FaiList<>(skuIdCountMap.size());
        skuIdCountMap.forEach((skuId, count)->{
            Param data = new Param();
            data.setInt(HoldingRecordEntity.Info.AID, aid);
            data.setInt(HoldingRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(HoldingRecordEntity.Info.SKU_ID, skuId);
            data.setString(HoldingRecordEntity.Info.RL_ORDER_CODE, rlOrderCode);
            data.setInt(HoldingRecordEntity.Info.COUNT, count);
            data.setCalendar(HoldingRecordEntity.Info.EXPIRE_TIME, expireTime);
            data.setCalendar(HoldingRecordEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        });

        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"batchAdd error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
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
            rt = Errno.ERROR;
            Log.logErr(rt,"size no match;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;rowRef.value=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode, rowRef.value);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;rowRef.value=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode, rowRef.value);
        return rt;
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

    public int getNotDelListFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null){
            Log.logErr("get error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
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
