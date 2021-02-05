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
            Log.logStd("add error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
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
            Log.logStd(rt,"batchAdd error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
        }
        return rt;
    }

    // 逻辑删除
    public int batchLogicDel(int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode){
        if(aid <= 0 || unionPriId <= 0 || (skuIdList != null && skuIdList.isEmpty()) || Str.isEmpty(rlOrderCode)){
            Log.logStd("batchLogicDel error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        if(skuIdList != null){
            matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        }
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setBoolean(HoldingRecordEntity.Info.ALREADY_DEL, true);
        int rt = m_daoCtrl.update(updater, matcher);
        if(rt != Errno.OK){
            Log.logStd(rt,"dao update error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
        }
        return rt;
    }

    public int getFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null || Str.isEmpty(rlOrderCode)){
            Log.logStd("get error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK || rt != Errno.NOT_FOUND){
            Log.logStd(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdList, rlOrderCode);
        }
        return rt;
    }

    private int m_flow;
    private HoldingRecordDaoCtrl m_daoCtrl;
}
