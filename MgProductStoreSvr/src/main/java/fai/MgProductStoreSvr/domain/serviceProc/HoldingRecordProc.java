package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.Misc2;
import fai.MgProductStoreSvr.domain.entity.HoldingRecordEntity;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.repository.HoldingRecordDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.InOutStoreRecordDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;

public class HoldingRecordProc {
    public HoldingRecordProc(HoldingRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int add(int aid, int unionPriId, long skuId, int rlOrderId, int count, int expireTimeSeconds) {
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 || count <= 0 || expireTimeSeconds <= 0){
            Log.logStd("add error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuId, rlOrderId, count, expireTimeSeconds);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        Param data = new Param();
        data.setInt(HoldingRecordEntity.Info.AID, aid);
        data.setInt(HoldingRecordEntity.Info.UNION_PRI_ID, unionPriId);
        data.setLong(HoldingRecordEntity.Info.SKU_ID, skuId);
        data.setInt(HoldingRecordEntity.Info.RL_ORDER_ID, rlOrderId);
        data.setInt(HoldingRecordEntity.Info.COUNT, count);
        data.setCalendar(HoldingRecordEntity.Info.EXPIRE_TIME, Misc2.addSecond(now, expireTimeSeconds));
        data.setCalendar(HoldingRecordEntity.Info.SYS_CREATE_TIME, now);
        int rt = m_daoCtrl.insert(data);
        if(rt != Errno.OK){
            Log.logStd(rt,"add error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;expireTimeSeconds=%s", m_flow, aid, unionPriId, skuId, rlOrderId, count, expireTimeSeconds);
        }
        return rt;
    }
    // 逻辑删除
    public int logicDel(int aid, int unionPriId, long skuId, int rlOrderId){
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 ){
            Log.logStd("logicDel error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_ID, ParamMatcher.EQ, rlOrderId);
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setBoolean(HoldingRecordEntity.Info.ALREADY_DEL, true);
        int rt = m_daoCtrl.update(updater, matcher);
        if(rt != Errno.OK){
            Log.logStd(rt,"dao update error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
        }
        return rt;
    }

    public int getFromDao(int aid, int unionPriId, long skuId, int rlOrderId, Ref<Param> infoRef){
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 ){
            Log.logStd("get error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(HoldingRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(HoldingRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        matcher.and(HoldingRecordEntity.Info.RL_ORDER_ID, ParamMatcher.EQ, rlOrderId);
        matcher.and(HoldingRecordEntity.Info.ALREADY_DEL, ParamMatcher.EQ, false);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK || rt != Errno.NOT_FOUND){
            Log.logStd(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
        }
        return rt;
    }



    private int m_flow;
    private HoldingRecordDaoCtrl m_daoCtrl;
}
