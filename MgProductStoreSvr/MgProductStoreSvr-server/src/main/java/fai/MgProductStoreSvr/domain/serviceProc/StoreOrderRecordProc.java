package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.StoreOrderRecordEntity;
import fai.MgProductStoreSvr.domain.repository.SotreOrderRecordDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;

public class StoreOrderRecordProc {
    public StoreOrderRecordProc(SotreOrderRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int add(int aid, int unionPriId, long skuId, int rlOrderId, int count) {
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 || count <= 0 ){
            Log.logStd("add error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;expireTime=%s", m_flow, aid, unionPriId, skuId, rlOrderId, count);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        Param data = new Param();
        data.setInt(StoreOrderRecordEntity.Info.AID, aid);
        data.setInt(StoreOrderRecordEntity.Info.UNION_PRI_ID, unionPriId);
        data.setLong(StoreOrderRecordEntity.Info.SKU_ID, skuId);
        data.setInt(StoreOrderRecordEntity.Info.RL_ORDER_ID, rlOrderId);
        data.setInt(StoreOrderRecordEntity.Info.COUNT, count);
        data.setCalendar(StoreOrderRecordEntity.Info.SYS_CREATE_TIME, now);
        int rt = m_daoCtrl.insert(data);
        if(rt != Errno.OK){
            Log.logStd(rt,"add error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;", m_flow, aid, unionPriId, skuId, rlOrderId, count);
        }
        return rt;
    }
    public int del(int aid, int unionPriId, long skuId, int rlOrderId){
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 ){
            Log.logStd("del error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(StoreOrderRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreOrderRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreOrderRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        matcher.and(StoreOrderRecordEntity.Info.RL_ORDER_ID, ParamMatcher.EQ, rlOrderId);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt,"dao delete error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
        }
        return rt;
    }

    public int getFromDao(int aid, int unionPriId, long skuId, int rlOrderId){
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 ){
            Log.logStd("get error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(StoreOrderRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreOrderRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreOrderRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        matcher.and(StoreOrderRecordEntity.Info.RL_ORDER_ID, ParamMatcher.EQ, rlOrderId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK || rt != Errno.NOT_FOUND){
            Log.logStd(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
        }
        return rt;
    }


    private int m_flow;
    private SotreOrderRecordDaoCtrl m_daoCtrl;
}
