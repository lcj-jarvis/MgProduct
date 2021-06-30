package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.StoreOrderRecordEntity;
import fai.MgProductStoreSvr.domain.repository.StoreOrderRecordDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

public class StoreOrderRecordProc {
    public StoreOrderRecordProc(StoreOrderRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public StoreOrderRecordProc(int flow, int aid, TransactionCtrl tc) {
        m_daoCtrl = StoreOrderRecordDaoCtrl.getInstanceWithRegistered(flow, aid, tc);
        if(m_daoCtrl == null){
            throw new RuntimeException(String.format("StoreOrderRecordDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int batchAdd(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, String rlOrderCode) {
        if(aid <= 0 || unionPriId <= 0 || skuIdCountMap == null || Str.isEmpty(rlOrderCode) ){
            Log.logErr("add error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;expireTime=%s", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(skuIdCountMap.size());
        skuIdCountMap.forEach((skuId, count)->{
            Param data = new Param();
            data.setInt(StoreOrderRecordEntity.Info.AID, aid);
            data.setInt(StoreOrderRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(StoreOrderRecordEntity.Info.SKU_ID, skuId);
            data.setString(StoreOrderRecordEntity.Info.RL_ORDER_CODE, rlOrderCode);
            data.setInt(StoreOrderRecordEntity.Info.COUNT, count);
            data.setCalendar(StoreOrderRecordEntity.Info.SYS_CREATE_TIME, now);

            dataList.add(data);
        });
        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"add error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode);
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, skuIdCountMap, rlOrderCode);
        return rt;
    }

    public int clearData(int aid, Integer unionPriId) {
        return clearData(aid, new FaiList<>(Arrays.asList(unionPriId)));
    }
    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }

        ParamMatcher matcher = new ParamMatcher(StoreOrderRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreOrderRecordEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int getFromDao(int aid, int unionPriId, long skuId, int rlOrderId){
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0 ){
            Log.logErr("get error;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", m_flow, aid, unionPriId, skuId, rlOrderId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(StoreOrderRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(StoreOrderRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(StoreOrderRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        matcher.and(StoreOrderRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderId);
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
    private StoreOrderRecordDaoCtrl m_daoCtrl;


}
