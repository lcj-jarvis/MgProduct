package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.RefundRecordEntity;
import fai.MgProductStoreSvr.domain.repository.RefundRecordDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

public class RefundRecordProc {
    public RefundRecordProc(RefundRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public RefundRecordProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = RefundRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null){
            throw new RuntimeException(String.format("RefundRecordDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    public int batchAdd(int aid, int unionPriId, Map<Long, Integer> skuIdCountMap, String rlRefundId) {
        if(aid <= 0 || unionPriId <= 0 || skuIdCountMap == null || Str.isEmpty(rlRefundId) ){
            Log.logErr("add error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdCountMap, rlRefundId);
            return Errno.ARGS_ERROR;
        }
        Calendar now = Calendar.getInstance();

        FaiList<Param> dataList = new FaiList<>(skuIdCountMap.size());
        skuIdCountMap.forEach((skuId, count)->{
            Param data = new Param();
            data.setInt(RefundRecordEntity.Info.AID, aid);
            data.setInt(RefundRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(RefundRecordEntity.Info.SKU_ID, skuId);
            data.setString(RefundRecordEntity.Info.RL_REFUND_ID, rlRefundId);
            data.setInt(RefundRecordEntity.Info.COUNT, count);
            data.setCalendar(RefundRecordEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        });

        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"batchAdd error;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdCountMap, rlRefundId);
        }
        Log.logStd("ok;flow=%d;aid=%d;unionPriId=%s;skuIdCountMap=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdCountMap, rlRefundId);
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
        ParamMatcher matcher = new ParamMatcher(RefundRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(RefundRecordEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int getListFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, String rlRefundId, Ref<FaiList<Param>> listRef){
        if(aid <= 0 || unionPriId <= 0 || skuIdList == null || Str.isEmpty(rlRefundId)){
            Log.logErr("get error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdList, rlRefundId);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(RefundRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(RefundRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(RefundRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        matcher.and(RefundRecordEntity.Info.RL_REFUND_ID, ParamMatcher.EQ, rlRefundId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"dao selectFirst error;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdList, rlRefundId);
        }
        Log.logStd(rt,"ok;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;rlRefundId=%s;", m_flow, aid, unionPriId, skuIdList, rlRefundId);
        return rt;
    }

    private int m_flow;
    private RefundRecordDaoCtrl m_daoCtrl;
}
