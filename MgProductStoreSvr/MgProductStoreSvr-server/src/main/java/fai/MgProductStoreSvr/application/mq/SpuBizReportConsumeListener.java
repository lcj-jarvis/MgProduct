package fai.MgProductStoreSvr.application.mq;

import fai.MgProductStoreSvr.application.service.StoreService;
import fai.MgProductStoreSvr.domain.entity.SpuBizStoreSalesReportEntity;
import fai.comm.mq.api.ConsumeContext;
import fai.comm.mq.api.ConsumerStatus;
import fai.comm.mq.api.MessageListener;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;

public class SpuBizReportConsumeListener implements MessageListener {
    public SpuBizReportConsumeListener(StoreService storeService) {
        this.m_storeService = storeService;
    }


    @Override
    public ConsumerStatus consume(FaiMqMessage message, ConsumeContext consumeContext) {
        Param recvInfo = message.getBody(Param.class);
        String key = message.getKey();
        int flow = message.getFlow();
        if(Str.isEmpty(recvInfo)){
            Log.logErr("recv info is empty key=%s;flow=%s;", key, flow);
            Oss.logAlarm(String.format("recv info is empty key=%s;flow=%s;", key, flow));
            return ConsumerStatus.CommitMessage;
        }
        int aid = recvInfo.getInt(SpuBizStoreSalesReportEntity.Info.AID, 0);
        int unionPriId = recvInfo.getInt(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, 0);
        int pdId = recvInfo.getInt(SpuBizStoreSalesReportEntity.Info.PD_ID, 0);
        if(aid == 0 || unionPriId == 0 || pdId == 0){
            return ConsumerStatus.CommitMessage;
        }
        int rt = m_storeService.reportSpuBizSummary(flow, aid, unionPriId, pdId);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr("reportBizSales err key=%s;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", key, flow, aid, unionPriId, pdId);
            return ConsumerStatus.ReconsumeLater;
        }
        return ConsumerStatus.CommitMessage;
    }

    private StoreService m_storeService;
}
