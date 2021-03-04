package fai.MgProductStoreSvr.application.mq;

import fai.MgProductStoreSvr.application.service.SummaryService;
import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.comm.mq.api.ConsumeContext;
import fai.comm.mq.api.ConsumerStatus;
import fai.comm.mq.api.MessageListener;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;

public class SpuBizReportConsumeListener implements MessageListener {
    public SpuBizReportConsumeListener(SummaryService summaryService) {
        this.summaryService = summaryService;
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
        int aid = recvInfo.getInt(SpuBizSummaryEntity.Info.AID, 0);
        int unionPriId = recvInfo.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, 0);
        int pdId = recvInfo.getInt(SpuBizSummaryEntity.Info.PD_ID, 0);
        if(aid == 0 || unionPriId == 0 || pdId == 0){
            Log.logStd( "flow=%s;recvInfo=%s;", flow, recvInfo);
            return ConsumerStatus.CommitMessage;
        }
        try {
            int rt = summaryService.reportSpuBizSummary(flow, aid, unionPriId, pdId);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr("reportBizSales err key=%s;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", key, flow, aid, unionPriId, pdId);
                return ConsumerStatus.ReconsumeLater;
            }
        }catch (Exception e){
            Log.logErr(e, "flow=%s;recvInfo=%s;", flow, recvInfo);
            return ConsumerStatus.ReconsumeLater;
        }
        return ConsumerStatus.CommitMessage;
    }

    private SummaryService summaryService;
}
