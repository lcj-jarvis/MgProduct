package fai.MgProductStoreSvr.application.mq;

import fai.MgProductStoreSvr.application.service.SummaryService;
import fai.MgProductStoreSvr.domain.entity.SkuSummaryEntity;
import fai.comm.mq.api.ConsumeContext;
import fai.comm.mq.api.ConsumerStatus;
import fai.comm.mq.api.MessageListener;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;

public class SkuSummaryReportConsumeListener implements MessageListener {
    public SkuSummaryReportConsumeListener(SummaryService summaryService) {
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
        int aid = recvInfo.getInt(SkuSummaryEntity.Info.AID, 0);
        long skuId = recvInfo.getLong(SkuSummaryEntity.Info.SKU_ID, 0L);

        if(aid == 0 || skuId == 0 ){
            return ConsumerStatus.CommitMessage;
        }
        int rt = summaryService.reportSkuSummary(flow, aid, skuId);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr("reportBizSales err key=%s;flow=%s;aid=%s;skuId=%s;", key, flow, aid, skuId);
            return ConsumerStatus.ReconsumeLater;
        }
        return ConsumerStatus.CommitMessage;
    }

    private SummaryService summaryService;
}
