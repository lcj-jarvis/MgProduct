package fai.MgProductStoreSvr.application.mq;

import fai.MgProductStoreSvr.application.service.SummaryService;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.comm.mq.api.ConsumeContext;
import fai.comm.mq.api.ConsumerStatus;
import fai.comm.mq.api.MessageListener;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.Log;

public class ReportConsumeListener implements MessageListener {
    public ReportConsumeListener(SummaryService summaryService) {
        this.summaryService = summaryService;
        this.skuSummaryReportConsumeListener = new SkuSummaryReportConsumeListener(summaryService);
        this.spuBizReportConsumeListener = new SpuBizReportConsumeListener(summaryService);
    }

    @Override
    public ConsumerStatus consume(FaiMqMessage message, ConsumeContext context) {
        String key = message.getKey();
        int flow = message.getFlow();
        Log.logStd("key=%s;flow=%s;tag=%s;", key, flow, message.getTag());
        switch (message.getTag()){
            case MqConfig.SpuBizReport.TAG:
                return spuBizReportConsumeListener.consume(message, context);
            case MqConfig.SkuReport.TAG:
                return skuSummaryReportConsumeListener.consume(message, context);
        }

        Log.logStd("not found tag;key=%s;flow=%s;tag=%s;", key, flow, message.getTag());
        return ConsumerStatus.ReconsumeLater;
    }

    private SummaryService summaryService;

    private SkuSummaryReportConsumeListener skuSummaryReportConsumeListener;
    private SpuBizReportConsumeListener spuBizReportConsumeListener;
}
