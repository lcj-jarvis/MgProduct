package fai.MgProductStoreSvr.application.mq;

import fai.MgProductStoreSvr.application.service.StoreService;
import fai.comm.mq.api.ConsumeContext;
import fai.comm.mq.api.ConsumerStatus;
import fai.comm.mq.api.MessageListener;
import fai.comm.mq.message.FaiMqMessage;

public class RefreshPdScSkuSalesStoreComsumeListener implements MessageListener {
    public RefreshPdScSkuSalesStoreComsumeListener(StoreService storeService) {
        this.m_storeService = storeService;
    }
    @Override
    public ConsumerStatus consume(FaiMqMessage message, ConsumeContext context) {

        return ConsumerStatus.CommitMessage;
    }

    private StoreService m_storeService;
}
