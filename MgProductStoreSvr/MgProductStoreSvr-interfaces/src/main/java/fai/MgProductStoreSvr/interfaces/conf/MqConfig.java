package fai.MgProductStoreSvr.interfaces.conf;

/**
 * mq配置
 */
public class MqConfig {
    /**
     * 公共的生成消费配置
     */
    public class Comm {
        public static final String PRODUCER = "mgProductStoreProducer";
        public static final String CONSUMER = "mgProductStoreConsumer";
    }
    public class SpuBizReport extends Comm {
        public static final String TOPIC = "mg_productStore_spuBizReport_topic";
        public static final String TAG = "mg_productStore_spuBizReport_tag";
    }

    public class SkuReport extends Comm {
        public static final String TOPIC = "mg_productStore_skuReport_topic";
        public static final String TAG = "mg_productStore_skuReport_tag";
    }

}
