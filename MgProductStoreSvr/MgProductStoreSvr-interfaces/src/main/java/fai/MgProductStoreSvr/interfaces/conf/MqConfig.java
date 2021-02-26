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
    public class Report extends Comm{
        public static final String TOPIC = "mg_productStore_report_topic";
    }
    public class SpuBizReport extends Report {
        public static final String TAG = "mg_productStore_spuBizReport_tag";
    }
    public class SkuReport extends Report {
        public static final String TAG = "mg_productStore_skuReport_tag";
    }

}
