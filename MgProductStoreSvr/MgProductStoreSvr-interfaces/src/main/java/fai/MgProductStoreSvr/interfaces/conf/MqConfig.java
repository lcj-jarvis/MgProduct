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
    public class BizSalesReport extends Comm {
        public static final String TOPIC = "mg_productStore_bizSalesReport_topic";
        public static final String TAG = "mg_productStore_bizSalesReport_tag";
    }

    public class StoreSkuReport extends Comm {
        public static final String TOPIC = "mg_productStore_storeSkuReport_topic";
        public static final String TAG = "mg_productStore_storeSkuReport_tag";
    }
}
