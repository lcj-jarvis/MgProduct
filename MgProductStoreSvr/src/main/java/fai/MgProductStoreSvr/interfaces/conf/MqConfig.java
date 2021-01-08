package fai.MgProductStoreSvr.interfaces.conf;

/**
 * mq配置
 */
public class MqConfig {
    public class BizSalesReport{
        public static final String PRODUCER = "mgProductStoreProducer";
        public static final String CONSUMER = "mgProductStoreConsumer";
        public static final String TOPIC = "mg_productStore_bizSalesReport_topic";
        public static final String TAG = "mg_productStore_bizSalesReport_tag";

    }
}
