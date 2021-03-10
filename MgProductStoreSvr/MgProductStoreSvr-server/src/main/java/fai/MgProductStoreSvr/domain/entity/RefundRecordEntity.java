package fai.MgProductStoreSvr.domain.entity;

public class RefundRecordEntity {

    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String SKU_ID = "skuId";                                // int sku id  (Primary Key 3)
        public static final String RL_REFUND_ID = "rlRefundId";                     // varchar(32) 退款id  (Primary Key 4)

        public static final String COUNT = "count";                                 // int 数量
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
