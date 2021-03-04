package fai.MgProductStoreSvr.interfaces.entity;

public class HoldingRecordEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String SKU_ID = "skuId";                                // int sku id  (Primary Key 3)
        public static final String RL_ORDER_CODE = "rlOrderCode";                   // varchar(32) 业务订单code  (Primary Key 4)
        public static final String ITEM_ID = "itemId";                              // tinyint 重复项id  (Primary Key 5)

        public static final String COUNT = "count";                                 // int 预扣数量
        public static final String EXPIRE_TIME  = "expireTime";                     // datetime 失效时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
