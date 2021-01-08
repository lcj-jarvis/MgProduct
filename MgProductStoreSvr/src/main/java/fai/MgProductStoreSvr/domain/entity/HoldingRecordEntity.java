package fai.MgProductStoreSvr.domain.entity;

/**
 * 预扣记录
 */
public class HoldingRecordEntity { // 过期时间
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String SKU_ID = "skuId";                                // int sku id  (Primary Key 3)
        public static final String RL_ORDER_ID = "rlOrderId";                       // int 业务订单 id  (Primary Key 4)
        public static final String ALREADY_DEL = "alreadyDel";                      // bit 是否已经删除
        public static final String COUNT = "count";                                 // int 预扣数量
        public static final String EXPIRE_TIME  = "expireTime";                     // datatime 失效时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}