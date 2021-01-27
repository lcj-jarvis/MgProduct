package fai.MgProductStoreSvr.interfaces.entity;

/**
 * 商品规格库存销售sku
 */
public class StoreSalesSkuEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String PD_ID = "pdId";                                  // int 商品 id    (Primary Key 3)
        public static final String SKU_ID = "skuId";                                // long SKU id    (Primary Key 4)
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String SKU_TYPE = "skuType";                            // int sku 类型(比如一个站点下想把部分库存搞活动)
        public static final String SORT = "sort";                                   // int 排序
        public static final String COUNT = "count";                                 // int 设置的库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 剩余库存
        public static final String HOLDING_COUNT = "holdingCount";                  // int 预扣库存
        public static final String PRICE = "price";                                 // long 商品价格(交易价格、促销价)
        public static final String ORIGIN_PRICE = "originPrice";                    // long 商品原价(或者是 sku 的市场价)
        public static final String MIN_AMOUNT = "minAmount";                        // int 起购量
        public static final String MAX_AMOUNT = "maxAmount";                        // int 限购量
        public static final String DURATION = "duration";                           // double 预约时长
        public static final String VIRTUAL_COUNT = "virtualCount";                  // int 虚拟库存
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // Calendar 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // Calendar 修改时间
    }

}