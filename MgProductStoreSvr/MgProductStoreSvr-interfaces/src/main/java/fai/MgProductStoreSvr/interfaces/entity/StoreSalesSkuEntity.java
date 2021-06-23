package fai.MgProductStoreSvr.interfaces.entity;

/**
 * 商品规格库存销售sku
 */
public class StoreSalesSkuEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String SKU_ID = "skuId";                                // bigint SKU id    (Primary Key 3)
        public static final String PD_ID = "pdId";                                  // int 商品 id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String SKU_TYPE = "skuType";                            // int sku 类型(比如一个站点下想把部分库存搞活动)
        public static final String SORT = "sort";                                   // int 排序
        public static final String COUNT = "count";                                 // int 设置的库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 剩余库存
        public static final String HOLDING_COUNT = "holdingCount";                  // int 预扣库存
        public static final String PRICE = "price";                                 // long 商品价格(交易价格、促销价)
        public static final String ORIGIN_PRICE = "originPrice";                    // long 商品原价(或者是 sku 的市场价)
        public static final String FIFO_TOTAL_COST = "fifoTotalCost";               // bigint 先进先出方式计算的总成本
        public static final String MW_TOTAL_COST = "mwTotalCost";                   // bigint 移动加权方式计算的总成本
        public static final String MW_COST = "mwCost";                              // bigint 移动加权方式计算的成本单价
        public static final String MIN_AMOUNT = "minAmount";                        // int 起购量
        public static final String MAX_AMOUNT = "maxAmount";                        // int 限购量
        public static final String DURATION = "duration";                           // double 预约时长
        public static final String VIRTUAL_COUNT = "virtualCount";                  // int 虚拟库存
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // Calendar 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // Calendar 修改时间




        ////////////////////////////// 临时字段 ↓↓↓ ////////////////////////////////////////
        public static final String HOLDING_ORDER_LIST = "holdingOrderList";         // 预扣状态的预扣记录集
        public static final String IN_PD_SC_STR_ID_LIST = "inPdScStrIdList";        // 规格值 FaiList<Integer>
        public static final String COST_PRICE = "costPrice";                        // long 商品成本单价，商品导入用，不入库存销售信息库
        ////////////////////////////// 临时字段 ↑↑↑ ////////////////////////////////////////
    }

}
