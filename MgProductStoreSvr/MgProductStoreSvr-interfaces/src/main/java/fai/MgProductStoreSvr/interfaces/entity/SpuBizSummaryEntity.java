package fai.MgProductStoreSvr.interfaces.entity;

/**
 * spu 业务库存销售汇总表
 */
public class SpuBizSummaryEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String PD_ID = "pdId";                                  // int 商品 id    (Primary Key 3)
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String PRICE_TYPE = "priceType";                        // int 价格类型（定价、面议）
        public static final String MODE_TYPE = "modeType";                          // int 服务预约模式
        public static final String MARKET_PRICE = "marketPrice";                    // bigint 市场价格
        public static final String MIN_PRICE = "minPrice";                          // bigint 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // bigint 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String VIRTUAL_SALES = "virtualSales";                  // int 虚拟销售量
        public static final String SALES = "sales";                                 // int 实际销售量（数据不完全实时，做展示，实时数据看订单）
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String DISTRIBUTE_LIST = "distributeList";              // varchar(255) 配送方式（支持多个） TODO
        public static final String KEEP_PROP1 = "keepProp1";                        // varchar(255) 字符串 保留字段1 TODO
        public static final String KEEP_INT_PROP1 = "keepIntProp1";                 // int 整型 保留字段1 TODO
    }

}
