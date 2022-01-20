package fai.MgProductStoreSvr.domain.entity;

/**
 * spu库存销售汇总表
 */
public class SpuSummaryEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String PD_ID = "pdId";                                  // int 商品 id    (Primary Key 2)
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id
        public static final String MIN_PRICE = "minPrice";                          // bigint 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // bigint 商品交易最大价格(做搜索，从 sku 冗余)
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // int 数据状态
    }

    private static final String[] MAX_UPDATE_AND_PRI_KEYS = new String[]{
            Info.AID,
            Info.PD_ID,
            Info.MIN_PRICE,
            Info.MAX_PRICE,
            Info.COUNT,
            Info.REMAIN_COUNT,
            Info.HOLDING_COUNT,
            Info.SYS_UPDATE_TIME,
            Info.STATUS
    };

    /**
     * 获取能修改的字段，以及主键
     */
    public static String[] getMaxUpdateAndPriKeys(){
        return MAX_UPDATE_AND_PRI_KEYS;
    }

}
