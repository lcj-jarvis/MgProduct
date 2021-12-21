package fai.MgProductStoreSvr.domain.entity;

import fai.comm.util.Str;

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
        public static final String SYS_TYPE = "sysType";                            // int 系统类型 (商品/服务)
        public static final String SKU_TYPE = "skuType";                            // int sku 类型(比如一个站点下想把部分库存搞活动)
        public static final String SORT = "sort";                                   // int 排序
        public static final String COUNT = "count";                                 // int 设置的库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 剩余库存
        public static final String HOLDING_COUNT = "holdingCount";                  // int 预扣库存
        public static final String PRICE = "price";                                 // bigint 商品价格(交易价格、促销价)
        public static final String ORIGIN_PRICE = "originPrice";                    // bigint 商品原价(或者是 sku 的市场价)
        public static final String FIFO_TOTAL_COST = "fifoTotalCost";               // bigint 先进先出方式计算的总成本
        public static final String MW_TOTAL_COST = "mwTotalCost";                   // bigint 移动加权方式计算的总成本
        public static final String MW_COST = "mwCost";                              // bigint 移动加权方式计算的成本单价
        public static final String MIN_AMOUNT = "minAmount";                        // int 起购量
        public static final String MAX_AMOUNT = "maxAmount";                        // int 限购量
        public static final String DURATION = "duration";                           // double 预约时长
        public static final String VIRTUAL_COUNT = "virtualCount";                  // int 虚拟库存
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间

        public static final String COST_PRICE = "costPrice";                        // long 商品成本单价，商品导入用，不入库存销售信息库
    }

    private static final String[] MAX_UPDATE_AND_PRI_KEYS = new String[]{
            Info.AID,
            Info.UNION_PRI_ID,
            Info.SKU_ID,
            Info.SKU_TYPE,
            Info.SORT,
            Info.PRICE,
            Info.ORIGIN_PRICE,
            Info.COUNT,
            Info.REMAIN_COUNT,
            Info.HOLDING_COUNT,
            Info.MIN_AMOUNT,
            Info.MAX_AMOUNT,
            Info.DURATION,
            Info.VIRTUAL_COUNT,
            Info.FLAG,
            Info.SYS_UPDATE_TIME
    };

    /**
     * 获取能修改的字段，以及主键
     */
    public static String[] getMaxUpdateAndPriKeys(){
        return MAX_UPDATE_AND_PRI_KEYS;
    }

    private static final String[] VALID_KEYS = new String[]{
            Info.SKU_ID,
            Info.SKU_TYPE,
            Info.SORT,
            Info.PRICE,
            Info.ORIGIN_PRICE,
            Info.COUNT,
            Info.REMAIN_COUNT,
            Info.HOLDING_COUNT,
            Info.MIN_AMOUNT,
            Info.MAX_AMOUNT,
            Info.DURATION,
            Info.VIRTUAL_COUNT,
            Info.FLAG
    };
    /**
     * 支持批量更新的字段
     */
    public static String[] getValidKeys(){
        return VALID_KEYS;
    }

    public static final class ReportInfo{
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id
        public static final String SUM_COUNT = "sumCount";
        public static final String SUM_REMAIN_COUNT = "sumRemainCount";
        public static final String SUM_HOLDING_COUNT = "sumHoldingCount";
        public static final String MIN_PRICE = "minPrice";
        public static final String MAX_PRICE = "maxPrice";
        public static final String SUM_FIFO_TOTAL_COST = "sumFifoTotalCost";
        public static final String SUM_MW_TOTAL_COST = "sumMwTotalCost";
        public static final String BIT_OR_FLAG = "bitOrFlag";
    }

    /**
     * @see SkuSummaryEntity.Info
     */
    private static final String skuBizSummaryFields = Str.join(",",
            new String[]{
                    Info.AID
                    , Info.SKU_ID
                    , Info.PD_ID
                    , Info.PRICE
                    , Info.COUNT
                    , Info.REMAIN_COUNT
                    , Info.HOLDING_COUNT
                    , Info.FIFO_TOTAL_COST
                    , Info.MW_TOTAL_COST
                    , Info.MW_COST
                    , Info.RL_PD_ID
                    , Info.SYS_TYPE
            }
    );
    public static String getSkuBizSummaryFields(){
        return skuBizSummaryFields;
    }
}
