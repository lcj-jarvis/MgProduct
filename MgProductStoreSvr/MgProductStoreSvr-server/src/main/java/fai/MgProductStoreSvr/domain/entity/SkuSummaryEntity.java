package fai.MgProductStoreSvr.domain.entity;

/**
 * sku维度的库存销售汇总表
 * https://doc.weixin.qq.com/txdoc/apply_page?k=AH8AXweGABE2BtDL66AF8A3wbJAAk
 * 1、先进先出成本核算方法：
 *  是指先购入的存货应先发出（销售）这样一种存货实物流动假设为前提，对发出存货进行计价的一种方法。（需要系统记录每次入库的批次号）
 * 2、移动加权平均成本核算方法：
 *  移动加权平均法指以每次进货的成本加上原有库存存货的成本的合计额，除以每次进货数量加上原有库存存货的数量的合计数，
 *  据以计算加权平均单位成本，作为在下次进货前计算各次发出存货成本依据的一种方法，即在每次进货之后重新按照加权平均的方法核算库存成本单价。
 */
public class SkuSummaryEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String SKU_ID = "skuId";                                // bigint skuId     (Primary Key 2)
        public static final String PD_ID = "pdId";                                  // int 商品 id    冗余 做查询  可异步更新
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id
        public static final String MIN_PRICE = "minPrice";                          // bigint 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // bigint 商品交易最大价格(做搜索，从 sku 冗余)
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String FIFO_TOTAL_COST = "fifoTotalCost";               // bigint 先进先出方式计算的总成本（数据不完全实时，做搜索，从 sku 冗余）
        public static final String MW_TOTAL_COST = "mwTotalCost";                   // bigint 移动加权方式计算的总成本（数据不完全实时，做搜索，从 sku 冗余）
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // int 数据状态
        public static final String SYS_TYPE = "sysType";                            // int 系统类型
    }

    private static final String[] MAX_UPDATE_AND_PRI_KEYS = new String[]{
            Info.AID,
            Info.SKU_ID,
            Info.MIN_PRICE,
            Info.MAX_PRICE,
            Info.COUNT,
            Info.REMAIN_COUNT,
            Info.HOLDING_COUNT,
            Info.FIFO_TOTAL_COST,
            Info.MW_TOTAL_COST,
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
