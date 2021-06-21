package fai.MgProductInfSvr.interfaces.entity;


import fai.comm.util.FaiList;

import java.util.Arrays;

public class ProductStoreEntity {
    /**
     * 库存销售业务sku
     */
    public class StoreSalesSkuInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String SKU_ID = "skuId";                                // long SKU id
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String SKU_TYPE = "skuType";                            // int sku 类型(比如一个站点下想把部分库存搞活动)
        public static final String SORT = "sort";                                   // int 排序
        public static final String COUNT = "count";                                 // int 设置的库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 剩余库存
        public static final String HOLDING_COUNT = "holdingCount";                  // int 预扣库存
        public static final String PRICE = "price";                                 // long 商品价格(交易价格、促销价)
        public static final String ORIGIN_PRICE = "originPrice";                    // long 商品原价
        public static final String FIFO_TOTAL_COST = "fifoTotalCost";               // long 先进先出方式计算的总成本
        public static final String MW_TOTAL_COST = "mwTotalCost";                   // long 移动加权方式计算的总成本
        public static final String MW_COST = "mwCost";                              // long 移动加权方式计算的成本单价
        public static final String MIN_AMOUNT = "minAmount";                        // int 起购量
        public static final String MAX_AMOUNT = "maxAmount";                        // int 限购量
        public static final String DURATION = "duration";                           // double 预约时长
        public static final String VIRTUAL_COUNT = "virtualCount";                  // int 虚拟库存
        /**
         * @see ProductStoreValObj.StoreSalesSku.FLag
         */
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // Calendar 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // Calendar 修改时间

        // for init
        public static final String IN_PD_SC_STR_NAME_LIST = ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST;
        public static final String COST_PRICE = "costPrice";                        // long 商品成本单价，商品导入用，不入库存销售信息库，为了设置出入库的成本价
        public static final String OWNER_RL_PD_ID = "ownerRlPdId";                  // int 该商品所属者的商品业务id，批量添加库存信息时，兼容商品信息未绑定过来
    }

    /**
     * 出入库记录
     */
    public class InOutStoreRecordInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String SKU_ID = "skuId";                                // long SKU id
        public static final String IN_OUT_STORE_REC_ID = "ioStoreRecId";            // int 出入库存记录id
        public static final String OWNER_RL_PD_ID = "ownerRlPdId";                  // int 该商品所属者的商品业务id
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        /**
         * @see ProductStoreValObj.InOutStoreRecord.OptType
         */
        public static final String OPT_TYPE = "optType";                            // int 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // int 总部出入库方式
        public static final String S_TYPE = "sType";                                // int 门店出入库方式
        public static final String CHANGE_COUNT = "changeCount";                    // int 变动库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 变动后剩余库存
        public static final String PRICE = "price";                                 // long 入库：采购单价（成本价） |  出库：先进先出方式计算的成本价
        public static final String MW_PRICE = "mwPrice";                            // long 出库：移动加权计算的成本价
        public static final String TOTAL_PRICE = "totalPrice";                      // long 入库：采购总价（成本价） |  出库：先进先出方式计算的总成本价
        public static final String MW_TOTAL_PRICE = "mwTotalPrice";                 // bigint 出库：移动加权计算的总成本价
        public static final String NUMBER = "number";                               // String 单号（天时间+4位以上顺序数字：如2010120001）
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String HEAD_SID = "headSid";                            // int 加减库存负责人 id
        public static final String OPT_TIME = "optTime";                            // datetime 进/出库时间
        /**
         * @see ProductStoreValObj.InOutStoreRecord.FLag
         */
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String REMARK = "remark";                               // String 备注
        public static final String RL_ORDER_CODE = "rlOrderCode";                   // String 业务订单code/id
        public static final String RL_REFUND_ID = "rlRefundId";                     // String 业务退款编号
        public static final String STATUS = "status";                               // int 数据状态
        // for init
        public static final String IN_PD_SC_STR_NAME_LIST = ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST;
    }

    /**
     * 出入库记录汇总
     */
    public class InOutStoreSumInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String IN_OUT_STORE_REC_ID = "ioStoreRecId";            // int 出入库存记录id
        /**
         * @see ProductStoreValObj.InOutStoreRecord.OptType
         */
        public static final String OPT_TYPE = "optType";                            // int 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // int 总部出入库方式
        public static final String S_TYPE = "sType";                                // int 门店出入库方式
        public static final String PRICE = "price";                                 // long 入库：采购总价（成本价） |  出库：先进先出方式计算的成本价
        public static final String MW_PRICE = "mwPrice";                            // long 出库：移动加权计算的成本价
        public static final String NUMBER = "number";                               // String 单号（天时间+4位以上顺序数字：如2010120001）
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String OPT_TIME = "optTime";                            // datetime 进/出库时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String REMARK = "remark";                               // String 备注
    }

    /**
     * spu 业务库存销售汇总信息
     */
    public static final class SpuBizSummaryInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String PRICE_TYPE = "priceType";                        // int 价格类型（定价、面议）
        public static final String MODE_TYPE = "modeType";                          // int 服务预约模式
        public static final String MARKET_PRICE = "marketPrice";                    // long 市场价格
        public static final String MIN_PRICE = "minPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String VIRTUAL_SALES = "virtualSales";                  // int 虚拟销售量
        public static final String SALES = "sales";                                 // int 实际销售量（数据不完全实时，做展示，实时数据看订单）
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间


        public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
        public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
        static {
            MANAGE_FIELDS = new FaiList<String>(
                    Arrays.asList(
                            MARKET_PRICE
                            , MIN_PRICE
                            , MAX_PRICE
                    )
            );

            VISITOR_FIELDS = new FaiList<String>(
                    Arrays.asList(
                            SALES
                            , COUNT
                            , REMAIN_COUNT
                            , HOLDING_COUNT
                    )
            );
        }
    }
    /**
     * spu 库存销售汇总信息
     */
    public static final class SpuSummaryInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务id
        public static final String MIN_PRICE = "minPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    /**
     * sku 库存销售汇总信息
     */
    public static final class SkuSummaryInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String SKU_ID = "skuId";                                // long skuId
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id    冗余 做查询  可异步更新
        public static final String MIN_PRICE = "minPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String FIFO_TOTAL_COST = "fifoTotalCost";               // long 先进先出方式计算的总成本（数据不完全实时，做搜索，从 sku 冗余）
        public static final String MW_TOTAL_COST = "mwTotalCost";                   // long 移动加权方式计算的总成本（数据不完全实时，做搜索，从 sku 冗余）

        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    /**
     * 库存数量变化信息
     */
    public static final class SkuCountChangeInfo {
        public static final String SKU_ID = "skuId";                                // long skuId
        public static final String ITEM_ID = "itemId";                              // int 订单项id
        public static final String COUNT = "count";                                 // int 数量
    }

    public static final class HoldingRecordInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String SKU_ID = "skuId";                                // long skuId
        public static final String RL_ORDER_CODE = "rlOrderCode";                   // String 业务订单code/id
        public static final String ITEM_ID = "itemId";                              // int 订单项id
        public static final String COUNT = "count";                                 // int 预扣数量
        public static final String EXPIRE_TIME  = "expireTime";                     // datetime 失效时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
