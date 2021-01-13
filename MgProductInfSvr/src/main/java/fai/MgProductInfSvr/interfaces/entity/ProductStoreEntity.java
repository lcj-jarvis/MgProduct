package fai.MgProductInfSvr.interfaces.entity;


public class ProductStoreEntity {
    /**
     * 库存销售业务sku
     */
    public class StoreSalesSkuInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String SKU_ID = "skuId";                                // long SKU id
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

        // for init
        public static final String IN_PD_SC_STR_NAME_LIST = ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST;
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
        public static final String OWNER_RL_PD_ID = "ownerPlPdId";                  // int 该商品所属者的业务商品id
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String OPT_TYPE = "optType";                            // int 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // int 总部出入库方式
        public static final String S_TYPE = "sType";                                // int 门店出入库方式
        public static final String CHANGE_COUNT = "changeCount";                    // int 变动库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 变动后剩余库存
        public static final String PRICE = "price";                                 // long 采购单价（成本价）
        public static final String NUMBER = "number";                               // String 单号（天时间+4位以上顺序数字：如2010120001）
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String HEAD_SID = "headSid";                            // int 加减库存负责人 id
        public static final String OPT_TIME = "optTime";                            // datetime 进/出库时间
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String REMARK = "remark";                               // String 备注

        // for init
        public static final String IN_PD_SC_STR_NAME_LIST = ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST;
    }

    /**
     * 商品业务销售总表
     */
    public static final class BizSalesSummaryInfo {
        public static final String AID = "aid";                                     // int 企业aid
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
    }
    /**
     * 商品销售总表
     */
    public static final class SalesSummaryInfo {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String PD_ID = "pdId";                                  // int 商品 id    (Primary Key 2)
        public static final String MIN_PRICE = "minPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String MAX_PRICE = "maxPrice";                          // long 商品交易最小价格(做搜索，从 sku 冗余)
        public static final String COUNT = "count";                                 // int 商品总库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String REMAIN_COUNT = "remainCount";                    // int 商品总剩余库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String HOLDING_COUNT = "holdingCount";                  // int 商品总预扣库存（数据不完全实时，做搜索，从 sku 冗余）
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
}
