package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品中台临时 Entity <br/>
 * 用于同步数据
 */
public class ProductTempEntity {
    public class Basic{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String TID = "tid";                                     // int 项目id
        public static final String SITE_ID = "siteId";                              // int 站点id
        public static final String LGID = "lgId";                                   // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                     // int 保留主键id1
        public static final String OWNER_RL_PD_ID = "ownerRlPdId";                  // int 该商品所属者的业务商品id
        public static final String RL_PD_ID = "rlPdId";                             // int 业务商品id
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间

        /**
         * 中台内部字段
         */
        public class Internal{
            public static final String UNION_PRI_ID = "unionPriId";                // int 联合主键id
            public static final String PD_ID = "pdId";                             // int 商品id
            public static final String SKU_ID = "skuId";                           // long skuId
        }
    }
    /**
     * 商品信息
     */
    public class ProductInfo extends Basic{
        public static final String PRICE = "price";                                 // long 商品价格
        public static final String SPEC_NAME = "specName";                          // String 规格
        public static final String SPEC_VAL_NAME = "specValName";                   // String 规格值
        public static final String COUNT = "count";                                 // int 设置的库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 剩余库存
        public static final String HOLDING_COUNT = "holdingCount";                  // int 预扣库存
    }

    /**
     * 库存记录
     */
    public class StoreRecordInfo extends Basic{
        public static final String IN_OUT_STORE_REC_ID = "ioStoreRecId";            // int 出入库存记录id
        public static final String OPT_TYPE = "optType";                            // int 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // int 总部出入库方式
        public static final String S_TYPE = "sType";                                // int 门店出入库方式
        public static final String CHANGE_COUNT = "changeCount";                    // int 变动库存
        public static final String REMAIN_COUNT = "remainCount";                    // int 变动后剩余库存
        public static final String REMARK = "remark";                               // String 备注
        public static final String RL_ORDER_CODE = "rlOrderCode";                   // String 业务订单code/id
        public static final String RL_REFUND_ID = "rlRefundId";                     // String 业务退款编号
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String HEAD_SID = "headSid";                            // int 加减库存负责人 id
        public static final String OPT_TIME = "optTime";                            // datetime 出/入库时间
    }

}
