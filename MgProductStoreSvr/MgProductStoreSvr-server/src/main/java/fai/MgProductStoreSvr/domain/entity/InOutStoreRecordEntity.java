package fai.MgProductStoreSvr.domain.entity;

/**
 * 出入库记录表
 */
public class InOutStoreRecordEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String SKU_ID = "skuId";                                // bigint SKU id    (Primary Key 3)
        public static final String IN_OUT_STORE_REC_ID = "ioStoreRecId";            // int 出入库存记录id (Primary Key 4)
        public static final String PD_ID = "pdId";                                  // int 商品 id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建商品的联合主键 id TODO
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String OPT_TYPE = "optType";                            // tinyInt 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // tinyInt 总部出入库方式
        public static final String S_TYPE = "sType";                                // tinyInt 门店出入库方式
        public static final String CHANGE_COUNT = "changeCount";                    // int 变动库存
        public static final String AVAILABLE_COUNT = "availableCount";              // int 当前入库记录剩余可用库存量
        public static final String REMAIN_COUNT = "remainCount";                    // int 变动后剩余库存
        public static final String PRICE = "price";                                 // bigint 采购单价（成本价）
        public static final String NUMBER = "number";                               // varchar(15) 单号（天时间+4位以上顺序数字：如2010120001）
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String HEAD_SID = "headSid";                            // int 加减库存负责人 id
        public static final String OPT_TIME = "optTime";                            // datetime 进/出库时间
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String REMARK = "remark";                               // varchar(100) 备注
        public static final String RL_ORDER_CODE = "rlOrderCode";                   // varchar(32) 业务订单code
        public static final String RL_REFUND_ID = "rlRefundId";                     // varchar(32) 业务退款编号
        public static final String KEEP_INT_PROP1 = "keepIntProp1";                 // int 整型 保留字段1 TODO
        public static final String KEEP_PROP1 = "keepProp1";                        // varchar(255) 字符串 保留字段1 TODO

    }


}
