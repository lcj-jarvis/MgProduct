package fai.MgProductStoreSvr.domain.entity;

public class InOutStoreSumEntity {
    public static class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String IN_OUT_STORE_REC_ID = "ioStoreRecId";            // int 出入库存记录id (Primary Key 4)
        public static final String OPT_TYPE = "optType";                            // tinyInt 出/入库操作？（或者其他操作状态）
        public static final String C_TYPE = "cType";                                // tinyInt 总部出入库方式
        public static final String S_TYPE = "sType";                                // tinyInt 门店出入库方式
        public static final String PRICE = "price";                                 // bigint 入库：总成本 |  出库：先进先出方式计算的成本价
        public static final String MW_PRICE = "mwPrice";                            // bigint 出库：移动加权计算的成本价
        public static final String NUMBER = "number";                               // varchar(15) 单号（天时间+4位以上顺序数字：如2010120001）
        public static final String OPT_SID = "optSid";                              // int 操作员工 id
        public static final String OPT_TIME = "optTime";                            // datetime 进/出库时间
        public static final String REMARK = "remark";                               // varchar(100) 备注
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
