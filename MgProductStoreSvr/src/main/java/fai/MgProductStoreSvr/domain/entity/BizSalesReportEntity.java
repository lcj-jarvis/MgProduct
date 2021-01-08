package fai.MgProductStoreSvr.domain.entity;

/**
 * 业务销售上报
 */
public class BizSalesReportEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String PD_ID = "pdId";                                  // int 商品 id  (Primary Key 3)
        public static final String FLAG = "flag";                                   // int 标志位
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
