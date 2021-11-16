package fai.MgProductSpecSvr.interfaces.entity;

/**
 * 规格模板业务关联
 */
public class SpecTempBizRelEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id  (Primary Key 2)
        public static final String RL_LIB_ID = "rlLibId";                           // int 库业务 id   (Primary Key 3)
        public static final String RL_TP_SC_ID = "rlTpScId";                        // int 规格模板业务 Id
        public static final String TP_SC_ID = "tpScId";                             // int 规格模板 id
        public static final String SORT = "sort";                                   // int 排序
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String SYS_TYPE = "sysType";                            // int 商品系统类型 (服务/商品)
    }
}
