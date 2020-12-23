package fai.MgProductInfSvr.interfaces.entity;

/**
 * 规格模板 - 对外
 */
public class SpecTempEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_TP_SC_ID = SpecTempBizRelEntity.Info.RL_TP_SC_ID;
        public static final String RL_LIB_ID = SpecTempBizRelEntity.Info.RL_LIB_ID;
        public static final String TP_SC_ID = SpecTempBizRelEntity.Info.TP_SC_ID;
        public static final String NAME = "name";                                   // varchar(100) 规格模板名称
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格模板的 项目id
        public static final String FLAG = SpecTempBizRelEntity.Info.FLAG;
        public static final String SORT = SpecTempBizRelEntity.Info.SORT;
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
}
