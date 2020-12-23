package fai.MgProductInfSvr.interfaces.entity;

/**
 * 规格模板详情
 */
public class SpecTempDetailEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_TP_SC_ID = SpecTempBizRelEntity.Info.RL_TP_SC_ID; // int 规格模板 id
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id
        public static final String NAME = SpecStrEntity.Info.NAME;                  // 规格字符串
        public static final String TP_SC_DT_ID = "tpScDtId";                        // int 规格模板详情 id
        public static final String SORT = "sort";                                   // int 规格排序
        public static final String FLAG = "flag";                                   // int flag
        /**
         * @see SpecTempDetailValObj.InScValList
         * */
        public static final String IN_SC_VAL_LIST = "inScValList";                  // 规格值(FaiList<Param>) 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
}
