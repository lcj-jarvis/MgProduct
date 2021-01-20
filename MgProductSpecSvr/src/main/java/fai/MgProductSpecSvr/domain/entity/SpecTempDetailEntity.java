package fai.MgProductSpecSvr.domain.entity;

/**
 * 规格模板详情
 */
public class SpecTempDetailEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String TP_SC_ID = "tpScId";                             // int 规格模板 id  (Primary Key 2)
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id (Primary Key 3)
        public static final String TP_SC_DT_ID = "tpScDtId";                        // int 规格模板详情 id
        public static final String SORT = "sort";                                   // int 规格排序
        public static final String FLAG = "flag";                                   // int flag
        public static final String IN_SC_VAL_LIST = "inScValList";                  // varchar(3500) 规格值(FaiList<Param>([{scStrId1,FileId}...])), 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    private static final String[] VALID_KEYS = new String[]{
            Info.SC_STR_ID,
            Info.TP_SC_DT_ID,
            Info.SORT,
            Info.FLAG,
            Info.IN_SC_VAL_LIST
    };
    /**
     * 支持批量更新的字段
     */
    public static String[] getValidKeys(){
        return VALID_KEYS;
    }
}
