package fai.MgProductSpecSvr.domain.entity;

/**
 * 规格模板
 */
public class SpecTempEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String TP_SC_ID = "tpScId";                             // int 规格模板 id （aid下自增） (Primary Key 2)
        public static final String NAME = "name";                                   // varchar(100) 规格模板名称
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格模板的 项目id
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    private static final String[] VALID_KEYS = new String[]{
          Info.TP_SC_ID,
          Info.NAME,
          Info.SOURCE_TID
    };
    /**
     * 支持批量更新的字段
     */
    public static String[] getValidKeys(){
        return VALID_KEYS;
    }
}
