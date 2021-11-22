package fai.MgProductSpecSvr.domain.entity;

/**
 * 产品规格
 */
public class ProductSpecEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String PD_ID = "pdId";                                  // int 商品 id    (Primary Key 2)
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id (Primary Key 3)
        public static final String PD_SC_ID = "pdScId";                             // int 商品规格 id
        public static final String SOURCE_TID = "sourceTid";                        // int 创建产品规格的 项目id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建产品规格的 联合主键id
        public static final String SORT = "sort";                                   // int 排序
        public static final String FLAG = "flag";                                   // int flag
        public static final String IN_PD_SC_VAL_LIST = "inPdScValList";             // varchar(4000) 规格值FaiList<Param>, 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // int 数据状态
    }
    private static final String[] VALID_KEYS = new String[]{
            Info.SC_STR_ID,
            Info.PD_SC_ID,
            Info.SORT,
            Info.FLAG,
            Info.IN_PD_SC_VAL_LIST,
            Info.STATUS
    };
    /**
     * 支持批量更新的字段
     */
    public static String[] getValidKeys(){
        return VALID_KEYS;
    }

    /**
     * Saga 修改需要记录的所有字段
     */
    private static final String[] UPDATE_KEYS = new String[]{
            Info.AID,
            Info.PD_ID,
            Info.SC_STR_ID,
            Info.PD_SC_ID,
            Info.SORT,
            Info.FLAG,
            Info.IN_PD_SC_VAL_LIST,
            Info.SYS_UPDATE_TIME,
            Info.STATUS
    };

    public static String[] getPriAndUpdateKey() {
        return UPDATE_KEYS;
    }
}
