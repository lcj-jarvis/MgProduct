package fai.MgProductSpecSvr.domain.entity;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 1)
        public static final String SKU_ID = "skuId";                                // bigInt SKU id（aid下自增），系统对内, 也做排序 (Primary Key 2)
        public static final String PD_ID = "pdId";                                  // int 商品 id
        public static final String SORT = "sort";                                   // int 排序
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格的 项目id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建产品规格的 联合主键id
        public static final String SKU_CODE = "skuCode";                            // varchar(32) 条码
        public static final String IN_PD_SC_STR_ID_LIST = "inPdScStrIdList";        // varchar(150) 规格值 FaiList<Integer>, 支持最大 15 种规格  存储到db时需要排序下，用于查询
        public static final String IN_PD_SC_LIST = "inPdScList";                    // varchar(255) 规格id-规格值id FaiList<String>, 规格值id和inPdScStrIdList一一对应, 若修改了inPdScStrIdList，需同步修改inPdScList
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // tinyint 状态
    }
    private static final String[] VALID_KEYS = new String[]{
            Info.SKU_ID,
            Info.SORT,
            Info.FLAG,
            Info.SKU_CODE,
            Info.IN_PD_SC_STR_ID_LIST
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
    private static final String[] SAGA_KEYS = new String[]{
            Info.AID,
            Info.SKU_ID,
            Info.SORT,
            Info.FLAG,
            Info.SKU_CODE,
            Info.IN_PD_SC_STR_ID_LIST,
            Info.IN_PD_SC_LIST,
            Info.STATUS,
            Info.SYS_UPDATE_TIME
    };

    public static String[] getSagaKeys() {
        return SAGA_KEYS;
    }
}
