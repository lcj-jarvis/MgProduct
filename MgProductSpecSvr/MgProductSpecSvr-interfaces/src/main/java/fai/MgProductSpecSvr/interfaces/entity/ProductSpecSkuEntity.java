package fai.MgProductSpecSvr.interfaces.entity;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String PD_ID = "pdId";                                  // int 商品 id
        public static final String SKU_ID = "skuId";                                // bigInt SKU id（aid下自增），系统对内, 也做排序
        public static final String SORT = "sort";                                   // int 排序
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格的 项目id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建产品规格的 联合主键id
        public static final String SKU_NUM = "skuNum";                              // String
        /**
         * 每个元素值:
         * @see SpecStrEntity.Info#SC_STR_ID
         */
        public static final String IN_PD_SC_STR_ID_LIST = "inPdScStrIdList";        // 规格值 id FaiList<Integer>, 支持最大 15 种规格
        /**
         * 每个元素值:
         * @see SpecStrEntity.Info#NAME
         */
        public static final String IN_PD_SC_STR_NAME_LIST = "inPdScStrNameList"; // 规格值 name FaiList<String>, 支持最大 15 种规格
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // tinyint 状态
    }
}
