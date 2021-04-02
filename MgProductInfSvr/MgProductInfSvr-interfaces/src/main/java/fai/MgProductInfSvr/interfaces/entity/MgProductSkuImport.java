package fai.MgProductInfSvr.interfaces.entity;

/**
 * 用于导入sku维度的商品数据
 */
public class MgProductSkuImport {
    public static final class Info{
        public static final String AID = "aid";                                 // int 企业id
        public static final String TID = "tid";                                 // int 项目id
        public static final String SITE_ID = "siteId";                          // int 站点id
        public static final String LG_ID = "lgId";                              // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1";                 // int 保留主键1
        public static final String RL_PD_ID = "rlPdId";                         // int 商品业务id
        public static final String NAME = "name";                               // String 商品名称

        public static final String PD_NUM_LIST = "pdNumList";                   // FaiList<String> 商品条码，最多10个，全局唯一
        public static final String SPEC_NAME_LIST = "specNameList";             // FaiList<String> 规格名称 最多10个
        public static final String SPEC_VAL_LIST = "specValList";               // FaiList<String> 规格值
        public static final String SKU_NUM_LIST = "skuNumList";                 // FaiList<String> 规格条码，最多10个，全局唯一
        public static final String PRICE = "price";                             // long 售价
        public static final String COUNT = "count";                             // int 初始库存
    }

}
