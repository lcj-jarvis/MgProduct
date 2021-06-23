package fai.MgProductInfSvr.interfaces.entity;

public class MgProductEntity {
    public final class Info{
        public static final String TID = "tid";  // int 项目id
        public static final String SITE_ID = "siteId"; // int 站点id
        public static final String LGID = "lgId"; // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1"; //int 保留主键1

        /** @see ProductBasicEntity.ProductInfo */
        public static final String BASIC = "basic";                         // Param 基础信息

        /** @see ProductSpecEntity.SpecInfo */
        public static final String SPEC = "spec";                           // FaiList<Param> 规格

        /** @see ProductSpecEntity.SpecSkuInfo */
        public static final String SPEC_SKU = "specSku";                    // FaiList<Param> 规格sku

        /** @see ProductStoreEntity.StoreSalesSkuInfo */
        public static final String STORE_SALES = "storeSales";              // FaiList<Param> 库存销售

        /** @see ProductStoreEntity.StoreSalesSkuInfo */
        public static final String SPU_SALES = "spuSales";                  // FaiList<Param> spu库存销售

        /**
         * @see fai.mgproduct.comm.MgProductErrno
         */
        public static final String ERRNO = "errno";                         // int 错误码 目前用于批量导入，导入失败的错误码


        public static final String PD_ID = "pdId";                          // int 商品id 商品中台内部使用
        public static final String RL_PD_ID = "rlPdId";                     // int 商品业务id 商品中台内部使用
    }
}
