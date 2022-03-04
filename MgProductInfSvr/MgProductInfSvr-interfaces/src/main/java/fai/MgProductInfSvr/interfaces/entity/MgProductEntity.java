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
        public static final String ADD_SPEC = "addSpec";                    // FaiList<Param>
        public static final String DEL_SPEC = "delSpec";                    // FaiList<Integer>
        public static final String UP_SPEC = "upSpec";                      // FaiList<ParamUpdater>

        public static final String FROM_RL_PD_ID = "fromRlPdId";
        public static final String TO_RL_PD_ID = "toRlPdId";

        public static final String FROM_RL_ID = "fromRlId";
        public static final String TO_RL_ID = "toRlId";
    }

    /**
     * 商品中台所有数据选项, 之后但凡是有业务控制要操作哪些数据的，都用这个
     */
    public final class Option {
        public static final String BASIC = "basic";  // 基础数据
        public static final String GROUP = "group";  // 分类数据
        public static final String LIB = "lib";      // 商品库数据
        public static final String TAG = "tag";      // 标签数据
        public static final String PROP = "prop";    // 参数数据
    }
}
