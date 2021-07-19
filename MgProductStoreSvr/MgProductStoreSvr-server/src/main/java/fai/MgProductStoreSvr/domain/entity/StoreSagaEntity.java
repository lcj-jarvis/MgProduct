package fai.MgProductStoreSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/12 12:00
 */
public class StoreSagaEntity {

    public static final class Info {
        public static final String AID = "aid";
        public static final String XID = "xid";
        public static final String BRANCH_ID = "branchId";
        public static final String PROP = "prop";
        public static final String STATUS = "status";
        public static final String SYS_CREATE_TIME = "sysCreateTime";
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";
    }

    public static final class PropInfo {

        /** @see StoreSaleSKU */
        public static final String STORE_SALE_SKU = "storeSaleSku";            // 规格库存销售 SKU 的补偿信息  FaiList<Param>

        public static final String SPU_BIZ_SUMMARY = "SpuBizSummary";          // 规格库存销售 SPU 的补偿信息 FaiList<Param> 包含 pdId unionPriId

        public static final String SKU_SUMMARY = "skuSummary";                 // sku 汇总的补偿信息 FaiList<Param> 包含 skuId

        public static final String SPU_SUMMARY = "spuSummary";                 // spu 汇总的补偿信息 FaiList<Param> 包含 pdId

        public static final String IN_OUT_STORE_RECORD = "inOutStoreRecord";   // 出入库记录记录的补偿信息

        public static final String IN_OUT_STORE_SUM = "inOutStoreSum";         // 出入库记录汇总的补偿信息

        public static final String IO_STORE_REC_ID = "ioStoreRecId";           // 出入库记录id

        public static final String PD_ID_SET = "pdIdSet";

        public static final String SKU_ID_SET = "skuIdSet";

        public static final class StoreSaleSKU {
            public static final String UNION_PRI_ID = "unionPriId";
            public static final String SKU_ID = "skuId";
        }

    }
}
