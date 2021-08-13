package fai.MgProductSpecSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/27 9:48
 */
public class SpecSagaEntity {

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

        public static final String SPEC = "spec";                    // 商品规格表的补偿信息
        public static final String SPEC_SKU = "specSku";             // 商品规格SKU表的补偿信息
        public static final String SPEC_SKU_CODE = "specSkuCode";    // 商品规格skuCode表的补偿信息
        public static final String SPEC_STR = "specStr";             // 规格字符串表的补偿信息

        public static final String PD_ID_LIST = "pdIdList";
        public static final String DEL_SKU_ID_LIST = "delSkuIdList";
        public static final String SOFT_DEL = "softDel";
        public static final String UNION_PRI_ID = "unionPriId";

        /** specSku 表 补偿需要的字段 **/
        public static final String OLD_DATA_LIST = "oldDataList";       // 一般存放修改前的旧数据集合
        public static final String DO_BATCH_UPDATER = "doBatchUpdater"; // 更新的字段

        /** specSkuCode 表 补偿需要的字段 **/
        public static final String ADD_SKU_CODE_LIST = "addSkuCodeList";        // 添加的 skuCode 数据 FaiList<String> 记录 skuCode
        public static final String DEL_SKU_CODE_LIST = "addSkuCodeList";        // 删除的 skuCode 数据的主键以及原来的更新时间 FaiList<Param>
        public static final String UPDATE_SKU_CODE_LIST = "updateSkuCodeList";  // 需要修改 skuCode 的 old data FaiList<Param>
        public static final String SORT_SKU_CODE_LIST = "sortSkuCodeList";      // 需要修改 sort 的 old data FaiList<Param>

        /** mgSpecStr 表 补偿需要的字段 **/
        public static final String SC_STR_NAME_LIST = "scStrNameList";          // 规格字符串名称列表 FaiList<String>
    }
}
