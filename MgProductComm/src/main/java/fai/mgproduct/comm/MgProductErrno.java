package fai.mgproduct.comm;

/**
 * 商品中台Errno
 */
public class MgProductErrno {
    /**
     * 库存服务相关Errno
     * [1000, 1200)
     */
    public static final class Store{
        /**
         * 库存不足
         */
        public static final int SHORTAGE = 1000;
        /**
         * 重复扣减的库存量不同
         */
        public static final int REPEAT_REDUCE_COUNT_DIF = 1001;
        /**
         * 重复补尝的库存量不同
         */
        public static final int REPEAT_MAKEUP_COUNT_DIF = 1002;
    }

    /**
     * 商品导入相关错误码
     * [1200, 1500)
     */
    public static final class Import{
        /**
         * skuCode已经存在
         */
        public static final int SKU_CODE_ALREADY_EXISTS = 1200;
        /**
         * skuCode长度超过限制
         */
        public static final int SKU_CODE_LEN_LIMIT = 1201;
        /**
         * skuCode 数量超过限制
         */
        public static final int SKU_CODE_SIZE_LIMIT = 1202;
        /**
         * 基础信息为空
         */
        public static final int BASIC_IS_EMPTY = 1203;
        /**
         * 规格名称长度超过限制
         */
        public static final int SPEC_NAME_LEN_LIMIT = 1204;
        /**
         * 规格值名称长度超过限制
         */
        public static final int SPEC_VAL_NAME_LEN_LIMIT = 1205;
        /**
         * 规格值集合为空
         */
        public static final int IN_PD_SC_VAL_LIST_IS_EMPTY = 1206;
        /**
         * 规格值名称重复
         */
        public static final int SPEC_VAL_NAME_REPEAT = 1207;
        /**
         * 重复导入
         */
        public static final int REPEAT_IMPORT = 1208;
    }
}
