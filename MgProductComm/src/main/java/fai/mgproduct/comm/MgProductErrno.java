package fai.mgproduct.comm;

/**
 * 商品中台Errno
 */
public class MgProductErrno {
    /**
     * 库存服务相关Errno
     * 1000~2000
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
}
