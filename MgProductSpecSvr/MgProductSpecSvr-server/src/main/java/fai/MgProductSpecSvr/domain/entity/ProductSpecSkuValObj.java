package fai.MgProductSpecSvr.domain.entity;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuValObj {
    /**
     * 限制
     */
    public static final class Limit{
        // 单个产品下sku数量的最大限制
        public static final int SINGLE_PRODUCT_MAX_SIZE = 10000;
        public static final class SkuNum {
            public static final int MIN_LEN = 1; // 最小长度
            public static final int MAX_LEN = 20; // 最大长度
        }
        public static final class InPdScValIdSkuList{
            public static final int MAX_SIZE = 15; // 最多15个元素
        }
    }
}
