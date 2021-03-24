package fai.MgProductSpecSvr.interfaces.entity;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuValObj {
    public static final class Limit {
        // 单个产品下sku数量的最大限制
        public static final int SINGLE_PRODUCT_MAX_SIZE = 10000;
    }
    /**
     * 数据状态
     */
    public static final class Status{
        public static final int DEL = -1;       // 删除, 删除状态统一用 -1
        public static final int DEFAULT = 0;    // 默认
    }

    /**
     * 标志位
     */
    public static final class FLag{
        public static final int ALLOW_EMPTY = 0x2; // 允许是空规格值组合的sku
    }
}
