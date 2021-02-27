package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品规格库存销售sku
 */
public class ProductStoreValObj {
    public static final class StoreSalesSku{
        /**
         * 扣减库存模式
         */
        public static final class ReduceMode{
            /**
             * 直接扣除库存：
             *  真实库存-1
             */
            public static final int DIRECT = 1;
            /**
             * 预计扣除库存：
             *  1、真实库存-1，预扣库存+1
             *  2、预扣库存-1
             */
            public static final int HOLDING = 2;
        }
        /**
         * 标志位
         */
        public static final class FLag{
            public static final int SETED_PRICE = 0x1;                              // 是否已设置价格
            public static final int OPEN_MIN_AMOUNT = 0x2;                          // 开启起购量
            public static final int OPEN_MAX_AMOUNT = 0x4;                          // 开启限购量
            public static final int OPEN_VIRTUAL_COUNT = 0x8;                       // 开启虚拟库存
        }
    }
    /**
     * 出入库记录
     */
    public static class InOutStoreRecord{
        /**
         * 限制
         */
        public static final class Limit{
            /**
             * @see ProductStoreEntity.InOutStoreRecordInfo#REMARK
             */
            public static final class Remark{
                public static final int MAX_LEN = 100; // 最大长度
            }
        }
        /**
         * 标志位
         */
        public static final class FLag{
            public static final int NOT_CHANGE_COUNT = 0x1;   // 不改变总库存
        }

        /**
         * 操作类型
         */
        public static final class OptType{
            public static final int IN = 1;  // 入库操作
            public static final int OUT = 2;   // 出库操作
        }
    }
}
