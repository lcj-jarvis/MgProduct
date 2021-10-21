package fai.MgProductInfSvr.interfaces.entity;

public class ProductPropValObj {

    /**
     * 参数的值对象
     */
    public static final class Prop {

        public static final class RlFlag {
            public static final int CLOSED = 0x1; // 是否停用，默认为启用，置起为停用
        }

        public static final class Flag {}
    }

    /**
     * 参数值的值对象
     */
    public static final class PropVal {
        public static final class Default {
            public static final int DATA_TYPE = DataType.STRING;
            public static final int SORT = 0;
        }

        /**
         * 参数值数据类型
         */
        public static final class DataType {
            public static final int STRING = 0;
            public static final int INT = 1;
            public static final int FLOAT = 2;
            public static final int DOUBLE = 3;
            public static final int LONG = 4;
            public static final int BOOLEAN = 5;
        }
    }
}
