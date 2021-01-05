package fai.MgProductInfSvr.interfaces.entity;

public class ProductBasicValObj {

    public static final class ProductValObj {
        public static class Limit {
            public static final int NAME_MAXLEN = 100;
            public static final int COUNT_MAX = 300000;
        }
    }

    public static final class ProductRelValObj {
        public static class Status {
            public static final int UP = 1; // 上架
            public static final int DOWN = 2; // 下架
            public static final int DEL = 3; // 删除
        }

        public static class Limit {
            public static final int COUNT_MAX = 300000;
        }
    }
}
