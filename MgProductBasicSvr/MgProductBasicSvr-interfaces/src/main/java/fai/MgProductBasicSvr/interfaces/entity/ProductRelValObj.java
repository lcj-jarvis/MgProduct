package fai.MgProductBasicSvr.interfaces.entity;

public class ProductRelValObj {
    public static class Status {
        public static final int UP = 1; // 上架
        public static final int DOWN = 2; // 下架
        public static final int DEL = 3; // 删除
    }

    public static class Limit {
        public static final int COUNT_MAX = 300000;
    }
}
