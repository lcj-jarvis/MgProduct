package fai.MgProductBasicSvr.domain.entity;

public class ProductRelValObj {
    public static class Default {
        public static final int SORT = 0; // 默认
    }

    public static class Status {
        public static final int DOWN = 0; // 默认，下架
        public static final int UP = 1; // 上架
        public static final int DEL = -1; // 删除, 删除状态统一用 -1
    }

    public static class SysType {
        public static final int DEFAULT = 0; // 默认，商品
        public static final int SERVICE = 1; // 服务
    }

    public static class Limit {
        public static final int COUNT_MAX = 300000;
    }
}
