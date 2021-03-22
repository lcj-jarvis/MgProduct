package fai.MgProductBasicSvr.interfaces.entity;

public class ProductValObj {

    public static class Limit {
        public static final int NAME_MAXLEN = 100;
        public static final int COUNT_MAX = 300000;
    }

    public static class Status {
        public static final int DEL = -1; // 删除, 删除状态统一用 -1
    }
}
