package fai.MgProductInfSvr.interfaces.entity;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/3 13:48
 */
public class ProductGroupValObj {

    public static final class Flag {

    }

    public static final class Default {
        public static final int SORT = 0;
        public static final int RL_FLAG = 0;
    }

    public static class Limit {
        public static final int COUNT_MAX = 2000;
    }

    public static class SysType {
        public static final int PRODUCT = 1;
        public static final int SERVICE = 2;
    }

    public static class Status {
        public static final int DEFAULT = 0;
        public static final int DEL = -1;
    }
}
