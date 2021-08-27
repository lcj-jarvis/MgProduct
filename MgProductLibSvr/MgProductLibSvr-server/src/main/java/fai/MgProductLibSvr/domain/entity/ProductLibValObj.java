package fai.MgProductLibSvr.domain.entity;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:20
 */
public class ProductLibValObj {

    public static final class Default {
        public static final int FLAG = 0;
    }

    /**
     * 一个aid下最多有多少个库，先设为100
     */
    public static class Limit {
        public static final int COUNT_MAX = 100;
    }
}
