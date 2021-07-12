package fai.MgProductTagSvr.application.domain.entity;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:59
 */
public class ProductTagValObj {

    public static final class Default {
        public static final int FLAG = 0;
    }

    /**
     * 一个aid下最多有多少个标签，先设为100
     */
    public static class Limit {
        public static final int COUNT_MAX = 100;
    }
}
