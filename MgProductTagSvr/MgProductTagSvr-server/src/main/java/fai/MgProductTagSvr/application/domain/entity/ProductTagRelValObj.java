package fai.MgProductTagSvr.application.domain.entity;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:59
 */
public class ProductTagRelValObj {

    public static final class Default {
        public static final int SORT = 0;
        public static final int RL_FLAG = 0;
    }

    /**
     * 一个aid下最多100个标签
     */
    public static class Limit {
        public static final int COUNT_MAX = 100;
    }
}
