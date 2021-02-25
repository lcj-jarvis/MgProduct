package fai.MgProductSearchSvr.domain.entity;

public class MgProductSearchResultEntity {
    public static final class Info {
        public static final String CACHE_TIME = "ct";    // long  System.currentTimeMillis(), 搜索结果缓存的最大时间
        public static final String ID_LIST = "il";       //  缓存的 idList
        public static final String TOTAL = "tl";         //  缓存搜索的结果总量
    }
}
