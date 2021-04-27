package fai.MgProductInfSvr.interfaces.utils;

public class MgProductSearchResult {
    public static final class Info {
        public static final String MANAGE_DATA_CACHE_TIME = "mdct";    // 搜索时，相关管理态数据最后修改的时间
        public static final String VISTOR_DATA_CACHE_TIME = "vdct";    // 搜索时，相关访客数据最后修改的时间
        public static final String ID_LIST = "il";       //  缓存的 idList
        public static final String INFO_LIST = "fl";     //  InfoList
        public static final String TOTAL = "tl";         //  缓存搜索的结果总量
    }
}
