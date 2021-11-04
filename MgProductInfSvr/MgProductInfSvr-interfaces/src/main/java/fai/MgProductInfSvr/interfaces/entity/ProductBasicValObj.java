package fai.MgProductInfSvr.interfaces.entity;

import fai.comm.util.Parser;

import java.util.Calendar;

public class ProductBasicValObj {

    public static final class ProductValObj {
        public static class SysType {
            public static final int DEFAULT = 0; // 默认，商品
            public static final int SERVICE = 1; // 服务
        }

        public static class Limit {
            public static final int NAME_MAXLEN = 100;
            public static final int COUNT_MAX = 300000;
        }

        public static class Status {
            public static final int DOWN = 0; // 下架
            public static final int UP = 1; // 上架
            public static final int DEL = -1; // 删除
        }

        public static class Top {
            public static final Calendar DEFAULT = Parser.parseCalendar("1970-01-01 08:00:00", "yyyy-MM-dd HH:mm:ss");
        }
    }

    @Deprecated
    // 对外统一使用 ProductValObj
    public static final class ProductRelValObj {
        public static class Status {
            public static final int UP = 1; // 上架
            public static final int DOWN = 2; // 下架
            public static final int DEL = -1; // 删除
        }

        public static class Limit {
            public static final int COUNT_MAX = 300000;
        }
    }
}
