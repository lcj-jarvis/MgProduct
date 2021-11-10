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

    public static final class Flag {
        // 门店通flag定义
        public static class YkRlFlag {
            public static final int IS_SHOW = 0x1;                    //门店独立修改 是否展示在会员端
            public static final int IF_TOP = 0x2;                 //是否置顶状态;True表示处于置顶状态；false表示没有处于置顶状态；门店独立修改
        }
        public static class YkFlag {
            public static final int HEAD_SET_COUNT = 0x4;                    //true 总部设置过库存
            public static final int NOT_ORIGIN_BUY = 0x8;                        //为ture 不能原价购买（普通开单） 只能在积分商城兑换
            public static final int ALL_DISTRIBUTE_TYPES = 0x10;                //为ture时 拥有全部配送方式 false 没有全部
            public static final int LINK_SHOP = 0x20;                        //为ture时 关联积分商城
            public static final int MULTI_SPEC = 0x40;                   //是否为多规格;true:多规格;默认false为统一规格
            public static final int IS_DEFAULT_SPEC = 0x80;                   //是否系统默认生成的规格;true:系统默认生成规格; false:用户自定义规格;
            public static final int IS_JOIN_GROUP = 0x100;                   //是否正在参与拼团活动，在查询时更新，目前只有查询列表时会更新
            public static final int NEED_INIT_IN_STORAGE_COST_PRICE = 0x200;                   //是否需要初始化成本;true需要;
        }

        // 建站、商城flag定义
        public static class SiteRlFlag {

        }
        public static class SiteFlag {

        }
    }

    @Deprecated
    // 对外统一使用 ProductValObj
    public static final class ProductRelValObj {
        public static class Status {
            public static final int UP = ProductValObj.Status.UP; // 上架
            public static final int DOWN = ProductValObj.Status.DOWN; // 下架
            public static final int DEL = ProductValObj.Status.DEL; // 删除
        }

        public static class Limit {
            public static final int COUNT_MAX = 300000;
        }
    }
}
