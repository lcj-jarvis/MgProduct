package fai.MgProductInfSvr.interfaces.entity;

import fai.comm.util.FaiList;
import fai.comm.util.Param;

/**
 * 规格服务 - 值对象
 */
public class ProductSpecValObj {
    /**
     * 商品规格
     */
    public static final class Spec {
        /**
         * 限制
         */
        public static final class Limit{
            public static final class InPdScValList{
                public static final int MAX_SIZE = 50; // 最多50个元素
            }
        }
        /**
         * 默认值
         */
        public static final class Default{
            public static final int SORT = 1;
            public static final class InPdScValList{
                public static final FaiList<Param> IN_PD_SC_VAL_LIST = new FaiList<Param>();
                public static final class Item{
                    public static final boolean CHECK = false;
                    public static final String FILE_ID = "";
                }
            }
            public static final String ALL_PRODUCT_SPEC_NAME = "全部"; // 商品规格无规格时的规格名称，配合Flag.ALL使用，因为有可能客户自定义了
        }
        /**
         * 标志位
         */
        public static final class FLag{
            public static final int ALL = 0x1; // 是否是全部规格 - 用于兼容商品从无规格过渡到有规格
            public static final int IN_PD_SC_VAL_LIST_CHECKED = 0x2; // InPdScValList是否有勾选 for 查询
            public static final int ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY= 0x4;  //允许 inPdScValList 是否为空
        }
        /**
         * 规格值定义
         */
        public static final class InPdScValList{
            public static final class Item{
                public static final String SC_STR_ID = "si"; // 规格字符串id
                public static final String NAME = "n";  // 规格字符串
                public static final String CHECK = "c"; // 是否勾选
                public static final String FILE_ID = "fi"; // 关联文件id
            }
        }
    }

    /**
     * 商品规格sku
     */
    public static final class SpecSku {

        public static final class Limit {
            // 单个产品下sku数量的最大限制
            public static final int SINGLE_PRODUCT_MAX_SIZE = 10000;
        }

        /**
         * 数据状态
         */
        public static final class Status{
            public static final int DEL = -1;       // 删除, 删除状态统一用 -1
            public static final int DEFAULT = 0;    // 默认
        }

        /**
         * 标志位
         */
        public static final class FLag{
            public static final int ALLOW_EMPTY = 0x2; // 允许是空规格值组合的sku
        }
    }
    /**
     * 规格模板
     */
    public static final class SpecTempValObj {
        /**
         * 限制
         */
        public static final class Limit{
            public static final class Name{
                public static final int MIN_LEN = 1; // 最小长度
                public static final int MAX_LEN = 100; // 最大长度
            }

        }
        public final class Flag{

        }
    }
    /**
     * 规格模板详情
     */
    public static final class SpecTempDetail{
        /**
         * 限制
         */
        public static final class Limit{
            public static final class InScValList{
                public static final int MAX_SIZE = 50; // 最多50个元素
            }
        }
        /**
         * 默认值
         */
        public static final class Default{
            public static final int SORT = 1;
            public static final FaiList<Param> IN_SC_VAL_LIST = new FaiList<Param>();
        }

        /**
         * flag
         */
        public static final class Flag{
            public static final int DISABLE = 0x1;  //是否禁用
        }

        /**
         * 规格值定义
         */
        public static final class InScValList{
            public static final class Item{
                public static final String SC_STR_ID = "si";
                public static final String NAME = "n";
                public static final String FILE_ID = "fi";
            }
        }
    }

}
