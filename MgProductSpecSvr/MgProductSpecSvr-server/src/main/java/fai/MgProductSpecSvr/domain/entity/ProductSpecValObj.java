package fai.MgProductSpecSvr.domain.entity;

/**
 * 产品规格 - 值对象
 */
public class ProductSpecValObj {
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
            public static final String IN_PD_SC_VAL_LIST_STR = "[{}]";
            public static final class Item{
                public static final boolean CHECK = false;
                public static final String FILE_ID = "";
            }
            public static final String NAME = "全部"; // 规格值名称
        }
        public static final String ALL_PRODUCT_SPEC_NAME = "全部"; // 商品规格无规格时的规格名称，配合Flag.ALL使用，因为有可能客户自定义了
    }

    /**
     * 标志位
     */
    public static final class FLag{
        @Deprecated
        public static final int ALL = 0x1; // 是否是全部规格 - 用于兼容商品从无规格过渡到有规格
        public static final int IN_PD_SC_VAL_LIST_CHECKED = 0x2; // InPdScValList是否有勾选 for 查询
        public static final int ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY= 0x4;  //允许 inPdScValList 是否为空
        public static final int SYS_SPEC = 0x8; // 系统规格，置起为true，不能删除，可修改
        public static final int SYS_FIX_SPEC = 0x10; // 系统规格，置起为true，不能删除，不能修改
    }
    /**
     * 规格值定义
     *
     */
    public static final class InPdScValList{
        public static final class Item{
            public static final String SC_STR_ID = "si"; //规格字符串id (必)
            public static final String CHECK = "c"; // 是否勾选
            public static final String FILE_ID = "fi"; // 关联文件id
        }
    }

    /**
     * 数据状态
     */
    public static final class Status{
        public static final int DEL = -1;       // 删除, 删除状态统一用 -1
        public static final int DEFAULT = 0;    // 默认
    }
}
