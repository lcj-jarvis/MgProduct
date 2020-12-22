package fai.MgProductSpecSvr.domain.entity;

import fai.comm.util.FaiList;
import fai.comm.util.Param;

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
            public static final FaiList<Param> IN_PD_SC_VAL_LIST = new FaiList<Param>();
            public static final class Item{
                public static final boolean CHECK = false;
                public static final String FILE_ID = "";
            }
        }
    }

    /**
     * 标志位
     */
    public static final class FLag{
        public static final int ALL = 0x1; // 是否是全部规格 - 用于兼容商品从无规格过渡到有规格
        public static final int IN_PD_SC_VAL_LIST_CHECKED = 0x2; // InPdScValList是否有勾选 for 查询
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
}
