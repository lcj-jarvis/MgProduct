package fai.MgProductSpecSvr.domain.entity;

import fai.comm.util.FaiList;
import fai.comm.util.Param;

/**
 * 规格模板详情 - 值对象
 */
public class SpecTempDetailValObj {
    /**
     * 限制
     */
    public static final class Limit{
        public static final class InScValList{
            public static final int MIN_LEN = 0; // 最小长度
            public static final int MAX_LEN = 3000; // 最大长度
            public static final int MAX_SIZE = 50; // 最多50个元素
        }
    }

    /**
     * 默认值
     */
    public static final class Default{
        public static final int SORT = 1;
        public static final FaiList<Param> IN_SC_VAL_LIST = new FaiList<>();
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
            public static final String FILE_ID = "fi";
        }
    }

}
