package fai.MgProductStoreSvr.domain.entity;


/**
 * 出入库记录
 */
public class InOutStoreRecordValObj {
    /**
     * 限制
     */
    public static final class Limit{

    }
    /**
     * 默认值
     */
    public static final class Default{
        public static final int SORT = 1;
    }

    /**
     * 标志位
     */
    public static final class FLag{

    }

    /**
     * 操作类型
     */
    public static final class OptType{
        public static final int IN = 1;  // 入库操作
        public static final int OUT = 2;   // 出库操作
    }

}
