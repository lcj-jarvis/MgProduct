package fai.MgProductStoreSvr.domain.entity;

public class SpuBizSummaryValObj {

    /**
     * 标志位
     */
    public static final class FLag{
        public static final int SETED_PRICE = 0x1;                              // 是否已设置价格
    }

    /**
     * 数据状态
     */
    public static final class Status{
        public static final int DEL = -1;       // 删除, 删除状态统一用 -1
        public static final int DEFAULT = 0;    // 默认
    }
}
