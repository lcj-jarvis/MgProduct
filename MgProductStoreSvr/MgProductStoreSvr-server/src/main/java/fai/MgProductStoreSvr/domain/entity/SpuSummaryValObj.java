package fai.MgProductStoreSvr.domain.entity;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/12/8 15:40
 */
public class SpuSummaryValObj {

    /**
     * 数据状态
     */
    public static final class Status{
        public static final int DEL = -1;       // 删除, 删除状态统一用 -1
        public static final int DEFAULT = 0;    // 默认
    }
}
