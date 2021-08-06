package fai.MgProductBasicSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 19:30
 */
public class BasicSagaValObj {

    public static final class Status {
        /** 初始化 */
        public static final int INIT = 0;
        /** 已补偿 */
        public static final int ROLLBACK_OK = 1;
    }

    public static final class SagaOp {
        /** 新增操作 */
        public static final int ADD = 1;
        /** 更新操作 */
        public static final int UPDATE = 2;
        /** 删除操作 */
        public static final int DEL = 3;
    }
}
