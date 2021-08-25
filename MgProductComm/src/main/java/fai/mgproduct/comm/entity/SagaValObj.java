package fai.mgproduct.comm.entity;

public class SagaValObj {

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

