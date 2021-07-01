package fai.MgProductBasicSvr.domain.entity;

/**
 * Saga模式 补偿记录定义
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 17:28
 */
public class ProductSagaEntity {

    public static final class Info {
        public static final String AID = "aid";
        public static final String XID = "xid";
        public static final String BRANCH_ID = "branchId";
        public static final String ROLLBACK_INFO = "rollbackInfo";
        public static final String STATUS = "status";
        public static final String SYS_CREATE_TIME = "sysCreateTime";
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";
    }
}
