package fai.mgproduct.comm.entity;

public class SagaEntity {
    public static final class Info {
        public static final String AID = "aid";
        public static final String XID = "xid";
        public static final String BRANCH_ID = "branchId";
        public static final String STATUS = "status";
        public static final String SYS_CREATE_TIME = "sysCreateTime";
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";
    }

    public static final class Common {
        public static final String XID = Info.XID;
        public static final String BRANCH_ID = Info.BRANCH_ID;
        public static final String SAGA_OP = "sagaOp";
        public static final String SAGA_TIME = "sagaTime";
    }
}
