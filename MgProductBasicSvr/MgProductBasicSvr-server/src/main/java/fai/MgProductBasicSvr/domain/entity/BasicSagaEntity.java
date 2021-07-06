package fai.MgProductBasicSvr.domain.entity;

/**
 * Saga模式 补偿记录定义
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 17:28
 */
public class BasicSagaEntity {

    public static final class Info {
        public static final String AID = "aid";
        public static final String XID = "xid";
        public static final String BRANCH_ID = "branchId";
        public static final String PROP = "prop";
        public static final String STATUS = "status";
        public static final String SYS_CREATE_TIME = "sysCreateTime";
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";
    }

    public static final class PropInfo {
        public static final String UNION_PRI_ID = ProductRelEntity.Info.UNION_PRI_ID;
        public static final String RL_PD_ID = ProductRelEntity.Info.RL_PD_ID;
        public static final String RL_GROUP_IDS = ProductRelEntity.Info.RL_GROUP_IDS;
        public static final String RL_PROPS = ProductRelEntity.Info.RL_PROPS;
    }
}
