package fai.MgProductSpecSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/27 9:48
 */
public class SpecSagaEntity {

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
        public static final String PD_ID_LIST = "pdIdList";
        public static final String DEL_SKU_ID_LIST = "delSkuIdList";
        public static final String SOFT_DEL = "softDel";
    }
}
