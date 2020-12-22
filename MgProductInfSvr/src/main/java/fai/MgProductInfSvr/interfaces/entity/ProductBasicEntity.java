package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品基础信息对外Entity
 */
public class ProductBasicEntity {
    public static class BindPropInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务id
        public static final String RL_PROP_IDS = "rlPropIds"; // int 参数业务id集合

        public static final String RL_PROP_ID = "rlPropId"; // int 参数业务id
        public static final String PROP_VAL_ID = "propValId"; // int 参数值id
        public static final String VAL = "val"; // varchar 参数值
    }
}
