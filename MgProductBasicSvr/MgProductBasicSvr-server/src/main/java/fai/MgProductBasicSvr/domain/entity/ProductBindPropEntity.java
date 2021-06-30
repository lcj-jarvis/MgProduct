package fai.MgProductBasicSvr.domain.entity;

import fai.comm.util.FaiList;

public class ProductBindPropEntity {

    public static final class BUSINESS {
        public static final String ADD_LIST = "addList";
        public static final String DEL_LIST = "delList";
    }

    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String RL_PROP_ID = "rlPropId"; // int 参数业务 id
        public static final String PROP_VAL_ID = "propValId"; // int 参数值 id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    static {
        MANAGE_FIELDS = new FaiList<String>();
        MANAGE_FIELDS.add(Info.RL_PD_ID);
        MANAGE_FIELDS.add(Info.PROP_VAL_ID);
        MANAGE_FIELDS.add(Info.PD_ID);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<String>();
        VISITOR_FIELDS.setReadOnly(true);
    }
}
