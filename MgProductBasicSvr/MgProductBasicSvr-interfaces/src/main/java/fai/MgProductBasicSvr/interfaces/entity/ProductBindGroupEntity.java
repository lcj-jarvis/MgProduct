package fai.MgProductBasicSvr.interfaces.entity;

import fai.comm.util.FaiList;

public class ProductBindGroupEntity {
    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String SYS_TYPE = "sysType"; // int 系统分类(商品/服务)
        public static final String RL_GROUP_ID = "rlGroupId"; // int 分类业务 id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    static {
        MANAGE_FIELDS = new FaiList<String>();
        MANAGE_FIELDS.add(Info.RL_GROUP_ID);
        MANAGE_FIELDS.add(Info.RL_PD_ID);
        MANAGE_FIELDS.add(Info.PD_ID);
        MANAGE_FIELDS.add(Info.SYS_TYPE);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<String>();
        VISITOR_FIELDS.setReadOnly(true);
    }
}
