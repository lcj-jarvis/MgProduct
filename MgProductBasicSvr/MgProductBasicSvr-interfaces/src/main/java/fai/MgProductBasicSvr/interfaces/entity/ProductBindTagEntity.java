package fai.MgProductBasicSvr.interfaces.entity;

import fai.comm.util.FaiList;

public class ProductBindTagEntity {

    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_TAG_ID = "rlTagId"; // int 标签业务 id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
    }

    public static final class Business {
        public static final String ADD_TAG_IDS = "addTagIds"; // 添加绑定的标签id
        public static final String DEL_TAG_IDS = "delTagIds"; // 删除绑定的标签id
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    static {
        MANAGE_FIELDS = new FaiList<String>();
        MANAGE_FIELDS.add(Info.RL_TAG_ID);
        MANAGE_FIELDS.add(Info.RL_PD_ID);
        MANAGE_FIELDS.add(Info.PD_ID);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<String>();
        VISITOR_FIELDS.setReadOnly(true);
    }

}
