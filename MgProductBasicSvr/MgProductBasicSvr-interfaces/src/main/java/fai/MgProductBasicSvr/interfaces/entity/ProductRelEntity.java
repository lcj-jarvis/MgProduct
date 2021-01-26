package fai.MgProductBasicSvr.interfaces.entity;

public class ProductRelEntity {

    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String RL_LIB_ID = "rlLibId"; // int 库业务id
        public static final String SOURCE_TID = "sourceTid"; // int 创建商品的项目 id
        public static final String ADD_TIME = "addedTime"; // datetime 录入时间
        public static final String ADD_SID = "addedSid"; // int 录入商品的员工ID
        public static final String LAST_SID = "lastOptManagerSid"; // int 最后操作的管理员ID
        public static final String LAST_UPDATE_TIME = "lastUpdateTime"; // datetime 更新时间
        public static final String STATUS = "status"; // int datetime 商品状态
        public static final String UP_SALE_TIME = "upSaleTime"; // datetime 上架时间
        public static final String FLAG = "rlFlag"; // int
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间

        public static final String INFO_CHECK = "infoCheck"; // boolean 是否要校验info中的数据，中台内部使用。业务方接入可能需要添加一些空数据
        public static final String BIND_LIST = "bindList"; // FaiList 要绑定的商品关系表数据集合，给悦客接入进销存临时使用
    }
}
