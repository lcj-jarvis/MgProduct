package fai.MgProductBasicSvr.domain.entity;

import fai.comm.util.FaiList;

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
        public static final String STATUS = "status"; // int 商品状态
        public static final String UP_SALE_TIME = "upSaleTime"; // datetime 上架时间
        public static final String FLAG = "rlFlag"; // int
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
        public static final String PD_TYPE = "productType"; // int 商品类型 (为了方便搜索，冗余商品表的该字段)
        public static final String SYS_TYPE = "sysType"; // int 商品系统类型 (服务/商品)
        public static final String SORT = "sort"; // int 排序

        /*** 未入库属性 ***/
        public static final String RL_GROUP_IDS = "rlGroupIds"; // FaiList<Integer> 商品分类业务id集合
        public static final String RL_TAG_IDS = "rlTagIds"; // FaiList<Integer> 商品标签业务id集合
        public static final String RL_PROPS = "rlProps"; // FaiList<Param> 商品参数业务绑定关系集合

        public static final String INFO_CHECK = "infoCheck"; // boolean 是否要校验info中的数据，中台内部使用。业务方接入可能需要添加一些空数据
        public static final String BIND_LIST = "bindList"; // FaiList 要绑定的商品关系表数据集合
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    public static final FaiList<String> UPDATE_FIELDS; // 可修改字段
    static {
        MANAGE_FIELDS = new FaiList<>();
        MANAGE_FIELDS.add(Info.RL_PD_ID);
        MANAGE_FIELDS.add(Info.PD_ID);
        MANAGE_FIELDS.add(Info.RL_LIB_ID);
        MANAGE_FIELDS.add(Info.PD_TYPE);
        MANAGE_FIELDS.add(Info.ADD_TIME);
        MANAGE_FIELDS.add(Info.LAST_UPDATE_TIME);
        MANAGE_FIELDS.add(Info.STATUS);
        MANAGE_FIELDS.add(Info.UP_SALE_TIME);
        MANAGE_FIELDS.add(Info.FLAG);
        MANAGE_FIELDS.add(Info.SORT);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<>();
        VISITOR_FIELDS.setReadOnly(true);

        UPDATE_FIELDS = new FaiList<>();
        UPDATE_FIELDS.add(Info.RL_LIB_ID);
        UPDATE_FIELDS.add(Info.ADD_TIME);
        UPDATE_FIELDS.add(Info.PD_TYPE);
        UPDATE_FIELDS.add(Info.LAST_SID);
        UPDATE_FIELDS.add(Info.LAST_UPDATE_TIME);
        UPDATE_FIELDS.add(Info.STATUS);
        UPDATE_FIELDS.add(Info.UP_SALE_TIME);
        UPDATE_FIELDS.add(Info.UPDATE_TIME);
        UPDATE_FIELDS.add(Info.FLAG);
        UPDATE_FIELDS.add(Info.SORT);
        UPDATE_FIELDS.setReadOnly(true);
    }
}
