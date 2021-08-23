package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品基础信息对外Entity
 */
public class ProductBasicEntity {
    /**
     * 商品与参数值关联Entity
     */
    public static class BindPropInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String SYS_TYPE = "sysType"; // int 系统分类(商品/服务)
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务id
        public static final String RL_PROP_IDS = "rlPropIds"; // int 参数业务id集合

        public static final String RL_PROP_ID = "rlPropId"; // int 参数业务id
        public static final String PROP_VAL_ID = "propValId"; // int 参数值id
        public static final String VAL = "val"; // varchar 参数值
    }

    /**
     * 商品与分类关联Entity
     */
    public static class BindGroupInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String SYS_TYPE = "sysType"; // int 系统分类(商品/服务)
        public static final String RL_GROUP_ID = "rlGroupId"; // int 分类业务 id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
    }

    /**
     * 商品与标签关联Entity
     */
    public static class BindTagInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String SYS_TYPE = "sysType"; // int 系统分类(商品/服务)
        public static final String RL_TAG_ID = "rlTagId"; // int 标签业务 id
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
    }

    /**
     * 商品Entity
     */
    public static class ProductInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String SOURCE_TID = "sourceTid"; // int 创建商品的项目 id
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建商品的联合主键 id
        public static final String NAME = "name"; // varchar(255) 商品名称
        public static final String PD_TYPE = "productType"; // int 商品类型
        public static final String IMG_LIST = "imgList"; // varchar(255) 商品图片
        public static final String VIDEO_LIST = "videoList"; // varchar(255) 商品视频
        public static final String UNIT = "unit"; // tinyInt(4) 单位
        public static final String FLAG = "flag"; // int
        public static final String FLAG1 = "flag1"; // int
        public static final String KEEP_PROP1 = "keepProp1"; // varchar(255) 保留字段1
        public static final String KEEP_PROP2 = "keepProp2"; // varchar(255) 保留字段2
        public static final String KEEP_PROP3 = "keepProp3"; // varchar(255) 保留字段3
        public static final String KEEP_INT_PROP1 = "keepIntProp1"; // int 整型 保留字段1
        public static final String KEEP_INT_PROP2 = "keepIntProp2"; // int 整型 保留字段2
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间

        public static final String SYS_TYPE = "sysType"; // int 系统分类(商品/服务)
        public static final String RL_PD_ID = "rlPdId"; // int 商品业务 id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String RL_LIB_ID = "rlLibId"; // int 库业务id
        public static final String ADD_TIME = "addedTime"; // datetime 录入时间
        public static final String ADD_SID = "addedSid"; // int 录入商品的员工ID
        public static final String LAST_SID = "lastOptManagerSid"; // int 最后操作的管理员ID
        public static final String LAST_UPDATE_TIME = "lastUpdateTime"; // datetime 更新时间
        public static final String STATUS = "status"; // int 商品状态
        public static final String UP_SALE_TIME = "upSaleTime"; // datetime 上架时间
        public static final String RL_FLAG = "rlFlag"; // int

        public static final String TID = "tid";  // int 项目id
        public static final String SITE_ID = "siteId"; // int 站点id
        public static final String LGID = "lgId"; // int 多语言id
        public static final String KEEP_PRI_ID1 = "keepPriId1"; //int 保留主键1

        public static final String RL_GROUP_IDS = "rlGroupIds"; // 商品分类业务ids
        public static final String RL_TAG_IDS = "rlTagIds"; // 商品标签业务ids
        public static final String RL_PROPS = "rlProps"; // 商品参数绑定关系

        public static final String REMARK = "remark"; // 富文本(商品详情)
        public static final String REMARK1 = "remark1"; // 富文本1
        public static final String REMARK2 = "remark2"; // 富文本2
        public static final String REMARK3 = "remark3"; // 富文本3
        public static final String REMARK4 = "remark4"; // 富文本4
        public static final String REMARK5 = "remark5"; // 富文本5
        public static final String REMARK6 = "remark6"; // 富文本6
        public static final String REMARK7 = "remark7"; // 富文本7
        public static final String REMARK8 = "remark8"; // 富文本8
        public static final String REMARK9 = "remark9"; // 富文本9
        public static final String REMARK10 = "remark10"; // 富文本10
        public static final String REMARK11 = "remark11"; // 富文本11
    }
}
