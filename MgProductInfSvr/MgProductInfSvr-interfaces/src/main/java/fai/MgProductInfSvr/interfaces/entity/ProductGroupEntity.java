package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品分类服务对外Entity
 */
public class ProductGroupEntity {
    public static class GroupInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_GROUP_ID = "rlGroupId"; // int 分类业务id
        public static final String GROUP_ID = "groupId"; // int 分类id
        public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
        public static final String SORT = "sort"; // int 排序
        public static final String RL_FLAG = "rlFlag"; // int 各业务独立的flag
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间

        public static final String SOURCE_TID = "sourceTid"; // int 创建分类的项目id
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建分类的联合主键 id
        public static final String PARENT_ID = "parentId"; // int 分类父id
        public static final String GROUP_NAME = "groupName"; // varchar(100) 分类名称
        public static final String ICON_LIST = "iconList"; // varchar(255) 分类ICON
        public static final String FLAG = "flag"; // int
        public static final String SYS_TYPE = "sysType"; // int 分类类型
        public static final String STATUS = "status"; // int 状态
    }
}
