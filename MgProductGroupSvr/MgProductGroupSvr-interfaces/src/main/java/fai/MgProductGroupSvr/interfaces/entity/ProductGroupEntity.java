package fai.MgProductGroupSvr.interfaces.entity;

public class ProductGroupEntity {

	public static final class Info {
		public static final String AID = "aid"; // int 企业id
		public static final String GROUP_ID = "groupId"; // int 分类id
		public static final String SOURCE_TID = "sourceTid"; // int 创建分类的项目id
		public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建分类的联合主键 id
		public static final String GROUP_NAME = "groupName"; // varchar(100) 分类名称
		public static final String ICON_LIST = "iconList"; // varchar(255) 分类ICON
		public static final String FLAG = "flag"; // int
		public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
		public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
        public static final String SYS_TYPE = "groupType";      // int 分类类型
		public static final String STATUS = "status";           // int 状态

		public static final String CHILDREN = "children";       // 树结构 子节点 （非真实存在字段）

		public static final String IS_ADD = "isAdd";            // TODO 临时字段，判断是否为添加数据，双写后可废除
    }
}
