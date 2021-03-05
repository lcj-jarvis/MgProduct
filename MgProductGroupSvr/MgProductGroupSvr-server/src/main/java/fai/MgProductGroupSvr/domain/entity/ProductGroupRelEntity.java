package fai.MgProductGroupSvr.domain.entity;

public class ProductGroupRelEntity {

	public static final class Info {
		public static final String AID = "aid"; // int 企业id
		public static final String RL_GROUP_ID = "rlGroupId"; // int 分类业务id
		public static final String GROUP_ID = "groupId"; // int 分类id
		public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
		public static final String SORT = "sort"; // int 排序
		public static final String RL_FLAG = "rlFlag"; // int
		public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
		public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
	}

	public static final String[] MANAGE_FIELDS = new String[]{
			Info.AID, Info.RL_GROUP_ID, Info.GROUP_ID, Info.UNION_PRI_ID, Info.SORT, Info.RL_FLAG, Info.CREATE_TIME, Info.UPDATE_TIME
	};
}
