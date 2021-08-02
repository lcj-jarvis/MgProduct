package fai.MgProductGroupSvr.domain.entity;

import fai.comm.util.FaiList;

public class ProductGroupRelEntity {

	public static final class Info {
		public static final String AID = "aid"; // int 企业id
		public static final String RL_GROUP_ID = "rlGroupId"; // int 分类业务id
		public static final String GROUP_ID = "groupId"; // int 分类id
		public static final String GROUP_TYPE = "groupType"; // int 分类类型
		public static final String UNION_PRI_ID = "unionPriId"; // int 联合主键id
		public static final String SORT = "sort"; // int 排序
		public static final String RL_FLAG = "rlFlag"; // int
		public static final String STATUS = "status"; // int
		public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
		public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
	}

	public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
	public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
	static {
		MANAGE_FIELDS = new FaiList<String>();
		MANAGE_FIELDS.add(Info.AID);
		MANAGE_FIELDS.add(Info.RL_GROUP_ID);
		MANAGE_FIELDS.add(Info.GROUP_ID);
		MANAGE_FIELDS.add(Info.GROUP_TYPE);
		MANAGE_FIELDS.add(Info.UNION_PRI_ID);
		MANAGE_FIELDS.add(Info.SORT);
		MANAGE_FIELDS.add(Info.RL_FLAG);
		MANAGE_FIELDS.add(Info.STATUS);
		MANAGE_FIELDS.add(Info.CREATE_TIME);
		MANAGE_FIELDS.add(Info.UPDATE_TIME);
		MANAGE_FIELDS.setReadOnly(true);

		VISITOR_FIELDS = new FaiList<String>();
		VISITOR_FIELDS.setReadOnly(true);
	}
}
