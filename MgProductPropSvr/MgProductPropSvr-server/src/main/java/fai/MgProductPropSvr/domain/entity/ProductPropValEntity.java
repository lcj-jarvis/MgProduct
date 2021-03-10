package fai.MgProductPropSvr.domain.entity;

import fai.comm.util.FaiList;

/**
 * 服务内使用的实体定义
 */
public class ProductPropValEntity {

	public static final class Info {
		public static final String AID = "aid"; // int 企业id
		public static final String PROP_VAL_ID = "propValId"; // int 参数值id
		public static final String PROP_ID = "propId"; // int 参数id
		public static final String VAL = "val"; // varchar(100) 参数值
		public static final String SORT = "sort"; // int 排序
		public static final String DATA_TYPE = "dataType"; // tinyint(2) 参数值数据类型
		public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
		public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
		public static final String RL_PROP_ID = "rlPropId"; // int 参数业务id, 表中没有，最后整合的
	}

	public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
	public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
	static {
		MANAGE_FIELDS = new FaiList<String>();
		MANAGE_FIELDS.add(Info.AID);
		MANAGE_FIELDS.add(Info.PROP_VAL_ID);
		MANAGE_FIELDS.add(Info.PROP_ID);
		MANAGE_FIELDS.add(Info.VAL);
		MANAGE_FIELDS.add(Info.SORT);
		MANAGE_FIELDS.add(Info.DATA_TYPE);
		MANAGE_FIELDS.add(Info.UPDATE_TIME);
		MANAGE_FIELDS.setReadOnly(true);

		VISITOR_FIELDS = new FaiList<String>();
		VISITOR_FIELDS.setReadOnly(true);
	}
}
