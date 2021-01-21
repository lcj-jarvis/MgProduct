package fai.MgProductPropSvr.interfaces.entity;

public class ProductPropEntity {

	public static final class Info {
		public static final String AID = "aid"; // int 企业id
		public static final String PROP_ID = "propId"; // int 参数id
		public static final String SOURCE_TID = "sourceTid"; // int 创建该参数的项目id
		public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建商品参数的联合主键 id
		public static final String NAME = "name"; // varchar(100) 参数名称
		public static final String TYPE = "type"; // tinyInt(4) 参数类型
		public static final String FLAG = "flag"; // int
		public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
		public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
	}
}
