package fai.MgProductInfSvr.interfaces.entity;

/**
 * 商品参数服务对外Entity
 */
public class ProductPropEntity {
    public static class PropInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_PROP_ID = "rlPropId"; // int 参数业务id
        public static final String RL_LIB_ID = "rlLibId"; // int 库业务id
        public static final String NAME = "name"; // varchar(100) 参数名称
        public static final String SORT = "sort"; // int 排序
        public static final String TYPE = "type"; // tinyInt(4) 参数类型
        public static final String FLAG = "flag"; // int
        public static final String RL_FLAG = "rlFlag"; // int
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
        public static final String SOURCE_TID = "sourceTid"; // int 创建商品参数的项目id
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建商品参数的联合主键 id
        public static final String PROP_ID = "propId"; // int 参数id
    }

    public static class PropValInfo {
        public static final String AID = "aid"; // int 企业id
        public static final String RL_PROP_ID = "rlPropId"; // int 参数业务id
        public static final String PROP_VAL_ID = "propValId"; // int 参数值id
        public static final String VAL = "val"; // varchar(100) 参数值
        public static final String SORT = "sort"; // int 排序
        public static final String DATA_TYPE = "dataType"; // tinyint(2) 参数值数据类型
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
    }
}
