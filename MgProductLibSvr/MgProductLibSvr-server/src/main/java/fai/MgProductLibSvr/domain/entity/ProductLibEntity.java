package fai.MgProductLibSvr.domain.entity;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:19
 */
public class ProductLibEntity {

    /**
     * 对应库表
     * AID : int 企业id
     * LIB_ID: int 库id
     * SOURCE_TID：int 创建库的项目id
     * SOURCE_UNIONPRIID：int 创建库的联合主键 id
     * LIB_NAME：varchar(100) 库名称
     * LIB_TYPE：tinyint(3) 库类型
     * FLAG：int
     * CREATE_TIME：datetime 创建时间
     * UPDATE_TIME：datetime 更新时间
     */
    public static final class Info {
        public static final String AID = "aid";
        public static final String LIB_ID = "libId";
        public static final String SOURCE_TID = "sourceTid";
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId";
        public static final String LIB_NAME = "libName";
        public static final String LIB_TYPE = "libType";
        public static final String FLAG = "flag";
        public static final String CREATE_TIME = "sysCreateTime";
        public static final String UPDATE_TIME = "sysUpdateTime";
    }

}
