package fai.MgProductTagSvr.interfaces.entity;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:48
 */
public class ProductTagEntity {

    /**
     * 对应标签表
     * AID : int 企业id
     * TAG_ID: int 标签id
     * SOURCE_TID：int 创建标签的项目id
     * SOURCE_UNIONPRIID：int 创建标签的联合主键 id
     * TAG_NAME：varchar(100) 标签名称
     * TAG_TYPE：tinyint(3) 标签类型
     * FLAG：int
     * CREATE_TIME：datetime 创建时间
     * UPDATE_TIME：datetime 更新时间
     */
    public static final class Info {
        public static final String AID = "aid";
        public static final String TAG_ID = "tagId";
        public static final String SOURCE_TID = "sourceTid";
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId";
        public static final String TAG_NAME = "tagName";
        public static final String TAG_TYPE = "tagType";
        public static final String FLAG = "flag";
        public static final String CREATE_TIME = "sysCreateTime";
        public static final String UPDATE_TIME = "sysUpdateTime";
    }
}
