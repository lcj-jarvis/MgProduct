package fai.MgProductBasicSvr.domain.entity;

public class ProductEntity {
    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String SOURCE_TID = "sourceTid"; // int 创建商品的项目 id
        public static final String NAME = "name"; // int 创建商品的项目 id
        public static final String PD_TYPE = "productType"; // int 商品类型
        public static final String IMG_LIST = "imgList"; // varchar(255) 商品图片
        public static final String VIDEO_LIST = "videoList"; // varchar(255) 商品视频
        public static final String UNIT = "unit"; // varchar 商品视频
        public static final String FLAG = "flag"; // int
        public static final String FLAG1 = "flag1"; // int
        public static final String KEEP_PROP1 = "keepProp1"; // varchar(255) 保留字段1
        public static final String KEEP_PROP2 = "keepProp2"; // varchar(255) 保留字段2
        public static final String KEEP_PROP3 = "keepProp3"; // varchar(255) 保留字段3
        public static final String KEEP_INT_PROP1 = "keepIntProp1"; // int 整型 保留字段1
        public static final String KEEP_INT_PROP2 = "keepIntProp2"; // int 整型 保留字段2
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
    }
}
