package fai.MgProductBasicSvr.domain.entity;

import fai.comm.util.FaiList;

public class ProductEntity {
    public static final class Info {
        public static final String AID = "aid"; // int 企业id
        public static final String PD_ID = "pdId"; // int 商品id
        public static final String SOURCE_TID = "sourceTid"; // int 创建商品的项目 id
        public static final String SOURCE_UNIONPRIID = "sourceUnionPriId"; // int 创建商品的联合主键 id
        public static final String NAME = "name"; // varchar(255) 商品名称
        public static final String PD_TYPE = "productType"; // int 商品类型
        public static final String IMG_LIST = "imgList"; // varchar(255) 商品图片
        public static final String VIDEO_LIST = "videoList"; // varchar(255) 商品视频
        public static final String UNIT = "unit"; // int 单位
        public static final String FLAG = "flag"; // int
        public static final String FLAG1 = "flag1"; // int
        public static final String KEEP_PROP1 = "keepProp1"; // varchar(255) 保留字段1
        public static final String KEEP_PROP2 = "keepProp2"; // varchar(255) 保留字段2
        public static final String KEEP_PROP3 = "keepProp3"; // varchar(255) 保留字段3
        public static final String KEEP_INT_PROP1 = "keepIntProp1"; // int 整型 保留字段1
        public static final String KEEP_INT_PROP2 = "keepIntProp2"; // int 整型 保留字段2
        public static final String CREATE_TIME = "sysCreateTime"; // datetime 创建时间
        public static final String UPDATE_TIME = "sysUpdateTime"; // datetime 更新时间
        public static final String STATUS = "status"; // int 数据状态
    }

    public static final class Business {
        public static final String ADD_COUNT = "addCount"; // int 记录条数变化，用于事务补偿
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    public static final FaiList<String> UPDATE_FIELDS; // 可修改字段

    static {
        MANAGE_FIELDS = new FaiList<>();
        MANAGE_FIELDS.add(Info.PD_ID);
        MANAGE_FIELDS.add(Info.NAME);
        MANAGE_FIELDS.add(Info.PD_TYPE);
        MANAGE_FIELDS.add(Info.FLAG);
        MANAGE_FIELDS.add(Info.FLAG1);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<>();
        VISITOR_FIELDS.setReadOnly(true);

        UPDATE_FIELDS = new FaiList<>();
        UPDATE_FIELDS.add(Info.NAME);
        UPDATE_FIELDS.add(Info.PD_TYPE);
        UPDATE_FIELDS.add(Info.IMG_LIST);
        UPDATE_FIELDS.add(Info.VIDEO_LIST);
        UPDATE_FIELDS.add(Info.UNIT);
        UPDATE_FIELDS.add(Info.FLAG);
        UPDATE_FIELDS.add(Info.FLAG1);
        UPDATE_FIELDS.add(Info.KEEP_PROP1);
        UPDATE_FIELDS.add(Info.KEEP_PROP2);
        UPDATE_FIELDS.add(Info.KEEP_PROP3);
        UPDATE_FIELDS.add(Info.KEEP_INT_PROP1);
        UPDATE_FIELDS.add(Info.KEEP_INT_PROP2);
        UPDATE_FIELDS.add(Info.UPDATE_TIME);
        // status这个字段因为软删除统一字段，所以名称和商品业务表一致，目前就是软删除的时候可以改。如果之后其他场景要修改这个字段的话，为避免和业务表修改弄混，需要另外提供接口
        //UPDATE_FIELDS.add(Info.STATUS);
        UPDATE_FIELDS.setReadOnly(true);
    }
}
