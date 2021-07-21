package fai.MgProductTagSvr.application.domain.entity;

import fai.comm.util.FaiList;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:53
 *
 */
public class ProductTagRelEntity {

    /**
     * 对应标签业务表
     * AID : int 企业id
     * RL_TAG_ID:int 标签业务id
     * TAG_ID: int 标签id
     * UNIONPRIID：int 联合主键id
     * SORT：int 排序字段
     * FLAG：int
     * CREATE_TIME：datetime 创建时间
     * UPDATE_TIME：datetime 更新时间
     */
    public static final class Info {
        public static final String AID = "aid";
        public static final String RL_TAG_ID = "rlTagId";
        public static final String TAG_ID = "tagId";
        public static final String UNION_PRI_ID = "unionPriId";
        public static final String SORT = "sort";
        public static final String RL_FLAG = "rlFlag";
        public static final String CREATE_TIME = "sysCreateTime";
        public static final String UPDATE_TIME = "sysUpdateTime";
    }

    /**
     * 管理态字段
     */
    public static final FaiList<String> MANAGE_FIELDS;

    /**
     * 访客态字段
     */
    public static final FaiList<String> VISITOR_FIELDS;

    static {
        MANAGE_FIELDS = new FaiList<String>();
        MANAGE_FIELDS.add(Info.AID);
        MANAGE_FIELDS.add(Info.RL_TAG_ID);
        MANAGE_FIELDS.add(Info.TAG_ID);
        MANAGE_FIELDS.add(Info.UNION_PRI_ID);
        MANAGE_FIELDS.add(Info.SORT);
        MANAGE_FIELDS.add(Info.RL_FLAG);
        MANAGE_FIELDS.add(Info.CREATE_TIME);
        MANAGE_FIELDS.add(Info.UPDATE_TIME);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<String>();
        VISITOR_FIELDS.setReadOnly(true);
    }
}
