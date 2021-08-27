package fai.MgProductLibSvr.interfaces.entity;

import fai.comm.util.FaiList;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:17
 */
public class ProductLibRelEntity {

    /**
     * 对应库的表
     * AID : int 企业id
     * RL_LIB_ID:int 库业务id
     * LIB_ID: int 库id
     * UNIONPRIID：int 联合主键id
     * LIB_TYPE：tinyint(3) 库类型
     * SORT：int 排序字段
     * FLAG：int
     * CREATE_TIME：datetime 创建时间
     * UPDATE_TIME：datetime 更新时间
     */
    public static final class Info {
        public static final String AID = "aid";
        public static final String RL_LIB_ID = "rlLibId";
        public static final String LIB_ID = "libId";
        public static final String UNION_PRI_ID = "unionPriId";
        public static final String LIB_TYPE = "libType";
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
     *  访客态字段
     */
    public static final FaiList<String> VISITOR_FIELDS;
    static {
        MANAGE_FIELDS = new FaiList<String>();
        MANAGE_FIELDS.add(Info.AID);
        MANAGE_FIELDS.add(Info.RL_LIB_ID);
        MANAGE_FIELDS.add(Info.LIB_ID);
        MANAGE_FIELDS.add(Info.UNION_PRI_ID);
        MANAGE_FIELDS.add(Info.LIB_TYPE);
        MANAGE_FIELDS.add(Info.SORT);
        MANAGE_FIELDS.add(Info.RL_FLAG);
        MANAGE_FIELDS.add(Info.CREATE_TIME);
        MANAGE_FIELDS.add(Info.UPDATE_TIME);
        MANAGE_FIELDS.setReadOnly(true);

        VISITOR_FIELDS = new FaiList<String>();
        VISITOR_FIELDS.setReadOnly(true);
    }
}
