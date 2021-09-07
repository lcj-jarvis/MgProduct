package fai.MgProductTagSvr.interfaces.dto;

import fai.MgProductTagSvr.interfaces.entity.ProductTagEntity;
import fai.MgProductTagSvr.interfaces.entity.ProductTagRelEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:48
 */
public class ProductTagRelDto {
    //标签业务表
    private static ParamDef g_tagRelDtoDef = new ParamDef();

    static {
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.AID, 0, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.RL_TAG_ID, 1, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.TAG_ID, 2, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.SORT, 4, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.RL_FLAG, 5, Var.Type.INT);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.CREATE_TIME, 6, Var.Type.CALENDAR);
        g_tagRelDtoDef.add(ProductTagRelEntity.Info.UPDATE_TIME, 7, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_tagRelDtoDef;
    }

    // 完整数据，包括标签表和标签业务表
    private static ParamDef g_tagAllDtoDef = new ParamDef();

    static {
        g_tagAllDtoDef.add(ProductTagEntity.Info.AID, 0, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.TAG_ID, 1, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.TAG_NAME, 4, Var.Type.STRING);
        g_tagAllDtoDef.add(ProductTagEntity.Info.TAG_TYPE, 5, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.FLAG, 6, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_tagAllDtoDef.add(ProductTagEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);

        g_tagAllDtoDef.add(ProductTagRelEntity.Info.RL_TAG_ID, 10, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagRelEntity.Info.UNION_PRI_ID, 11, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagRelEntity.Info.SORT, 12, Var.Type.INT);
        g_tagAllDtoDef.add(ProductTagRelEntity.Info.RL_FLAG, 13, Var.Type.INT);
    }

    public static ParamDef getAllInfoDto() {
        return g_tagAllDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int TAG_ID = 3;
        public static final int RL_TAG_ID = 4;
        public static final int TOTAL_SIZE = 5;
        public static final int UNION_PRI_ID = 6;
        public static final int TID = 7;
        public static final int SEARCH_ARG = 8;
        public static final int UPDATERLIST = 9;
        public static final int RL_TAG_IDS = 10;
        public static final int DATA_STATUS = 11;

        public static final int FROM_AID = 12;
        public static final int CLONE_UNION_PRI_IDS = 13;
        public static final int FROM_UNION_PRI_ID = 14;

        public static final int BACKUP_INFO = 15;
        public static final int RESTORE_ID = 16;
    }
}
