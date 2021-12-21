package fai.MgProductBasicSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductBindTagEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductBindTagDto {

    private static ParamDef g_infoDtoDef = new ParamDef();

    static {
        g_infoDtoDef.add(ProductBindTagEntity.Info.AID, 0, Var.Type.INT);
        g_infoDtoDef.add(ProductBindTagEntity.Info.RL_TAG_ID, 1, Var.Type.INT);
        g_infoDtoDef.add(ProductBindTagEntity.Info.RL_PD_ID, 2, Var.Type.INT);
        g_infoDtoDef.add(ProductBindTagEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_infoDtoDef.add(ProductBindTagEntity.Info.PD_ID, 4, Var.Type.INT);
        g_infoDtoDef.add(ProductBindTagEntity.Info.CREATE_TIME, 5, Var.Type.CALENDAR);
        g_infoDtoDef.add(ProductBindTagEntity.Info.SYS_TYPE, 6, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_infoDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int UNION_PRI_ID = 3;
        public static final int RL_PD_ID = 4;
        public static final int RL_PD_IDS = 5;
        public static final int RL_TAG_IDS = 6;
        public static final int DEL_RL_TAG_IDS = 7;
        public static final int SEARCH_ARG = 8;
        public static final int TOTAL_SIZE = 9;
        public static final int DATA_STATUS = 10;
        public static final int SYS_TYPE = 11;
    }
}
