package fai.MgProductBasicSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.comm.util.ParamDef;

public class ProductRelDto {
    private static ParamDef g_infoDtoDef = new ParamDef();
    private static ParamDef g_reducedInfoDtoDef = new ParamDef();
    private static ParamDef g_relAndPdDtoDef = new ParamDef();

    static {
        g_infoDtoDef.add(ProductRelEntity.Info.AID, 0);
        g_infoDtoDef.add(ProductRelEntity.Info.RL_PD_ID, 1);
        g_infoDtoDef.add(ProductRelEntity.Info.PD_ID, 2);
        g_infoDtoDef.add(ProductRelEntity.Info.UNION_PRI_ID, 3);
        g_infoDtoDef.add(ProductRelEntity.Info.RL_LIB_ID, 4);
        g_infoDtoDef.add(ProductRelEntity.Info.SOURCE_TID, 5);
        g_infoDtoDef.add(ProductRelEntity.Info.ADD_TIME, 6);
        g_infoDtoDef.add(ProductRelEntity.Info.ADD_SID, 7);
        g_infoDtoDef.add(ProductRelEntity.Info.LAST_SID, 8);
        g_infoDtoDef.add(ProductRelEntity.Info.LAST_UPDATE_TIME, 9);
        g_infoDtoDef.add(ProductRelEntity.Info.STATUS, 10);
        g_infoDtoDef.add(ProductRelEntity.Info.UP_SALE_TIME, 11);
        g_infoDtoDef.add(ProductRelEntity.Info.FLAG, 12);
        g_infoDtoDef.add(ProductRelEntity.Info.CREATE_TIME, 13);
        g_infoDtoDef.add(ProductRelEntity.Info.UPDATE_TIME, 14);
    }

    static {
        g_reducedInfoDtoDef.add(ProductRelEntity.Info.AID, 0);
        g_reducedInfoDtoDef.add(ProductRelEntity.Info.RL_PD_ID, 1);
        g_reducedInfoDtoDef.add(ProductRelEntity.Info.PD_ID, 2);
        g_reducedInfoDtoDef.add(ProductRelEntity.Info.UNION_PRI_ID, 3);
    }

    static {
        g_relAndPdDtoDef.add(ProductRelEntity.Info.AID, 0);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.RL_PD_ID, 1);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.PD_ID, 2);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.UNION_PRI_ID, 3);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.RL_LIB_ID, 4);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.SOURCE_TID, 5);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.ADD_TIME, 6);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.ADD_SID, 7);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.LAST_SID, 8);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.LAST_UPDATE_TIME, 9);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.STATUS, 10);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.UP_SALE_TIME, 11);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.FLAG, 12);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.CREATE_TIME, 13);
        g_relAndPdDtoDef.add(ProductRelEntity.Info.UPDATE_TIME, 14);

        g_relAndPdDtoDef.add(ProductEntity.Info.NAME, 15);
        g_relAndPdDtoDef.add(ProductEntity.Info.PD_TYPE, 16);
        g_relAndPdDtoDef.add(ProductEntity.Info.IMG_LIST, 17);
        g_relAndPdDtoDef.add(ProductEntity.Info.VIDEO_LIST, 18);
        g_relAndPdDtoDef.add(ProductEntity.Info.UNIT, 19);
        g_relAndPdDtoDef.add(ProductEntity.Info.FLAG, 20);
        g_relAndPdDtoDef.add(ProductEntity.Info.FLAG1, 21);
        g_relAndPdDtoDef.add(ProductEntity.Info.KEEP_PROP1, 22);
        g_relAndPdDtoDef.add(ProductEntity.Info.KEEP_PROP2, 23);
        g_relAndPdDtoDef.add(ProductEntity.Info.KEEP_PROP3, 24);
        g_relAndPdDtoDef.add(ProductEntity.Info.KEEP_INT_PROP1, 25);
        g_relAndPdDtoDef.add(ProductEntity.Info.KEEP_INT_PROP2, 26);
    }

    public static ParamDef getInfoDto() {
        return g_infoDtoDef;
    }

    public static ParamDef getReducedInfoDto() {
        return g_reducedInfoDtoDef;
    }

    public static ParamDef getRelAndPdDto() {
        return g_relAndPdDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int UNION_PRI_ID = 3;
        public static final int TID = 4;
        public static final int RL_PD_ID = 5;
        public static final int PD_ID = 6;
        public static final int RL_PD_IDS = 7;
        public static final int PD_IDS = 8;
        public static final int REDUCED_INFO = 9;
    }
}
