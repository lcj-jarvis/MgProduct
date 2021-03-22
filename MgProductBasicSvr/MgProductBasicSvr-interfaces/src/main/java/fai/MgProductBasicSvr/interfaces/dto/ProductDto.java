package fai.MgProductBasicSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.comm.util.ParamDef;

public class ProductDto {
    private static ParamDef g_infoDtoDef = new ParamDef();

    static {
        g_infoDtoDef.add(ProductEntity.Info.AID, 0);
        g_infoDtoDef.add(ProductEntity.Info.PD_ID, 1);
        g_infoDtoDef.add(ProductEntity.Info.SOURCE_TID, 2);
        g_infoDtoDef.add(ProductEntity.Info.NAME, 3);
        g_infoDtoDef.add(ProductEntity.Info.PD_TYPE, 4);
        g_infoDtoDef.add(ProductEntity.Info.IMG_LIST, 5);
        g_infoDtoDef.add(ProductEntity.Info.VIDEO_LIST, 6);
        g_infoDtoDef.add(ProductEntity.Info.UNIT, 7);
        g_infoDtoDef.add(ProductEntity.Info.FLAG, 8);
        g_infoDtoDef.add(ProductEntity.Info.FLAG1, 9);
        g_infoDtoDef.add(ProductEntity.Info.KEEP_PROP1, 10);
        g_infoDtoDef.add(ProductEntity.Info.KEEP_PROP2, 11);
        g_infoDtoDef.add(ProductEntity.Info.KEEP_PROP3, 12);
        g_infoDtoDef.add(ProductEntity.Info.KEEP_INT_PROP1, 13);
        g_infoDtoDef.add(ProductEntity.Info.KEEP_INT_PROP2, 14);
        g_infoDtoDef.add(ProductEntity.Info.CREATE_TIME, 15);
        g_infoDtoDef.add(ProductEntity.Info.UPDATE_TIME, 16);
        g_infoDtoDef.add(ProductEntity.Info.SOURCE_UNIONPRIID, 17);
        g_infoDtoDef.add(ProductEntity.Info.STATUS, 18);
    }

    public static ParamDef getInfoDto() {
        return g_infoDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int UNION_PRI_ID = 3;
        public static final int TID = 4;
        public static final int RL_PD_ID = 5;
        public static final int DATA_STATUS = 6;
        public static final int TOTAL_SIZE = 7;
        public static final int SEARCH_ARG = 8;
    }
}
