package fai.MgProductBasicSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductBindPropEntity;
import fai.comm.util.ParamDef;

public class ProductBindPropDto {
    private static ParamDef g_infoDtoDef = new ParamDef();

    static {
        g_infoDtoDef.add(ProductBindPropEntity.Info.AID, 0);
        g_infoDtoDef.add(ProductBindPropEntity.Info.RL_PD_ID, 1);
        g_infoDtoDef.add(ProductBindPropEntity.Info.RL_PROP_ID, 2);
        g_infoDtoDef.add(ProductBindPropEntity.Info.PROP_VAL_ID, 3);
        g_infoDtoDef.add(ProductBindPropEntity.Info.UNION_PRI_ID, 4);
        g_infoDtoDef.add(ProductBindPropEntity.Info.PD_ID, 5);
        g_infoDtoDef.add(ProductBindPropEntity.Info.CREATE_TIME, 6);
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
        public static final int PROP_BIND = 6;
        public static final int DEL_PROP_BIND = 7;
        public static final int RL_PD_IDS = 8;
        public static final int DATA_STATUS = 9;
        public static final int TOTAL_SIZE = 10;
        public static final int SEARCH_ARG = 11;
        public static final int RL_PROP_ID = 12;
        public static final int RL_PROP_IDS = 13;
        public static final int PROP_VAL_IDS = 14;
    }
}
