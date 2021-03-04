package fai.MgProductBasicSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductGroupAssocEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductGroupAssocDto {

    private static ParamDef g_infoDtoDef = new ParamDef();

    static {
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.AID, 0, Var.Type.INT);
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.RL_GROUP_ID, 1, Var.Type.INT);
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.RL_PD_ID, 2, Var.Type.INT);
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.PD_ID, 4, Var.Type.INT);
        g_infoDtoDef.add(ProductGroupAssocEntity.Info.CREATE_TIME, 5, Var.Type.CALENDAR);
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
        public static final int RL_GROUP_IDS = 6;
        public static final int DEL_RL_GROUP_IDS = 7;
    }
}
