package fai.MgProductGroupSvr.interfaces.dto;

import fai.MgProductGroupSvr.interfaces.entity.ProductGroupEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductGroupDto {
    private static ParamDef g_groupDtoDef = new ParamDef();

    static {
        g_groupDtoDef.add(ProductGroupEntity.Info.AID, 0, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.GROUP_ID, 1, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.PARENT_ID, 4, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.GROUP_NAME, 5, Var.Type.STRING);
        g_groupDtoDef.add(ProductGroupEntity.Info.ICON_LIST, 6, Var.Type.STRING);
        g_groupDtoDef.add(ProductGroupEntity.Info.FLAG, 7, Var.Type.INT);
        g_groupDtoDef.add(ProductGroupEntity.Info.CREATE_TIME, 8, Var.Type.CALENDAR);
        g_groupDtoDef.add(ProductGroupEntity.Info.UPDATE_TIME, 9, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_groupDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int RL_GROUP_IDS = 3;
    }
}
