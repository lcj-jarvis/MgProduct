package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductGroupEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductGroupDto {
    private static ParamDef g_pdGroupDtoDef = new ParamDef();

    static {
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.AID, 0, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.RL_GROUP_ID, 1, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.GROUP_ID, 2, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.UNION_PRI_ID, 3, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.SORT, 4, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.RL_FLAG, 5, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.CREATE_TIME, 6, Var.Type.CALENDAR);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.UPDATE_TIME, 7, Var.Type.CALENDAR);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.SOURCE_TID, 8, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.SOURCE_UNIONPRIID, 9, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.PARENT_ID, 10, Var.Type.INT);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.GROUP_NAME, 11, Var.Type.STRING);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.ICON_LIST, 12, Var.Type.STRING);
        g_pdGroupDtoDef.add(ProductGroupEntity.PropInfo.FLAG, 13, Var.Type.INT);
    }

    public static ParamDef getPdGroupDto() {
        return g_pdGroupDtoDef;
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int RL_GROUP_ID = 5;
        public static final int INFO = 6;
        public static final int INFO_LIST = 7;
        public static final int TOTAL_SIZE = 8;
        public static final int UNION_PRI_ID = 9;
        public static final int RL_GROUP_IDS = 10;
        public static final int SEARCH_ARG = 11;
        public static final int UPDATERLIST = 12;
    }
}
