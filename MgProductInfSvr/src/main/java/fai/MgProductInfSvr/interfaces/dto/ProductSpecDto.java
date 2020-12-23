package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductSpecDto {
    public static ParamDef getInPdScValListDtoDef() {
        return g_inPdScValItemDtoDef;
    }
    private static ParamDef g_inPdScValItemDtoDef = new ParamDef();
    static {
        g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.SC_STR_ID, 0, Var.Type.INT);
        g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.NAME, 1, Var.Type.STRING);
        g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.CHECK, 2, Var.Type.BOOLEAN);
        g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.FILE_ID, 3, Var.Type.STRING);
    }

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(ProductSpecEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.RL_PD_ID, 1, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SC_STR_ID, 2, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.NAME, 3, Var.Type.STRING);
        g_dtoDef.add(ProductSpecEntity.Info.PD_SC_ID, 4, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SOURCE_TID, 5, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SORT, 6, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.FLAG, 7, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, 8, getInPdScValListDtoDef(), Var.Type.FAI_LIST);
        g_dtoDef.add(ProductSpecEntity.Info.SYS_CREATE_TIME, 9, Var.Type.CALENDAR);
        g_dtoDef.add(ProductSpecEntity.Info.SYS_UPDATE_TIME, 10, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int RL_PD_ID = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID_LIST = 8;
        public static final int RL_TP_SC_ID = 9;
        public static final int ONLY_GET_CHECKED = 10;
    }

}
