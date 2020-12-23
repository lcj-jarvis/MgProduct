package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.SpecTempDetailEntity;
import fai.MgProductInfSvr.interfaces.entity.SpecTempDetailValObj;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpecTempDetailDto {
    public static ParamDef getInScValListDtoDef() {
        return g_inScValItemDtoDef;
    }
    private static ParamDef g_inScValItemDtoDef = new ParamDef();
    static {
        g_inScValItemDtoDef.add(SpecTempDetailValObj.InScValList.Item.SC_STR_ID, 0, Var.Type.INT);
        g_inScValItemDtoDef.add(SpecTempDetailValObj.InScValList.Item.NAME, 1, Var.Type.STRING);
        g_inScValItemDtoDef.add(SpecTempDetailValObj.InScValList.Item.FILE_ID, 2, Var.Type.STRING);
    }

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SpecTempDetailEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.RL_TP_SC_ID, 1, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.SC_STR_ID, 2, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.NAME, 3, Var.Type.STRING);
        g_dtoDef.add(SpecTempDetailEntity.Info.TP_SC_DT_ID, 4, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.SORT, 5, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.FLAG, 6, Var.Type.INT);
        g_dtoDef.add(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, 7, getInScValListDtoDef(), Var.Type.FAI_LIST);
        g_dtoDef.add(SpecTempDetailEntity.Info.SYS_CREATE_TIME, 8, Var.Type.CALENDAR);
        g_dtoDef.add(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, 9, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int RL_TP_SC_ID = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID_LIST = 8;
    }
}
