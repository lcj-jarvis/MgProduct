package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.SpecTempEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpecTempDto {
    private static ParamDef g_dtoDef = new ParamDef();

    static {
        g_dtoDef.add(SpecTempEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.RL_TP_SC_ID, 1, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.RL_LIB_ID, 2, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.NAME, 3, Var.Type.STRING);
        g_dtoDef.add(SpecTempEntity.Info.SOURCE_TID, 4, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.FLAG, 5, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.SORT, 6, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.SYS_CREATE_TIME, 7, Var.Type.CALENDAR);
        g_dtoDef.add(SpecTempEntity.Info.SYS_UPDATE_TIME, 8, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int INFO_LIST = 5;
        public static final int UPDATER_LIST = 6;
        public static final int ID_LIST = 7;
    }
}
