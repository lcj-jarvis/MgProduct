package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.SpecTempEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpecTempDto {
    private static ParamDef g_dtoDef = new ParamDef();

    static {
        g_dtoDef.add(SpecTempEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.RL_TP_SC_ID, 1, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.RL_LIB_ID, 2, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.NAME, 3, Var.Type.STRING);
        g_dtoDef.add(SpecTempEntity.Info.FLAG, 4, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.SORT, 5, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.SYS_CREATE_TIME, 6);
        g_dtoDef.add(SpecTempEntity.Info.SYS_UPDATE_TIME, 7);
        g_dtoDef.add(SpecTempEntity.Info.SOURCE_TID, 8, Var.Type.INT);
        g_dtoDef.add(SpecTempEntity.Info.SOURCE_UNION_PRI_ID, 9, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key {
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int INFO = 3;
        public static final int INFO_LIST = 4;
        public static final int UPDATER_LIST = 5;
        public static final int ID = 6;
        public static final int ID_LIST = 7;
    }
}
