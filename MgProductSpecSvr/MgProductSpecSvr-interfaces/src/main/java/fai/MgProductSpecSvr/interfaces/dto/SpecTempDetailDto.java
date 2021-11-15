package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailEntity;
import fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj;
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
        g_dtoDef.add(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, 6, getInScValListDtoDef(), Var.Type.FAI_LIST);
        g_dtoDef.add(SpecTempDetailEntity.Info.SYS_CREATE_TIME, 7);
        g_dtoDef.add(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, 8);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }



    public static final class CacheDto{
        public static ParamDef getInScValListDtoDef() {
            return g_inScValItemDtoDef;
        }
        private static ParamDef g_inScValItemDtoDef = new ParamDef();
        static {
            g_inScValItemDtoDef.add(SpecTempDetailValObj.InScValList.Item.SC_STR_ID, 0, Var.Type.INT);
            g_inScValItemDtoDef.add(SpecTempDetailValObj.InScValList.Item.FILE_ID, 2, Var.Type.STRING);
        }
        private static ParamDef g_dtoDef = new ParamDef();
        static {
            g_dtoDef.add(SpecTempDetailEntity.Info.AID, 0, Var.Type.INT);
            g_dtoDef.add(SpecTempDetailEntity.Info.TP_SC_DT_ID, 4, Var.Type.INT);
            g_dtoDef.add(SpecTempDetailEntity.Info.SORT, 5, Var.Type.INT);
            g_dtoDef.add(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, 6, getInScValListDtoDef(), Var.Type.FAI_LIST);
            g_dtoDef.add(SpecTempDetailEntity.Info.SYS_CREATE_TIME, 7);
            g_dtoDef.add(SpecTempDetailEntity.Info.SYS_UPDATE_TIME, 8);
        }

        public static ParamDef getDtoDef() {
            return g_dtoDef;
        }
    }

    public static class Key {
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int RL_TP_SC_ID = 3;
        public static final int INFO = 4;
        public static final int INFO_LIST = 5;
        public static final int UPDATER_LIST = 6;
        public static final int ID = 7;
        public static final int ID_LIST = 8;
        public static final int SYS_TYPE = 9;
    }
}
