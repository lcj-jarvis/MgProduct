package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj;
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
        g_dtoDef.add(ProductSpecEntity.Info.PD_ID, 1, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SC_STR_ID, 2, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.NAME, 3, Var.Type.STRING);
        g_dtoDef.add(ProductSpecEntity.Info.PD_SC_ID, 4, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SOURCE_TID, 5, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.SORT, 6, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.FLAG, 7, Var.Type.INT);
        g_dtoDef.add(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, 8, getInPdScValListDtoDef(), Var.Type.FAI_LIST);
        g_dtoDef.add(ProductSpecEntity.Info.SYS_CREATE_TIME, 9);
        g_dtoDef.add(ProductSpecEntity.Info.SYS_UPDATE_TIME, 10);
        g_dtoDef.add(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, 11, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key {
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int PD_ID = 3;
        public static final int RL_PD_ID = 4;
        public static final int INFO = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID = 8;
        public static final int ID_LIST = 9;
        public static final int RL_TP_SC_ID = 10;
        public static final int PD_ID_LIST = 11;
        public static final int SOFT_DEL = 12;
        public static final int SKU_INFO_LIST = 13;
        public static final int ONLY_CHECKED = 14;
    }

    /**
     * 缓存DTO
     */
    public static final class CacheDto{
        public static ParamDef getInPdScValListDtoDef() {
            return g_inPdScValItemDtoDef;
        }
        private static ParamDef g_inPdScValItemDtoDef = new ParamDef();
        static {
            g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.SC_STR_ID, 0, Var.Type.INT);
            g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.CHECK, 2, Var.Type.BOOLEAN);
            g_inPdScValItemDtoDef.add(ProductSpecValObj.InPdScValList.Item.FILE_ID, 3, Var.Type.STRING);
        }
        private static ParamDef g_cacheDtoDef = new ParamDef();
        static {
            g_cacheDtoDef.add(ProductSpecEntity.Info.AID, 0, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.PD_ID, 1, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SC_STR_ID, 2, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.PD_SC_ID, 4, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SOURCE_TID, 5, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SORT, 6, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.FLAG, 7, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, 8, getInPdScValListDtoDef(), Var.Type.FAI_LIST);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SYS_CREATE_TIME, 9);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SYS_UPDATE_TIME, 10);
            g_cacheDtoDef.add(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, 11, Var.Type.INT);
        }

        public static ParamDef getCacheDto() {
            return g_cacheDtoDef;
        }
    }
}
