package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductSpecSkuDto {
    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(ProductSpecSkuEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.PD_ID, 1, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SKU_ID, 2, Var.Type.LONG);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SORT, 3, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SOURCE_TID, 4, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SKU_NUM, 5, Var.Type.STRING);
        g_dtoDef.add(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, 6, Var.Type.FAI_LIST);
        g_dtoDef.add(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST, 7, Var.Type.FAI_LIST);
        g_dtoDef.add(ProductSpecSkuEntity.Info.FLAG, 8, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, 9);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, 10);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, 11, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.STATUS, 12, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SKU_NUM_LIST, 13, Var.Type.FAI_LIST);
        g_dtoDef.add(ProductSpecSkuEntity.Info.SPU, 14, Var.Type.BOOLEAN);
    }
    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }



    public static class Key {
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int PD_ID = 3;
        public static final int INFO = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID = 8;
        public static final int ID_LIST = 9;
        public static final int PD_ID_LIST = 10;
        public static final int SKU_NUM_LIST = 11;
        public static final int SKU_NUM = 12;

    }

    /**
     * 缓存DTO
     */
    public static final class CacheDto{

        private static ParamDef g_cacheDtoDef = new ParamDef();
        static {
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.AID, 0, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.PD_ID, 1, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SKU_ID, 2, Var.Type.LONG);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SORT, 3, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SOURCE_TID, 4, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SKU_NUM, 5, Var.Type.STRING);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, 6, Var.Type.FAI_LIST);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.FLAG, 8, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, 9);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, 10);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, 11, Var.Type.INT);
            g_cacheDtoDef.add(ProductSpecSkuEntity.Info.STATUS, 12, Var.Type.INT);
        }

        public static ParamDef getCacheDto() {
            return g_cacheDtoDef;
        }
    }
}
