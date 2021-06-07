package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class MgProductDto {
    private static ParamDef g_infoDtoDef = new ParamDef();
    static {
        g_infoDtoDef.add(MgProductEntity.Info.BASIC, 0, ProductBasicDto.getProductDto(), Var.Type.PARAM);
        g_infoDtoDef.add(MgProductEntity.Info.SPEC, 1, ProductSpecDto.Spec.getInfoDto(), Var.Type.FAI_LIST);
        g_infoDtoDef.add(MgProductEntity.Info.SPEC_SKU, 2, ProductSpecDto.SpecSku.getInfoDto(), Var.Type.FAI_LIST);
        g_infoDtoDef.add(MgProductEntity.Info.STORE_SALES, 3, ProductStoreDto.StoreSalesSku.getInfoDto(), Var.Type.FAI_LIST);
        g_infoDtoDef.add(MgProductEntity.Info.ERRNO, 4, Var.Type.INT);
    }
    public static ParamDef getInfoDto() {
        return g_infoDtoDef;
    }

    private static ParamDef g_summaryInfoDtoDef = new ParamDef();
    static {
        g_summaryInfoDtoDef.add(MgProductEntity.Info.BASIC, 0, ProductBasicDto.getProductDto(), Var.Type.PARAM);
        g_summaryInfoDtoDef.add(MgProductEntity.Info.SPEC, 1, ProductSpecDto.Spec.getInfoDto(), Var.Type.FAI_LIST);
        g_summaryInfoDtoDef.add(MgProductEntity.Info.SPEC_SKU, 2, ProductSpecDto.SpecSku.getInfoDto(), Var.Type.FAI_LIST);
        g_summaryInfoDtoDef.add(MgProductEntity.Info.STORE_SALES, 3, ProductStoreDto.SkuSummary.getInfoDto(), Var.Type.FAI_LIST);
        g_summaryInfoDtoDef.add(MgProductEntity.Info.SPU_SALES, 4, ProductStoreDto.SpuSummary.getInfoDto(), Var.Type.FAI_LIST);
    }
    public static ParamDef getSummaryInfoDto() {
        return g_summaryInfoDtoDef;
    }

    private static ParamDef g_combinedInfoDtoDef = new ParamDef();
    static {
        g_combinedInfoDtoDef.add(MgProductEntity.Info.BASIC, 0, Var.Type.BOOLEAN);
        g_combinedInfoDtoDef.add(MgProductEntity.Info.SPEC, 1, Var.Type.BOOLEAN);
        g_combinedInfoDtoDef.add(MgProductEntity.Info.SPEC_SKU, 2, Var.Type.BOOLEAN);
        g_combinedInfoDtoDef.add(MgProductEntity.Info.STORE_SALES, 3, Var.Type.BOOLEAN);
        g_combinedInfoDtoDef.add(MgProductEntity.Info.SPU_SALES, 4, Var.Type.BOOLEAN);
    }
    public static ParamDef getCombinedInfoDto() {
        return g_combinedInfoDtoDef;
    }

    public static ParamDef g_primaryKeyDef = new ParamDef();
    static {
        g_primaryKeyDef.add(MgProductEntity.Info.TID, 0, Var.Type.INT);
        g_primaryKeyDef.add(MgProductEntity.Info.SITE_ID, 1, Var.Type.INT);
        g_primaryKeyDef.add(MgProductEntity.Info.LGID, 2, Var.Type.INT);
        g_primaryKeyDef.add(MgProductEntity.Info.KEEP_PRI_ID1, 3, Var.Type.INT);
    }
    public static ParamDef getPrimaryKeyDto() {
        return g_primaryKeyDef;
    }

    public static class Key {
        public static final int TID  = 1;
        public static final int SITE_ID  = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int ID = 5;
        public static final int INFO = 6;
        public static final int INFO_LIST = 7;
        public static final int IN_OUT_STORE_RECORD_INFO = 8;
        public static final int USE_BASIC = 9;
        public static final int RL_PD_IDS = 10;
        public static final int COMBINED = 11;
    }
}
