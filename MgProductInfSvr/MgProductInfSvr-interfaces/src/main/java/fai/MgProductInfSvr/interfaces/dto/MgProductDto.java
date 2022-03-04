package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class MgProductDto {
    private static ParamDef g_infoDtoDef = new ParamDef();
    static {
        g_infoDtoDef.add(MgProductEntity.Info.BASIC, 0, ProductBasicDto.getProductDto());
        g_infoDtoDef.add(MgProductEntity.Info.SPEC, 1, ProductSpecDto.Spec.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.SPEC_SKU, 2, ProductSpecDto.SpecSku.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.STORE_SALES, 3, ProductStoreDto.StoreSalesSku.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.ERRNO, 4, Var.Type.INT);
        g_infoDtoDef.add(MgProductEntity.Info.SPU_SALES, 5, ProductStoreDto.SpuBizSummary.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.ADD_SPEC, 6, ProductSpecDto.Spec.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.DEL_SPEC, 7, ProductSpecDto.Spec.getInfoDto());
        g_infoDtoDef.add(MgProductEntity.Info.UP_SPEC, 8, ProductSpecDto.Spec.getInfoDto());
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

    // 商品中台所有数据选项, 之后但凡是有业务控制要操作哪些数据的，都用这个
    public static ParamDef g_optionDef = new ParamDef();
    static {
        g_optionDef.add(MgProductEntity.Option.BASIC, 0, Var.Type.BOOLEAN);
        g_optionDef.add(MgProductEntity.Option.GROUP, 1, Var.Type.BOOLEAN);
        g_optionDef.add(MgProductEntity.Option.LIB, 2, Var.Type.BOOLEAN);
        g_optionDef.add(MgProductEntity.Option.TAG, 3, Var.Type.BOOLEAN);
        g_optionDef.add(MgProductEntity.Option.PROP, 4, Var.Type.BOOLEAN);
    }
    public static ParamDef getOptionDto() {
        return g_optionDef;
    }


    // for es
    public static ParamDef g_esPdInfoDef = new ParamDef();
    static {
        g_esPdInfoDef.add(ProductBasicEntity.ProductInfo.AID, 0, Var.Type.INT);
        g_esPdInfoDef.add(ProductBasicEntity.ProductInfo.UNION_PRI_ID, 1, Var.Type.INT);
        g_esPdInfoDef.add(ProductBasicEntity.ProductInfo.PD_ID, 2, Var.Type.INT);
        g_esPdInfoDef.add(ProductBasicEntity.ProductInfo.NAME, 3, Var.Type.STRING);
        g_esPdInfoDef.add(ProductBasicEntity.ProductInfo.STATUS, 4, Var.Type.INT);
    }
    public static ParamDef getEsPdInfoDto() {
        return g_esPdInfoDef;
    }

    public static ParamDef g_idMapInfoDef = new ParamDef();
    static {
        g_idMapInfoDef.add(MgProductEntity.Info.TO_RL_PD_ID, 0, Var.Type.INT);
        g_idMapInfoDef.add(MgProductEntity.Info.FROM_RL_PD_ID, 1, Var.Type.INT);
    }
    public static ParamDef getIdMapDto() {
        return g_idMapInfoDef;
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
        public static final int PRIMARY_KEYS = 12;
        public static final int BIND_PD_INFO = 13;
        public static final int XID = 14;
        public static final int PRIMARY_KEY = 15;
        public static final int FROM_PRIMARY_KEY = 16;
        public static final int FROM_AID = 17;
        public static final int OPTION = 18;
        public static final int RL_BACKUPID = 19;
        public static final int UNIONPRI_ID = 20;
        public static final int PD_ID = 21;
        public static final int SYS_TYPE = 22;
        public static final int TOTAL = 23;
        public static final int RL_RESTOREID = 24;
        public static final int UPDATER = 25;
        public static final int ID_MAP = 26;
        public static final int TO_SYS_TYPE = 27;
        public static final int FROM_SYS_TYPE = 28;
    }
}
