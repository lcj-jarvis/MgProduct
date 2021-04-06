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
    }
    public static ParamDef getInfoDto() {
        return g_infoDtoDef;
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
    }
}
