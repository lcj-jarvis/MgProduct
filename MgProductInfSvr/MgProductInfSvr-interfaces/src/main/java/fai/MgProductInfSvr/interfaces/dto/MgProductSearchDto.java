package fai.MgProductInfSvr.interfaces.dto;

import fai.comm.util.ParamDef;
import fai.comm.util.Var;
import fai.MgProductInfSvr.interfaces.entity.MgProductSearchResultEntity;

public class MgProductSearchDto {

    private static ParamDef g_productSearchDtoDef = new ParamDef();
    static{
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.ID_LIST, 0, Var.Type.FAI_LIST);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.TOTAL, 1, Var.Type.INT);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.MANAGE_DATA_CACHE_TIME, 2, Var.Type.LONG);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.VISTOR_DATA_CACHE_TIME, 3, Var.Type.LONG);
    }
    public static ParamDef getProductSearchDto() {
        return g_productSearchDtoDef;
    }

    public static class Key {
        public static final int RESULT_INFO = 0;
        public static final int SEARCH_PARAM_STRING = 1;
        public static final int TID = 2;
        public static final int SITE_ID = 3;
        public static final int LGID = 4;
        public static final int KEEP_PRIID1 = 5;
        public static final int UNION_PRI_ID = 6;
        public static final int PRODUCT_COUNT = 7;
    }
}
