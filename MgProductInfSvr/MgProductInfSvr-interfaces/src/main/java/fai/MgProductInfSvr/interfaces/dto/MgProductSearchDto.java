package fai.MgProductInfSvr.interfaces.dto;

import fai.comm.util.ParamDef;
import fai.comm.util.Var;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;

public class MgProductSearchDto {

    private static ParamDef g_productSearchDtoDef = new ParamDef();
    static{
        g_productSearchDtoDef.add(MgProductSearchResult.Info.ID_LIST, 0, Var.Type.FAI_LIST);
        // g_productSearchDtoDef.add(MgProductSearchResult.Info.TOTAL, 1, Var.Type.INT);
        g_productSearchDtoDef.add(MgProductSearchResult.Info.TOTAL, 1);
        g_productSearchDtoDef.add(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, 2, Var.Type.LONG);
        g_productSearchDtoDef.add(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, 3, Var.Type.LONG);
    }

    public static ParamDef getProductSearchDto() {
        return g_productSearchDtoDef;
    }

    public static class Key {
        public static final int RESULT_INFO = 0;
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int UNION_PRI_ID = 5;
        public static final int PRODUCT_COUNT = 6;
        public static final int ES_SEARCH_PARAM_STRING = 7;
        public static final int DB_SEARCH_PARAM_STRING = 8;
        public static final int PAGE_INFO_STRING = 9;
    }
}
