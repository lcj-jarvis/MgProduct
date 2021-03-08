package fai.MgProductSearchSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearchResultEntity;
import fai.comm.util.ParamDef;

public class MgProductSearchDto {

    private static ParamDef g_productSearchDtoDef = new ParamDef();
    static{
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.MANAGE_DATA_CACHE_TIME, 0);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.VISTOR_DATA_CACHE_TIME, 1);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.ID_LIST, 2);
        g_productSearchDtoDef.add(MgProductSearchResultEntity.Info.TOTAL, 3);
    }
    public static ParamDef getProductSearchDto() {
        return g_productSearchDtoDef;
    }

    public static class Key {
        public static final int RESULT_INFO = 0;
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int TOTAL_SIZE = 3;
        public static final int SEARCH_PARAM_STRING = 4;
    }
}
