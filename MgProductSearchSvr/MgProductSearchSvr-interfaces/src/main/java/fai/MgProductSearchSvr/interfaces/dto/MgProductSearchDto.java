package fai.MgProductSearchSvr.interfaces.dto;

import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.comm.util.ParamDef;

public class MgProductSearchDto {

    private static ParamDef g_searchParameterDtoDef = new ParamDef();
    static{
        g_searchParameterDtoDef.add(MgProductSearch.Info.START, 0);
        g_searchParameterDtoDef.add(MgProductSearch.Info.LIMIT, 1);
        g_searchParameterDtoDef.add(MgProductSearch.Info.FIRST_COMPARATOR_KEY, 2);
        g_searchParameterDtoDef.add(MgProductSearch.Info.FIRST_COMPARATOR_KEY_ORDER, 3);
        g_searchParameterDtoDef.add(MgProductSearch.Info.SECOND_COMPARATOR_KEY, 4);
        g_searchParameterDtoDef.add(MgProductSearch.Info.SECOND_COMPARATOR_KEY_ORDER, 5);
        g_searchParameterDtoDef.add(MgProductSearch.Info.UP_SALES_STATUS, 6);
    }

    public static ParamDef getSearchDtoDto() {
        return g_searchParameterDtoDef;
    }

    private static ParamDef g_productDetailDtoDef = new ParamDef();
    static{
        g_productDetailDtoDef.add(ProductEntity.Info.AID, 0);
        g_productDetailDtoDef.add(ProductEntity.Info.PD_ID, 1);
        g_productDetailDtoDef.add(ProductEntity.Info.SOURCE_TID, 2);
        g_productDetailDtoDef.add(ProductEntity.Info.NAME, 3);
        g_productDetailDtoDef.add(ProductEntity.Info.PD_TYPE, 4);
        g_productDetailDtoDef.add(ProductEntity.Info.IMG_LIST, 5);
        g_productDetailDtoDef.add(ProductEntity.Info.VIDEO_LIST, 6);
        g_productDetailDtoDef.add(ProductEntity.Info.UNIT, 7);
        g_productDetailDtoDef.add(ProductEntity.Info.FLAG, 8);
        g_productDetailDtoDef.add(ProductEntity.Info.FLAG1, 9);
        g_productDetailDtoDef.add(ProductEntity.Info.KEEP_PROP1, 10);
        g_productDetailDtoDef.add(ProductEntity.Info.KEEP_PROP2, 11);
        g_productDetailDtoDef.add(ProductEntity.Info.KEEP_PROP3, 12);
        g_productDetailDtoDef.add(ProductEntity.Info.KEEP_INT_PROP1, 13);
        g_productDetailDtoDef.add(ProductEntity.Info.KEEP_INT_PROP2, 14);
        g_productDetailDtoDef.add(ProductEntity.Info.CREATE_TIME, 15);
        g_productDetailDtoDef.add(ProductEntity.Info.UPDATE_TIME, 16);
        g_productDetailDtoDef.add(ProductEntity.Info.SOURCE_UNIONPRIID, 17);
    }
    public static ParamDef getDetailDto() {
        return g_productDetailDtoDef;
    }

    public static class Key {
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int SEARCH_LIST = 3;
        public static final int SEARCH_PARAM_STRING = 4;
        public static final int TOTAL_SIZE = 5;
    }
}
