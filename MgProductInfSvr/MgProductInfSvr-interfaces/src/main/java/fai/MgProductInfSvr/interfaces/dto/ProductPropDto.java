package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductPropEntity;
import fai.comm.util.ParamDef;

/**
 * 商品参数服务对外Dto
 */
public class ProductPropDto {
    private static ParamDef g_productPropDtoDef = new ParamDef();
    private static ParamDef g_productPropValDtoDef = new ParamDef();

    static {
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.AID, 0);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.RL_PROP_ID, 1);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.RL_LIB_ID, 2);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.NAME, 3);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.SORT, 4);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.TYPE, 5);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.FLAG, 6);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.RL_FLAG, 7);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.CREATE_TIME, 8);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.UPDATE_TIME, 9);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.SOURCE_TID, 10);
        g_productPropDtoDef.add(ProductPropEntity.PropInfo.SOURCE_UNIONPRIID, 11);
    }

    static {
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.AID, 0);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.RL_PROP_ID, 1);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.PROP_VAL_ID, 2);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.VAL, 3);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.SORT, 4);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.DATA_TYPE, 5);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.CREATE_TIME, 6);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.UPDATE_TIME, 7);
        g_productPropValDtoDef.add(ProductPropEntity.PropValInfo.RL_PROP_ID, 8);
    }

    public static ParamDef getPropInfoDto() {
        return g_productPropDtoDef;
    }

    public static ParamDef getPropValInfoDto() {
        return g_productPropValDtoDef;
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int LIB_ID = 5;
        public static final int PROP_LIST = 6;
        public static final int VAL_LIST = 7;
        public static final int TOTAL_SIZE = 8;
        public static final int SEARCH_ARG = 9;
        public static final int UPDATERLIST = 10;
        public static final int RL_PROP_ID = 11;
        public static final int RL_PROP_IDS = 12;
        public static final int PROP_INFO = 13;
        public static final int UPDATER = 14;
        public static final int VAL_IDS = 15;
    }

}
