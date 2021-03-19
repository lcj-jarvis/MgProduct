package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.util.FaiList;
import fai.comm.util.ParamDef;

/**
 * 商品基础服务对外Dto
 */
public class ProductBasicDto {

    /*** 商品数据 ***/
    private static ParamDef g_productDef = new ParamDef();
    static {
        g_productDef.add(ProductBasicEntity.ProductRelInfo.AID, 0);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.RL_PD_ID, 1);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.RL_LIB_ID, 2);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.ADD_TIME, 3);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.ADD_SID, 4);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.LAST_SID, 5);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.LAST_UPDATE_TIME, 6);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.STATUS, 7);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.UP_SALE_TIME, 8);
        g_productDef.add(ProductBasicEntity.ProductRelInfo.FLAG, 9);

        g_productDef.add(ProductBasicEntity.ProductInfo.PD_ID, 10);
        g_productDef.add(ProductBasicEntity.ProductInfo.SOURCE_TID, 11);
        g_productDef.add(ProductBasicEntity.ProductInfo.NAME, 12);
        g_productDef.add(ProductBasicEntity.ProductInfo.PD_TYPE, 13);
        g_productDef.add(ProductBasicEntity.ProductInfo.IMG_LIST, 14);
        g_productDef.add(ProductBasicEntity.ProductInfo.VIDEO_LIST, 15);
        g_productDef.add(ProductBasicEntity.ProductInfo.UNIT, 16);
        g_productDef.add(ProductBasicEntity.ProductInfo.FLAG, 17);
        g_productDef.add(ProductBasicEntity.ProductInfo.FLAG1, 18);
        g_productDef.add(ProductBasicEntity.ProductInfo.KEEP_PROP1, 19);
        g_productDef.add(ProductBasicEntity.ProductInfo.KEEP_PROP2, 20);
        g_productDef.add(ProductBasicEntity.ProductInfo.KEEP_PROP3, 21);
        g_productDef.add(ProductBasicEntity.ProductInfo.KEEP_INT_PROP1, 22);
        g_productDef.add(ProductBasicEntity.ProductInfo.KEEP_INT_PROP2, 23);
        g_productDef.add(ProductBasicEntity.ProductInfo.CREATE_TIME, 24);
        g_productDef.add(ProductBasicEntity.ProductInfo.UPDATE_TIME, 25);
        g_productDef.add(ProductBasicEntity.ProductInfo.SOURCE_UNIONPRIID, 26);
    }

    public static ParamDef getProductDto() {
        return g_productDef;
    }

    /*** 商品业务关联数据 ***/
    private static ParamDef g_productRelDef = new ParamDef();

    static {
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.AID, 0);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.RL_PD_ID, 1);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.PD_ID, 2);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.RL_LIB_ID, 3);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.SOURCE_TID, 4);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.ADD_TIME, 5);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.ADD_SID, 6);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.LAST_SID, 7);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.LAST_UPDATE_TIME, 8);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.STATUS, 9);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.UP_SALE_TIME, 10);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.FLAG, 11);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.CREATE_TIME, 12);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.UPDATE_TIME, 13);

        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.SITE_ID, 14);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.LGID, 15);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.KEEP_PRI_ID1, 16);
        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.TID, 17);

        g_productRelDef.add(ProductBasicEntity.ProductRelInfo.PD_TYPE, 18);
    }

    public static ParamDef getProductRelDto() {
        return g_productRelDef;
    }

    /*** 商品设置的参数及参数值 ***/
    private static ParamDef g_bindPropValDef = new ParamDef();
    static {
        g_bindPropValDef.add(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, 0);
        g_bindPropValDef.add(ProductBasicEntity.BindPropInfo.RL_PROP_ID, 1);
    }

    public static ParamDef getBindPropDto(FaiList<Integer> propIds) {
        ParamDef def = new ParamDef();
        int bufKey = 0;
        def.add(ProductBasicEntity.BindPropInfo.RL_PROP_IDS, bufKey++);
        for(Integer propId : propIds) {
            def.add(BIND_PROP_DTO_PREFIX + propId, bufKey++, getBindPropValDto());
        }
        return def;
    }

    public static ParamDef getBindPropValDto() {
        return g_bindPropValDef;
    }

    public static class Key {
        public static final int BIND_PROP_INFO  = 1;
        public static final int TID  = 2;
        public static final int SITE_ID  = 3;
        public static final int LGID = 4;
        public static final int KEEP_PRIID1  = 5;
        public static final int RL_LIB_ID  = 6;
        public static final int RL_PD_ID  = 7;
        public static final int PROP_BIND = 8;
        public static final int DEL_PROP_BIND = 9;
        public static final int SERIALIZE_TMP_DEF = 10;
        public static final int RL_PD_IDS = 11;
        public static final int PD_INFO = 12;
        public static final int PD_REL_INFO_LIST = 13;
        public static final int PD_REL_INFO = 14;
        public static final int PD_BIND_INFO = 15;
        public static final int SOFT_DEL = 16;
    }


    public static final String BIND_PROP_DTO_PREFIX = "prop_";
}
