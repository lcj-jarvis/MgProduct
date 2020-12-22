package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.util.FaiList;
import fai.comm.util.ParamDef;

/**
 * 商品基础服务对外Dto
 */
public class ProductBasicDto {

    private static ParamDef g_propValDef = new ParamDef();
    static {
        g_propValDef.add(ProductBasicEntity.BindPropInfo.RL_PROP_ID, 0);
        g_propValDef.add(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, 1);
    }

    private static ParamDef g_bindPropValDef = new ParamDef();
    static {
        g_bindPropValDef.add(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, 0);
        g_bindPropValDef.add(ProductBasicEntity.BindPropInfo.VAL, 1);
    }

    /**
     * 商品设置的参数及参数值
     */
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

    public static ParamDef getBindPropValDto(FaiList<Integer> propIds) {
        return g_propValDef;
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
    }


    public static final String BIND_PROP_DTO_PREFIX = "prop_";
}
