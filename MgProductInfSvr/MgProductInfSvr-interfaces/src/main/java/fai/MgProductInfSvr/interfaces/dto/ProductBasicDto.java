package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.util.FaiList;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 商品基础服务对外Dto
 */
public class ProductBasicDto {

    /*** 商品数据 ***/
    private static ParamDef g_productDef = new ParamDef();
    static {
        g_productDef.add(ProductBasicEntity.ProductInfo.AID, 0);
        g_productDef.add(ProductBasicEntity.ProductInfo.RL_PD_ID, 1);
        g_productDef.add(ProductBasicEntity.ProductInfo.RL_LIB_ID, 2);
        g_productDef.add(ProductBasicEntity.ProductInfo.ADD_TIME, 3);
        g_productDef.add(ProductBasicEntity.ProductInfo.ADD_SID, 4);
        g_productDef.add(ProductBasicEntity.ProductInfo.LAST_SID, 5);
        g_productDef.add(ProductBasicEntity.ProductInfo.LAST_UPDATE_TIME, 6);
        g_productDef.add(ProductBasicEntity.ProductInfo.STATUS, 7);
        g_productDef.add(ProductBasicEntity.ProductInfo.UP_SALE_TIME, 8);
        g_productDef.add(ProductBasicEntity.ProductInfo.RL_FLAG, 9);

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

        g_productDef.add(ProductBasicEntity.ProductInfo.RL_GROUP_IDS, 28);
        //g_productDef.add(ProductBasicEntity.ProductInfo.RL_PROP_IDS, 29);
        //g_productDef.add(ProductBasicEntity.ProductInfo.PROP_VAL_IDS, 30);

        /* 修改使用 */
        //g_productDef.add(ProductBasicEntity.BindGroupInfo.DEL_RL_GROUP_IDS, 31, Var.Type.FAI_LIST);
        //g_productDef.add(ProductBasicEntity.BindGroupInfo.ADD_RL_GROUP_IDS, 32, Var.Type.FAI_LIST);
        //g_productDef.add(ProductBasicEntity.BindPropInfo.ADD_PROP_LIST, 33, ProductPropDto.getPropValInfoDto(), Var.Type.FAI_LIST);
        //g_productDef.add(ProductBasicEntity.BindPropInfo.DEL_PROP_LIST, 34, ProductPropDto.getPropValInfoDto(), Var.Type.FAI_LIST);

        g_productDef.add(ProductBasicEntity.ProductInfo.RL_PROPS, 35, ProductPropDto.getPropValInfoDto(), Var.Type.FAI_LIST);
        g_productDef.add(ProductBasicEntity.ProductInfo.RL_TAG_IDS, 36, Var.Type.FAI_LIST);
        g_productDef.add(ProductBasicEntity.ProductInfo.SYS_TYPE, 37, Var.Type.INT);
        g_productDef.add(ProductBasicEntity.ProductInfo.SORT, 38);

        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK, 39, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK1, 40, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK2, 41, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK3, 42, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK4, 43, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK5, 44, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK6, 45, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK7, 46, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK8, 47, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK9, 48, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK10, 49, Var.Type.STRING);
        g_productDef.add(ProductBasicEntity.ProductInfo.REMARK11, 50, Var.Type.STRING);
    }

    public static ParamDef getProductDto() {
        return g_productDef;
    }

    /*** 商品业务关联数据 ***/
    private static ParamDef g_productRelDef = new ParamDef();

    static {
        g_productRelDef.add(ProductBasicEntity.ProductInfo.AID, 0);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.RL_PD_ID, 1);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.PD_ID, 2);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.RL_LIB_ID, 3);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.SOURCE_TID, 4);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.ADD_TIME, 5);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.ADD_SID, 6);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.LAST_SID, 7);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.LAST_UPDATE_TIME, 8);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.STATUS, 9);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.UP_SALE_TIME, 10);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.FLAG, 11);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.CREATE_TIME, 12);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.UPDATE_TIME, 13);

        g_productRelDef.add(ProductBasicEntity.ProductInfo.SITE_ID, 14);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.LGID, 15);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, 16);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.TID, 17);

        g_productRelDef.add(ProductBasicEntity.ProductInfo.PD_TYPE, 18);
        g_productRelDef.add(ProductBasicEntity.ProductInfo.SYS_TYPE, 19);
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

    /*** 商品设置的分类 ***/
    private static ParamDef g_bindGroupDef = new ParamDef();
    static {
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.AID, 0);
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.RL_GROUP_ID, 1);
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.RL_PD_ID, 2);
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.PD_ID, 3);
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.CREATE_TIME, 4);
        g_bindGroupDef.add(ProductBasicEntity.BindGroupInfo.SYS_TYPE, 5);
    }
    public static ParamDef getBindGroupDto() {
        return g_bindGroupDef;
    }

    /*** 商品设置的分类 ***/
    private static ParamDef g_bindTagDef = new ParamDef();
    static {
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.AID, 0);
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.RL_TAG_ID, 1);
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.RL_PD_ID, 2);
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.PD_ID, 3);
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.CREATE_TIME, 4);
        g_bindTagDef.add(ProductBasicEntity.BindTagInfo.SYS_TYPE, 5);
    }
    public static ParamDef getBindTagDto() {
        return g_bindTagDef;
    }


    private static ParamDef g_pdReduceDef = new ParamDef();
    static {
        g_pdReduceDef.add(ProductBasicEntity.ProductInfo.TID, 0, Var.Type.INT);
        g_pdReduceDef.add(ProductBasicEntity.ProductInfo.SITE_ID, 1, Var.Type.INT);
        g_pdReduceDef.add(ProductBasicEntity.ProductInfo.LGID, 2, Var.Type.INT);
        g_pdReduceDef.add(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, 3, Var.Type.INT);
        g_pdReduceDef.add(ProductBasicEntity.ProductInfo.RL_PD_ID, 4, Var.Type.INT);
    }
    public static ParamDef getPdReduceDto() {
        return g_pdReduceDef;
    }

    /*** 商品绑定的业务 ***/
    private static ParamDef g_bindBizDef = new ParamDef();
    static {
        g_bindBizDef.add(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
        g_bindBizDef.add(ProductBasicEntity.ProductInfo.BIND_BIZ, 1, g_pdReduceDef);
    }
    public static ParamDef getBindBizDto() {
        return g_bindBizDef;
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
        public static final int PD_LIST = 17;
        public static final int BIND_GROUP_LIST = 18;
        public static final int BIND_GROUP_IDS = 19;
        public static final int DEL_BIND_GROUP_IDS = 20;
        public static final int UPDATER = 21;
        public static final int UNION_INFO = 22;
        public static final int IN_OUT_RECOED = 23;

        public static final int BIND_TAG_LIST = 24;
        public static final int BIND_TAG_IDS = 25;
        public static final int DEL_BIND_TAG_IDS = 26;
        public static final int SYS_TYPE = 27;
        public static final int XID = 28;
    }


    public static final String BIND_PROP_DTO_PREFIX = "prop_";
}
