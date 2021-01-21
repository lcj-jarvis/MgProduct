package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.*;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 规格服务 - DTO
 */
public class ProductSpecDto {
    /**
     * 商品规格 DTO
     */
    public static final class Spec {
        public static ParamDef getInPdScValListDtoDef() {
            return g_inPdScValItemDtoDef;
        }
        private static ParamDef g_inPdScValItemDtoDef = new ParamDef();
        static {
            g_inPdScValItemDtoDef.add(ProductSpecValObj.Spec.InPdScValList.Item.SC_STR_ID, 0, Var.Type.INT);
            g_inPdScValItemDtoDef.add(ProductSpecValObj.Spec.InPdScValList.Item.NAME, 1, Var.Type.STRING);
            g_inPdScValItemDtoDef.add(ProductSpecValObj.Spec.InPdScValList.Item.CHECK, 2, Var.Type.BOOLEAN);
            g_inPdScValItemDtoDef.add(ProductSpecValObj.Spec.InPdScValList.Item.FILE_ID, 3, Var.Type.STRING);
        }

        private static ParamDef g_dtoDef = new ParamDef();
        static {
            g_dtoDef.add(ProductSpecEntity.SpecInfo.AID, 0, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.RL_PD_ID, 1, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.SC_STR_ID, 2, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.NAME, 3, Var.Type.STRING);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.PD_SC_ID, 4, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.SOURCE_TID, 5, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.SORT, 6, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.FLAG, 7, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST, 8, getInPdScValListDtoDef(), Var.Type.FAI_LIST);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.SYS_CREATE_TIME, 9, Var.Type.CALENDAR);
            g_dtoDef.add(ProductSpecEntity.SpecInfo.SYS_UPDATE_TIME, 10, Var.Type.CALENDAR);
        }

        public static ParamDef getInfoDto() {
            return g_dtoDef;
        }
    }
    /**
     * 商品规格SKU DTO
     */
    public static final class SpecSku {
        private static ParamDef g_dtoDef = new ParamDef();
        static {
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.AID, 0, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.RL_PD_ID, 1, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SKU_ID, 2, Var.Type.LONG);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SORT, 3, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SOURCE_TID, 4, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SKU_NUM, 5, Var.Type.STRING);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_ID_LIST, 6, Var.Type.FAI_LIST);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, 7, Var.Type.FAI_LIST);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.FLAG, 8, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SYS_CREATE_TIME, 9, Var.Type.CALENDAR);
            g_dtoDef.add(ProductSpecEntity.SpecSkuInfo.SYS_UPDATE_TIME, 10, Var.Type.CALENDAR);
        }
        public static ParamDef getInfoDto() {
            return g_dtoDef;
        }
    }
    /**
     * 规格模板 DTO
     */
    public static final class SpecTemp{
        private static ParamDef g_dtoDef = new ParamDef();

        static {
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.AID, 0, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.RL_TP_SC_ID, 1, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.RL_LIB_ID, 2, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.NAME, 3, Var.Type.STRING);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.SOURCE_TID, 4, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.FLAG, 5, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.SORT, 6, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.SYS_CREATE_TIME, 7, Var.Type.CALENDAR);
            g_dtoDef.add(ProductSpecEntity.SpecTempInfo.SYS_UPDATE_TIME, 8, Var.Type.CALENDAR);
        }

        public static ParamDef getInfoDto() {
            return g_dtoDef;
        }
    }
    /**
     * 规格模板详情 DTO
     */
    public static final class SpecTempDetail{
        public static ParamDef getInScValListDtoDef() {
            return g_inScValItemDtoDef;
        }
        private static ParamDef g_inScValItemDtoDef = new ParamDef();
        static {
            g_inScValItemDtoDef.add(ProductSpecValObj.SpecTempDetail.InScValList.Item.SC_STR_ID, 0, Var.Type.INT);
            g_inScValItemDtoDef.add(ProductSpecValObj.SpecTempDetail.InScValList.Item.NAME, 1, Var.Type.STRING);
            g_inScValItemDtoDef.add(ProductSpecValObj.SpecTempDetail.InScValList.Item.FILE_ID, 2, Var.Type.STRING);
        }

        private static ParamDef g_dtoDef = new ParamDef();
        static {
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.AID, 0, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.RL_TP_SC_ID, 1, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.SC_STR_ID, 2, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.NAME, 3, Var.Type.STRING);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.TP_SC_DT_ID, 4, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.SORT, 5, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.FLAG, 6, Var.Type.INT);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.IN_SC_VAL_LIST, 7, getInScValListDtoDef(), Var.Type.FAI_LIST);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.SYS_CREATE_TIME, 8, Var.Type.CALENDAR);
            g_dtoDef.add(ProductSpecEntity.SpecTempDetailInfo.SYS_UPDATE_TIME, 9, Var.Type.CALENDAR);
        }

        public static ParamDef getInfoDto() {
            return g_dtoDef;
        }
    }


    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int RL_PD_ID = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID_LIST = 8;
        public static final int RL_TP_SC_ID = 9;
        public static final int ONLY_GET_CHECKED = 10;
    }

}
