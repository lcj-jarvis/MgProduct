package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductTempEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 商品中台临时 DTO
 */
public class ProductTempDto {
    /**
     * 商品信息
     */
    public static final class Info{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.TID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.SITE_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.LGID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.KEEP_PRI_ID1, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.OWNER_RL_PD_ID, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.RL_PD_ID, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.SYS_UPDATE_TIME, 7, Var.Type.CALENDAR);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.SYS_CREATE_TIME, 8, Var.Type.CALENDAR);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.PRICE, 9, Var.Type.LONG);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.SPEC_NAME, 10, Var.Type.STRING);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.COUNT, 11, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.REMAIN_COUNT, 12, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.ProductInfo.HOLDING_COUNT, 13, Var.Type.INT);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    /**
     * 库存记录
     */
    public static final class StoreRecord{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.TID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.SITE_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.LGID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.KEEP_PRI_ID1, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.OWNER_RL_PD_ID, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.RL_PD_ID, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.IN_OUT_STORE_REC_ID, 7, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.OPT_TYPE, 8, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.CHANGE_COUNT, 9, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.REMAIN_COUNT, 10, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.REMARK, 11, Var.Type.STRING);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.RL_ORDER_CODE, 12, Var.Type.STRING);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.RL_REFUND_ID, 13, Var.Type.STRING);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.OPT_SID, 14, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.HEAD_SID, 15, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.SYS_UPDATE_TIME, 16, Var.Type.CALENDAR);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.SYS_CREATE_TIME, 17, Var.Type.CALENDAR);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.C_TYPE, 18, Var.Type.INT);
            g_infoDtoDef.add(ProductTempEntity.StoreRecordInfo.S_TYPE, 19, Var.Type.INT);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static class Key {
        public static final int TID = 1;
        public static final int SITE_ID = 2;
        public static final int LGID = 3;
        public static final int KEEP_PRIID1 = 4;
        public static final int RL_PD_ID = 5;
        public static final int INFO_LIST = 6;

    }
}
