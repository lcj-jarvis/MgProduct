package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 库存服务
 */
public class ProductStoreDto {

    public static final class PrimaryKey {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.TID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.LGID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, 4, Var.Type.INT);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class StoreSalesSku{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID, 3, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.RL_PD_ID, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SKU_TYPE, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SORT, 7, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.COUNT, 8, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.REMAIN_COUNT, 9, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.HOLDING_COUNT, 10, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.PRICE, 11, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, 12, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MIN_AMOUNT, 13, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MAX_AMOUNT, 14, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.DURATION, 15, Var.Type.DOUBLE);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, 16, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.FLAG, 17, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SYS_UPDATE_TIME, 18);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SYS_CREATE_TIME, 19);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST, 20, Var.Type.FAI_LIST);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.FIFO_TOTAL_COST, 21, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MW_TOTAL_COST, 22, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.TID, 23, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, 24, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.LGID, 25, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, 26, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.COST_PRICE, 27, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MW_COST, 28, Var.Type.LONG);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class InOutStoreRecord{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.TID, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.LGID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SKU_ID, 5, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.IN_OUT_STORE_REC_ID, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.RL_PD_ID, 7, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_TYPE, 8, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.C_TYPE, 9, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.S_TYPE, 10, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.CHANGE_COUNT, 11, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.PRICE, 12, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.NUMBER, 13, Var.Type.STRING);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_SID, 14, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.HEAD_SID, 15, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_TIME, 16);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.FLAG, 17, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SYS_UPDATE_TIME, 18);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SYS_CREATE_TIME, 19);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.REMARK, 20, Var.Type.STRING);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OWNER_RL_PD_ID, 21, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.REMAIN_COUNT, 22, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.IN_PD_SC_STR_NAME_LIST, 23, Var.Type.FAI_LIST);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.RL_ORDER_CODE, 24, Var.Type.STRING);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.RL_REFUND_ID, 25, Var.Type.STRING);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.MW_PRICE, 26, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.TOTAL_PRICE, 27, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.MW_TOTAL_PRICE, 28, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.STATUS, 29, Var.Type.INT);

        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }
    public static final class SpuBizSummary {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.RL_PD_ID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.PRICE_TYPE, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.MODE_TYPE, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.MARKET_PRICE, 6, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, 7, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, 8, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.VIRTUAL_SALES, 9, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.SALES, 10, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.COUNT, 11, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, 12, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.HOLDING_COUNT, 13, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.FLAG, 14, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.SYS_CREATE_TIME, 15);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.SYS_UPDATE_TIME, 16);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.TID, 17, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.SITE_ID, 18, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.LGID, 19, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuBizSummaryInfo.KEEP_PRI_ID1, 20, Var.Type.INT);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class SpuSummary {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.RL_PD_ID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.MIN_PRICE, 2, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.MAX_PRICE, 3, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.COUNT, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.REMAIN_COUNT, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.HOLDING_COUNT, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.SYS_CREATE_TIME, 7);
            g_infoDtoDef.add(ProductStoreEntity.SpuSummaryInfo.SYS_UPDATE_TIME, 8);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class SkuSummary {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.SKU_ID, 1, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.RL_PD_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.COUNT, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.REMAIN_COUNT, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.HOLDING_COUNT, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.FIFO_TOTAL_COST, 6, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.MW_TOTAL_COST, 7, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.SYS_CREATE_TIME, 8);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.SYS_UPDATE_TIME, 9);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.MIN_PRICE, 10, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SkuSummaryInfo.MAX_PRICE, 11, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MW_COST, 12, Var.Type.LONG);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }


    public static final class HoldingRecord{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.TID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.SITE_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.LGID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.KEEP_PRI_ID1, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.SKU_ID, 5, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.RL_ORDER_CODE, 6, Var.Type.STRING);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.ITEM_ID, 7, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.COUNT, 8, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.EXPIRE_TIME, 9, Var.Type.CALENDAR);
            g_infoDtoDef.add(ProductStoreEntity.HoldingRecordInfo.SYS_CREATE_TIME, 10, Var.Type.CALENDAR);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    /**
     * 库存变化dto，例如下单扣减啥的
     */
    public static final class SkuCountChange {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.SkuCountChangeInfo.SKU_ID, 3, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SkuCountChangeInfo.ITEM_ID, 7, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SkuCountChangeInfo.COUNT, 8, Var.Type.INT);
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
        public static final int UPDATER_LIST = 7;
        public static final int ID_LIST = 9;
        public static final int SKU_ID = 10;

        public static final int RL_ORDER_CODE = 11;
        public static final int COUNT = 12;
        public static final int REDUCE_MODE = 13;
        public static final int EXPIRE_TIME_SECONDS = 14;

        public static final int SEARCH_ARG = 15;
        public static final int TOTAL_SIZE = 16;
        public static final int IN_OUT_STORE_RECORD = 17;
        public static final int IS_BIZ = 18;
        public static final int STR_LIST = 19;
        public static final int IN_OUT_STORE_RECORD_ID = 20;
        public static final int RL_REFUND_ID = 21;
        public static final int PRI_IDS = 22;
        public static final int PRICE = 23;
        public static final int OPT_TIME = 24;
        public static final int PRIMARY_KEYS = 25;
    }
}
