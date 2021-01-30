package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 库存服务
 */
public class ProductStoreDto {

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

        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }
    public static final class BizSalesSummary{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.RL_PD_ID, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.PRICE_TYPE, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MODE_TYPE, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MARKET_PRICE, 6, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MIN_PRICE, 7, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MAX_PRICE, 8, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.VIRTUAL_SALES, 9, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SALES, 10, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.COUNT, 11, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.REMAIN_COUNT, 12, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.HOLDING_COUNT, 13, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.FLAG, 14, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SYS_CREATE_TIME, 15);
            g_infoDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SYS_UPDATE_TIME, 16);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class SalesSummary{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.RL_PD_ID, 1, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.MIN_PRICE, 2, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.MAX_PRICE, 3, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.COUNT, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.REMAIN_COUNT, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.HOLDING_COUNT, 6, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.SYS_CREATE_TIME, 7);
            g_infoDtoDef.add(ProductStoreEntity.SalesSummaryInfo.SYS_UPDATE_TIME, 8);
        }
        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }

    public static final class StoreSkuSummary{
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.AID, 0, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.SKU_ID, 1, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.RL_PD_ID, 2, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.COUNT, 3, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.REMAIN_COUNT, 4, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.HOLDING_COUNT, 5, Var.Type.INT);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.FIFO_TOTAL_COST, 6, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.MW_TOTAL_COST, 7, Var.Type.LONG);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.SYS_CREATE_TIME, 8);
            g_infoDtoDef.add(ProductStoreEntity.StoreSkuSummaryInfo.SYS_UPDATE_TIME, 9);
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
        public static final int IN_OUT_STORE_RECODR = 17;
        public static final int IS_BIZ = 18;
        public static final int STR_LIST = 19;
    }
}
