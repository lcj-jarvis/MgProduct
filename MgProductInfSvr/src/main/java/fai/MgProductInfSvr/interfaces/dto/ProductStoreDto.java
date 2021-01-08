package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductStoreDto {

    private static ParamDef g_storeSalesSkuDtoDef = new ParamDef();
    static {
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.AID, 0, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID, 3, Var.Type.LONG);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.RL_PD_ID, 4, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SKU_TYPE, 6, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SORT, 7, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.COUNT, 8, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.REMAIN_COUNT, 9, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.HOLDING_COUNT, 10, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.PRICE, 11, Var.Type.LONG);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, 12, Var.Type.LONG);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MIN_AMOUNT, 13, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.MAX_AMOUNT, 14, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.DURATION, 15, Var.Type.DOUBLE);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, 16, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.FLAG, 17, Var.Type.INT);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SYS_UPDATE_TIME, 18, Var.Type.CALENDAR);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.SYS_CREATE_TIME, 19, Var.Type.CALENDAR);
        g_storeSalesSkuDtoDef.add(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST, 20, Var.Type.FAI_LIST);
    }
    public static ParamDef getStoreSalesSkuDto() {
        return g_storeSalesSkuDtoDef;
    }

    private static ParamDef g_inOutStoreRecordDtoDef = new ParamDef();
    static {
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.AID, 0, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 1, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.LGID, 2, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 3, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SKU_ID, 5, Var.Type.LONG);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.IN_OUT_STORE_REC_ID, 6, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.RL_PD_ID, 7, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_TYPE, 8, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.C_TYPE, 9, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.S_TYPE, 10, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.CHANGE_COUNT, 11, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.PRICE, 12, Var.Type.LONG);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.NUMBER, 13, Var.Type.STRING);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_SID, 14, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.HEAD_SID, 15, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.OPT_TIME, 16, Var.Type.CALENDAR);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.FLAG, 17, Var.Type.INT);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SYS_UPDATE_TIME, 18, Var.Type.CALENDAR);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.SYS_CREATE_TIME, 19, Var.Type.CALENDAR);
        g_inOutStoreRecordDtoDef.add(ProductStoreEntity.InOutStoreRecordInfo.REMARK, 20, Var.Type.STRING);

    }
    public static ParamDef getInOutStoreRecordDto() {
        return g_inOutStoreRecordDtoDef;
    }

    private static ParamDef g_bizSalesSummaryDtoDef = new ParamDef();
    static {
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.AID, 0, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.RL_PD_ID, 3, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.PRICE_TYPE, 4, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MODE_TYPE, 5, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MARKET_PRICE, 6, Var.Type.LONG);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MIN_PRICE, 7, Var.Type.LONG);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.MAX_PRICE, 8, Var.Type.LONG);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.VIRTUAL_SALES, 9, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SALES, 10, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.COUNT, 11, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.REMAIN_COUNT, 11, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.HOLDING_COUNT, 12, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.FLAG, 13, Var.Type.INT);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SYS_CREATE_TIME, 14, Var.Type.CALENDAR);
        g_bizSalesSummaryDtoDef.add(ProductStoreEntity.BizSalesSummaryInfo.SYS_UPDATE_TIME, 15, Var.Type.CALENDAR);
    }
    public static ParamDef getBizSalesSummaryDto() {
        return g_bizSalesSummaryDtoDef;
    }

    private static ParamDef g_salesSummaryDtoDef = new ParamDef();
    static {
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.AID, 0, Var.Type.INT);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.PD_ID, 1, Var.Type.INT);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.MIN_PRICE, 2, Var.Type.LONG);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.MAX_PRICE, 3, Var.Type.LONG);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.COUNT, 4, Var.Type.INT);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.REMAIN_COUNT, 5, Var.Type.INT);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.HOLDING_COUNT, 6, Var.Type.INT);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.SYS_CREATE_TIME, 7, Var.Type.CALENDAR);
        g_salesSummaryDtoDef.add(ProductStoreEntity.SalesSummaryInfo.SYS_UPDATE_TIME, 8, Var.Type.CALENDAR);
    }
    public static ParamDef getSalesSummaryDto() {
        return g_salesSummaryDtoDef;
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

        public static final int RL_ORDER_ID = 11;
        public static final int COUNT = 12;
        public static final int REDUCE_MODE = 13;
        public static final int EXPIRE_TIME_SECONDS = 14;
    }
}
