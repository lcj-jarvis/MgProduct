package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpuBizSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SpuBizSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.RL_PD_ID, 3, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.PRICE_TYPE, 4, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MODE_TYPE, 5, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MARKET_PRICE, 6, Var.Type.LONG);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MIN_PRICE, 7, Var.Type.LONG);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MAX_PRICE, 8, Var.Type.LONG);
        g_dtoDef.add(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 9, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.SALES, 10, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.COUNT, 11, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.REMAIN_COUNT, 12, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.HOLDING_COUNT, 13, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.FLAG, 14, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, 15);
        g_dtoDef.add(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, 16);
        g_dtoDef.add(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, 17, Var.Type.STRING);
        g_dtoDef.add(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, 18, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.SYS_TYPE, 19, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.STATUS, 20, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.WEIGHT, 21, Var.Type.DOUBLE);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MIN_AMOUNT, 23, Var.Type.INT);
        g_dtoDef.add(SpuBizSummaryEntity.Info.MAX_AMOUNT, 24, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key{
        public static final int INFO = 0;
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int PD_ID = 3;
        public static final int INFO_LIST = 6;
        public static final int ID_LIST = 8;
        public static final int UPDATER = 9;
        public static final int XID = 10;
        public static final int SEARCH_ARG = 11;
        public static final int TOTAL_SIZE = 12;
        public static final int UID_LIST = 13;
        public static final int DATA_STATUS = 16;
    }
}
