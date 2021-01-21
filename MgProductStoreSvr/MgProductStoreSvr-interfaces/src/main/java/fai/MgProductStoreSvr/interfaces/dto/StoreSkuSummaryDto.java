package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.StoreSkuSummaryEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class StoreSkuSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(StoreSkuSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.SKU_ID, 1, Var.Type.LONG);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.COUNT, 4, Var.Type.INT);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.REMAIN_COUNT, 5, Var.Type.INT);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.HOLDING_COUNT, 6, Var.Type.INT);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.FIFO_TOTAL_COST, 7, Var.Type.LONG);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.MW_TOTAL_COST, 8, Var.Type.LONG);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.SYS_CREATE_TIME, 9, Var.Type.CALENDAR);
        g_dtoDef.add(StoreSkuSummaryEntity.Info.SYS_UPDATE_TIME, 10, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
        public static final int SEARCH_ARG = 11;
        public static final int TOTAL_SIZE = 12;
    }
}
