package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.SalesSummaryEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SalesSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SalesSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SalesSummaryEntity.Info.PD_ID, 1, Var.Type.INT);
        g_dtoDef.add(SalesSummaryEntity.Info.MIN_PRICE, 2, Var.Type.LONG);
        g_dtoDef.add(SalesSummaryEntity.Info.MAX_PRICE, 3, Var.Type.LONG);
        g_dtoDef.add(SalesSummaryEntity.Info.COUNT, 4, Var.Type.INT);
        g_dtoDef.add(SalesSummaryEntity.Info.REMAIN_COUNT, 5, Var.Type.INT);
        g_dtoDef.add(SalesSummaryEntity.Info.HOLDING_COUNT, 6, Var.Type.INT);
        g_dtoDef.add(SalesSummaryEntity.Info.SYS_CREATE_TIME, 7, Var.Type.CALENDAR);
        g_dtoDef.add(SalesSummaryEntity.Info.SYS_UPDATE_TIME, 8, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
    }
}
