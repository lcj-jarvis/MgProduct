package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.BizSalesSummaryEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class BizSalesSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(BizSalesSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.RL_PD_ID, 3, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.PRICE_TYPE, 4, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.MODE_TYPE, 5, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.MARKET_PRICE, 6, Var.Type.LONG);
        g_dtoDef.add(BizSalesSummaryEntity.Info.MIN_PRICE, 7, Var.Type.LONG);
        g_dtoDef.add(BizSalesSummaryEntity.Info.MAX_PRICE, 8, Var.Type.LONG);
        g_dtoDef.add(BizSalesSummaryEntity.Info.VIRTUAL_SALES, 9, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.SALES, 10, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.COUNT, 11, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.REMAIN_COUNT, 12, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.HOLDING_COUNT, 13, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.FLAG, 14, Var.Type.INT);
        g_dtoDef.add(BizSalesSummaryEntity.Info.SYS_CREATE_TIME, 15);
        g_dtoDef.add(BizSalesSummaryEntity.Info.SYS_UPDATE_TIME, 16);
        g_dtoDef.add(BizSalesSummaryEntity.Info.DISTRIBUTE_LIST, 17, Var.Type.STRING);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
    }
}
