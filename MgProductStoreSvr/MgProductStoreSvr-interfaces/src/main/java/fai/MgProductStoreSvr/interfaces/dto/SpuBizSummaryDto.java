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
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
    }
}
