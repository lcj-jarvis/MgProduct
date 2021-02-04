package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.SpuSummaryEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpuSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SpuSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SpuSummaryEntity.Info.PD_ID, 1, Var.Type.INT);
        g_dtoDef.add(SpuSummaryEntity.Info.MIN_PRICE, 2, Var.Type.LONG);
        g_dtoDef.add(SpuSummaryEntity.Info.MAX_PRICE, 3, Var.Type.LONG);
        g_dtoDef.add(SpuSummaryEntity.Info.COUNT, 4, Var.Type.INT);
        g_dtoDef.add(SpuSummaryEntity.Info.REMAIN_COUNT, 5, Var.Type.INT);
        g_dtoDef.add(SpuSummaryEntity.Info.HOLDING_COUNT, 6, Var.Type.INT);
        g_dtoDef.add(SpuSummaryEntity.Info.SYS_CREATE_TIME, 7);
        g_dtoDef.add(SpuSummaryEntity.Info.SYS_UPDATE_TIME, 8);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
    }
}
