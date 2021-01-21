package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.InOutStoreRecordEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class InOutStoreRecordDto {
    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(InOutStoreRecordEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SKU_ID, 3, Var.Type.LONG);
        g_dtoDef.add(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, 4, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.RL_PD_ID, 5, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.OPT_TYPE, 6, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.C_TYPE, 7, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.S_TYPE, 8, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.CHANGE_COUNT, 9, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.PRICE, 10, Var.Type.LONG);
        g_dtoDef.add(InOutStoreRecordEntity.Info.NUMBER, 11, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.OPT_SID, 12, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.HEAD_SID, 13, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.OPT_TIME, 14, Var.Type.CALENDAR);
        g_dtoDef.add(InOutStoreRecordEntity.Info.FLAG, 15, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, 16, Var.Type.CALENDAR);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, 17, Var.Type.CALENDAR);
        g_dtoDef.add(InOutStoreRecordEntity.Info.REMARK, 18, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 19, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.REMAIN_COUNT, 20, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.RL_ORDER_CODE, 21, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.RL_REFUND_ID, 22, Var.Type.STRING);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
    }
}
