package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.interfaces.entity.InOutStoreSumEntity;
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
        g_dtoDef.add(InOutStoreRecordEntity.Info.OPT_TIME, 14);
        g_dtoDef.add(InOutStoreRecordEntity.Info.FLAG, 15, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, 16);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, 17);
        g_dtoDef.add(InOutStoreRecordEntity.Info.REMARK, 18, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 19, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.REMAIN_COUNT, 20, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.RL_ORDER_CODE, 21, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.RL_REFUND_ID, 22, Var.Type.STRING);
        g_dtoDef.add(InOutStoreRecordEntity.Info.MW_PRICE, 23, Var.Type.LONG);
        g_dtoDef.add(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, 24, Var.Type.FAI_LIST);
        g_dtoDef.add(InOutStoreRecordEntity.Info.TOTAL_PRICE, 25, Var.Type.LONG);
        g_dtoDef.add(InOutStoreRecordEntity.Info.MW_TOTAL_PRICE, 26, Var.Type.LONG);
        g_dtoDef.add(InOutStoreRecordEntity.Info.STATUS, 27, Var.Type.INT);
        g_dtoDef.add(InOutStoreRecordEntity.Info.SYS_TYPE, 28, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    /*** inout store summary dto ***/
    private static ParamDef g_sumDtoDef = new ParamDef();
    static {
        g_sumDtoDef.add(InOutStoreSumEntity.Info.AID, 0, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.IN_OUT_STORE_REC_ID, 2, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.OPT_TYPE, 3, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.C_TYPE, 4, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.S_TYPE, 5, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.PRICE, 6, Var.Type.LONG);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.NUMBER, 7, Var.Type.STRING);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.OPT_SID, 8, Var.Type.INT);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.OPT_TIME, 9);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.REMARK, 10, Var.Type.STRING);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.MW_PRICE, 11, Var.Type.LONG);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.SYS_UPDATE_TIME, 12);
        g_sumDtoDef.add(InOutStoreSumEntity.Info.SYS_CREATE_TIME, 13);
    }
    public static ParamDef getSumInfoDto() {
        return g_sumDtoDef;
    }

    public static class Key extends CommDtoKey{
        public static final int TOTAL_SIZE = 11;
        public static final int IS_SOURCE = 12;
        public static final int SEARCH_ARG = 13;
        public static final int SYS_TYPE = 14;
    }
}
