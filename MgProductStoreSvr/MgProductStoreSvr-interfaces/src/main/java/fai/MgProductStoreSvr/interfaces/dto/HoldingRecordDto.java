package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.HoldingRecordEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class HoldingRecordDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(HoldingRecordEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(HoldingRecordEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(HoldingRecordEntity.Info.SKU_ID, 2, Var.Type.LONG);
        g_dtoDef.add(HoldingRecordEntity.Info.RL_ORDER_CODE, 3, Var.Type.STRING);
        g_dtoDef.add(HoldingRecordEntity.Info.ITEM_ID, 4, Var.Type.INT);
        g_dtoDef.add(HoldingRecordEntity.Info.COUNT, 5, Var.Type.INT);
        g_dtoDef.add(HoldingRecordEntity.Info.EXPIRE_TIME, 6, Var.Type.CALENDAR);
        g_dtoDef.add(HoldingRecordEntity.Info.SYS_CREATE_TIME, 7, Var.Type.CALENDAR);

    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }
    public static class Key{
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int INFO_LIST = 6;
        public static final int ID_LIST = 8;
    }
}
