package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class StoreSalesSkuDto {


    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(StoreSalesSkuEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SKU_ID, 3, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.RL_PD_ID, 4, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SKU_TYPE, 6, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SORT, 7, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.COUNT, 8, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, 9, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, 10, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.PRICE, 11, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.ORIGIN_PRICE, 12, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.MIN_AMOUNT, 13, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.MAX_AMOUNT, 14, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.DURATION, 15, Var.Type.DOUBLE);
        g_dtoDef.add(StoreSalesSkuEntity.Info.VIRTUAL_COUNT, 16, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.FLAG, 17, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, 18, Var.Type.CALENDAR);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, 19, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
        public static final int RL_ORDER_ID = 11;
        public static final int COUNT = 12;
        public static final int REDUCE_MODE = 13;
        public static final int EXPIRE_TIME_SECONDS = 14;
    }


}
