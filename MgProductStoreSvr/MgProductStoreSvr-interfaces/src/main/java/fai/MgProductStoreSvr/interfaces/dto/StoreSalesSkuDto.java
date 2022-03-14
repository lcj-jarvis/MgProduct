package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class StoreSalesSkuDto {
    private static ParamDef g_holderOrderDtoDef = new ParamDef();
    static {
        g_holderOrderDtoDef.add("orderId", 0);
        g_holderOrderDtoDef.add("count", 1);
    }
    private static ParamDef getHoldingOrderDef(){
        return g_holderOrderDtoDef;
    }

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
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, 18);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, 19);
        g_dtoDef.add(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 20, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 21, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, 22, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.HOLDING_ORDER_LIST, 23, getHoldingOrderDef());
        g_dtoDef.add(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, 24, Var.Type.FAI_LIST);
        g_dtoDef.add(StoreSalesSkuEntity.Info.COST_PRICE, 25, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.MW_COST, 26, Var.Type.LONG);
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_TYPE, 27, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.STATUS, 28, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.WEIGHT, 29, Var.Type.DOUBLE);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    private static ParamDef g_copyDtoDef = new ParamDef();
    static {
        g_copyDtoDef.add(StoreSalesSkuEntity.Info.UNION_PRI_ID, 0, Var.Type.INT);
        g_copyDtoDef.add(StoreSalesSkuEntity.Info.PD_ID, 1, Var.Type.INT);
    }

    public static ParamDef getCopyDto() {
        return g_copyDtoDef;
    }

    public static class Key{
        public static final int INFO = 0;
        public static final int UNION_PRI_ID = 1;
        public static final int TID = 2;
        public static final int PD_ID = 3;
        public static final int RL_PD_ID = 4;
        public static final int SKU_ID = 5;
        public static final int INFO_LIST = 6;
        public static final int UPDATER_LIST = 7;
        public static final int ID_LIST = 8;
        public static final int SOFT_DEL = 9;
        public static final int RL_ORDER_CODE = 11;
        public static final int UNION_PRI_IDS = 12;
        @Deprecated
        public static final int REPORT_COUNT = 13; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        @Deprecated
        public static final int REPORT_PRICE = 14; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        @Deprecated
        public static final int FROM_UNION_PRI_ID = 15; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        @Deprecated
        public static final int REDUCE_MODE = 13; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        @Deprecated
        public static final int EXPIRE_TIME_SECONDS = 14; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        @Deprecated
        public static final int IN_OUT_STORE_RECORD_INFO = 15; // TODO 重复了，暂时先标记废弃，用来提示开发不要同时使用
        public static final int SKU_ID_COUNT_LIST = 16;
        public static final int UID_LIST = 17;
        public static final int STR_LIST = 18;
        public static final int IN_OUT_STORE_REC_ID = 20;
        public static final int RL_REFUND_ID = 21;
        public static final int XID = 22;
        public static final int SYS_TYPE = 23;
        public static final int PD_IDS = 24;
    }

    public static class CacheDto{
        // 管理态
        private static ParamDef g_manageInfoDef = new ParamDef();
        static {
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.AID, 0, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.PD_ID, 2, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SKU_ID, 3, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.RL_PD_ID, 4, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SKU_TYPE, 6, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SORT, 7, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.PRICE, 11, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.ORIGIN_PRICE, 12, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.MIN_AMOUNT, 13, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.MAX_AMOUNT, 14, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.DURATION, 15, Var.Type.DOUBLE);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.FLAG, 17, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, 18);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, 19);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 20, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 21, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, 22, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.MW_COST, 23, Var.Type.LONG);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.SYS_TYPE, 24, Var.Type.INT);
            g_manageInfoDef.add(StoreSalesSkuEntity.Info.STATUS, 25, Var.Type.INT);
        }

        public static ParamDef getManageInfoDto() {
            return g_manageInfoDef;
        }


        //访客态
        private static ParamDef g_VisitorInfoDef = new ParamDef();

        static {
            g_VisitorInfoDef.add(StoreSalesSkuEntity.Info.COUNT, 8, Var.Type.INT);
            g_VisitorInfoDef.add(StoreSalesSkuEntity.Info.REMAIN_COUNT, 9, Var.Type.INT);
            g_VisitorInfoDef.add(StoreSalesSkuEntity.Info.HOLDING_COUNT, 10, Var.Type.INT);
            g_VisitorInfoDef.add(StoreSalesSkuEntity.Info.VIRTUAL_COUNT, 16, Var.Type.INT);
        }

        public static ParamDef getVisitorInfoDto() {
            return g_manageInfoDef;
        }
    }

}
