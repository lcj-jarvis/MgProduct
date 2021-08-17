package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.SkuSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SkuSummaryDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SkuSummaryEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(SkuSummaryEntity.Info.SKU_ID, 1, Var.Type.LONG);
        g_dtoDef.add(SkuSummaryEntity.Info.PD_ID, 2, Var.Type.INT);
        g_dtoDef.add(SkuSummaryEntity.Info.COUNT, 4, Var.Type.INT);
        g_dtoDef.add(SkuSummaryEntity.Info.REMAIN_COUNT, 5, Var.Type.INT);
        g_dtoDef.add(SkuSummaryEntity.Info.HOLDING_COUNT, 6, Var.Type.INT);
        g_dtoDef.add(SkuSummaryEntity.Info.FIFO_TOTAL_COST, 7, Var.Type.LONG);
        g_dtoDef.add(SkuSummaryEntity.Info.MW_TOTAL_COST, 8, Var.Type.LONG);
        g_dtoDef.add(SkuSummaryEntity.Info.SYS_CREATE_TIME, 9);
        g_dtoDef.add(SkuSummaryEntity.Info.SYS_UPDATE_TIME, 10);
        g_dtoDef.add(SkuSummaryEntity.Info.MIN_PRICE, 11, Var.Type.LONG);
        g_dtoDef.add(SkuSummaryEntity.Info.MAX_PRICE, 12, Var.Type.LONG);
        g_dtoDef.add(SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, 13, Var.Type.INT);
        g_dtoDef.add(StoreSalesSkuEntity.Info.MW_COST, 14, Var.Type.LONG); // 门店维度获取数据需要返回成本单价
        g_dtoDef.add(StoreSalesSkuEntity.Info.RL_PD_ID, 15, Var.Type.INT); // 门店维度获取数据需要返回商品业务id
        g_dtoDef.add(StoreSalesSkuEntity.Info.SYS_TYPE, 16, Var.Type.INT); // 门店维度获取数据需要返回商品系统类型
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key extends CommDtoKey{
        public static final int SEARCH_ARG = 11;
        public static final int TOTAL_SIZE = 12;
    }
}
