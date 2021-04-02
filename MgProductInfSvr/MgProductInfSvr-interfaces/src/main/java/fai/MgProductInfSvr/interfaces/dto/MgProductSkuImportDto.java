package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.MgProductSkuImport;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class MgProductSkuImportDto {
    private static ParamDef g_infoDef = new ParamDef();
    static {
        g_infoDef.add(MgProductSkuImport.Info.AID, 0, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.TID, 1, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.SITE_ID, 2, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.LG_ID, 3, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.KEEP_PRI_ID1, 4, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.RL_PD_ID, 5, Var.Type.INT);
        g_infoDef.add(MgProductSkuImport.Info.NAME, 6, Var.Type.STRING);
        g_infoDef.add(MgProductSkuImport.Info.PD_CODE_LIST, 7, Var.Type.FAI_LIST);
        g_infoDef.add(MgProductSkuImport.Info.SPEC_NAME_LIST, 8, Var.Type.FAI_LIST);
        g_infoDef.add(MgProductSkuImport.Info.SPEC_VAL_LIST, 9, Var.Type.FAI_LIST);
        g_infoDef.add(MgProductSkuImport.Info.SKU_CODE_LIST, 10, Var.Type.FAI_LIST);
        g_infoDef.add(MgProductSkuImport.Info.PRICE, 11, Var.Type.LONG);
        g_infoDef.add(MgProductSkuImport.Info.COUNT, 12, Var.Type.INT);
    }
}
