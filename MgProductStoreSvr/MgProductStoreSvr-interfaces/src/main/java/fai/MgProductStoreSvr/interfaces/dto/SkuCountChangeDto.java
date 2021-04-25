package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.SkuCountChangeEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SkuCountChangeDto {

    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(SkuCountChangeEntity.Info.SKU_ID, 3, Var.Type.LONG);
        g_dtoDef.add(SkuCountChangeEntity.Info.ITEM_ID, 7, Var.Type.INT);
        g_dtoDef.add(SkuCountChangeEntity.Info.COUNT, 8, Var.Type.INT);
    }

    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }
}
