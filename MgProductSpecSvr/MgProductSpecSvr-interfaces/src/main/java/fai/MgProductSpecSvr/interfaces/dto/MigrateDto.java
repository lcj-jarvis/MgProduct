package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/11/16 10:36
 */
public class MigrateDto {

    public static ParamDef getReturnListDtoDef() {
        return g_returnListDef;
    }

    private static ParamDef g_returnListDef = new ParamDef();
    static {
        g_returnListDef.add(ProductSpecSkuEntity.Info.AID, 0, Var.Type.INT);
        g_returnListDef.add(ProductSpecSkuEntity.Info.PD_ID, 1, Var.Type.INT);
        g_returnListDef.add(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, 3, Var.Type.INT);
        g_returnListDef.add(ProductSpecSkuEntity.Info.SKU_ID, 4, Var.Type.LONG);
    }
}
