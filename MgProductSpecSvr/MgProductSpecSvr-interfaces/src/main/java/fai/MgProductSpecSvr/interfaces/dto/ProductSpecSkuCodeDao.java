package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuCodeEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductSpecSkuCodeDao {
    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.AID, 0, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, 1, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.SKU_CODE, 2, Var.Type.STRING);
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.SKU_ID, 3, Var.Type.LONG);
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.SORT, 4, Var.Type.INT);
        g_dtoDef.add(ProductSpecSkuCodeEntity.Info.PD_ID, 5, Var.Type.INT);
    }
    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }

    public static class Key{
        public static final int UNION_PRI_ID = 1;
        public static final int INFO = 5;
        public static final int INFO_LIST = 6;
        public static final int DATA_STATUS = 7;
        public static final int SEARCH_ARG = 13;
        public static final int TOTAL_SIZE = 15;
    }
}
