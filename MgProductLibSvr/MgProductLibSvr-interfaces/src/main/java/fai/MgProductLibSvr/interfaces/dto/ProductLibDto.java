package fai.MgProductLibSvr.interfaces.dto;

import fai.MgProductLibSvr.interfaces.entity.ProductLibEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:16
 */
public class ProductLibDto {
    private static ParamDef g_libDtoDef = new ParamDef();

    static {
        g_libDtoDef.add(ProductLibEntity.Info.AID, 0, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.LIB_ID, 1, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.LIB_NAME, 4, Var.Type.STRING);
        g_libDtoDef.add(ProductLibEntity.Info.LIB_TYPE, 5, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.FLAG, 6, Var.Type.INT);
        g_libDtoDef.add(ProductLibEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_libDtoDef.add(ProductLibEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_libDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
    }
}
