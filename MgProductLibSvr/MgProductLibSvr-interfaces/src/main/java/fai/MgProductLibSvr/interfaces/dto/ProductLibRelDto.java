package fai.MgProductLibSvr.interfaces.dto;

import fai.MgProductLibSvr.interfaces.entity.ProductLibEntity;
import fai.MgProductLibSvr.interfaces.entity.ProductLibRelEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:16
 */
public class ProductLibRelDto {

    //库业务表
    private static ParamDef g_libRelDtoDef = new ParamDef();

    static {
        g_libRelDtoDef.add(ProductLibRelEntity.Info.AID, 0, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.RL_LIB_ID, 1, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.LIB_ID, 2, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.LIB_TYPE, 4, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.SORT, 5, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.RL_FLAG, 6, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_libRelDtoDef.add(ProductLibRelEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_libRelDtoDef;
    }

    // 完整数据，包括库表和库业务表
    private static ParamDef g_libAllDtoDef = new ParamDef();
    static {
        g_libAllDtoDef.add(ProductLibEntity.Info.AID, 0, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.LIB_ID, 1, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.LIB_NAME, 4, Var.Type.STRING);
        g_libAllDtoDef.add(ProductLibEntity.Info.LIB_TYPE, 5, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.FLAG, 6, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_libAllDtoDef.add(ProductLibEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);

        g_libAllDtoDef.add(ProductLibRelEntity.Info.RL_LIB_ID, 10, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibRelEntity.Info.UNION_PRI_ID, 11, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibRelEntity.Info.SORT, 12, Var.Type.INT);
        g_libAllDtoDef.add(ProductLibRelEntity.Info.RL_FLAG, 13, Var.Type.INT);
    }
    public static ParamDef getAllInfoDto() {
        return g_libAllDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int LIB_ID = 3;
        public static final int RL_LIB_ID = 4;
        public static final int TOTAL_SIZE = 5;
        public static final int UNION_PRI_ID = 6;
        public static final int TID = 7;
        public static final int SEARCH_ARG = 8;
        public static final int UPDATERLIST = 9;
        public static final int RL_LIB_IDS = 10;
        public static final int DATA_STATUS = 11;
    }

}
