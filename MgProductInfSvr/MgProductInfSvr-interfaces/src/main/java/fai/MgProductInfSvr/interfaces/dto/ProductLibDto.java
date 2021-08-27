package fai.MgProductInfSvr.interfaces.dto;

import fai.MgProductInfSvr.interfaces.entity.ProductLibEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author LuChaoJi
 * @date 2021-07-01 10:54
 */
public class ProductLibDto {

    // 完整数据，包括库表和库业务表
    private static ParamDef g_PdLibDtoDef = new ParamDef();
    static {
        g_PdLibDtoDef.add(ProductLibEntity.Info.AID, 0, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.LIB_ID, 1, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.LIB_NAME, 4, Var.Type.STRING);
        g_PdLibDtoDef.add(ProductLibEntity.Info.LIB_TYPE, 5, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.FLAG, 6, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_PdLibDtoDef.add(ProductLibEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);

        g_PdLibDtoDef.add(ProductLibEntity.Info.RL_LIB_ID, 10, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.UNION_PRI_ID, 11, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.SORT, 12, Var.Type.INT);
        g_PdLibDtoDef.add(ProductLibEntity.Info.RL_FLAG, 13, Var.Type.INT);
    }
    public static ParamDef getPdLibDto() {
        return g_PdLibDtoDef;
    }

    //库业务表的数据
    private static ParamDef g_libRelDtoDef = new ParamDef();
    static {
        g_libRelDtoDef.add(ProductLibEntity.Info.AID, 0, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.RL_LIB_ID, 1, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.LIB_ID, 2, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.LIB_TYPE, 4, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.SORT, 5, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.RL_FLAG, 6, Var.Type.INT);
        g_libRelDtoDef.add(ProductLibEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_libRelDtoDef.add(ProductLibEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);
    }
    public static ParamDef getPdRelLibDto() {
        return g_libRelDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int LIB_ID = 3;
        public static final int RL_LIB_ID = 4;
        public static final int TOTAL_SIZE = 5;
        public static final int UNION_PRI_ID = 6;
        public static final int TID = 7;
        public static final int SITE_ID = 8;
        public static final int LGID = 9;
        public static final int KEEP_PRIID1 = 10;
        public static final int SEARCH_ARG = 11;
        public static final int UPDATERLIST = 12;
        public static final int RL_LIB_IDS = 13;
        public static final int DATA_STATUS = 14;
    }
}
