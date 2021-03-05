package fai.MgProductGroupSvr.interfaces.dto;

import fai.MgProductGroupSvr.interfaces.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.interfaces.entity.ProductGroupRelEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;
import fai.mgproduct.comm.DataStatus;

public class ProductGroupRelDto {
    private static ParamDef g_groupRelDtoDef = new ParamDef();

    static {
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.AID, 0, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.RL_GROUP_ID, 1, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.GROUP_ID, 2, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.UNION_PRI_ID, 3, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.SORT, 4, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.RL_FLAG, 5, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.CREATE_TIME, 6, Var.Type.CALENDAR);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.UPDATE_TIME, 7, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_groupRelDtoDef;
    }

    private static ParamDef g_groupAllDtoDef = new ParamDef(); // 完整数据，包括分类表和分类业务表
    static {
        g_groupAllDtoDef.add(ProductGroupEntity.Info.AID, 0, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.GROUP_ID, 1, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.PARENT_ID, 4, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.GROUP_NAME, 5, Var.Type.STRING);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.ICON_LIST, 6, Var.Type.STRING);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.FLAG, 7, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.CREATE_TIME, 8, Var.Type.CALENDAR);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.UPDATE_TIME, 9, Var.Type.CALENDAR);

        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.RL_GROUP_ID, 10, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.UNION_PRI_ID, 11, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.SORT, 12, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.RL_FLAG, 13, Var.Type.INT);
    }
    public static ParamDef getAllInfoDto() {
        return g_groupAllDtoDef;
    }

    private static ParamDef g_dataStatusDef = new ParamDef(); // 数据状态dto，包括数据量，管理态数据更新时间，访客态数据更新时间
    static {
        g_dataStatusDef.add(DataStatus.Info.TOTAL_SIZE, 0, Var.Type.INT);
        g_dataStatusDef.add(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 1, Var.Type.LONG);
        g_dataStatusDef.add(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 2, Var.Type.LONG);
    }
    public static ParamDef getDataStatusDto() {
        return g_dataStatusDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;
        public static final int GROUP_ID = 3;
        public static final int RL_GROUP_ID = 4;
        public static final int TOTAL_SIZE = 5;
        public static final int UNION_PRI_ID = 6;
        public static final int TID = 7;
        public static final int SEARCH_ARG = 8;
        public static final int UPDATERLIST = 9;
        public static final int RL_GROUP_IDS = 10;
        public static final int DATA_STATUS = 11;
    }
}
