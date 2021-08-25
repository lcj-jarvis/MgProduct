package fai.MgProductGroupSvr.interfaces.dto;

import fai.MgProductGroupSvr.interfaces.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.interfaces.entity.ProductGroupRelEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class ProductGroupRelDto {
    private static ParamDef g_groupRelDtoDef = new ParamDef();

    static {
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.AID, 0, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.RL_GROUP_ID, 1, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.GROUP_ID, 2, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.SYS_TYPE, 3, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.UNION_PRI_ID, 4, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.SORT, 5, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.RL_FLAG, 6, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.STATUS, 7, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.PARENT_ID, 8, Var.Type.INT);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.CREATE_TIME, 9, Var.Type.CALENDAR);
        g_groupRelDtoDef.add(ProductGroupRelEntity.Info.UPDATE_TIME, 10, Var.Type.CALENDAR);
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
        g_groupAllDtoDef.add(ProductGroupEntity.Info.GROUP_NAME, 4, Var.Type.STRING);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.ICON_LIST, 5, Var.Type.STRING);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.FLAG, 6, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_groupAllDtoDef.add(ProductGroupEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);

        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.RL_GROUP_ID, 9, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.UNION_PRI_ID, 10, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.SORT, 11, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.RL_FLAG, 12, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.SYS_TYPE, 13, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.STATUS, 14, Var.Type.INT);
        g_groupAllDtoDef.add(ProductGroupRelEntity.Info.PARENT_ID, 15, Var.Type.INT);
    }
    public static ParamDef getAllInfoDto() {
        return g_groupAllDtoDef;
    }

    private static ParamDef g_groupTreeDtoDef = new ParamDef(); // 树形结构数据
    static {
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.AID, 0, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.GROUP_ID, 1, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.GROUP_NAME, 4, Var.Type.STRING);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.ICON_LIST, 5, Var.Type.STRING);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.FLAG, 6, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);

        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.RL_GROUP_ID, 9, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.UNION_PRI_ID, 10, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.SORT, 11, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.RL_FLAG, 12, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.SYS_TYPE, 13, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.STATUS, 14, Var.Type.INT);
        g_groupTreeDtoDef.add(ProductGroupRelEntity.Info.PARENT_ID, 15, Var.Type.INT);

        g_groupTreeDtoDef.add(ProductGroupEntity.Info.CHILDREN, 16, ProductGroupRelDto.getTreeInfoDto(), Var.Type.FAI_LIST);
        g_groupTreeDtoDef.add(ProductGroupEntity.Info.IS_ADD, 17, Var.Type.BOOLEAN);
    }
    public static ParamDef getTreeInfoDto() {
        return g_groupTreeDtoDef;
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
        public static final int FROM_AID = 12;
        public static final int CLONE_UNION_PRI_IDS = 13;
        public static final int FROM_UNION_PRI_ID = 14;
        public static final int BACKUP_INFO = 15;
        public static final int SOFT_DEL = 16;
        public static final int SYS_TYPE = 17;
        public static final int GROUP_LEVEL = 18;
    }
}
