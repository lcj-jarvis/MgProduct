package fai.mgproduct.comm;

import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class CloneDef {
    public static class Info {
        public static final String TO_UNIONPRIID = "toUid";
        public static final String FROM_UNIONPRIID = "fromUid";
    }

    public static class Dto {
        private static ParamDef g_infoDef = new ParamDef(); // 数据状态dto，包括数据量，管理态数据更新时间，访客态数据更新时间
        static {
            g_infoDef.add(CloneDef.Info.TO_UNIONPRIID, 0, Var.Type.INT);
            g_infoDef.add(Info.FROM_UNIONPRIID, 1, Var.Type.INT);
        }

        public static ParamDef getDto() {
            return g_infoDef;
        }
    }
}
