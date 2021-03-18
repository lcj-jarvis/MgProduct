package fai.mgproduct.comm;

import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class DataStatus {
    public static class Info {
        public static final String TOTAL_SIZE = "ts";
        public static final String MANAGE_LAST_UPDATE_TIME = "mlut";
        public static final String VISITOR_LAST_UPDATE_TIME = "vlut";
    }

    public static class Dto {
        private static ParamDef g_dataStatusDef = new ParamDef(); // 数据状态dto，包括数据量，管理态数据更新时间，访客态数据更新时间
        static {
            g_dataStatusDef.add(DataStatus.Info.TOTAL_SIZE, 0, Var.Type.INT);
            g_dataStatusDef.add(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 1, Var.Type.LONG);
            g_dataStatusDef.add(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 2, Var.Type.LONG);
        }

        public static ParamDef getDataStatusDto() {
            return g_dataStatusDef;
        }
    }
}
