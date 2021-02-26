package fai.MgProductStoreSvr.interfaces.dto;

import fai.MgProductStoreSvr.interfaces.entity.DataStatus;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class DataStatusDto {
    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(DataStatus.Info.TOTAL_SIZE, 0, Var.Type.INT);
        g_dtoDef.add(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 1);
        g_dtoDef.add(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 2);

    }
    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }
}
