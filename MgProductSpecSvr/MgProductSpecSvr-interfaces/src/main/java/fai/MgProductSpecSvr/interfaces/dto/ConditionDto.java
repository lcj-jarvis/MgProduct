package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.Condition;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * 条件 DTO
 */
public class ConditionDto {
    private static ParamDef g_dtoDef = new ParamDef();
    static {
        g_dtoDef.add(Condition.Info.FUZZY, 0, Var.Type.BOOLEAN);
        g_dtoDef.add(Condition.Info.RETURN_FULL_INFO, 1, Var.Type.BOOLEAN);
    }
    public static ParamDef getInfoDto() {
        return g_dtoDef;
    }
}
