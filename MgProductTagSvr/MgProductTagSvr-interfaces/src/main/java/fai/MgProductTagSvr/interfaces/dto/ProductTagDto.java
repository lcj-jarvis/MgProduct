package fai.MgProductTagSvr.interfaces.dto;

import fai.MgProductTagSvr.interfaces.entity.ProductTagEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:47
 */
public class ProductTagDto {

    private static ParamDef g_tagDtoDef = new ParamDef();

    static {
        g_tagDtoDef.add(ProductTagEntity.Info.AID, 0, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.TAG_ID, 1, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.SOURCE_TID, 2, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.SOURCE_UNIONPRIID, 3, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.TAG_NAME, 4, Var.Type.STRING);
        g_tagDtoDef.add(ProductTagEntity.Info.TAG_TYPE, 5, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.FLAG, 6, Var.Type.INT);
        g_tagDtoDef.add(ProductTagEntity.Info.CREATE_TIME, 7, Var.Type.CALENDAR);
        g_tagDtoDef.add(ProductTagEntity.Info.UPDATE_TIME, 8, Var.Type.CALENDAR);
    }

    public static ParamDef getInfoDto() {
        return g_tagDtoDef;
    }

    public static class Key {
        public static final int INFO = 1;
        public static final int INFO_LIST = 2;


    }
}
