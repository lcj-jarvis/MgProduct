package fai.MgProductPropSvr.interfaces.dto;

import fai.MgProductPropSvr.interfaces.entity.ProductPropValEntity;
import fai.comm.util.ParamDef;

public class ProductPropValDto {
	private static ParamDef g_productPropValDtoDef = new ParamDef();

	private static ParamDef g_cacheDtoDef = new ParamDef();

	static {
		g_productPropValDtoDef.add(ProductPropValEntity.Info.AID, 0);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.PROP_VAL_ID, 1);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.PROP_ID, 2);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.VAL, 3);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.SORT, 4);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.DATA_TYPE, 5);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.CREATE_TIME, 6);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.UPDATE_TIME, 7);
		g_productPropValDtoDef.add(ProductPropValEntity.Info.RL_PROP_ID, 8);
	}

	static {
		g_cacheDtoDef.add(ProductPropValEntity.Info.AID, 0);
		g_cacheDtoDef.add(ProductPropValEntity.Info.PROP_VAL_ID, 1);
		g_cacheDtoDef.add(ProductPropValEntity.Info.PROP_ID, 2);
		g_cacheDtoDef.add(ProductPropValEntity.Info.VAL, 3);
		g_cacheDtoDef.add(ProductPropValEntity.Info.SORT, 4);
		g_cacheDtoDef.add(ProductPropValEntity.Info.DATA_TYPE, 5);
		g_cacheDtoDef.add(ProductPropValEntity.Info.CREATE_TIME, 6);
		g_cacheDtoDef.add(ProductPropValEntity.Info.UPDATE_TIME, 7);
	}

	public static ParamDef getInfoDto() {
		return g_productPropValDtoDef;
	}

	public static ParamDef getCacheInfoDto() {
		return g_cacheDtoDef;
	}

	public static class Key {
		public static final int INFO = 1;
		public static final int INFO_LIST = 2;
		public static final int RL_PROP_ID = 3;
		public static final int UNION_PRI_ID = 4;
		public static final int TID = 5;
		public static final int LIB_ID = 6;
		public static final int UPDATERLIST = 7;
		public static final int RL_PROP_IDS = 8;
		public static final int VAL_IDS = 9;
		public static final int DATA_STATUS = 10;
		public static final int TOTAL_SIZE = 11;
		public static final int SEARCH_ARG = 12;
	}
}
