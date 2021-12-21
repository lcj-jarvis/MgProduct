package fai.MgProductPropSvr.interfaces.dto;

import fai.MgProductPropSvr.interfaces.entity.ProductPropEntity;
import fai.MgProductPropSvr.interfaces.entity.ProductPropRelEntity;
import fai.comm.util.ParamDef;

public class ProductPropDto {
	private static ParamDef g_productPropDtoDef = new ParamDef();
	private static ParamDef g_cacheDtoDef = new ParamDef();

	static {
		g_productPropDtoDef.add(ProductPropEntity.Info.AID, 0);
		g_productPropDtoDef.add(ProductPropEntity.Info.PROP_ID, 1);
		g_productPropDtoDef.add(ProductPropEntity.Info.SOURCE_TID, 2);
		g_productPropDtoDef.add(ProductPropEntity.Info.NAME, 3);
		g_productPropDtoDef.add(ProductPropEntity.Info.TYPE, 4);
		g_productPropDtoDef.add(ProductPropEntity.Info.FLAG, 5);
		g_productPropDtoDef.add(ProductPropEntity.Info.CREATE_TIME, 6);
		g_productPropDtoDef.add(ProductPropEntity.Info.UPDATE_TIME, 7);

		g_productPropDtoDef.add(ProductPropRelEntity.Info.RL_PROP_ID, 8);
		g_productPropDtoDef.add(ProductPropRelEntity.Info.UNION_PRI_ID, 9);
		g_productPropDtoDef.add(ProductPropRelEntity.Info.RL_LIB_ID, 10);
		g_productPropDtoDef.add(ProductPropRelEntity.Info.SORT, 11);
		g_productPropDtoDef.add(ProductPropRelEntity.Info.RL_FLAG, 12);

		g_productPropDtoDef.add(ProductPropEntity.Info.SOURCE_UNIONPRIID, 13);
	}

	static {
		g_cacheDtoDef.add(ProductPropEntity.Info.AID, 0);
		g_cacheDtoDef.add(ProductPropEntity.Info.PROP_ID, 1);
		g_cacheDtoDef.add(ProductPropEntity.Info.SOURCE_TID, 2);
		g_cacheDtoDef.add(ProductPropEntity.Info.NAME, 3);
		g_cacheDtoDef.add(ProductPropEntity.Info.TYPE, 4);
		g_cacheDtoDef.add(ProductPropEntity.Info.FLAG, 5);
		g_cacheDtoDef.add(ProductPropEntity.Info.CREATE_TIME, 6);
		g_cacheDtoDef.add(ProductPropEntity.Info.UPDATE_TIME, 7);
		g_cacheDtoDef.add(ProductPropEntity.Info.SOURCE_UNIONPRIID, 8);
	}

	public static ParamDef getInfoDto() {
		return g_productPropDtoDef;
	}

	public static ParamDef getCacheInfoDto() {
		return g_cacheDtoDef;
	}

	public static class Key {
		public static final int INFO = 1;
		public static final int INFO_LIST = 2;
		public static final int TOTAL_SIZE = 3;
		public static final int UNION_PRI_ID = 4;
		public static final int TID = 5;
		public static final int LIB_ID = 6;
		public static final int SEARCH_ARG = 7;
		public static final int UPDATERLIST = 8;
		public static final int RL_PROP_ID = 9;
		public static final int RL_PROP_IDS = 10;
		public static final int UNION_PRI_IDS = 11;
		public static final int BACKUP_INFO = 12;
		public static final int RESTORE_ID = 13;
		public static final int FROM_AID = 14;
		public static final int FROM_UNION_PRI_ID = 15;
		public static final int CLONE_UNION_PRI_IDS = 16;
	}
}
