package fai.MgProductPropSvr.interfaces.dto;

import fai.MgProductPropSvr.interfaces.entity.ProductPropRelEntity;
import fai.comm.util.ParamDef;

public class ProductPropRelDto {
	private static ParamDef g_cacheDtoDef = new ParamDef();

	static {
		g_cacheDtoDef.add(ProductPropRelEntity.Info.AID, 0);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.RL_PROP_ID, 1);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.PROP_ID, 2);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.UNION_PRI_ID, 3);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.RL_LIB_ID, 4);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.SORT, 5);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.RL_FLAG, 6);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.CREATE_TIME, 7);
		g_cacheDtoDef.add(ProductPropRelEntity.Info.UPDATE_TIME, 8);
	}

	public static ParamDef getCacheInfoDto() {
		return g_cacheDtoDef;
	}

	public static class Key {
		public static final int INFO = 1;
	}
}
