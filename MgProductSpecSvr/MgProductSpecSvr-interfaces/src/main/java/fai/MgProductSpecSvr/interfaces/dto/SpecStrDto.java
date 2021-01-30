package fai.MgProductSpecSvr.interfaces.dto;

import fai.MgProductSpecSvr.interfaces.entity.SpecStrEntity;
import fai.comm.util.ParamDef;
import fai.comm.util.Var;

public class SpecStrDto {
	private static ParamDef g_dtoDef = new ParamDef();

	static {
		g_dtoDef.add(SpecStrEntity.Info.AID, 0, Var.Type.INT);
		g_dtoDef.add(SpecStrEntity.Info.SC_STR_ID, 1, Var.Type.INT);
		g_dtoDef.add(SpecStrEntity.Info.NAME, 2, Var.Type.STRING);
		g_dtoDef.add(SpecStrEntity.Info.SYS_CREATE_TIME, 3);
	}

	public static ParamDef getDtoDef() {
		return g_dtoDef;
	}

	public static class Key {
		public static final int INFO = 1;
		public static final int INFO_LIST = 2;
		public static final int ID = 3;
		public static final int ID_LIST = 4;
	}
}
