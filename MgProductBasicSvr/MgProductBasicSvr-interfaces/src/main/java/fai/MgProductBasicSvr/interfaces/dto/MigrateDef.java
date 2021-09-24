package fai.MgProductBasicSvr.interfaces.dto;

import fai.comm.util.ParamDef;

public class MigrateDef {
    public static class Info {
        public static final String ADD_PD = "addPdInfo";
        public static final String BIND_PD_REL = "bindPdRel";
        public static final String BIND_RL_GROUP = "bindRlGroup";
    }

    public static class Dto {
        private static ParamDef g_infoDtoDef = new ParamDef();
        static {
            g_infoDtoDef.add(Info.ADD_PD, 0, ProductRelDto.getRelAndPdDto());
            g_infoDtoDef.add(Info.BIND_PD_REL, 1, ProductRelDto.getRelAndPdDto());
            g_infoDtoDef.add(Info.BIND_RL_GROUP, 2);
        }

        public static ParamDef getInfoDto() {
            return g_infoDtoDef;
        }
    }
}
