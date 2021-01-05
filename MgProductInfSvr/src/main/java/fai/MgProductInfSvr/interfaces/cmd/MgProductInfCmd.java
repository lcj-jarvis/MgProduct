package fai.MgProductInfSvr.interfaces.cmd;

public class MgProductInfCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	// PropCmd.GET_LIST
        public static final int NUM2 = 5001; // PropCmd.GET_VAL_LIST
        public static final int NUM3 = 5002; // BasicCmd.GET_PROP_LIST

        /**@see SpecTempCmd#GET_INFO */
        public static final int NUM4 = 5004;
        /**@see SpecTempCmd#GET_LIST */
        public static final int NUM5 = 5005;
        /**@see SpecTempDetailCmd#GET_INFO */
        public static final int NUM6 = 5006;
        /**@see SpecTempDetailCmd#GET_LIST */
        public static final int NUM7 = 5007;
        /**@see ProductSpecCmd#GET_INFO */
        public static final int NUM8 = 5008;
        /**@see ProductSpecCmd#GET_LIST */
        public static final int NUM9 = 5009;
        /**@see ProductSpecCmd#GET_CHECKED_LIST */
        public static final int NUM10 = 5010;
        /**@see ProductSpecSkuCmd#GET_LIST */
        public static final int NUM11 = 5011;

        /**@see BasicCmd#GET_RLPDIDS_BY_PROP */
        public static final int NUM12 = 5012;
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000; // PropCmd.BATCH_ADD
        public static final int NUM2 = 1001; // PropCmd.BATCH_DEL
        public static final int NUM3 = 1002; // PropCmd.BATCH_SET
        public static final int NUM4 = 1003; // PropCmd.ADD_WITH_VAL
        public static final int NUM5 = 1004; // PropCmd.SET_WITH_VAL
        public static final int NUM6 = 1005; // BasicCmd.BATCH_SET_VAL
        public static final int NUM7 = 1006; // BasicCmd.SET_PROP_LIST

        /**@see SpecTempCmd#ADD_LIST */
        public static final int NUM8 = 1008;
        /**@see SpecTempCmd#SET_LIST */
        public static final int NUM9 = 1009;
        /**@see SpecTempCmd#DEL_LIST */
        public static final int NUM10 = 1010;
        /**@see SpecTempDetailCmd#ADD_LIST */
        public static final int NUM11 = 1011;
        /**@see SpecTempDetailCmd#SET_LIST */
        public static final int NUM12 = 1012;
        /**@see SpecTempDetailCmd#DEL_LIST */
        public static final int NUM13 = 1013;
        /**@see ProductSpecCmd#IMPORT */
        public static final int NUM14 = 1014;
        /**@see ProductSpecCmd#UNION_SET */
        public static final int NUM15 = 1015;
        /**@see ProductSpecSkuCmd#SET_LIST */
        public static final int NUM16 = 1016;

        /**@see BasicCmd#ADD_PD_AND_REL */
        public static final int NUM17 = 1017;
        /**@see BasicCmd#ADD_PD_BIND */
        public static final int NUM18 = 1018;
        /**@see BasicCmd#BATCH_ADD_PD_BIND */
        public static final int NUM19 = 1019;
        /**@see BasicCmd#BATCH_DEL_PD_BIND */
        public static final int NUM20 = 1020;
        /**@see BasicCmd#BATCH_DEL_PDS */
        public static final int NUM21 = 1021;
    }

    /**
     * cmd对外定义，实际做cmd读写分离
     */
    public static class PropCmd {
        //读命令
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_VAL_LIST = ReadCmdNum.NUM2;

        //写命令
        public static final int BATCH_ADD = WriteCmdNum.NUM;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int BATCH_SET = WriteCmdNum.NUM3;
        public static final int ADD_WITH_VAL = WriteCmdNum.NUM4;
        public static final int SET_WITH_VAL = WriteCmdNum.NUM5;
        public static final int BATCH_SET_VAL = WriteCmdNum.NUM6;
    }

    public static class BasicCmd {
        //读命令
        public static final int GET_PROP_LIST = ReadCmdNum.NUM3;
        public static final int GET_RLPDIDS_BY_PROP = ReadCmdNum.NUM12;

        //写命令
        public static final int SET_PROP_LIST = WriteCmdNum.NUM7;
        public static final int ADD_PD_AND_REL = WriteCmdNum.NUM17;
        public static final int ADD_PD_BIND = WriteCmdNum.NUM18;
        public static final int BATCH_ADD_PD_BIND = WriteCmdNum.NUM19;
        public static final int BATCH_DEL_PD_BIND = WriteCmdNum.NUM20;
        public static final int BATCH_DEL_PDS = WriteCmdNum.NUM21;
    }



    /**
     * 规格模板相关cmd
     */
    public static class SpecTempCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM4;
        public static final int GET_LIST = ReadCmdNum.NUM5;

        //写命令
        public static final int ADD_LIST = WriteCmdNum.NUM8;
        public static final int SET_LIST = WriteCmdNum.NUM9;
        public static final int DEL_LIST = WriteCmdNum.NUM10;
    }

    /**
     * 规格模板详情相关cmd
     */
    public static class SpecTempDetailCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM6;
        public static final int GET_LIST = ReadCmdNum.NUM7;

        //写命令
        public static final int ADD_LIST = WriteCmdNum.NUM11;
        public static final int SET_LIST = WriteCmdNum.NUM12;
        public static final int DEL_LIST = WriteCmdNum.NUM13;
    }

    /**
     * 商品规格相关cmd
     */
    public static class ProductSpecCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM8;
        public static final int GET_LIST = ReadCmdNum.NUM9;
        public static final int GET_CHECKED_LIST = ReadCmdNum.NUM10;

        //写命令
        public static final int IMPORT = WriteCmdNum.NUM14;
        public static final int UNION_SET = WriteCmdNum.NUM15;
    }

    /**
     * 商品规格sku 相关 cmd
     */
    public static class ProductSpecSkuCmd {
        //读命令
        public static final int GET_LIST = ReadCmdNum.NUM11;

        //写命令
        public static final int SET_LIST = WriteCmdNum.NUM16;
    }
}
