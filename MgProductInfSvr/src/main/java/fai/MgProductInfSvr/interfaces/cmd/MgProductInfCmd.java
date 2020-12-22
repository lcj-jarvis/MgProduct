package fai.MgProductInfSvr.interfaces.cmd;

public class MgProductInfCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	// PropCmd.GET_LIST
        public static final int NUM2 = 5001; // PropCmd.GET_VAL_LIST
        public static final int NUM3 = 5002; // BasicCmd.GET_PROP_LIST
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

        //写命令
        public static final int SET_PROP_LIST = WriteCmdNum.NUM7;
    }
}
