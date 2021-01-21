package fai.MgProductBasicSvr.interfaces.cmd;

public class MgProductBasicCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	// BindPropCmd.GET_LIST
        public static final int NUM2 = 5001; // BindPropCmd.GET_PD_BIND_PROP
        public static final int NUM3 = 5002; // BasicCmd.GET_REL
        public static final int NUM4 = 5003; // BasicCmd.GET_REL_LIST
        public static final int NUM5 = 5004; // BasicCmd.GET_REDUCED_REL_LIST
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000; // BindPropCmd.BATCH_SET
        public static final int NUM2 = 1001; // BasicCmd.ADD_PD_AND_REL
        public static final int NUM3 = 1002; // BasicCmd.ADD_REL_BIND
        public static final int NUM4 = 1003; // BasicCmd.DEL_PDS
        public static final int NUM5 = 1004; // BasicCmd.DEL_REL_BIND
        public static final int NUM6 = 1005; // BasicCmd.BATCH_ADD_REL_BIND
    }

    /**
     * cmd对外定义，实际做cmd读写分离
     */
    public static class BindPropCmd {
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_LIST_BY_PROP = ReadCmdNum.NUM2;

        public static final int BATCH_SET = WriteCmdNum.NUM;
    }

    public static class BasicCmd {
        public static final int GET_REL = ReadCmdNum.NUM3;
        public static final int GET_REL_LIST = ReadCmdNum.NUM4;
        public static final int GET_REDUCED_REL_LIST = ReadCmdNum.NUM5;

        public static final int ADD_PD_AND_REL = WriteCmdNum.NUM2;
        public static final int ADD_REL_BIND = WriteCmdNum.NUM3;
        public static final int DEL_PDS = WriteCmdNum.NUM4;
        public static final int DEL_REL_BIND = WriteCmdNum.NUM5;
        public static final int BATCH_ADD_REL_BIND = WriteCmdNum.NUM6;
    }

}
