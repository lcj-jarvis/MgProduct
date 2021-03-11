package fai.MgProductBasicSvr.interfaces.cmd;

public class MgProductBasicCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	 /** @see BindPropCmd#GET_LIST */
        public static final int NUM1 = 5001; /** @see BindPropCmd#GET_LIST_BY_PROP */
        public static final int NUM2 = 5002; /** @see BasicCmd#GET_REL */
        public static final int NUM3 = 5003; /** @see BasicCmd#GET_REL_LIST */
        public static final int NUM4 = 5004; /** @see BasicCmd#GET_REDUCED_REL_LIST */
        public static final int NUM5 = 5005; /** @see BindGroupCmd#GET_LIST */
        public static final int NUM6 = 5006; /** @see BindGroupCmd#GET_PD_BY_GROUP */
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000;  /** @see BindPropCmd#BATCH_SET */
        public static final int NUM1 = 1001; /** @see BasicCmd#ADD_PD_AND_REL */
        public static final int NUM2 = 1002; /** @see BasicCmd#ADD_REL_BIND */
        public static final int NUM3 = 1003; /** @see BasicCmd#DEL_PDS */
        public static final int NUM4 = 1004; /** @see BasicCmd#DEL_REL_BIND */
        public static final int NUM5 = 1005; /** @see BasicCmd#BATCH_ADD_REL_BIND */
        public static final int NUM6 = 1006; /** @see BasicCmd#BATCH_ADD_PD_AND_REL */
        public static final int NUM7 = 1007; /** @see BasicCmd#BATCH_ADD_PDS_REL_BIND */
        public static final int NUM8 = 1008; /** @see BindGroupCmd#BATCH_SET */
    }

    /**
     * cmd对外定义，实际做cmd读写分离
     */
    public static class BindPropCmd {
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_LIST_BY_PROP = ReadCmdNum.NUM1;

        public static final int BATCH_SET = WriteCmdNum.NUM;
    }

    public static class BindGroupCmd {
        public static final int GET_LIST = ReadCmdNum.NUM5;
        public static final int GET_PD_BY_GROUP = ReadCmdNum.NUM6;

        public static final int BATCH_SET = WriteCmdNum.NUM8;
    }

    public static class BasicCmd {
        public static final int GET_REL = ReadCmdNum.NUM2;
        public static final int GET_REL_LIST = ReadCmdNum.NUM3;
        public static final int GET_REDUCED_REL_LIST = ReadCmdNum.NUM4;

        public static final int ADD_PD_AND_REL = WriteCmdNum.NUM1;
        public static final int ADD_REL_BIND = WriteCmdNum.NUM2;
        public static final int DEL_PDS = WriteCmdNum.NUM3;
        public static final int DEL_REL_BIND = WriteCmdNum.NUM4;
        public static final int BATCH_ADD_REL_BIND = WriteCmdNum.NUM5;
        public static final int BATCH_ADD_PD_AND_REL = WriteCmdNum.NUM6;
        public static final int BATCH_ADD_PDS_REL_BIND = WriteCmdNum.NUM7;
    }

}
