package fai.MgProductGroupSvr.interfaces.cmd;

public class MgProductGroupCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	/** @see GroupCmd#GET_LIST */
        public static final int NUM1 = 5001;	/** @see GroupCmd#GET_REL_DATA_STATUS */
        public static final int NUM2 = 5002;	/** @see GroupCmd#SEARCH_REL */
        public static final int NUM3 = 5003;	/** @see GroupCmd#GET_ALL_REL */
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000; /** @see GroupCmd#ADD */
        public static final int NUM1 = 1001; /** @see GroupCmd#BATCH_SET */
        public static final int NUM2 = 1002; /** @see GroupCmd#BATCH_DEL */
        public static final int NUM3 = 1003; /** @see GroupCmd#UNION_SET_GROUP_LIST */
        public static final int NUM4 = 1004; /** @see GroupCmd#CLONE */
        public static final int NUM5 = 1005; /** @see GroupCmd#INCR_CLONE */
        public static final int NUM6 = 1006; /** @see GroupCmd#BACKUP */
        public static final int NUM7 = 1007; /** @see GroupCmd#RESTORE */
        public static final int NUM8 = 1008; /** @see GroupCmd#DEL_BACKUP */
        public static final int NUM9 = 1009; /** @see GroupCmd#SET_ALL_GROUP_LIST */
        public static final int NUM10 = 1010; /** @see GroupCmd#CLEAR_ACCT */
    }

    public static class GroupCmd {
        public static final int ADD = WriteCmdNum.NUM;
        public static final int BATCH_SET = WriteCmdNum.NUM1;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int UNION_SET_GROUP_LIST = WriteCmdNum.NUM3;
        public static final int CLONE = WriteCmdNum.NUM4;
        public static final int INCR_CLONE = WriteCmdNum.NUM5;
        public static final int BACKUP = WriteCmdNum.NUM6;
        public static final int RESTORE = WriteCmdNum.NUM7;
        public static final int DEL_BACKUP = WriteCmdNum.NUM8;
        public static final int SET_ALL_GROUP_LIST = WriteCmdNum.NUM9;

        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_REL_DATA_STATUS = ReadCmdNum.NUM1;
        public static final int SEARCH_REL = ReadCmdNum.NUM2;
        public static final int GET_ALL_REL = ReadCmdNum.NUM3;
        public static final int CLEAR_ACCT = WriteCmdNum.NUM10;
    }
}
