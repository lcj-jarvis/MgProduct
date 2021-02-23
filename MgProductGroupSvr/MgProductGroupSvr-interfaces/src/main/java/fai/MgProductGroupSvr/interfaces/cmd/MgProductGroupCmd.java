package fai.MgProductGroupSvr.interfaces.cmd;

public class MgProductGroupCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	/** @see GroupCmd#GET_LIST */
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000; /** @see GroupCmd#ADD */
        public static final int NUM1 = 1001; /** @see GroupCmd#BATCH_SET */
        public static final int NUM2 = 1002; /** @see GroupCmd#BATCH_DEL */
    }

    public static class GroupCmd {
        public static final int ADD = WriteCmdNum.NUM;
        public static final int BATCH_SET = WriteCmdNum.NUM1;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int GET_LIST = ReadCmdNum.NUM;
    }
}
