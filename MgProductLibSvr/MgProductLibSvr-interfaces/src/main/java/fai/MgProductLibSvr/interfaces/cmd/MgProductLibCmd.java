package fai.MgProductLibSvr.interfaces.cmd;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:15
 */
public class MgProductLibCmd {

    /**
     * 读命令 5000以上
     */
    public static class ReadCmdNum{
        public static final int NUM = 5000;/** @see LibCmd#GET_LIST */
        public static final int NUM1 = 5001;/** @see LibCmd#GET_REL_DATA_STATUS */
        public static final int NUM2 = 5002;/** @see LibCmd#SEARCH_REL */
        public static final int NUM3 = 5003;/** @see LibCmd#GET_ALL_REL */
    }

    /**
     * 写命令 1000 到 5000 的范围
      */
    public static class WriteCmdNum{
        public static final int NUM = 1000;/** @see LibCmd#ADD */
        public static final int NUM1 = 1001;/** @see LibCmd#BATCH_SET */
        public static final int NUM2 = 1002;/** @see LibCmd#BATCH_DEL */
        public static final int NUM4 = 1003;/** @see LibCmd#UNION_SET_LIB_LIST*/
        public static final int NUM5 = 1004;/** @see LibCmd#CLONE*/
        public static final int NUM6 = 1005;/** @see LibCmd#INCR_CLONE*/
    }

    public static class LibCmd {
        //写操作
        public static final int ADD = WriteCmdNum.NUM;
        public static final int BATCH_SET = WriteCmdNum.NUM1;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int UNION_SET_LIB_LIST = WriteCmdNum.NUM4;
        public static final int CLONE = WriteCmdNum.NUM5;
        public static final int INCR_CLONE = WriteCmdNum.NUM6;

        //读操作
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_REL_DATA_STATUS = ReadCmdNum.NUM1;
        public static final int SEARCH_REL = ReadCmdNum.NUM2;
        public static final int GET_ALL_REL = ReadCmdNum.NUM3;
    }

}
