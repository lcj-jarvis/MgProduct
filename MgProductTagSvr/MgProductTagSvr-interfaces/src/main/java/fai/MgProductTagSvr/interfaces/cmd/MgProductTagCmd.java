package fai.MgProductTagSvr.interfaces.cmd;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:46
 */
public class MgProductTagCmd {

    /**
     * 读命令 5000以上
     */
    public static class ReadCmdNum{
        public static final int NUM = 5000; /** @see TagCmd#GET_LIST */
        public static final int NUM1 = 5001;/** @see TagCmd#GET_REL_DATA_STATUS */
        public static final int NUM2 = 5002;/** @see TagCmd#SEARCH_REL */
        public static final int NUM3 = 5003;/** @see TagCmd#GET_ALL_REL */
    }

    /**
     * 写命令1000到5000的范围
     */
    public static class WriteCmdNum{
        public static final int NUM = 1000;/** @see TagCmd#ADD */
        public static final int NUM1 = 1001;/** @see TagCmd#BATCH_SET */
        public static final int NUM2 = 1002;/** @see TagCmd#BATCH_DEL */
        public static final int NUM4 = 1003;/** @see TagCmd#UNION_SET_TAG_LIST*/
        public static final int NUM5 = 1004;/** @see TagCmd#CLONE*/
        public static final int NUM6 = 1005; /** @see TagCmd#INCR_CLONE*/
        public static final int NUM7 = 1006; /** @see TagCmd#BACKUP*/
        public static final int NUM8 = 1007; /** @see TagCmd#RESTORE*/
        public static final int NUM9 = 1008; /** @see TagCmd#DEL_BACKUP*/

    }

    public static class TagCmd {
        //写命令
        public static final int ADD = WriteCmdNum.NUM;
        public static final int BATCH_SET = WriteCmdNum.NUM1;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int UNION_SET_TAG_LIST = WriteCmdNum.NUM4;
        public static final int CLONE = WriteCmdNum.NUM5;
        public static final int INCR_CLONE = WriteCmdNum.NUM6;
        public static final int BACKUP = WriteCmdNum.NUM7;
        public static final int RESTORE = WriteCmdNum.NUM8;
        public static final int DEL_BACKUP = WriteCmdNum.NUM9;
        
        //读命令
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_REL_DATA_STATUS = ReadCmdNum.NUM1;
        public static final int SEARCH_REL = ReadCmdNum.NUM2;
        public static final int GET_ALL_REL = ReadCmdNum.NUM3;
    }
}
