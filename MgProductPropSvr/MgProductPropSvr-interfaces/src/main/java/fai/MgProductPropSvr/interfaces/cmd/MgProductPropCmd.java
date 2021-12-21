package fai.MgProductPropSvr.interfaces.cmd;

public class MgProductPropCmd {
	// 5000  以上的范围
	public static class ReadCmdNum{
		public static final int NUM = 5000;	 /** @see PropCmd#GET_LIST */
		public static final int NUM1 = 5001; /** @see PropValCmd#GET_LIST */
		public static final int NUM2 = 5002; /** @see PropValCmd#GET_DATA_STATUS */
		public static final int NUM3 = 5003; /** @see PropValCmd#SEARCH_FROM_DB */
		public static final int NUM4 = 5004; /** @see PropValCmd#GET_ALL_DATA */
	}


	// 1000 到 5000 的范围
	public static class WriteCmdNum{
		public static final int NUM = 1000;  /** @see PropCmd#BATCH_ADD */
		public static final int NUM1 = 1001; /** @see PropCmd#BATCH_DEL */
		public static final int NUM2 = 1002; /** @see PropCmd#BATCH_SET */
		public static final int NUM3 = 1003; /** @see PropCmd#ADD_WITH_VAL */
		public static final int NUM4 = 1004; /** @see PropValCmd#BATCH_SET */
		public static final int NUM5 = 1005; /** @see PropCmd#UNION_SET */
		public static final int NUM6 = 1006; /** @see PropCmd#CLEAR_REL_DATA */
		public static final int NUM7 = 1007; /** @see PropCmd#CLEAR_ACCT */
		public static final int NUM8 = 1008; /** @see Cmd#BACKUP */
		public static final int NUM9 = 1009; /** @see Cmd#RESTORE */
		public static final int NUM10 = 1010; /** @see Cmd#DEL_BACKUP */
		public static final int NUM11 = 1011; /** @see Cmd#CLONE */
		public static final int NUM12 = 1012; /** @see Cmd#INCR_CLONE */
	}

	/**
	 * cmd对外定义，实际做cmd读写分离
	 */
	public static class Cmd {

		public static final int BACKUP = WriteCmdNum.NUM8;
		public static final int RESTORE = WriteCmdNum.NUM9;
		public static final int DEL_BACKUP = WriteCmdNum.NUM10;
		public static final int CLONE = WriteCmdNum.NUM11;
		public static final int INCR_CLONE = WriteCmdNum.NUM12;
	}

	public static class PropCmd {
		public static final int GET_LIST = ReadCmdNum.NUM;

		public static final int BATCH_ADD = WriteCmdNum.NUM;
		public static final int BATCH_DEL = WriteCmdNum.NUM1;
		public static final int BATCH_SET = WriteCmdNum.NUM2;
		public static final int ADD_WITH_VAL = WriteCmdNum.NUM3;
		public static final int UNION_SET = WriteCmdNum.NUM5;
		public static final int CLEAR_REL_DATA = WriteCmdNum.NUM6;
		public static final int CLEAR_ACCT = WriteCmdNum.NUM7;
    }

	public static class PropValCmd {
		public static final int GET_LIST = ReadCmdNum.NUM1;
		public static final int GET_DATA_STATUS = ReadCmdNum.NUM2;
		public static final int SEARCH_FROM_DB = ReadCmdNum.NUM3;
		public static final int GET_ALL_DATA = ReadCmdNum.NUM4;

		public static final int BATCH_SET = WriteCmdNum.NUM4;
	}
}
