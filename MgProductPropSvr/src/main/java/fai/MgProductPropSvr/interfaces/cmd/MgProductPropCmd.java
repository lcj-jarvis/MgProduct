package fai.MgProductPropSvr.interfaces.cmd;

public class MgProductPropCmd {
	// 5000  以上的范围
	public static class ReadCmdNum{
		public static final int NUM = 5000;	// PropCmd.GET_LIST
		public static final int NUM2 = 5001;// PropValCmd.GET_LIST
	}


	// 1000 到 5000 的范围
	public static class WriteCmdNum{
		public static final int NUM = 1000; // PropCmd.BATCH_ADD
		public static final int NUM2 = 1001; // PropCmd.BATCH_DEL
		public static final int NUM3 = 1002; // PropCmd.BATCH_SET
		public static final int NUM4 = 1003; // PropCmd.ADD
		public static final int NUM5 = 1004; // PropValCmd.BATCH_SET
	}

	/**
	 * cmd对外定义，实际做cmd读写分离
	 */
	public static class PropCmd {
		public static final int GET_LIST = ReadCmdNum.NUM;

		public static final int BATCH_ADD = WriteCmdNum.NUM;
		public static final int BATCH_DEL = WriteCmdNum.NUM2;
		public static final int BATCH_SET = WriteCmdNum.NUM3;
		public static final int ADD_WITH_VAL = WriteCmdNum.NUM4;
	}

	public static class PropValCmd {
		public static final int GET_LIST = ReadCmdNum.NUM2;

		public static final int BATCH_SET = WriteCmdNum.NUM5;
	}
}
