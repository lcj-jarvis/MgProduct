package fai.MgProductSpecSvr.interfaces.cmd;


public class SpecificationCmd {
	// 5000  以上的范围
	public static class ReadCmdNum{
		public static final int NUM = 5000;
		public static final int NUM2 = 5001;
		public static final int NUM3 = 5002;
		public static final int NUM4 = 5003;
		public static final int NUM5 = 5004;
		public static final int NUM6 = 5005;
		public static final int NUM7 = 5006;
		public static final int NUM8 = 5007;
	}


	// 1000 到 5000 的范围
	protected static class WriteCmdNum{
		public static final int NUM = 1000;
		public static final int NUM2 = 1001;
		public static final int NUM3 = 1002;
		public static final int NUM4 = 1003;
		public static final int NUM5 = 1004;
		public static final int NUM6 = 1005;
		public static final int NUM7 = 1006;
		public static final int NUM8 = 1007;
		public static final int NUM9 = 1008;
		public static final int NUM10 = 1009;
	}

	////////////////// cmd对外定义，实际做cmd读写分离 ////////////////////

	/**
	 * 规格模板相关cmd
	 */
	public static class SpecTempCmd {
		public static final int GET_INFO = ReadCmdNum.NUM;
		public static final int GET_LIST = ReadCmdNum.NUM2;

		public static final int ADD_LIST = WriteCmdNum.NUM;
		public static final int SET_LIST = WriteCmdNum.NUM2;
		public static final int DEL_LIST = WriteCmdNum.NUM3;
	}

	/**
	 * 规格模板详情相关cmd
	 */
	public static class SpecTempDetailCmd {
		public static final int GET_INFO = ReadCmdNum.NUM3;
		public static final int GET_LIST = ReadCmdNum.NUM4;

		public static final int ADD_LIST = WriteCmdNum.NUM4;
		public static final int SET_LIST = WriteCmdNum.NUM5;
		public static final int DEL_LIST = WriteCmdNum.NUM6;
	}

	/**
	 * 商品规格相关cmd
	 */
	public static class ProductSpecCmd {
		public static final int GET_INFO = ReadCmdNum.NUM5;
		public static final int GET_LIST = ReadCmdNum.NUM6;
		public static final int GET_CHECKED_LIST = ReadCmdNum.NUM7;

		public static final int IMPORT = WriteCmdNum.NUM7;
		public static final int UNION_SET = WriteCmdNum.NUM8;
	}

	/**
	 * 商品规格sku 相关 cmd
	 */
	public static class ProductSpecSkuCmd {
		public static final int GET_LIST = ReadCmdNum.NUM8;

		public static final int SET_LIST = WriteCmdNum.NUM9;
	}
}
