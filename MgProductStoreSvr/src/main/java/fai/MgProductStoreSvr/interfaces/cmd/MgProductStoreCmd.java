package fai.MgProductStoreSvr.interfaces.cmd;


public class MgProductStoreCmd {
	// 5000  以上的范围
	protected static class ReadCmdNum{
		public static final int NUM = 5000;
		public static final int NUM1 = 5001;
		public static final int NUM2 = 5002;
	}


	// 1000 到 5000 的范围
	protected static class WriteCmdNum{
		public static final int NUM = 1000;
		public static final int NUM2 = 1002;
		public static final int NUM3 = 1003;
		public static final int NUM4 = 1004;
		public static final int NUM5 = 1005;
		public static final int NUM6 = 1006;
		public static final int NUM7 = 1007;
	}

	////////////////// cmd对外定义，实际做cmd读写分离 ////////////////////

	/**
	 * 商品规格库存销售sku 相关 cmd
	 */
	public static class StoreSalesSkuCmd {
		public static final int GET_LIST = ReadCmdNum.NUM;

		public static final int REFRESH = WriteCmdNum.NUM;
		public static final int SET_LIST = WriteCmdNum.NUM2;
		public static final int REDUCE_STORE = WriteCmdNum.NUM3;
		public static final int REDUCE_HOLDING_STORE = WriteCmdNum.NUM4;
		public static final int MAKE_UP_STORE = WriteCmdNum.NUM7;

	}

	/**
	 * 出入库存记录 相关 cmd
	 */
	public static class InOutStoreRecordCmd {
		public static final int ADD_LIST = WriteCmdNum.NUM5;
	}

	/**
	 * 商品业务销售汇总
	 */
	public static class BizSalesSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM1;
	}

	/**
	 * 商品销售汇总
	 */
	public static class SalesSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM2;
	}

}
