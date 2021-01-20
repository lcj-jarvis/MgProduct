package fai.MgProductStoreSvr.interfaces.cmd;


public class MgProductStoreCmd {
	// 5000  以上的范围
	protected static class ReadCmdNum{
		public static final int NUM = 5000;
		public static final int NUM1 = 5001;
		public static final int NUM2 = 5002;
		public static final int NUM3= 5003;
		public static final int NUM4= 5004;
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
		public static final int NUM8 = 1008;
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
		public static final int BATCH_DEL_PD_ALL_STORE_SALES = WriteCmdNum.NUM8;

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
		public static final int GET_LIST_BY_PD_ID = ReadCmdNum.NUM1;
		public static final int GET_LIST = ReadCmdNum.NUM3;
	}

	/**
	 * 商品销售汇总
	 */
	public static class SalesSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM2;
	}

	/**
	 * 库存sku汇总
	 */
	public static class StoreSkuSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM4;
	}
}
