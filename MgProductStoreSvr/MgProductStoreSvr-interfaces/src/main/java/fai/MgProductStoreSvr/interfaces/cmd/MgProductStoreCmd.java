package fai.MgProductStoreSvr.interfaces.cmd;


public class MgProductStoreCmd {
	// 5000  以上的范围
	protected static class ReadCmdNum{
		public static final int NUM = 5000;
		public static final int NUM1 = 5001;
		public static final int NUM2 = 5002;
		public static final int NUM3 = 5003;
		public static final int NUM4 = 5004;
		public static final int NUM5 = 5005;
		public static final int NUM6 = 5006;
		public static final int NUM7 = 5007;
		public static final int NUM8 = 5008;
		public static final int NUM9 = 5009;
		public static final int NUM10 = 5010;
		public static final int NUM11 = 5011;
		public static final int NUM12 = 5012;
		public static final int NUM13 = 5013;
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
		public static final int NUM9 = 1009;
		public static final int NUM10 = 1010;
		public static final int NUM11 = 1011;
		public static final int NUM12 = 1012;
		public static final int NUM13 = 1013;
	}

	////////////////// cmd对外定义，实际做cmd读写分离 ////////////////////

	/**
	 * 商品规格库存销售sku 相关 cmd
	 */
	public static class StoreSalesSkuCmd {
		public static final int GET_LIST = ReadCmdNum.NUM;
		public static final int GET_LIST_BY_SKU_ID_AND_UID_LIST = ReadCmdNum.NUM7;
		public static final int GET_LIST_BY_PD_ID = ReadCmdNum.NUM8;
		public static final int GET_LIST_BY_SKU_ID_LIST = ReadCmdNum.NUM13;

		public static final int REFRESH = WriteCmdNum.NUM;
		public static final int SET_LIST = WriteCmdNum.NUM2;

		public static final int REDUCE_STORE = WriteCmdNum.NUM3;
		public static final int REDUCE_HOLDING_STORE = WriteCmdNum.NUM4;
		public static final int MAKE_UP_STORE = WriteCmdNum.NUM7;

		public static final int BATCH_DEL_PD_ALL_STORE_SALES = WriteCmdNum.NUM8;

		public static final int BATCH_REDUCE_STORE = WriteCmdNum.NUM9;
		public static final int BATCH_REDUCE_HOLDING_STORE = WriteCmdNum.NUM10;
		public static final int BATCH_MAKE_UP_STORE = WriteCmdNum.NUM11;

		public static final int BATCH_SYN_SPU_TO_SKU = WriteCmdNum.NUM12;
	}

	/**
	 * 出入库存记录 相关 cmd
	 */
	public static class InOutStoreRecordCmd {
		public static final int GET_LIST = ReadCmdNum.NUM6;

		public static final int ADD_LIST = WriteCmdNum.NUM5;
		public static final int BATCH_SYN_RECORD = WriteCmdNum.NUM13;
	}

	/**
	 * spu业务库存销售汇总
	 */
	public static class SpuBizSummaryCmd {
		public static final int GET_LIST_BY_PD_ID = ReadCmdNum.NUM1;
		public static final int GET_LIST = ReadCmdNum.NUM3;
		/**
		 * 获取数据状态
		 */
		public static final int GET_DATA_STATUS = ReadCmdNum.NUM9;
		/**
		 * 获取全部数据的部分字段
		 */
		public static final int GET_ALL_DATA_PART_FIELD = ReadCmdNum.NUM10;
		/**
		 * 直接从db搜索，返回部分字段
		 */
		public static final int SEARCH_PART_FIELD = ReadCmdNum.NUM11;
	}

	/**
	 * spu库存销售汇总
	 */
	public static class SpuSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM2;
	}

	/**
	 * sku库存销售汇总
	 */
	public static class SkuSummaryCmd {
		public static final int GET_LIST = ReadCmdNum.NUM4;
		public static final int BIZ_GET_LIST = ReadCmdNum.NUM5;
	}

	/**
	 * 预扣记录相关
	 */
	public static class HoldingRecordCmd{
		public static final int GET_LIST = ReadCmdNum.NUM12;
	}
}
