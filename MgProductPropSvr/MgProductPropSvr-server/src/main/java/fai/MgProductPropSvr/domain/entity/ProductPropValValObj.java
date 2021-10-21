package fai.MgProductPropSvr.domain.entity;

import fai.comm.util.FaiList;

public class ProductPropValValObj {
	public static final class DataType {
		public static final int STRING = 0;
		public static final int INT = 1;
		public static final int FLOAT = 2;
		public static final int DOUBLE = 3;
		public static final int LONG = 4;
		public static final int BOOLEAN = 5;

		public boolean isValid(int dataType) {
			return dataTyeList.contains(dataType);
		}

		public static FaiList<Integer> dataTyeList = new FaiList<>();
		static {
			dataTyeList.add(STRING);
			dataTyeList.add(INT);
			dataTyeList.add(LONG);
			dataTyeList.add(FLOAT);
			dataTyeList.add(DOUBLE);
			dataTyeList.add(BOOLEAN);
		}
	}

	public static class Limit {
		public static final int COUNT_MAX = 100;
	}
}
