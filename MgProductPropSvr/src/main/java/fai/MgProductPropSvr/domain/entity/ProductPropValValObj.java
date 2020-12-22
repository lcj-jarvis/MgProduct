package fai.MgProductPropSvr.domain.entity;

import fai.comm.util.FaiList;

public class ProductPropValValObj {
	public static final class DataType {
		public static final byte STRING = 1;
		public static final byte INT = 2;
		public static final byte LONG = 3;
		public static final byte FLOAT = 4;
		public static final byte DOUBLE = 5;
		public static final byte BOOLEAN = 6;

		public boolean isValid(int dataType) {
			return dataTyeList.contains(dataType);
		}

		public static FaiList<Byte> dataTyeList = new FaiList<Byte>();
		static {
			dataTyeList.add(STRING);
			dataTyeList.add(INT);
			dataTyeList.add(LONG);
			dataTyeList.add(FLOAT);
			dataTyeList.add(DOUBLE);
			dataTyeList.add(BOOLEAN);
		}
	}
}
