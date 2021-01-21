package fai.MgProductPropSvr.interfaces.entity;

public class ProductPropValObj {
	public static final class Default {
		public static final int DATA_TYPE = DataType.STRING;
		public static final int SORT = 0;
	}

	public static final class DataType {
		public static final int STRING = 0;
		public static final int INT = 1;
		public static final int FLOAT = 2;
		public static final int DOUBLE = 3;
	}
}
