package fai.MgProductGroupSvr.domain.entity;

public class ProductGroupValObj {
	public static final class Flag {

	}

	public static final class Default {
		public static final String ICON_LIST = "";
		public static final int FLAG = 0;
	}

	public static class Limit {
		public static final int COUNT_MAX = 2000;
	}

	public static class SysType {
		public static final int PRODUCT = 0;
		public static final int SERVICE = 1;
	}

	public static class Status {
		public static final int DEFAULT = 0;
		public static final int DEL = -1;
	}
}
