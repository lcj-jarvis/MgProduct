package fai.MgProductSearchSvr.interfaces.cmd;

public class MgProductSearchCmd {

    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	// PropCmd.GET_LIST
    }


    public static class SearchCmd {
        public static final int SEARCH_LIST = ReadCmdNum.NUM;
    }

}
