package fai.MgProductBasicSvr.interfaces.cmd;

public class MgProductBasicCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	 /** @see BindPropCmd#GET_LIST */
        public static final int NUM1 = 5001; /** @see BindPropCmd#GET_LIST_BY_PROP */
        public static final int NUM2 = 5002; /** @see BasicCmd#GET_REL */
        public static final int NUM3 = 5003; /** @see BasicCmd#GET_REL_LIST */
        public static final int NUM4 = 5004; /** @see BasicCmd#GET_REDUCED_REL_LIST */
        public static final int NUM5 = 5005; /** @see BindGroupCmd#GET_LIST */
        public static final int NUM6 = 5006; /** @see BindGroupCmd#GET_PD_BY_GROUP */
        public static final int NUM7 = 5007; /** @see BindGroupCmd#GET_DATA_STATUS */
        public static final int NUM8 = 5008; /** @see BindGroupCmd#GET_ALL_DATA */
        public static final int NUM9 = 5009; /** @see BindGroupCmd#SEARCH_FROM_DB */
        public static final int NUM10 = 5010; /** @see BindPropCmd#GET_DATA_STATUS */
        public static final int NUM11 = 5011; /** @see BindPropCmd#GET_ALL_DATA */
        public static final int NUM12 = 5012; /** @see BindPropCmd#SEARCH_FROM_DB */
        public static final int NUM13 = 5013; /** @see BasicCmd#PD_DATA_STATUS */
        public static final int NUM14 = 5014; /** @see BasicCmd#GET_ALL_PD */
        public static final int NUM15 = 5015; /** @see BasicCmd#SEARCH_PD_FROM_DB */
        public static final int NUM16 = 5016; /** @see BasicCmd#PD_REL_DATA_STATUS */
        public static final int NUM17 = 5017; /** @see BasicCmd#GET_ALL_PD_REL */
        public static final int NUM18 = 5018; /** @see BasicCmd#SEARCH_PD_REL_FROM_DB */
        public static final int NUM19 = 5019; /** @see BasicCmd#GET_PD_LIST */

        public static final int NUM20 = 5020; /** @see BindTagCmd#GET_LIST */
        public static final int NUM21 = 5021; /** @see BindTagCmd#GET_PD_BY_TAG */
        public static final int NUM22 = 5022; /** @see BindTagCmd#GET_DATA_STATUS */
        public static final int NUM23 = 5023; /** @see BindTagCmd#GET_ALL_DATA */
        public static final int NUM24 = 5024; /** @see BindTagCmd#SEARCH_FROM_DB */
        public static final int NUM25 = 5025; /** @see BasicCmd#GET_BY_PDID */
        public static final int NUM26 = 5026; /** @see BasicCmd#GET_PD_INFO */
        public static final int NUM27 = 5027; /** @see BasicCmd#GET_LIST_BY_PDID */
        public static final int NUM28 = 5028; /** @see BasicCmd#GET_BIND_BIZ */
        public static final int NUM29 = 5029; /** @see Cmd#MIGRATE_GET */
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000;  /** @see BindPropCmd#BATCH_SET */
        public static final int NUM1 = 1001; /** @see BasicCmd#ADD_PD_AND_REL */
        public static final int NUM2 = 1002; /** @see BasicCmd#ADD_REL_BIND */
        public static final int NUM3 = 1003; /** @see BasicCmd#DEL_PDS */
        public static final int NUM4 = 1004; /** @see BasicCmd#DEL_REL_BIND */
        public static final int NUM5 = 1005; /** @see BasicCmd#BATCH_ADD_REL_BIND */
        public static final int NUM6 = 1006; /** @see BasicCmd#BATCH_ADD_PD_AND_REL */
        public static final int NUM7 = 1007; /** @see BasicCmd#BATCH_ADD_PDS_REL_BIND */
        public static final int NUM8 = 1008; /** @see BindGroupCmd#BATCH_SET */
        public static final int NUM9 = 1009; /** @see BindPropCmd#DEL_BY_VAL_IDS */
        public static final int NUM10 = 1010; /** @see BindPropCmd#DEL_BY_PROP_IDS */
        public static final int NUM11 = 1011; /** @see BindGroupCmd#DEL */
        public static final int NUM12 = 1012; /** @see BasicCmd#SET_SINGLE_PD */
        public static final int NUM13 = 1013; /** @see BasicCmd#SET_PDS */
        public static final int NUM14 = 1014; /** @see BasicCmd#CLEAR_REL_DATA */
        //public static final int NUM15 = 1015; /** @see BindGroupCmd#SET_PD_BIND_GROUP_ROLLBACK */
        //public static final int NUM16 = 1016; /** @see BindGroupCmd#TRANSACTION_SET_PD_BIND_GROUP */
        //public static final int NUM17 = 1017; /** @see BindPropCmd#TRANSACTION_SET_PD_BIND_PROP */
        //public static final int NUM18 = 1018; /** @see BindPropCmd#SET_PD_BIND_PROP_ROLLBACK */
        public static final int NUM19 = 1019; /** @see BasicCmd#CLEAR_ACCT */
        public static final int NUM20 = 1020; /** @see BasicCmd#ADD_REL_BIND_ROLLBACK */

        public static final int NUM21 = 1021; /** @see BindTagCmd#BATCH_SET */
        public static final int NUM22 = 1022; /** @see BindTagCmd#DEL */
        //public static final int NUM23 = 1023; /** @see BindTagCmd#TRANSACTION_SET_PD_BIND_TAG */
        //public static final int NUM24 = 1024; /** @see BindTagCmd#SET_PD_BIND_TAG_ROLLBACK */
        public static final int NUM25 = 1025; /** @see BasicCmd#ADD_PD_AND_REL_ROLLBACK */
        public static final int NUM26 = 1026; /** @see BasicCmd#SET_SINGLE_PD_ROLLBACK */
        public static final int NUM27 = 1027; /** @see BasicCmd#DEL_PDS_ROLLBACK */
        public static final int NUM28 = 1028; /** @see BasicCmd#CLONE_BIZ_BIND */
        public static final int NUM29 = 1029; /** @see BasicCmd#BATCH_SET_4YK */
        public static final int NUM30 = 1030; /** @see BasicCmd#BATCH_SET_4YK_ROLLBACK */
        public static final int NUM31 = 1031; /** @see Cmd#MIGRATE */
        public static final int NUM32 = 1032; /** @see Cmd#BACKUP */
        public static final int NUM33 = 1033; /** @see Cmd#RESTORE */
        public static final int NUM34 = 1034; /** @see Cmd#DEL_BACKUP */
        public static final int NUM35 = 1035; /** @see Cmd#CLONE */
        public static final int NUM36 = 1036; /** @see Cmd#INCR_CLONE */
        public static final int NUM37 = 1037; /** @see BasicCmd#BATCH_ADD_REL_BIND_ROLLBACK */
        public static final int NUM38 = 1038; /** @see BasicCmd#SET_SORT */
        public static final int NUM39 = 1039; /** @see BasicCmd#BATCH_BIND_PDS_REL */
        public static final int NUM40 = 1040; /** @see BasicCmd#GET_REDUCED_LIST_BY_NAME */
    }

    /**
     * cmd对外定义，实际做cmd读写分离
     */
    public static class BindPropCmd {
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_LIST_BY_PROP = ReadCmdNum.NUM1;
        public static final int GET_DATA_STATUS = ReadCmdNum.NUM10;
        public static final int GET_ALL_DATA = ReadCmdNum.NUM11;
        public static final int SEARCH_FROM_DB = ReadCmdNum.NUM12;

        public static final int BATCH_SET = WriteCmdNum.NUM;
        public static final int DEL_BY_VAL_IDS = WriteCmdNum.NUM9;
        public static final int DEL_BY_PROP_IDS = WriteCmdNum.NUM10;
    }

    public static class BindGroupCmd {
        public static final int GET_LIST = ReadCmdNum.NUM5;
        public static final int GET_PD_BY_GROUP = ReadCmdNum.NUM6;
        public static final int GET_DATA_STATUS = ReadCmdNum.NUM7;
        public static final int GET_ALL_DATA = ReadCmdNum.NUM8;
        public static final int SEARCH_FROM_DB = ReadCmdNum.NUM9;

        public static final int BATCH_SET = WriteCmdNum.NUM8;
        public static final int DEL = WriteCmdNum.NUM11;
    }

    public static class BindTagCmd {
        public static final int GET_LIST = ReadCmdNum.NUM20;
        public static final int GET_PD_BY_TAG = ReadCmdNum.NUM21;
        public static final int GET_DATA_STATUS = ReadCmdNum.NUM22;
        public static final int GET_ALL_DATA = ReadCmdNum.NUM23;
        public static final int SEARCH_FROM_DB = ReadCmdNum.NUM24;

        public static final int BATCH_SET = WriteCmdNum.NUM21;
        public static final int DEL = WriteCmdNum.NUM22;
    }

    public static class BasicCmd {
        public static final int GET_REL = ReadCmdNum.NUM2;
        public static final int GET_REL_LIST = ReadCmdNum.NUM3;
        public static final int GET_REDUCED_REL_LIST = ReadCmdNum.NUM4;
        public static final int PD_DATA_STATUS = ReadCmdNum.NUM13;
        public static final int GET_ALL_PD = ReadCmdNum.NUM14;
        public static final int SEARCH_PD_FROM_DB = ReadCmdNum.NUM15;
        public static final int PD_REL_DATA_STATUS = ReadCmdNum.NUM16;
        public static final int GET_ALL_PD_REL = ReadCmdNum.NUM17;
        public static final int SEARCH_PD_REL_FROM_DB = ReadCmdNum.NUM18;
        public static final int GET_PD_LIST = ReadCmdNum.NUM19;
        public static final int GET_BY_PDID = ReadCmdNum.NUM25;
        public static final int GET_PD_INFO = ReadCmdNum.NUM26;
        public static final int GET_LIST_BY_PDID = ReadCmdNum.NUM27;
        public static final int GET_BIND_BIZ = ReadCmdNum.NUM28;

        public static final int ADD_PD_AND_REL = WriteCmdNum.NUM1;
        public static final int ADD_REL_BIND = WriteCmdNum.NUM2;
        public static final int DEL_PDS = WriteCmdNum.NUM3;
        public static final int DEL_REL_BIND = WriteCmdNum.NUM4;
        public static final int BATCH_ADD_REL_BIND = WriteCmdNum.NUM5;
        public static final int BATCH_ADD_PD_AND_REL = WriteCmdNum.NUM6;
        public static final int BATCH_ADD_PDS_REL_BIND = WriteCmdNum.NUM7;
        public static final int SET_SINGLE_PD = WriteCmdNum.NUM12;
        public static final int SET_PDS = WriteCmdNum.NUM13;
        public static final int CLEAR_REL_DATA = WriteCmdNum.NUM14;
        public static final int CLEAR_ACCT = WriteCmdNum.NUM19;
        public static final int ADD_REL_BIND_ROLLBACK = WriteCmdNum.NUM20;
        public static final int ADD_PD_AND_REL_ROLLBACK = WriteCmdNum.NUM25;
        public static final int SET_SINGLE_PD_ROLLBACK = WriteCmdNum.NUM26;
        public static final int DEL_PDS_ROLLBACK = WriteCmdNum.NUM27;
        public static final int CLONE_BIZ_BIND = WriteCmdNum.NUM28;
        public static final int BATCH_SET_4YK = WriteCmdNum.NUM29;
        public static final int BATCH_SET_4YK_ROLLBACK = WriteCmdNum.NUM30;
        public static final int BATCH_ADD_REL_BIND_ROLLBACK = WriteCmdNum.NUM37;
        public static final int SET_SORT = WriteCmdNum.NUM38;
        public static final int BATCH_BIND_PDS_REL = WriteCmdNum.NUM39;
        public static final int GET_REDUCED_LIST_BY_NAME = WriteCmdNum.NUM40;
    }

    public static class Cmd {
        public static final int MIGRATE = WriteCmdNum.NUM31;
        public static final int BACKUP = WriteCmdNum.NUM32;
        public static final int RESTORE = WriteCmdNum.NUM33;
        public static final int DEL_BACKUP = WriteCmdNum.NUM34;
        public static final int CLONE = WriteCmdNum.NUM35;
        public static final int INCR_CLONE = WriteCmdNum.NUM36;
        public static final int MIGRATE_GET = ReadCmdNum.NUM29;
    }
}
