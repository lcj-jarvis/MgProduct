package fai.MgProductInfSvr.interfaces.cmd;

public class MgProductInfCmd {
    // 5000  以上的范围
    public static class ReadCmdNum{
        public static final int NUM = 5000;	/**@see PropCmd#GET_LIST */
        public static final int NUM2 = 5001; /**@see PropCmd#GET_VAL_LIST */
        public static final int NUM3 = 5002; /**@see BasicCmd#GET_PROP_LIST */

        public static final int NUM4 = 5004; /**@see SpecTempCmd#GET_INFO */
        public static final int NUM5 = 5005; /**@see SpecTempCmd#GET_LIST */
        public static final int NUM6 = 5006; /**@see SpecTempDetailCmd#GET_INFO */
        public static final int NUM7 = 5007; /**@see SpecTempDetailCmd#GET_LIST */
        public static final int NUM8 = 5008; /**@see ProductSpecCmd#GET_INFO */
        public static final int NUM9 = 5009; /**@see ProductSpecCmd#GET_LIST */
        public static final int NUM10 = 5010; /**@see ProductSpecCmd#GET_CHECKED_LIST */
        public static final int NUM11 = 5011; /**@see ProductSpecSkuCmd#GET_LIST */

        public static final int NUM12 = 5012; /**@see BasicCmd#GET_RLPDIDS_BY_PROP */

        public static final int NUM13 = 5013;  /**@see StoreSalesSkuCmd#GET_LIST */

        public static final int NUM14 = 5014; /**@see SpuBizSummaryCmd#GET_ALL_BIZ_LIST_BY_PD_ID */

        public static final int NUM15 = 5015; /**@see SpuSummaryCmd#GET_LIST */

        public static final int NUM16 = 5016; /**@see SpuBizSummaryCmd#GET_LIST_BY_PD_ID_LIST */

        public static final int NUM17 = 5017; /**@see SkuSummaryCmd#GET_LIST */

        public static final int NUM18 = 5018; /**@see ProductSpecSkuCmd#GET_SKU_ID_LIST */

        public static final int NUM19 = 5019; /**@see InOutStoreRecordCmd#GET_LIST */

        public static final int NUM20 = 5020; /**@see StoreSalesSkuCmd#GET_LIST_BY_SKU_ID */

        public static final int NUM21= 5021; /**@see Cmd#GET_FULL_INFO */

        public static final int NUM22= 5022; /**@see ProductSpecSkuCmd#GET_LIST_BY_SKU_ID_LIST */
        public static final int NUM23 = 5023; /**@see HoldingRecordCmd#GET_LIST */

        public static final int NUM24 = 5024; /**@see StoreSalesSkuCmd#GET_LIST_BY_SKU_ID_LIST */

        public static final int NUM25 = 5025; /**@see SpuBizSummaryCmd#GET_ALL_BIZ_LIST_BY_PD_ID_LIST */

        public static final int NUM26 = 5026;/**@see BasicCmd#GET_PD_LIST */
        public static final int NUM27 = 5027;/**@see GroupCmd#GET_GROUP_LIST */
        public static final int NUM28 = 5028;/**@see BasicCmd#GET_PD_BIND_GROUPS */

        public static final int NUM29 = 5029; /**@see ProductSpecSkuCmd#GET_SKU_CODE_LIST */

        public static final int NUM30 = 5030; /**@see ProductSpecSkuCmd#SEARCH_SKU_ID_INFO_LIST_BY_SKU_CODE */
        public static final int NUM31 = 5031; /**@see ProductSpecSkuCmd#GET_ONLY_SPU_INFO_LIST*/

        public static final int NUM32 = 5032; /**@see MgProductSearchCmd#SEARCH_LIST*/
        public static final int NUM33 = 5033; /**@see InOutStoreRecordCmd#GET_SUM_LIST*/
        public static final int NUM34 = 5034; /**@see Cmd#GET_FULL_LIST_4ADM */
        public static final int NUM35 = 5035; /**@see Cmd#GET_SUM_LIST_4ADM */
        public static final int NUM36 = 5036; /**@see ProductSpecCmd#GET_LIST_4ADM */

        public static final int NUM37 = 5037; /**@see LibCmd#GET_LIB_LIST */
        public static final int NUM38 = 5038; /**@see LibCmd#GET_REL_LIB_LIST */

        public static final int NUM39 = 5039; /**@see TagCmd#GET_TAG_LIST */
        public static final int NUM40 = 5040; /**@see TagCmd#GET_REL_TAG_LIST */

        public static final int NUM41 = 5041; /**@see BasicCmd#GET_PD_BIND_TAGS */
        public static final int NUM42 = 5042; /**@see Cmd#GET_INFO_4ES */
        public static final int NUM43 = 5043; /**@see MgProductSearchCmd#SEARCH_PD */
        public static final int NUM44 = 5044; /**@see BasicCmd#GET_PD_BIND_BIZS */
    }

    // 1000 到 5000 的范围
    public static class WriteCmdNum{
        public static final int NUM = 1000; /**@see PropCmd#BATCH_ADD */
        public static final int NUM2 = 1001; /**@see PropCmd#BATCH_DEL */
        public static final int NUM3 = 1002; /**@see PropCmd#BATCH_SET */
        public static final int NUM4 = 1003; /**@see PropCmd#ADD_WITH_VAL */
        public static final int NUM5 = 1004; /**@see PropCmd#SET_WITH_VAL */
        public static final int NUM6 = 1005; /**@see PropCmd#BATCH_SET_VAL */
        public static final int NUM7 = 1006; /**@see BasicCmd#SET_PROP_LIST */

        public static final int NUM8 = 1008; /**@see SpecTempCmd#ADD_LIST */
        public static final int NUM9 = 1009; /**@see SpecTempCmd#SET_LIST */
        public static final int NUM10 = 1010; /**@see SpecTempCmd#DEL_LIST */
        public static final int NUM11 = 1011; /**@see SpecTempDetailCmd#ADD_LIST */
        public static final int NUM12 = 1012; /**@see SpecTempDetailCmd#SET_LIST */
        public static final int NUM13 = 1013; /**@see SpecTempDetailCmd#DEL_LIST */
        public static final int NUM14 = 1014; /**@see ProductSpecCmd#IMPORT */
        public static final int NUM15 = 1015; /**@see ProductSpecCmd#UNION_SET */
        public static final int NUM16 = 1016;  /**@see ProductSpecSkuCmd#SET_LIST */

        public static final int NUM17 = 1017; /**@see BasicCmd#ADD_PD_AND_REL */
        public static final int NUM18 = 1018; /**@see BasicCmd#ADD_PD_BIND */
        public static final int NUM19 = 1019; /**@see BasicCmd#BATCH_ADD_PD_BIND */
        public static final int NUM20 = 1020; /**@see BasicCmd#BATCH_DEL_PD_BIND */
        public static final int NUM21 = 1021; /**@see BasicCmd#BATCH_DEL_PDS */


        public static final int NUM22 = 1022; /**@see StoreSalesSkuCmd#SET_LIST */
        public static final int NUM23 = 1023; /**@see StoreSalesSkuCmd#BATCH_REDUCE_STORE */
        public static final int NUM24 = 1024; /**@see StoreSalesSkuCmd#BATCH_REDUCE_HOLDING_STORE */
        public static final int NUM25 = 1025; /**@see StoreSalesSkuCmd#BATCH_MAKE_UP_STORE */
        public static final int NUM26 = 1026; /**@see InOutStoreRecordCmd#ADD_LIST */


        public static final int NUM27 = 1027; /**@see TempCmd#SYN_SPU_TO_SKU */
        public static final int NUM28 = 1028; /**@see TempCmd#SYN_IN_OUT_STORE_RECORD */
        public static final int NUM29 = 1029; /**@see StoreSalesSkuCmd#BATCH_REFUND_STORE */
        public static final int NUM30 = 1030; /**@see GroupCmd#ADD_GROUP */
        public static final int NUM31 = 1031; /**@see GroupCmd#DEL_GROUP_LIST */
        public static final int NUM32 = 1032; /**@see GroupCmd#SET_GROUP_LIST */
        public static final int NUM33 = 1033; /**@see BasicCmd#SET_PD_BIND_GROUP */

        public static final int NUM34 = 1034; /**@see Cmd#IMPORT_PRODUCT */

        public static final int NUM35 = 1035; /**@see BasicCmd#SET_SINGLE_PD */
        public static final int NUM36 = 1036; /**@see BasicCmd#SET_PDS */
        public static final int NUM37 = 1037; /**@see StoreSalesSkuCmd#REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE */
        public static final int NUM38 = 1038; /**@see InOutStoreRecordCmd#BATCH_RESET_PRICE */
        public static final int NUM39 = 1039; /**@see PropCmd#UNION_SET_PROP_LIST */
        public static final int NUM40 = 1040; /**@see GroupCmd#UNION_SET_GROUP_LIST */
        public static final int NUM41 = 1041; /**@see StoreSalesSkuCmd#BATCH_SET_LIST */
        public static final int NUM42 = 1042; /**@see BasicCmd#ADD_PD_INFO */
        public static final int NUM43 = 1043; /**@see BasicCmd#SET_PD_INFO */
        public static final int NUM44 = 1044; /**@see StoreSalesSkuCmd#BATCH_ADD_LIST */
        public static final int NUM45 = 1045; /**@see Cmd#CLEAR_REL_DATA */
        public static final int NUM46 = 1046; /**@see Cmd#CLEAR_ACCT */
        public static final int NUM47 = 1047; /**@see LibCmd#ADD_LIB */
        public static final int NUM48 = 1048; /**@see LibCmd#DEL_LIB_LIST */
        public static final int NUM49 = 1049; /**@see LibCmd#SET_LIB_LIST */
        public static final int NUM50 = 1050; /**@see LibCmd#UNION_SET_LIB_LIST */
        public static final int NUM51 = 1051; /**@see TagCmd#ADD_TAG */
        public static final int NUM52 = 1052; /**@see TagCmd#DEL_TAG_LIST */
        public static final int NUM53 = 1053; /**@see TagCmd#SET_TAG_LIST */
        public static final int NUM54 = 1054; /**@see TagCmd#UNION_SET_TAG_LIST */
        public static final int NUM55 = 1055; /**@see BasicCmd#SET_PD_BIND_TAG */
        public static final int NUM56 = 1056; /**@see Cmd#CLONE_DATA */
        public static final int NUM57 = 1057; /**@see Cmd#INC_CLONE */
        public static final int NUM58 = 1058; /**@see Cmd#BACKUP */
        public static final int NUM59 = 1059; /**@see Cmd#RESTORE */
        public static final int NUM60 = 1060; /**@see Cmd#DEL_BACKUP */
        public static final int NUM61 = 1061; /**@see GroupCmd#SET_ALL_GROUP_LIST */
        public static final int NUM62 = 1062; /**@see BasicCmd#DEL_PD_TAG_LIST */

    }


    public static class MgProductSearchCmd{
        public static final int SEARCH_LIST = ReadCmdNum.NUM32;
        public static final int SEARCH_PD = ReadCmdNum.NUM43;
    }


    /**
     * cmd对外定义，实际做cmd读写分离
     */
    public static class PropCmd {
        //读命令
        public static final int GET_LIST = ReadCmdNum.NUM;
        public static final int GET_VAL_LIST = ReadCmdNum.NUM2;

        //写命令
        public static final int BATCH_ADD = WriteCmdNum.NUM;
        public static final int BATCH_DEL = WriteCmdNum.NUM2;
        public static final int BATCH_SET = WriteCmdNum.NUM3;
        public static final int ADD_WITH_VAL = WriteCmdNum.NUM4;
        public static final int SET_WITH_VAL = WriteCmdNum.NUM5;
        public static final int BATCH_SET_VAL = WriteCmdNum.NUM6;
        public static final int UNION_SET_PROP_LIST = WriteCmdNum.NUM39;
    }

    public static class BasicCmd {
        //读命令
        public static final int GET_PROP_LIST = ReadCmdNum.NUM3;
        public static final int GET_RLPDIDS_BY_PROP = ReadCmdNum.NUM12;
        public static final int GET_PD_LIST = ReadCmdNum.NUM26;
        public static final int GET_PD_BIND_GROUPS = ReadCmdNum.NUM28;
        public static final int GET_PD_BIND_TAGS = ReadCmdNum.NUM41;
        public static final int GET_PD_BIND_BIZS = ReadCmdNum.NUM44;

        //写命令
        public static final int SET_PROP_LIST = WriteCmdNum.NUM7;
        public static final int ADD_PD_AND_REL = WriteCmdNum.NUM17;
        public static final int ADD_PD_BIND = WriteCmdNum.NUM18;
        public static final int BATCH_ADD_PD_BIND = WriteCmdNum.NUM19;
        public static final int BATCH_DEL_PD_BIND = WriteCmdNum.NUM20;
        public static final int BATCH_DEL_PDS = WriteCmdNum.NUM21;
        public static final int SET_PD_BIND_GROUP = WriteCmdNum.NUM33;
        public static final int SET_SINGLE_PD = WriteCmdNum.NUM35;
        public static final int SET_PDS = WriteCmdNum.NUM36;
        public static final int ADD_PD_INFO = WriteCmdNum.NUM42;
        public static final int SET_PD_INFO = WriteCmdNum.NUM43;
        public static final int SET_PD_BIND_TAG = WriteCmdNum.NUM55;
        public static final int DEL_PD_TAG_LIST = WriteCmdNum.NUM62;
    }

    public static class GroupCmd {
        //读命令
        public static final int GET_GROUP_LIST = ReadCmdNum.NUM27;

        //写命令
        public static final int ADD_GROUP = WriteCmdNum.NUM30;
        public static final int DEL_GROUP_LIST = WriteCmdNum.NUM31;
        public static final int SET_GROUP_LIST = WriteCmdNum.NUM32;
        public static final int UNION_SET_GROUP_LIST = WriteCmdNum.NUM40;
        public static final int SET_ALL_GROUP_LIST = WriteCmdNum.NUM61;
    }

    public static class LibCmd {
        //读命令
        public static final int GET_LIB_LIST = ReadCmdNum.NUM37;
        public static final int GET_REL_LIB_LIST = ReadCmdNum.NUM38;

        //写命令
        public static final int ADD_LIB = WriteCmdNum.NUM47;
        public static final int DEL_LIB_LIST = WriteCmdNum.NUM48;
        public static final int SET_LIB_LIST = WriteCmdNum.NUM49;
        public static final int UNION_SET_LIB_LIST = WriteCmdNum.NUM50;
    }

    public static class TagCmd {
        //读命令
        public static final int GET_TAG_LIST = ReadCmdNum.NUM39;
        public static final int GET_REL_TAG_LIST = ReadCmdNum.NUM40;

        //写命令
        public static final int ADD_TAG = WriteCmdNum.NUM51;
        public static final int DEL_TAG_LIST = WriteCmdNum.NUM52;
        public static final int SET_TAG_LIST = WriteCmdNum.NUM53;
        public static final int UNION_SET_TAG_LIST = WriteCmdNum.NUM54;
    }


    /**
     * 规格模板 相关 cmd
     */
    public static class SpecTempCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM4;
        public static final int GET_LIST = ReadCmdNum.NUM5;

        //写命令
        public static final int ADD_LIST = WriteCmdNum.NUM8;
        public static final int SET_LIST = WriteCmdNum.NUM9;
        public static final int DEL_LIST = WriteCmdNum.NUM10;
    }

    /**
     * 规格模板详情 相关 cmd
     */
    public static class SpecTempDetailCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM6;
        public static final int GET_LIST = ReadCmdNum.NUM7;

        //写命令
        public static final int ADD_LIST = WriteCmdNum.NUM11;
        public static final int SET_LIST = WriteCmdNum.NUM12;
        public static final int DEL_LIST = WriteCmdNum.NUM13;
    }

    /**
     * 商品规格 相关 cmd
     */
    public static class ProductSpecCmd {
        //读命令
        public static final int GET_INFO = ReadCmdNum.NUM8;
        public static final int GET_LIST = ReadCmdNum.NUM9;
        public static final int GET_CHECKED_LIST = ReadCmdNum.NUM10;
        public static final int GET_LIST_4ADM = ReadCmdNum.NUM36;

        //写命令
        public static final int IMPORT = WriteCmdNum.NUM14;
        public static final int UNION_SET = WriteCmdNum.NUM15;
    }

    /**
     * 商品规格sku 相关 cmd
     */
    public static class ProductSpecSkuCmd {
        //读命令
        public static final int GET_LIST = ReadCmdNum.NUM11;
        public static final int GET_SKU_ID_LIST = ReadCmdNum.NUM18;
        public static final int GET_LIST_BY_SKU_ID_LIST = ReadCmdNum.NUM22;
        public static final int GET_SKU_CODE_LIST = ReadCmdNum.NUM29;
        public static final int SEARCH_SKU_ID_INFO_LIST_BY_SKU_CODE = ReadCmdNum.NUM30;
        public static final int GET_ONLY_SPU_INFO_LIST = ReadCmdNum.NUM31;

        //写命令
        public static final int SET_LIST = WriteCmdNum.NUM16;
    }

    /**
     * 商品规格库存销售sku 相关 cmd
     */
    public static class StoreSalesSkuCmd {
        public static final int GET_LIST = ReadCmdNum.NUM13;
        public static final int GET_LIST_BY_SKU_ID = ReadCmdNum.NUM20;
        public static final int GET_LIST_BY_SKU_ID_LIST = ReadCmdNum.NUM24;

        public static final int SET_LIST = WriteCmdNum.NUM22;
        public static final int BATCH_REDUCE_STORE = WriteCmdNum.NUM23;
        public static final int BATCH_REDUCE_HOLDING_STORE = WriteCmdNum.NUM24;
        public static final int BATCH_MAKE_UP_STORE = WriteCmdNum.NUM25;
        public static final int BATCH_REFUND_STORE = WriteCmdNum.NUM29;
        public static final int REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE = WriteCmdNum.NUM37;
        public static final int BATCH_SET_LIST = WriteCmdNum.NUM41;
        public static final int BATCH_ADD_LIST = WriteCmdNum.NUM44;
    }

    /**
     * 出入库存记录 相关 cmd
     */
    public static class InOutStoreRecordCmd {
        public static final int GET_LIST = ReadCmdNum.NUM19;
        public static final int GET_SUM_LIST = ReadCmdNum.NUM33;

        public static final int ADD_LIST = WriteCmdNum.NUM26;
        public static final int BATCH_RESET_PRICE = WriteCmdNum.NUM38;
    }

    /**
     * spu 业务汇总
     */
    public static class SpuBizSummaryCmd {
        public static final int GET_ALL_BIZ_LIST_BY_PD_ID = ReadCmdNum.NUM14;
        public static final int GET_LIST_BY_PD_ID_LIST = ReadCmdNum.NUM16;
        public static final int GET_ALL_BIZ_LIST_BY_PD_ID_LIST = ReadCmdNum.NUM25;
    }

    /**
     * spu 汇总
     */
    public static class SpuSummaryCmd {
        public static final int GET_LIST = ReadCmdNum.NUM15;
    }
    /**
     * sku 汇总
     */
    public static class SkuSummaryCmd {
        public static final int GET_LIST = ReadCmdNum.NUM17;
    }

    /**
     * 预扣记录
     */
    public static class HoldingRecordCmd{
        public static final int GET_LIST = ReadCmdNum.NUM23;
    }

    /**
     * 临时的 cmd
     */
    public static class TempCmd{
        // 同步spu 数据到sku
        public static final int SYN_SPU_TO_SKU = WriteCmdNum.NUM27;
        // 同步出入库记录
        public static final int SYN_IN_OUT_STORE_RECORD = WriteCmdNum.NUM28;
    }

    /**
     * 组合数据的cmd
     */
    public static class Cmd{
        public static final int GET_FULL_INFO = ReadCmdNum.NUM21;
        public static final int GET_FULL_LIST_4ADM = ReadCmdNum.NUM34;
        public static final int GET_SUM_LIST_4ADM = ReadCmdNum.NUM35;
        public static final int GET_INFO_4ES = ReadCmdNum.NUM42;

        public static final int IMPORT_PRODUCT = WriteCmdNum.NUM34;
        public static final int CLEAR_REL_DATA = WriteCmdNum.NUM45;
        public static final int CLEAR_ACCT = WriteCmdNum.NUM46;
        public static final int CLONE_DATA = WriteCmdNum.NUM56;
        public static final int INC_CLONE = WriteCmdNum.NUM57;
        public static final int BACKUP = WriteCmdNum.NUM58;
        public static final int RESTORE = WriteCmdNum.NUM59;
        public static final int DEL_BACKUP = WriteCmdNum.NUM60;
    }
}
