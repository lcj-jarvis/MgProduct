package fai.MgProductStoreSvr.domain.repository;

/**
 * 表和库映射
 */
public class TableDBMapping {
    /**
     * 表
     */
    public static final class Table{
        public static final String MG_SPU_BIZ_SUMMARY = "mgSpuBizSummary";
        public static final String MG_HOLDING_RECORD = "mgHoldingRecord";
        public static final String MG_IN_OUT_STORE_RECORD = "mgInOutStoreRecord";
        public static final String MG_IN_OUT_STORE_SUM = "mgInOutStoreSum";
        public static final String MG_SPU_SUMMARY = "mgSpuSummary";
        public static final String MG_STORE_ORDER_RECORD = "mgStoreOrderRecord";
        public static final String MG_STORE_SALE_SKU = "mgStoreSaleSKU";
        public static final String MG_SKU_SUMMARY = "mgSkuSummary";
        public static final String MG_REFUND_RECORD = "mgRefundRecord";
    }

    /**
     * DB
     */
    public static final class DB{
        public static final String MG_PRODUCT_STORE = "mgProductStore";
        public static final String MG_PRODUCT_STORE_SUMMARY = "mgProductStoreSummary";
        public static final String MG_PRODUCT_STORE_RECORD = "mgProductStoreRecord";
    }

    public enum TableEnum{
        MG_STORE_SALE_SKU(Table.MG_STORE_SALE_SKU, DB.MG_PRODUCT_STORE)
        , MG_HOLDING_RECORD(Table.MG_HOLDING_RECORD, DB.MG_PRODUCT_STORE)
        , MG_STORE_ORDER_RECORD(Table.MG_STORE_ORDER_RECORD, DB.MG_PRODUCT_STORE)
        , MG_REFUND_RECORD(Table.MG_REFUND_RECORD, DB.MG_PRODUCT_STORE)


        , MG_SPU_BIZ_SUMMARY(Table.MG_SPU_BIZ_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)
        , MG_SPU_SUMMARY(Table.MG_SPU_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)
        , MG_SKU_SUMMARY(Table.MG_SKU_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)

        , MG_IN_OUT_STORE_RECORD(Table.MG_IN_OUT_STORE_RECORD, DB.MG_PRODUCT_STORE_RECORD)
        , MG_IN_OUT_STORE_SUM(Table.MG_IN_OUT_STORE_SUM, DB.MG_PRODUCT_STORE_RECORD)
        ;
        private String table;
        private String db;
        TableEnum(String table, String db) {
            this.table = table;
            this.db = db;
        }

        public String getTable(){
            return this.table;
        }

        public String getDb(){
            return this.db;
        }
        public String getGroup(){
            return getDb();
        }
        public String getTaskGroup(){
            return "task:"+getDb();
        }
    }
}
