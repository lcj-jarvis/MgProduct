package fai.MgProductStoreSvr.domain.repository;

/**
 * 表和库映射
 */
public class TableDBMapping {
    /**
     * 表
     */
    public static final class Table{
        public static final String MG_BIZ_SALES_REPORT = "mgBizSalesReport";
        public static final String MG_BIZ_SALES_SUMMARY = "mgBizSalesSummary";
        public static final String MG_HOLDING_RECORD = "mgHoldingRecord";
        public static final String MG_IN_OUT_STORE_RECORD = "mgInOutStoreRecord";
        public static final String MG_SALES_SUMMARY = "mgSalesSummary";
        public static final String MG_STORE_ORDER_RECORD = "mgStoreOrderRecord";
        public static final String MG_STORE_SALE_SKU = "mgStoreSaleSKU";
        public static final String MG_STORE_SKU_SUMMARY = "mgStoreSkuSummary";
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
        ,MG_HOLDING_RECORD(Table.MG_HOLDING_RECORD, DB.MG_PRODUCT_STORE)
        ,MG_STORE_ORDER_RECORD(Table.MG_STORE_ORDER_RECORD, DB.MG_PRODUCT_STORE)
        ,MG_BIZ_SALES_REPORT(Table.MG_BIZ_SALES_REPORT, DB.MG_PRODUCT_STORE)


        ,MG_BIZ_SALES_SUMMARY(Table.MG_BIZ_SALES_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)
        ,MG_SALES_SUMMARY(Table.MG_SALES_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)
        ,MG_STORE_SKU_SUMMARY(Table.MG_STORE_SKU_SUMMARY, DB.MG_PRODUCT_STORE_SUMMARY)

        ,MG_IN_OUT_STORE_RECORD(Table.MG_IN_OUT_STORE_RECORD, DB.MG_PRODUCT_STORE_RECORD)
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
