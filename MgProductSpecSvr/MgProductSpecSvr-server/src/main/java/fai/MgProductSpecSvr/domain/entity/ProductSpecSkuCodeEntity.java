package fai.MgProductSpecSvr.domain.entity;

/**
 * 商品规格sku条码
 * primaryKey: skuCode,aid,unionPriId
 */
public class ProductSpecSkuCodeEntity {


    public static final class Info {
        public static final String SKU_CODE = "skuCode";                              // varchar(32) 条码 (Primary Key 1)
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 2)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 创建商品的unionPriId (Primary Key 3)
        public static final String SKU_ID = "skuId";                                // bigInt SKU id
        public static final String SORT = "sort";                                   // tinyint 排序方式
        public static final String PD_ID = "pdId";                                  // int 商品id
    }

    public static String[] getManageVisitorKeys() {
        return new String[]{
                Info.SKU_CODE,
                Info.PD_ID,
                Info.SKU_ID,
        };
    }

}
