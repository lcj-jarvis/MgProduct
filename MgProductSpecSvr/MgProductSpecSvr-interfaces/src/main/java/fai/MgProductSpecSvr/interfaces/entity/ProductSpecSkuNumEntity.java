package fai.MgProductSpecSvr.interfaces.entity;

import fai.comm.util.FaiList;

import java.util.Arrays;

/**
 * 商品规格sku条码
 */
public class ProductSpecSkuNumEntity {
    public static final class Info {
        public static final String SKU_NUM = "skuNum";                              // varchar(32) 条码 (Primary Key 1)
        public static final String AID = "aid";                                     // int 企业aid    (Primary Key 2)
        public static final String UNION_PRI_ID = "unionPriId";                     // int 创建商品的unionPriId (Primary Key 3)
        public static final String SKU_ID = "skuId";                                // bigInt SKU id
        public static final String SORT = "sort";                                   // 排序方式
        public static final String PD_ID = "pdId";                                  // 商品id
    }

    public static final FaiList<String> MANAGE_FIELDS; // 管理态字段
    public static final FaiList<String> VISITOR_FIELDS; // 访客态字段
    static {
        MANAGE_FIELDS = new FaiList<String>(
                Arrays.asList(
                        Info.SKU_NUM
                )
        );

        VISITOR_FIELDS = new FaiList<String>();
    }
}

