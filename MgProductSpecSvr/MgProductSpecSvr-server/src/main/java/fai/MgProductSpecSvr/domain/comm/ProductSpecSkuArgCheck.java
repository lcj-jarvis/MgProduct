package fai.MgProductSpecSvr.domain.comm;

import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuValObj;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuArgCheck {

    /**
     */
    public static boolean isValidSkuNum(String skuNum) {
        if(skuNum == null){
            return false;
        }
        int length = skuNum.length();
        return length >= ProductSpecSkuValObj.Limit.SkuNum.MIN_LEN && length <= ProductSpecSkuValObj.Limit.SkuNum.MAX_LEN;
    }
}
