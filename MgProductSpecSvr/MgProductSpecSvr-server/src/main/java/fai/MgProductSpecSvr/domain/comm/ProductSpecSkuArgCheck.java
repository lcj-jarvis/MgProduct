package fai.MgProductSpecSvr.domain.comm;

import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuValObj;

/**
 * 产品规格SKU
 */
public class ProductSpecSkuArgCheck {

    /**
     * 校验skuCode
     */
    public static boolean isValidSkuCode(String skuCode) {
        if(skuCode == null){
            return false;
        }
        int length = skuCode.length();
        return length >= ProductSpecSkuValObj.Limit.SkuCode.MIN_LEN && length <= ProductSpecSkuValObj.Limit.SkuCode.MAX_LEN;
    }
}
