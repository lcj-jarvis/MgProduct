package fai.MgProductInfSvr.domain.comm;

import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;

/**
 * 规格服务数据检查类
 */
public class ProductSpecCheck {
    /**
     * 产品规格
     */
    public static final class Spec {
        public static boolean checkName(String name){
           return SpecStr.checkName(name);
        }

    }
    /**
     * 产品规格SKU
     */
    public static final class SpecSku{
        public static boolean checkSkuCode(String skuCode){
            if(skuCode == null){
                return true;
            }
            return skuCode.length() >= ProductSpecValObj.SpecSku.Limit.SkuCode.MIN_LEN && skuCode.length() <= ProductSpecValObj.SpecSku.Limit.SkuCode.MAX_LEN;
        }

    }
    private static final class SpecStr{
        public static boolean checkName(String name){
            if(name == null){
                return false;
            }
            return name.length() >= ProductSpecValObj.SpecStr.Limit.Name.MIN_LEN && name.length() <= ProductSpecValObj.SpecStr.Limit.Name.MAX_LEN;
        }
    }

    public static void main(String[] args) {
        System.out.println(Spec.checkName("女短袖（多规格产品示例123123）"));
    }
}
