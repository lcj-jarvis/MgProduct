package fai.MgProductBasicSvr.domain.common;

import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.comm.util.Str;

public class MgProductCheck {

    public static boolean checkProductName(String name) {
        if(Str.isEmpty(name)) {
            return false;
        }
        if(name.length() > ProductValObj.Limit.NAME_MAXLEN) {
            return false;
        }
        return true;
    }

}
