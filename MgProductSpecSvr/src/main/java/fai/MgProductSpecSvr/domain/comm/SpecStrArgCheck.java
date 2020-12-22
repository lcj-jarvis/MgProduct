package fai.MgProductSpecSvr.domain.comm;

import fai.MgProductSpecSvr.domain.entity.SpecStrValObj;

/**
 * 规格相关字符串校验
 */
public class SpecStrArgCheck {

    /**
     * 校验名称
     * @param name
     * @return
     */
    public static boolean isValidName(String name) {
        if(name == null){
            return false;
        }
        int length = name.length();
        return length >= SpecStrValObj.Limit.Name.MIN_LEN && length <= SpecStrValObj.Limit.Name.MAX_LEN;
    }

}
