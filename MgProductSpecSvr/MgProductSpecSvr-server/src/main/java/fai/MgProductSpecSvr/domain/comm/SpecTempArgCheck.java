package fai.MgProductSpecSvr.domain.comm;


import fai.MgProductSpecSvr.interfaces.entity.SpecTempValObj;

public class SpecTempArgCheck {

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
        return length >= SpecTempValObj.Limit.Name.MIN_LEN && length <= SpecTempValObj.Limit.Name.MAX_LEN;
    }

}
