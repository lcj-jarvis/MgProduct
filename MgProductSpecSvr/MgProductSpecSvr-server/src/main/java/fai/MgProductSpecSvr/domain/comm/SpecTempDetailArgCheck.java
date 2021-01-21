package fai.MgProductSpecSvr.domain.comm;


import fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj;
import fai.comm.util.FaiList;
import fai.comm.util.Param;

public class SpecTempDetailArgCheck {


    public static boolean isInScValList(FaiList<Param> inScValList) {
        if(inScValList == null){
            return false;
        }
        return inScValList.size() <= SpecTempDetailValObj.Limit.InScValList.MAX_SIZE;
    }

}
