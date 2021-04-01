package fai.MgProductInfSvr.interfaces.comm;


import fai.MgProductInfSvr.interfaces.entity.MgProductSkuImport;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;
import fai.comm.util.Errno;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.Str;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 导入sku维度的商品数据检查
 */
public class MgProductSkuImportCheck {
    public static int check(FaiList<Param> list){
        if(list == null || list.isEmpty()){
            return Errno.ARGS_ERROR;
        }
        Map<Integer/*rlPdId*/, Set<String>/*specNameSet*/> rlPdIdSpecNameSet = new HashMap<Integer, Set<String>>();
        Map<Integer/*rlPdId*/, Map<Integer/*index*/, Set<String>/*specValSet*/>> rlPdIdIndexSpecValSetMap = new HashMap<Integer, Map<Integer, Set<String>>>();
        Integer lastRlPdId = -1;
        Set<String> numSet = new HashSet<String>();
        for (Param info : list) {
            Integer rlPdId = info.getInt(MgProductSkuImport.Info.RL_PD_ID);
            Set<String> specNameSet = null;
            boolean isFirst = false;
            if(lastRlPdId != rlPdId){
                specNameSet = rlPdIdSpecNameSet.get(rlPdId);
                if(isFirst=(specNameSet == null)){
                    specNameSet = new HashSet<String>();
                    rlPdIdSpecNameSet.put(rlPdId, specNameSet);
                }
                lastRlPdId = rlPdId;
            }
            FaiList<String> specNameList = info.getList(MgProductSkuImport.Info.SPEC_NAME_LIST);
            if(isFirst){
                for (String specName : specNameList) {
                    if(Str.isEmpty(specName)){
                        return Errno.ARGS_ERROR;
                    }
                    if(specName.length() > ProductSpecValObj.Spec.Limit.Name.MAX_LEN){
                        return Errno.LEN_LIMIT;
                    }
                    if(specNameSet.contains(specName)){
                        return Errno.ALREADY_EXISTED;
                    }
                    specNameSet.add(specName);
                }
            }else{
                for (String specName : specNameList) {
                    if(!specNameSet.contains(specName)){
                        return Errno.NOT_FOUND;
                    }
                }
            }
            FaiList<String> specValList = info.getList(MgProductSkuImport.Info.SPEC_VAL_LIST);
            if(specNameSet.size() != specValList.size()){
                return Errno.ARGS_ERROR;
            }
            Map<Integer, Set<String>> indexSpecValSetMap = rlPdIdIndexSpecValSetMap.get(rlPdId);
            for (int i = 0; i < specValList.size(); i++) {
                String specVal = specValList.get(i);
                if(Str.isEmpty(specVal)){
                    return Errno.ARGS_ERROR;
                }
                if(specVal.length() > ProductSpecValObj.Spec.Limit.Name.MAX_LEN){
                    return Errno.LEN_LIMIT;
                }
                Set<String> specValSet = indexSpecValSetMap.get(i);
                if(specValSet == null){
                    specValSet = new HashSet<String>();
                    indexSpecValSetMap.put(i, specNameSet);
                }
                specValSet.add(specVal);
                if(specValSet.size() > ProductSpecValObj.Spec.Limit.InPdScValList.MAX_SIZE){
                    return Errno.LEN_LIMIT;
                }
            }
            FaiList<String> pdNumList = info.getList(MgProductSkuImport.Info.PD_NUM_LIST);
            FaiList<String> skuNumList = info.getList(MgProductSkuImport.Info.SKU_NUM_LIST);
            int rt;
            if ((rt = checkNumList(numSet, pdNumList)) != Errno.OK) return rt;
            if ((rt = checkNumList(numSet, skuNumList)) != Errno.OK) return rt;
        }
        return Errno.OK;
    }

    private static Integer checkNumList(Set<String> numSet, FaiList<String> numList) {
        int rt;
        for (String num : numList) {
            if ((rt = checkNum(num)) != Errno.OK) return rt;
            if(numSet.contains(num)){
                return Errno.ALREADY_EXISTED;
            }
            numSet.add(num);
        }
        return rt = Errno.OK;
    }

    private static int checkNum(String num) {
        if(Str.isEmpty(num)){
            return Errno.ARGS_ERROR;
        }
        if(num.length() > ProductSpecValObj.SpecSku.Limit.SkuNum.MAX_LEN){
            return Errno.LEN_LIMIT;
        }
        return Errno.OK;
    }
}
