package fai.MgProductInfSvr.domain.comm;


import fai.MgProductInfSvr.interfaces.entity.MgProductSkuImport;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;
import fai.comm.util.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 导入sku维度的商品数据检查
 */
public class MgProductSkuImportCheck {
    /**
     * 检查导入数据
     */
    public static int check(int flow, int aid, FaiList<Param> list, Set<String> skuNumSet){
        int rt = Errno.ARGS_ERROR;
        if(list == null || list.isEmpty()){
            Log.logStd(rt,"list error;flow=%s;aid=%s;", flow, aid);
            return rt;
        }
        // 收集条码
        Set<String> numSet = skuNumSet;
        Map<Integer/*rlPdId*/, Set<String>/*specNameSet*/> rlPdIdSpecNameSet = new HashMap<Integer, Set<String>>();
        Map<Integer/*rlPdId*/, Map<Integer/*index*/, Set<String>/*specValSet*/>> rlPdIdIndexSpecValSetMap = new HashMap<Integer, Map<Integer, Set<String>>>();
        Integer lastRlPdId = -1;
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
            // 检查规格
            FaiList<String> specNameList = info.getList(MgProductSkuImport.Info.SPEC_NAME_LIST);
            if(isFirst){
                for (String specName : specNameList) {
                    if(Str.isEmpty(specName)){
                        Log.logStd(rt,"specName error;flow=%s;aid=%s;", flow, aid);
                        return rt;
                    }
                    if(specName.length() > ProductSpecValObj.Spec.Limit.Name.MAX_LEN){
                        rt = Errno.LEN_LIMIT;
                        Log.logStd(rt,"specName.length error;flow=%s;aid=%s;", flow, aid);
                        return rt;
                    }
                    if(specNameSet.contains(specName)){
                        rt = Errno.ALREADY_EXISTED;
                        Log.logStd(rt,"specName alreadyExisted error;flow=%s;aid=%s;specName=%s;", flow, aid, specName);
                        return rt;
                    }
                    specNameSet.add(specName);
                }
            }else{
                for (String specName : specNameList) {
                    if(!specNameSet.contains(specName)){
                        rt = Errno.NOT_FOUND;
                        Log.logStd(rt,"specName notFound error;flow=%s;aid=%s;specName=%s;", flow, aid, specName);
                        return rt;
                    }
                }
            }

            //检查规格值
            FaiList<String> specValList = info.getList(MgProductSkuImport.Info.SPEC_VAL_LIST);
            if(specNameSet.size() != specValList.size()){
                Log.logStd(rt,"no match error;flow=%s;aid=%s;", flow, aid);
                return rt;
            }
            Map<Integer, Set<String>> indexSpecValSetMap = rlPdIdIndexSpecValSetMap.get(rlPdId);
            for (int i = 0; i < specValList.size(); i++) {
                String specVal = specValList.get(i);
                if(Str.isEmpty(specVal)){
                    Log.logStd(rt,"specVal isEmpty error;flow=%s;aid=%s;", flow, aid);
                    return rt;
                }
                if(specVal.length() > ProductSpecValObj.Spec.Limit.Name.MAX_LEN){
                    rt = Errno.LEN_LIMIT;
                    Log.logStd(rt,"specVal.length error;flow=%s;aid=%s;", flow, aid);
                    return rt;
                }
                Set<String> specValSet = indexSpecValSetMap.get(i);
                if(specValSet == null){
                    specValSet = new HashSet<String>();
                    indexSpecValSetMap.put(i, specNameSet);
                }
                specValSet.add(specVal);
                if(specValSet.size() > ProductSpecValObj.Spec.Limit.InPdScValList.MAX_SIZE){
                    rt = Errno.SIZE_LIMIT;
                    Log.logStd(rt,"specVal sizeLimit error;flow=%s;aid=%s;", flow, aid);
                    return rt;
                }
            }
            // 检查条码
            FaiList<String> pdNumList = info.getList(MgProductSkuImport.Info.PD_CODE_LIST);
            FaiList<String> skuNumList = info.getList(MgProductSkuImport.Info.SKU_CODE_LIST);
            if ((rt = checkNumList(flow, aid, numSet, pdNumList)) != Errno.OK) return rt;
            if ((rt = checkNumList(flow, aid, numSet, skuNumList)) != Errno.OK) return rt;
        }
        return Errno.OK;
    }

    private static Integer checkNumList(int flow, int aid, Set<String> numSet, FaiList<String> numList) {
        int rt;
        for (String num : numList) {
            if ((rt = checkNum(flow, aid, num)) != Errno.OK) return rt;
            if(numSet.contains(num)){
                rt = Errno.ALREADY_EXISTED;
                Log.logStd(rt,"num alreadyExisted error;flow=%s;aid=%s;num=%s", flow, aid, num);
                return rt;
            }
            numSet.add(num);
        }
        return rt = Errno.OK;
    }

    private static int checkNum(int flow, int aid, String num) {
        int rt;
        if(Str.isEmpty(num)){
            rt = Errno.ARGS_ERROR;
            Log.logStd(rt,"num error;flow=%s;aid=%s;num=%s", flow, aid, num);
            return rt;
        }
        if(num.length() > ProductSpecValObj.SpecSku.Limit.SkuNum.MAX_LEN){
            rt = Errno.LEN_LIMIT;
            Log.logStd(rt,"num length error;flow=%s;aid=%s;num=%s", flow, aid, num);
            return rt;
        }
        return Errno.OK;
    }
}
