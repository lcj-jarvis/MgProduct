package fai.MgProductSpecSvr.application.service;


import fai.MgProductSpecSvr.domain.comm.*;
import fai.MgProductSpecSvr.domain.entity.*;
import fai.MgProductSpecSvr.domain.repository.ProductSpecDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuNumDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecProc;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecSkuNumProc;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecSkuProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecStrProc;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuNumDao;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class ProductSpecService extends ServicePub {
    /**
     * 导入规格模板
     */
    public int importPdScInfo(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, Param tpScDetailInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || unionPriId <= 0 || pdId <= 0 || Str.isEmpty(tpScDetailInfo)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;pdId=%s;tpScDetailInfo=%s;", flow, aid, tid, pdId, tpScDetailInfo);
                return rt;
            }
            FaiList<Param> tpScDetailInfoList = tpScDetailInfo.getList("detailList");
            if(tpScDetailInfoList == null || tpScDetailInfoList.isEmpty()){
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;tpScDetailInfoList=%s;", flow, aid, pdId, tpScDetailInfoList);
                return rt = Errno.ARGS_ERROR;
            }
            FaiList<Param> pdScInfoList = new FaiList<>(tpScDetailInfoList.size());
            tpScDetailInfoList.forEach((tpScDetail)->{
                Param data = new Param();
                pdScInfoList.add(data);

                data.setInt(ProductSpecEntity.Info.SC_STR_ID, tpScDetail.getInt(SpecTempDetailEntity.Info.SC_STR_ID));
                data.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
                data.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                data.setInt(ProductSpecEntity.Info.SORT, tpScDetail.getInt(SpecTempDetailEntity.Info.SORT));
                data.setInt(ProductSpecEntity.Info.FLAG, 0);
                data.setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, tpScDetail.getList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST));
            });
            ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        productSpecDaoCtrl.setAutoCommit(false);
                        rt = productSpecProc.batchDel(aid, pdId, null, null);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecProc.batchAdd(aid, pdId, pdScInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            productSpecDaoCtrl.rollback();
                            productSpecDaoCtrl.clearIdBuilderCache(aid);
                            return rt;
                        }
                        productSpecDaoCtrl.commit();
                    }
                    productSpecProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                productSpecDaoCtrl.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            session.write(rt);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量同步spu数据到sku
     * @return
     */
    public int batchSynchronousSPU2SKU(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> spuInfoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || unionPriId<=0 || spuInfoList == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;unionPriId=%s;", flow, aid, tid, unionPriId);
                return rt;
            }
            long startTime = System.currentTimeMillis();
            Set<String> specStrNameSet = new HashSet<>();
            // 添加默认规格值的name
            specStrNameSet.add(ProductSpecValObj.Default.InPdScValList.NAME);
            // 检查数据
            for (Param spuInfo : spuInfoList) {
                int pdId = spuInfo.getInt(ProductSpecEntity.Info.PD_ID, 0);
                if(pdId <= 0){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("arg pd err;flow=%d;aid=%d;tid=%s;unionPriId=%s;spuInfo=%s;", flow, aid, tid, unionPriId, spuInfo);
                    return rt;
                }
                trimProductSpecStrName(spuInfo);
                String name = spuInfo.getString(SpecStrEntity.Info.NAME);
                if(!SpecStrArgCheck.isValidName(name)){
                    Log.logErr("arg name err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                    return Errno.ARGS_ERROR;
                }
                specStrNameSet.add(name);
                FaiList<Param> inPdScValList = spuInfo.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                if(inPdScValList == null || inPdScValList.size() > 1){
                    Log.logErr("arg inPdScValList err;flow=%d;aid=%d;pdId=%s;inPdScValList=%s;", flow, aid, pdId, inPdScValList);
                    return Errno.ARGS_ERROR;
                }
                if(!inPdScValList.isEmpty()){
                    Param inPdScVal = inPdScValList.get(0);
                    String valName = inPdScVal.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(valName)){
                        Log.logErr("arg valName err;flow=%d;aid=%d;pdId=%s;valName=%s", flow, aid, pdId, valName);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameSet.add(valName);
                }
            }
            // 获取规格字符串 值-id map
            Param specStrNameIdMap = new Param(true);
            SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
            try {
                SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(specStrNameSet), specStrNameIdMap);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                specStrDaoCtrl.closeDao();
            }

            // 组装成sku
            Map<Integer, Param> pdId_pdScInfoMap = new HashMap<>(spuInfoList.size()*4/3+1);
            Map<Integer, Param> pdId_pdScSkuInfoMap = new HashMap<>(spuInfoList.size()*4/3+1);
            for (Param spuInfo : spuInfoList) {
                int pdId = spuInfo.getInt(ProductSpecEntity.Info.PD_ID);
                String name = spuInfo.getString(SpecStrEntity.Info.NAME);
                // 获取规格字符串 id
                int specStrId = specStrNameIdMap.getInt(name);
                FaiList<Param> inPdScValList = spuInfo.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                FaiList<Integer> inPdScStrIdList = new FaiList<>(1);
                if(!inPdScValList.isEmpty()){
                    Param inPdScVal = inPdScValList.get(0);
                    String valName = (String) inPdScVal.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);

                    int valScStrId = specStrNameIdMap.getInt(valName);
                    inPdScVal.setInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID, valScStrId);
                    inPdScStrIdList.add(valScStrId);
                }
                {
                    Param pdScInfo = new Param();
                    pdScInfo.setInt(ProductSpecEntity.Info.SC_STR_ID, specStrId);
                    pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
                    pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                    pdScInfo.setInt(ProductSpecEntity.Info.SORT, ProductSpecValObj.Default.SORT);
                    pdScInfo.setInt(ProductSpecEntity.Info.FLAG, ProductSpecValObj.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY|ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED);
                    pdScInfo.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, inPdScValList.toJson());
                    pdId_pdScInfoMap.put(pdId, pdScInfo);
                }
                {
                    Param pdScSkuInfo = new Param();
                    pdScSkuInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid);
                    pdScSkuInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                    pdScSkuInfo.setInt(ProductSpecSkuEntity.Info.SORT, ProductSpecValObj.Default.SORT);
                    int flag = ProductSpecSkuValObj.FLag.ALLOW_EMPTY;
                    pdScSkuInfo.setInt(ProductSpecSkuEntity.Info.FLAG, flag);
                    pdScSkuInfo.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
                    pdId_pdScSkuInfoMap.put(pdId, pdScSkuInfo);
                }
            }
            Map<Integer, Long> pdIdSkuIdMap = new HashMap<>(spuInfoList.size()*4/3+1);
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(productSpecDaoCtrl)){
                    Log.logErr("register ProductSpecDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                if(!transactionCtrl.register(productSpecSkuDaoCtrl)){
                    Log.logErr("register ProductSpecSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        Map<Integer, Integer> pdId_pdScIdMap = new HashMap<>(spuInfoList.size()*4/3+1);
                        rt = productSpecProc.batchSynchronousSPU2SKU(aid, pdId_pdScInfoMap, pdId_pdScIdMap);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecSkuProc.batchSynchronousSPU2SKU(aid, pdId_pdScSkuInfoMap, pdIdSkuIdMap);
                        if(rt != Errno.OK){
                            return rt;
                        }

                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                        productSpecProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiList<Param> simplePdScSkuInfoList = new FaiList<>(spuInfoList.size());
            pdIdSkuIdMap.forEach((pdId, skuId)->{
                simplePdScSkuInfoList.add(
                  new Param()
                          .setInt(ProductSpecSkuEntity.Info.PD_ID, pdId)
                          .setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId)
                );
            });
            sendPdSkuScInfoList(session, simplePdScSkuInfoList);
            long endTime = System.currentTimeMillis();
            Log.logStd("ok;flow=%d;aid=%d;consume=%s;pdIdSkuIdMap=%s;", flow, aid, (endTime-startTime), pdIdSkuIdMap);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int unionSetPdScInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, FaiList<Param> addPdScInfoList,
                                    FaiList<Integer> delPdScIdList, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || pdId <= 0 || unionPriId<=0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;pdId=%s;", flow, aid, tid, pdId);
                return rt;
            }
            boolean hasAdd = addPdScInfoList != null && !addPdScInfoList.isEmpty();
            boolean hasDel = delPdScIdList != null && !delPdScIdList.isEmpty();
            boolean hasSet = updaterList != null && !updaterList.isEmpty();
            if(!hasAdd && !hasDel && !hasSet){
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList);
                return rt;
            }

            if(hasAdd){
                rt = checkAndReplaceAddPdScInfoList(flow, aid, tid, unionPriId, pdId, addPdScInfoList);
                if(rt != Errno.OK){
                    return rt;
                }
            }

            Map<ProductSpecValKey, ProductSpecValKey> oldNewProductSpecValKeyMap = new HashMap<>();
            if(hasSet){
                rt = checkAndReplaceUpdaterList(flow, aid, pdId, updaterList, oldNewProductSpecValKeyMap);
                if(rt != Errno.OK){
                    return rt;
                }
            }

            Ref<FaiList<Param>> productSpecSkuListRef = new Ref<>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(productSpecDaoCtrl, productSpecSkuDaoCtrl, productSpecSkuNumDaoCtrl)){
                    return rt=Errno.ERROR;
                }
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        Ref<Boolean> needReFreshSkuRef = new Ref<>(false);
                        if (hasDel) {
                            rt = productSpecProc.batchDel(aid, pdId, delPdScIdList, needReFreshSkuRef);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (hasAdd) {
                            rt = productSpecProc.batchAdd(aid, pdId, addPdScInfoList, needReFreshSkuRef);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (hasSet) {
                            productSpecProc.batchSet(aid, pdId, updaterList, needReFreshSkuRef);
                        }
                        Ref<FaiList<Param>> pdScInfoListRef = new Ref<>();
                        rt = productSpecProc.getListFromDaoByPdScIdList(aid, pdId, null, pdScInfoListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        boolean allowInPdScValListIsEmpty = false;
                        for (Param pdScInfo : pdScInfoListRef.value) {
                            int flag = pdScInfo.getInt(ProductSpecEntity.Info.FLAG, 0);
                            allowInPdScValListIsEmpty |= Misc.checkBit(flag, ProductSpecValObj.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY);
                        }
                        // 如果存在 允许 InPdScValList 为空的规格时，规格只能有一个
                        if(allowInPdScValListIsEmpty && pdScInfoListRef.value.size() != 1){
                            rt = Errno.ARGS_ERROR;
                            Log.logErr(rt,"arg 1 error;flow=%s;aid=%s;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s;", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList);
                            return rt;
                        }
                        Map<Integer, Integer> idxPdScIdMap = new HashMap<>();
                        FaiList<FaiList<Integer>> skuList = genSkuList(pdScInfoListRef.value, idxPdScIdMap, ProductSpecSkuValObj.Limit.SINGLE_PRODUCT_MAX_SIZE, ProductSpecSkuValObj.Limit.InPdScValIdSkuList.MAX_SIZE, allowInPdScValListIsEmpty);
                        if (skuList == null) {
                            return rt = Errno.SIZE_LIMIT;
                        }
                        if(allowInPdScValListIsEmpty){
                            if(skuList.size() != 1){
                                rt = Errno.ARGS_ERROR;
                                Log.logErr(rt,"arg 2 error;flow=%s;aid=%s;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s;skuList=%s;", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList, skuList);
                                return rt;
                            }
                            rt = productSpecSkuProc.updateAllowEmptySku(aid, tid, unionPriId, pdId, skuList.get(0));
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }else{
                            if (!needReFreshSkuRef.value) {
                                HashMap<String, FaiList<Integer>> newSkuMap = new HashMap<>( skuList.size() * 4 / 3 +1);
                                skuList.forEach(sku -> {
                                    FaiList<Integer> tmpSku = new FaiList<>(sku);
                                    Collections.sort(tmpSku); // 排序
                                    newSkuMap.put(tmpSku.toJson(), sku); // toJson
                                });


                                Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
                                rt = productSpecSkuProc.getListFromDao(aid, pdId, pdScSkuInfoListRef);
                                if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                                    return rt;
                                }
                                FaiList<Param> pdScSkuInfoList = pdScSkuInfoListRef.value;
                                // 获取需要 删除 或 替换 的sku
                                FaiList<Long> delSkuIdList = new FaiList<>();
                                FaiList<ParamUpdater> skuUpdaterList = new FaiList<>();
                                for (Param pdScSkuInfo : pdScSkuInfoList) {
                                    FaiList<Integer> oldInPdScStrIdList = pdScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);

                                    FaiList<Integer> tmpInPdScSrtIdList = null;
                                    for (int i = 0; i < oldInPdScStrIdList.size(); i++) {
                                        Integer oldScStrId = oldInPdScStrIdList.get(i);
                                        if(!oldNewProductSpecValKeyMap.isEmpty()){
                                            Integer pdScId = idxPdScIdMap.get(i);
                                            ProductSpecValKey newPdScValKey = oldNewProductSpecValKeyMap.get(new ProductSpecValKey(pdScId, oldScStrId));
                                            if(newPdScValKey != null){
                                                int newScStrId = newPdScValKey.inPdScValListScStrId;
                                                if(tmpInPdScSrtIdList == null){
                                                    tmpInPdScSrtIdList = oldInPdScStrIdList.clone();
                                                }
                                                tmpInPdScSrtIdList.set(i, newScStrId);
                                            }
                                        }
                                    }
                                    if(tmpInPdScSrtIdList != null){ // 说明是规格值修改名称
                                        Log.logDbg("whalelog   tmpInPdScSrtIdList=%s", tmpInPdScSrtIdList);
                                        ParamUpdater updater = new ParamUpdater();
                                        updater.getData().assign(pdScSkuInfo, ProductSpecSkuEntity.Info.SKU_ID);
                                        // toJson
                                        updater.getData().setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, tmpInPdScSrtIdList.toJson());
                                        skuUpdaterList.add(updater);
                                    }
                                    if(tmpInPdScSrtIdList == null){
                                        tmpInPdScSrtIdList = oldInPdScStrIdList.clone();
                                    }
                                    // 排序
                                    Collections.sort(tmpInPdScSrtIdList);
                                    // toJson
                                    String oldSkuKey = tmpInPdScSrtIdList.toJson();
                                    // 从新的sku key集中移除 旧的sku key ，如果不存在旧的sku key,则当前的sku需要删除
                                    if (newSkuMap.remove(oldSkuKey) == null) {
                                        Log.logDbg("whalelog oldSkuKey=%s", oldSkuKey);
                                        delSkuIdList.add(pdScSkuInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID));
                                    }
                                }

                                if(delSkuIdList.size() > 0){
                                    // 删除没用的sku
                                    rt = productSpecSkuProc.batchSoftDel(aid, pdId, delSkuIdList);
                                    if (rt != Errno.OK) {
                                        return rt;
                                    }
                                }

                                if(skuUpdaterList.size() > 0){
                                    // 修改替换名称的sku
                                    rt = productSpecSkuProc.batchSet(aid, pdId, skuUpdaterList);
                                    if(rt != Errno.OK){
                                        return rt;
                                    }
                                }

                                if(newSkuMap.size() > 0){
                                    // 需要新增的sku
                                    FaiList<Param> addPdScSkuInfoList = new FaiList<>(newSkuMap.size());
                                    newSkuMap.forEach((skuKey, sku) -> {
                                        addPdScSkuInfoList.add(
                                                new Param()
                                                        .setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, sku)
                                                        .setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid)
                                                        .setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId)
                                        );
                                    });
                                    rt = productSpecSkuProc.batchAdd(aid, pdId, addPdScSkuInfoList);
                                    if (rt != Errno.OK) {
                                        return rt;
                                    }
                                }

                            } else {
                                rt = productSpecSkuProc.refreshSku(aid, tid, unionPriId, pdId, skuList);
                                if (rt != Errno.OK) {
                                    Log.logDbg(rt,"refreshSku err aid=%s;pdId=%s;", aid, pdId);
                                    return rt;
                                }
                            }
                        }

                        rt = productSpecSkuNumProc.batchDel(aid, unionPriId, productSpecSkuProc.getDeletedSkuIdList());
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        Log.logErr(e, "flow=%d;aid=%d;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList);
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                    productSpecProc.deleteDirtyCache(aid);
                    productSpecSkuProc.deleteDirtyCache(aid);
                    productSpecSkuNumProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
                rt = productSpecSkuProc.getListFromDao(aid, pdId, productSpecSkuListRef, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.FLAG);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiList<Param> productSpecSkuList = productSpecSkuListRef.value;
            Log.logDbg("flow=%d;aid=%d;pdId=%d;productSpecSkuList=%s", flow, aid, pdId, productSpecSkuList);

            rt = Errno.OK;
            sendPdSkuScInfoList(session, productSpecSkuList);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int batchDelPdAllSc(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList, boolean softDel) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;pdIdList=%s;", flow, aid, tid, pdIdList);
                return rt;
            }

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(productSpecDaoCtrl, productSpecSkuNumDaoCtrl, productSpecSkuDaoCtrl)){
                    return rt=Errno.ERROR;
                }

                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        if(!softDel){
                            rt = productSpecProc.batchDel(aid, pdIdList);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        rt = productSpecSkuProc.batchDel(aid, pdIdList, softDel);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecSkuNumProc.batchDel(aid, null, productSpecSkuProc.getDeletedSkuIdList());
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                    productSpecProc.deleteDirtyCache(aid);
                    productSpecSkuProc.deleteDirtyCache(aid);
                    productSpecSkuNumProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            session.write(rt);
            Log.logStd("ok;flow=%d;aid=%d;pdIdList=%s;softDel=%s;", flow, aid, pdIdList, softDel);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getPdScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                rt = productSpecProc.getList(aid, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecDaoCtrl.closeDao();
            }

            FaiList<Param> pdScInfoList = listRef.value;
            rt = initPdScSpecStr(flow, aid, pdScInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSpecStr err;aid=%d;pdId=%d;", aid, pdId);
                return rt;
            }
            sendPdScInfoList(session, pdScInfoList);
            Log.logDbg("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int getPdCheckedScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                rt = productSpecProc.getList(aid, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecDaoCtrl.closeDao();
            }

            FaiList<Param> pdScInfoList = listRef.value;
            pdScInfoList.forEach(pdSpInfo->{
                FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                for (int i = inPdScValList.size() - 1; i >= 0; i--) {
                    Param inpdscValInfo = inPdScValList.get(i);
                    if(!inpdscValInfo.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                        inPdScValList.remove(i);
                    }
                }
            });
            rt = initPdScSpecStr(flow, aid, pdScInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSpecStr err;aid=%d;pdId=%d;", aid, pdId);
                return rt;
            }
            sendPdScInfoList(session, pdScInfoList);
            Log.logDbg("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 批量生成代表spu的sku数据
     */
    public int batchGenSkuRepresentSpuInfo(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(pdIdList) || unionPriId<= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdIdList=%s;", flow, aid, pdIdList);
                return rt;
            }
            Map<Integer, Long> pdIdSkuIdMap = new HashMap<>(pdIdList.size()*4/3+1);
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        productSpecSkuDaoCtrl.setAutoCommit(false);
                        rt = productSpecSkuProc.batchGenSkuRepresentSpuInfo(aid, tid, unionPriId, pdIdList, pdIdSkuIdMap);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            productSpecSkuDaoCtrl.rollback();
                        }
                        productSpecSkuDaoCtrl.commit();
                        productSpecSkuProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            FaiList<Param> skuIdInfoList = new FaiList<>(pdIdList.size());
            pdIdSkuIdMap.forEach((pdId, skuId)->{
                skuIdInfoList.add(
                        new Param()
                        .setInt(ProductSpecSkuEntity.Info.PD_ID, pdId)
                        .setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId)
                );
            });
            sendPdSkuScInfoList(session, skuIdInfoList);
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdIdList=%s", flow, aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int setPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;updaterList=%s", flow, aid, pdId, updaterList);
                return rt;
            }
            boolean needGenSpuInfo = false;
            ParamUpdater spuUpdater = null;
            Set<String> inPdScStrNameSet = new HashSet<>();
            Map<FaiList<String>, Param> inPdScStrNameListInfoMap = new HashMap<>();
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    boolean isSpu = data.getBoolean(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SPU, false);
                    if(isSpu){
                        if(needGenSpuInfo){
                            rt = Errno.ARGS_ERROR;
                            Log.logErr("find repeat spuInfo err;flow=%d;aid=%d;pdId=%s;spuUpdater=%s;updater=%s", flow, aid, pdId, spuUpdater, updater);
                            return rt;
                        }
                        needGenSpuInfo = true;
                        spuUpdater = updater;
                        continue;
                    }
                    FaiList<String> inPdScStrNameList = data.getList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        rt = Errno.ARGS_ERROR;
                        Log.logErr("inPdScStrNameList err;flow=%d;aid=%d;pdId=%s;updater=%s", flow, aid, pdId, updater);
                        return rt;
                    }

                    inPdScStrNameSet.addAll(inPdScStrNameList);
                    inPdScStrNameListInfoMap.put(inPdScStrNameList, data);
                }
                FaiList<String> skuNumList = data.getList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST);
                if(skuNumList != null){
                    for (String skuNum : skuNumList) {
                        if(!ProductSpecSkuArgCheck.isValidSkuNum(skuNum)){
                            rt = Errno.ARGS_ERROR;
                            Log.logErr("skuNumList skuNum err;flow=%d;aid=%d;pdId=%s;updater=%s", flow, aid, pdId, updater);
                            return rt;
                        }
                    }
                }
                String skuNum = data.getString(ProductSpecSkuEntity.Info.SKU_NUM);
                if(skuNum != null){
                    if(!ProductSpecSkuArgCheck.isValidSkuNum(skuNum)){
                        rt = Errno.ARGS_ERROR;
                        Log.logErr("skuNum err;flow=%d;aid=%d;pdId=%s;updater=%s", flow, aid, pdId, updater);
                        return rt;
                    }
                    if(skuNumList == null){
                        skuNumList = new FaiList<>(Arrays.asList(skuNum));
                        data.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST, skuNumList);
                    }
                }
            }
            // 获取skuId 并移除 ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST 字段
            if(inPdScStrNameSet.size() != 0){
                Ref<Param> nameIdMapRef = new Ref<>();
                SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
                try {
                    SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
                    rt = specStrProc.getNameIdMapByNames(aid, new FaiList<>(inPdScStrNameSet), nameIdMapRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    specStrDaoCtrl.closeDao();
                }
                Map<String, Param> inPdScStrIdListJsonInfoMap = new HashMap<>(inPdScStrNameListInfoMap.size()*4/3+1);
                Param nameIdMap = nameIdMapRef.value;
                FaiList<String> inPdScStrIdLists = new FaiList<>(inPdScStrNameListInfoMap.size());
                for (Map.Entry<FaiList<String>, Param> entry : inPdScStrNameListInfoMap.entrySet()) {
                    FaiList<String> inPdScStrNameList = entry.getKey();
                    FaiList<Integer> inPdScStrIdList = new FaiList<>(inPdScStrNameList.size());
                    for (String inPdScStrName : inPdScStrNameList) {
                        Integer inPdScStrId = nameIdMap.getInt(inPdScStrName);
                        if(inPdScStrId == null){
                            Log.logErr("err aid=%d;pdId=%s;inPdScStrName=%s;", aid, pdId, inPdScStrName);
                            return Errno.ARGS_ERROR;
                        }
                        inPdScStrIdList.add(inPdScStrId);
                    }
                    String inPdScStrIdListJson = inPdScStrIdList.toJson();
                    inPdScStrIdLists.add(inPdScStrIdListJson);
                    Param data = entry.getValue();
                    data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdListJson);
                    inPdScStrIdListJsonInfoMap.put(inPdScStrIdListJson, data);
                }

                Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                try {
                    ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                    rt = productSpecSkuProc.getListByScStrIdListFromDao(aid, pdId, inPdScStrIdLists, pdScSkuInfoListRef, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "err aid=%d;pdId=%s;inPdScStrIdLists=%s;", aid, pdId, inPdScStrIdLists);
                        return rt;
                    }
                }finally {
                    productSpecSkuDaoCtrl.closeDao();
                }
                if(inPdScStrIdLists.size() != pdScSkuInfoListRef.value.size()){
                    rt = Errno.ERROR;
                    Log.logErr(rt,"size no match;aid=%d;pdId=%s;inPdScStrIdLists.size=%s;dbList.size=%s;", aid, pdId, inPdScStrIdLists.size(), pdScSkuInfoListRef.value.size());
                    return rt;
                }
                for (Param info : pdScSkuInfoListRef.value) {
                    String inPdScStrIdListJson = info.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                    Param data = inPdScStrIdListJsonInfoMap.remove(inPdScStrIdListJson);
                    if(data == null){
                        rt = Errno.ERROR;
                        Log.logErr(rt,"err aid=%d;pdId=%s;inPdScStrIdListJson=%s;", aid, pdId, inPdScStrIdListJson);
                        return rt;
                    }
                    data.assign(info, ProductSpecSkuEntity.Info.SKU_ID);
                    data.remove(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                }
            }
            Long skuIdRepresentSpu = null;
            if(needGenSpuInfo){
                Ref<Long> skuIdRef = new Ref<>();
                rt = genSkuRepresentSpuInfo(flow, aid, tid, unionPriId, pdId, skuIdRef);
                if(rt != Errno.OK){
                    return rt;
                }
                skuIdRepresentSpu = skuIdRef.value;
            }
            FaiList<Param> skuNumSortList = new FaiList<>();
            HashSet<Long> changeSkuNumSkuIdSet = new HashSet<>();
            boolean isFindSpu = false;
            Map<String, Long> newSkuNumSkuIdMap = new HashMap<>();
            FaiList<Long> needDelSkuNumSkuIdList = new FaiList<>();
            for (int i = updaterList.size() - 1; i >= 0; i--) {
                ParamUpdater updater = updaterList.get(i);
                Param data = updater.getData();
                Long skuId = data.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    boolean isSpu = data.getBoolean(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SPU, false);
                    if(!isSpu){
                        rt = Errno.ERROR;
                        Log.logErr(rt,"find not init skuId;aid=%d;pdId=%s;data=%s;", aid, pdId, data);
                    }
                    skuId = skuIdRepresentSpu;
                }
                FaiList<String> skuNumList = (FaiList<String>)data.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST);
                if(skuNumList != null){
                    changeSkuNumSkuIdSet.add(skuId);
                    if(skuNumList.size() > 0){
                        if(skuNumList.size() > ProductSpecSkuNumValObj.Limit.MAX_SIZE){
                            rt = Errno.SIZE_LIMIT;
                            Log.logErr(rt,"found not exist sku;flow=%s;aid=%s;pdId=%s;updater=%s;isFindSpu=%s;", flow, aid, pdId, updater, isFindSpu);
                            return rt;
                        }
                        for (int j = 0; j < skuNumList.size(); j++) {
                            String skuNum = skuNumList.get(j);
                            Long ifAbsent = newSkuNumSkuIdMap.putIfAbsent(skuNum, skuId);
                            if(ifAbsent != null){
                                rt = Errno.ALREADY_EXISTED;
                                Log.logErr(rt,"skuNum repeat;flow=%s;aid=%s;pdId=%s;updater=%s;isFindSpu=%s;skuNum=%s;ifAbsent=%s;", flow, aid, pdId, updater, isFindSpu, skuNum, ifAbsent);
                                return rt;
                            }
                            skuNumSortList.add(
                                    new Param()
                                            .setInt(ProductSpecSkuNumEntity.Info.SORT, j)
                                            .setInt(ProductSpecSkuNumEntity.Info.AID, aid)
                                            .setInt(ProductSpecSkuNumEntity.Info.UNION_PRI_ID, unionPriId)
                                            .setString(ProductSpecSkuNumEntity.Info.SKU_NUM, skuNum)
                            );
                        }
                    }else{
                        needDelSkuNumSkuIdList.add(skuId);
                    }
                }
            }
            Log.logDbg("whalelog  newSkuNumSkuIdMap=%s;needDelSkuNumSkuIdList=%s;", newSkuNumSkuIdMap, needDelSkuNumSkuIdList);

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, transactionCtrl);
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(flow, aid, transactionCtrl);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = productSpecSkuProc.batchSet(aid, pdId, updaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        rt = productSpecSkuNumProc.refresh(aid, unionPriId, pdId, newSkuNumSkuIdMap, needDelSkuNumSkuIdList, skuNumSortList, changeSkuNumSkuIdSet);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                    productSpecSkuProc.deleteDirtyCache(aid);
                    productSpecSkuNumProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            FaiList<Param> pdScSkuList = new FaiList<>();
            pdScSkuList.toBuffer(sendBuf, ProductSpecSkuDto.Key.INFO_LIST, ProductSpecSkuDto.getInfoDto());
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;skuIdRepresentSpu=%s;", flow, aid, pdId, skuIdRepresentSpu);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 生成代表spu的sku数据
     */
    private int genSkuRepresentSpuInfo(int flow, int aid, int tid, int unionPriId, int pdId, Ref<Long> skuIdRef){
        int rt = Errno.ERROR;
        ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
        try {
            ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
            rt = productSpecSkuProc.getSkuIdRepresentSpu(aid, pdId, skuIdRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(skuIdRef.value != null){
                return Errno.OK;
            }
            LockUtil.lock(aid);
            try {
                try {
                    productSpecSkuDaoCtrl.setAutoCommit(false);
                    rt = productSpecSkuProc.genSkuRepresentSpuInfo(aid, unionPriId, tid, pdId, skuIdRef);
                }finally {
                    if(rt != Errno.OK){
                        productSpecSkuProc.clearIdBuilderCache(aid);
                        productSpecSkuDaoCtrl.rollback();
                        return rt;
                    }
                    productSpecSkuDaoCtrl.commit();
                    productSpecSkuProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            productSpecSkuDaoCtrl.closeDao();
        }
        return rt;
    }

    public int getPdSkuScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getList(aid, pdId, pdScSkuInfoListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;pdId=%d;", aid, pdId);
                return rt;
            }
            rt = initPdScSkuNumList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuNumList err;aid=%d;pdId=%d;", aid, pdId);
                return rt;
            }
            sendPdSkuScInfoList(session, psScSkuInfoList, true);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int getPdSkuScInfoListBySkuIdList(FaiSession session, int flow, int aid, FaiList<Long> skuIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || skuIdList == null || skuIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuIdList=%s;", flow, aid, skuIdList);
                return rt;
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getList(aid, skuIdList, pdScSkuInfoListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;skuIdList=%s;", aid, skuIdList);
                return rt;
            }
            rt = initPdScSkuNumList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuNumList err;aid=%d;skuIdList=%s;", aid, skuIdList);
                return rt;
            }
            sendPdSkuScInfoList(session, psScSkuInfoList, true);
            Log.logDbg("flow=%s;aid=%s;skuIdList=%s;", flow, aid, skuIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    /**
     * 获取skuId
     */
    public int getPdSkuIdInfoList(FaiSession session, int flow, int aid, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdIdList=%s", flow, aid, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getListFromDaoByPdIdList(aid, pdIdList, pdScSkuInfoListRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取已经存在的 skuNumList
     */
    public int getExistsSkuNumList(FaiSession session, int flow, int aid, int unionPriId, FaiList<String> skuNumList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || skuNumList == null || skuNumList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuNumList=%s", flow, aid, skuNumList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                rt = productSpecSkuNumProc.getSkuNumListFromDao(aid, unionPriId, skuNumList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuNumDaoCtrl.closeDao();
            }
            FaiList<String> alreadyExistsSkuNumList = Utils.getValList(listRef.value, ProductSpecSkuNumEntity.Info.SKU_NUM);

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putString(ProductSpecSkuDto.Key.SKU_NUM_LIST, alreadyExistsSkuNumList.toJson());
            session.write(sendBuf);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;skuNumList=%s;", flow, aid, unionPriId, skuNumList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int searchPdSkuIdInfoListByLikeSkuNum(FaiSession session, int flow, int aid, int unionPriId, String skuNum) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Str.isEmpty(skuNum)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuNum=%s", flow, aid, skuNum);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                rt = productSpecSkuNumProc.searchByLikeSkuNum(aid, unionPriId, skuNum, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuNumDaoCtrl.closeDao();
            }
            Map<Long, FaiList<String>> skuIdSkuNumListMap = new HashMap<>();
            for (Param info : listRef.value) {
                Long skuId = info.getLong(ProductSpecSkuNumEntity.Info.SKU_ID);
                String tmpSkuNum = info.getString(ProductSpecSkuNumEntity.Info.SKU_NUM);
                FaiList<String> skuNumList = skuIdSkuNumListMap.getOrDefault(skuId, new FaiList<>());
                skuNumList.add(tmpSkuNum);
                if(skuNumList.size() == 1){
                    skuIdSkuNumListMap.put(skuId, skuNumList);
                }
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getList(aid, new FaiList<>(skuIdSkuNumListMap.keySet()), pdScSkuInfoListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }

            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;skuIdSkuNumListMap=%s;", aid, skuIdSkuNumListMap);
                return rt;
            }

            for (Param skuInfo : psScSkuInfoList) {
                Long skuId = skuInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                FaiList<String> skuNumList = skuIdSkuNumListMap.get(skuId);
                skuInfo.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST, skuNumList);
            }
            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;skuNum=%s;skuIdSkuNumListMap=%s;", flow, aid, unionPriId, skuNum, skuIdSkuNumListMap);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取 skuNum数据状态
     */
    public int getSkuNumDataStatus(FaiSession session, int flow, int aid, int unionPriId)throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<Param> dataStatusRef = new Ref<>();
            ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                rt = productSpecSkuNumProc.getDataStatus(aid, unionPriId, dataStatusRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuNumDaoCtrl.closeDao();
            }

            FaiBuffer sendBody = new FaiBuffer();
            dataStatusRef.value.toBuffer(sendBody, ProductSpecSkuNumDao.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
            session.write(sendBody);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取全部数据的部分字段
     */
    public int getSkuNumAllData(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                rt = productSpecSkuNumProc.getAllDataFromDao(aid, unionPriId, listRef, ProductSpecSkuNumEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                productSpecSkuNumDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSkuNum(session, listRef.value, null);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    /**
     * 搜索全部数据的部分字段
     */
    public int searchSkuNumFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {

            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
                rt = productSpecSkuNumProc.searchAllDataFromDao(aid, unionPriId, searchArg, listRef, ProductSpecSkuNumEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                productSpecSkuNumDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSkuNum(session, listRef.value, searchArg);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 导入商品规格和sku信息
     * @param specList 商品规格
     * @param specSkuList 商品规格sku
     */
    public int importPdScWithSku(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> specList, FaiList<Param> specSkuList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Param nameIdMap = new Param(true);
            rt = checkAndReplaceAddPdScInfoList(flow, aid, tid, unionPriId, null, specList, nameIdMap);
            if(rt != Errno.OK){
                return rt;
            }
            // 根据pdId进行分组
            Map<Integer/*pdId*/, List<Param>/*specList*/> pdIdSpecListMap = specList.stream().collect(Collectors.groupingBy(spec->spec.getInt(ProductSpecEntity.Info.PD_ID)));
            // 根据pdId，inPdScStrIdListJson 进行映射，提升效率
            Map<Integer/*pdId*/, Map<String /*inPdScStrIdListJson*/, Param/*specSkuInfo*/>> pdIdInPdScStrIdListJsonSpecSkuMap = new HashMap<>();
            {
                for (Param specSkuInfo : specSkuList) {
                    int pdId = specSkuInfo.getInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.PD_ID);
                    Map<String, Param> inPdScStrIdListJsonSpecSkuMap = pdIdInPdScStrIdListJsonSpecSkuMap.get(pdId);
                    if(inPdScStrIdListJsonSpecSkuMap == null){
                        inPdScStrIdListJsonSpecSkuMap = new HashMap<>();
                        pdIdInPdScStrIdListJsonSpecSkuMap.put(pdId, inPdScStrIdListJsonSpecSkuMap);
                    }
                    boolean spu = specSkuInfo.getBoolean(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SPU, false);
                    if(spu){
                        // 空字符串代表spu
                        inPdScStrIdListJsonSpecSkuMap.put("", specSkuInfo);
                        continue;
                    }
                    FaiList<String> inPdScStrNameList = specSkuInfo.getListNullIsEmpty(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList.isEmpty()){
                        rt = Errno.ARGS_ERROR;
                        Log.logErr(rt, "specSkuInfo err;flow=%s;aid=%s;unionPriId=%s;specSkuInfo=%s;", flow, aid, unionPriId, specSkuInfo);
                        return rt;
                    }
                    FaiList<Integer> inPdScStrIdList = new FaiList<>(inPdScStrNameList.size());
                    for (String pdScStrName : inPdScStrNameList) {
                        int pdScStrId = nameIdMap.getInt(pdScStrName, 0);
                        if(pdScStrId == 0){
                            rt = Errno.ARGS_ERROR;
                            Log.logErr(rt, "specSkuInfo pdScStrName not found;flow=%s;aid=%s;unionPriId=%s;specSkuInfo=%s;pdScStrName=%s;", flow, aid, unionPriId, specSkuInfo, pdScStrName);
                            return rt;
                        }
                        inPdScStrIdList.add(pdScStrId);
                    }
                    inPdScStrIdListJsonSpecSkuMap.put(inPdScStrIdList.toJson(), specSkuInfo);
                }
            }
            // pdId 和 pdScSkuList map
            Map<Integer/*pdId*/, FaiList<Param>/*pdScSkuList*/> pdIdPdScSkuListMap = new HashMap<>();
            for (Map.Entry<Integer, List<Param>> pdIdSpecListEntry : pdIdSpecListMap.entrySet()) {
                int pdId = pdIdSpecListEntry.getKey();
                List<Param> pdScInfoList = pdIdSpecListEntry.getValue();
                FaiList<FaiList<Integer>> skuList = genSkuList(new FaiList<>(pdScInfoList), null, ProductSpecSkuValObj.Limit.SINGLE_PRODUCT_MAX_SIZE, ProductSpecSkuValObj.Limit.InPdScValIdSkuList.MAX_SIZE, false);
                if (skuList == null) {
                    return rt = Errno.SIZE_LIMIT;
                }
                Map<String, Param> inPdScStrIdListJsonSpecSkuMap = pdIdInPdScStrIdListJsonSpecSkuMap.get(pdId);
                FaiList<Param> pdScSkuList = new FaiList<>(skuList.size()+1);
                // 生成一条代表spu的 sku数据 用于后面生成条码
                pdScSkuList.add(
                    new Param()
                            .setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid)
                            .setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId)
                            .setInt(ProductSpecSkuEntity.Info.FLAG, ProductSpecSkuValObj.FLag.SPU)
                );
                for (FaiList<Integer> inPdScStrIdList : skuList) {
                    Param addSpecSkuInfo = new Param();
                    addSpecSkuInfo.setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                    addSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_TID, tid);
                    addSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                    if(inPdScStrIdListJsonSpecSkuMap != null){
                        Param specSkuInfo = inPdScStrIdListJsonSpecSkuMap.get(inPdScStrIdList.toJson());
                        if(specSkuInfo != null){
                            addSpecSkuInfo.assign(specSkuInfo, fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SORT, ProductSpecSkuEntity.Info.SORT);
                        }
                    }
                    pdScSkuList.add(addSpecSkuInfo);
                }
                pdIdPdScSkuListMap.put(pdId, pdScSkuList);
            }
            FaiList<Param> rtPdSkuList = new FaiList<>();
            Map<Integer/*pdId*/,Map<String/*InPdScStrIdListJson*/, Long/*skuId*/>> pdIdInPdScStrIdListJsonSkuIdMap = new HashMap<>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, transactionCtrl);
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, transactionCtrl);
                ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(flow, aid, transactionCtrl);
                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = productSpecProc.batchAdd(aid, pdIdSpecListMap, null);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    rt = productSpecSkuProc.batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    FaiList<Param> skuNumInfoList = new FaiList<>();
                    for (Map.Entry<Integer, Map<String, Param>> entryMap : pdIdInPdScStrIdListJsonSpecSkuMap.entrySet()) {
                        Integer pdId = entryMap.getKey();
                        Map<String, Long> inPdScStrIdListJsonSkuIdMap = pdIdInPdScStrIdListJsonSkuIdMap.get(pdId);
                        Map<String, Param> inPdScStrIdListJsonSpecSkuMap = entryMap.getValue();
                        for (Map.Entry<String, Param> inPdScStrIdListJsonSpecSkuEntry : inPdScStrIdListJsonSpecSkuMap.entrySet()) {
                            String inPdScStrIdListJson = inPdScStrIdListJsonSpecSkuEntry.getKey();
                            Param specSkuInfo = inPdScStrIdListJsonSpecSkuEntry.getValue();
                            FaiList<String> inPdScStrNameList = specSkuInfo.getList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                            Long skuId = inPdScStrIdListJsonSkuIdMap.get(inPdScStrIdListJson);
                            Log.logDbg("inPdScStrIdListJson=%s;skuId=%s",inPdScStrIdListJson, skuId);
                            int flag = 0;
                            Param rtPdSku = new Param();
                            rtPdSkuList.add(rtPdSku);
                            if("".equals(inPdScStrIdListJson)){
                                flag |= ProductSpecSkuValObj.FLag.SPU;
                            }
                            rtPdSku.setLong(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_ID, skuId)
                                    .setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.PD_ID, pdId)
                                    .setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.FLAG, flag);
                            // 当表示为spu时， inPdScStrNameList值为null
                            rtPdSku.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST, inPdScStrNameList);

                            FaiList<String> skuNumList = specSkuInfo.getListNullIsEmpty(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST);
                            if(skuNumList.isEmpty()){
                                continue;
                            }
                            int sort = 0;
                            for (String skuNum : skuNumList) {
                                skuNumInfoList.add(
                                        new Param()
                                                .setInt(ProductSpecSkuNumEntity.Info.PD_ID, pdId)
                                                .setInt(ProductSpecSkuNumEntity.Info.SORT, sort++)
                                                .setLong(ProductSpecSkuNumEntity.Info.SKU_ID, skuId)
                                                .setString(ProductSpecSkuNumEntity.Info.SKU_NUM, skuNum)
                                );
                            }
                        }
                    }
                    if(!skuNumInfoList.isEmpty()){
                        rt = productSpecSkuNumProc.batchAdd(aid, unionPriId, skuNumInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        productSpecProc.clearIdBuilderCache(aid);
                        productSpecSkuProc.clearIdBuilderCache(aid);
                        transactionCtrl.rollback();
                    }
                    transactionCtrl.commit();
                }
                productSpecProc.deleteDirtyCache(aid);
                productSpecSkuProc.deleteDirtyCache(aid);
                productSpecSkuNumProc.deleteDirtyCache(aid);
            }finally {
                transactionCtrl.closeDao();
            }
            Log.logDbg("flow=%d;aid=%d;rtPdSkuList=%s", flow, aid, rtPdSkuList);

            rt = Errno.OK;
            sendPdSkuScInfoList(session, rtPdSkuList);
            Log.logStd("ok;flow=%d;aid=%d;", flow, aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    private void sendSkuNum(FaiSession session, FaiList<Param> infoList, SearchArg searchArg) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, ProductSpecSkuNumDao.Key.INFO_LIST, ProductSpecSkuNumDao.getInfoDto());
        if(searchArg != null && searchArg.totalSize != null){
            sendBuf.putInt(ProductSpecSkuNumDao.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    /**
     * 初始化skuNumList
     */
    private int initPdScSkuNumList(int flow, int aid, FaiList<Param> psScSkuInfoList) {
        if(psScSkuInfoList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        FaiList<Long> skuIdList = Utils.getValList(psScSkuInfoList, ProductSpecSkuEntity.Info.SKU_ID);
        Ref<Map<Long, FaiList<String>>> mapRef = new Ref<>();
        ProductSpecSkuNumDaoCtrl productSpecSkuNumDaoCtrl = ProductSpecSkuNumDaoCtrl.getInstance(flow, aid);
        try {
            ProductSpecSkuNumProc productSpecSkuNumProc = new ProductSpecSkuNumProc(productSpecSkuNumDaoCtrl, flow);
            rt = productSpecSkuNumProc.getList(aid, skuIdList, mapRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
        }finally {
            productSpecSkuNumDaoCtrl.closeDao();
        }
        for (Param info : psScSkuInfoList) {
            Long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            info.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_NUM_LIST, mapRef.value.getOrDefault(skuId, new FaiList<>()));
        }
        return rt;
    }

    /**
     * 初始化规格字符串
     */
    private int initPdScSkuSpecStr(int flow, int aid, FaiList<Param> psScSkuInfoList){
        Set<Integer> scStrIdSet = new HashSet<>();
        psScSkuInfoList.forEach(psScSkuInfo ->{
            scStrIdSet.addAll(psScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST));
        });
        Ref<FaiList<Param>> listRef = new Ref<>();
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            int rt = specStrProc.getList(aid, new FaiList<>(scStrIdSet), listRef);
            if(rt != Errno.OK){
                return rt;
            }
        }finally {
            specStrDaoCtrl.closeDao();
        }
        Map<Integer, Param> idNameMap = Utils.getMap(listRef.value, SpecStrEntity.Info.SC_STR_ID);
        psScSkuInfoList.forEach(psScSkuInfo->{
            FaiList<Integer> inPdScStrIdList = psScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            FaiList<String> inPdScStrNameList = new FaiList<>(inPdScStrIdList.size());
            psScSkuInfo.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST, inPdScStrNameList);
            inPdScStrIdList.forEach(scStrId ->{
                Param scStrInfo = idNameMap.get(scStrId);
                inPdScStrNameList.add(scStrInfo.getString(SpecStrEntity.Info.NAME));
            });
        });
        return Errno.OK;
    }

    private void sendPdSkuScInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
        sendPdSkuScInfoList(session, infoList, false);
    }
    private void sendPdSkuScInfoList(FaiSession session, FaiList<Param> infoList, boolean sort) throws IOException {
        if(sort){
            ParamComparator comparator = new ParamComparator(ProductSpecSkuEntity.Info.SORT);
            comparator.addKey(ProductSpecSkuEntity.Info.SKU_ID);
            comparator.sort(infoList);
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, ProductSpecSkuDto.Key.INFO_LIST, ProductSpecSkuDto.getInfoDto());
        session.write(sendBuf);
    }

    private int initPdScSpecStr(int flow, int aid, FaiList<Param> pdScInfoList){
        Set<Integer> scStrIdSet = new HashSet<>();
        pdScInfoList.forEach(pdSpInfo->{
            scStrIdSet.add(pdSpInfo.getInt(ProductSpecEntity.Info.SC_STR_ID));
            FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            inPdScValList.forEach(inpdscValInfo->{
                scStrIdSet.add(inpdscValInfo.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
            });
        });
        Ref<FaiList<Param>> listRef = new Ref<>();
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            int rt = specStrProc.getList(aid, new FaiList<>(scStrIdSet), listRef);
            if(rt != Errno.OK){
                return rt;
            }
        }finally {
            specStrDaoCtrl.closeDao();
        }

        Map<Integer, Param> idNameMap = Utils.getMap(listRef.value, SpecStrEntity.Info.SC_STR_ID);
        pdScInfoList.forEach(pdSpInfo->{
            Param specStrInfo = idNameMap.get(pdSpInfo.getInt(ProductSpecEntity.Info.SC_STR_ID));
            pdSpInfo.setString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME, specStrInfo.getString(SpecStrEntity.Info.NAME));
            FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            inPdScValList.forEach(inpdscValInfo->{
                Param specStrInfo2 = idNameMap.get(inpdscValInfo.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
                inpdscValInfo.setString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME, specStrInfo2.getString(SpecStrEntity.Info.NAME));
            });
        });
        return Errno.OK;
    }

    private void sendPdScInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
        ParamComparator comparator = new ParamComparator(ProductSpecEntity.Info.SORT);
        comparator.addKey(ProductSpecEntity.Info.PD_SC_ID);
        comparator.sort(infoList);
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.getInfoDto());
        session.write(sendBuf);
    }

    /**
     * 这里只替换 产品规格名称，规格值不替换！
     */
    private int checkAndReplaceUpdaterList(int flow, int aid, int pdId, FaiList<ParamUpdater> updaterList, Map<ProductSpecValKey, ProductSpecValKey> oldNewProductSpecValKeyMap) {
        Set<String> specStrNameSet = new HashSet<>();
        for (ParamUpdater updater : updaterList) {
            Param data = updater.getData();
            trimProductSpecStrName(data);
            Log.logDbg("whalelog   data=%s", data);
            Integer pdScId = data.getInt(ProductSpecEntity.Info.PD_SC_ID);
            if(pdScId == null){
                Log.logErr("arg pdScId err;flow=%d;aid=%d;pdId=%s;pdScId=%s", flow, aid, pdId, pdScId);
                return Errno.ARGS_ERROR;
            }
            String name = data.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
            if(name != null){
                if(!SpecStrArgCheck.isValidName(name)){
                    Log.logErr("arg name err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                    return Errno.ARGS_ERROR;
                }
                specStrNameSet.add(name);
            }
            FaiList<Param> inPdScValList = data.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(inPdScValList != null){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg name err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameSet.add(name);
                }
            }
        }
        Param nameIdMap = new Param(true);
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            try {
                LockUtil.lock(aid);
                int rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(specStrNameSet), nameIdMap);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            specStrDaoCtrl.closeDao();
        }
        Log.logDbg("whalog  nameIdMap=%s");

        for (ParamUpdater updater : updaterList) {
            Param data = updater.getData();
            Integer pdScId = data.getInt(ProductSpecEntity.Info.PD_SC_ID);
            String name = (String)data.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
            if (name != null) {
                Integer scStrId = nameIdMap.getInt(name);
                if(scStrId == null){
                    return Errno.ERROR;
                }
                data.setInt(ProductSpecEntity.Info.SC_STR_ID, scStrId);

            }
            FaiList<Param> inPdScValList = data.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            for (Param inPdScValInfo : inPdScValList) {
                name = (String)inPdScValInfo.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                Integer scStrId = nameIdMap.getInt(name);
                if(scStrId == null){
                    return Errno.ERROR;
                }
                Integer oldScSrcId = inPdScValInfo.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID);
                if (oldScSrcId != null && !scStrId.equals(oldScSrcId)) { // 说明发生了修改
                    oldNewProductSpecValKeyMap.put(new ProductSpecValKey(pdScId, oldScSrcId), new ProductSpecValKey(pdScId, scStrId));
                }
                inPdScValInfo.setInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID, scStrId);
            }
            Log.logDbg("whalelog   data=%s", data);
        }
        return Errno.OK;
    }

    private int checkAndReplaceAddPdScInfoList(int flow, int aid, int tid, int unionPriId, Integer pdId, FaiList<Param> addPdScInfoList){
        return checkAndReplaceAddPdScInfoList(flow, aid, tid, unionPriId, pdId, addPdScInfoList, null);
    }
    /**
     * 检查并替换商品规格的名称和规格值
     * @param pdId
     * @param addPdScInfoList
     */
    private int checkAndReplaceAddPdScInfoList(int flow, int aid, int tid, int unionPriId, Integer pdId, FaiList<Param> addPdScInfoList, Param nameIdMap){
        Set<String> specStrNameSet = new HashSet<>();
        for (Param pdScInfo : addPdScInfoList) {
            if(pdId == null){
                pdId = pdScInfo.getInt(ProductSpecEntity.Info.PD_ID);
            }
            trimProductSpecStrName(pdScInfo);
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            String name = pdScInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
            if(!SpecStrArgCheck.isValidName(name)){
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                return Errno.ARGS_ERROR;
            }
            specStrNameSet.add(name);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameSet.add(name);
                }
            }
        }
        if(nameIdMap == null){
            nameIdMap = new Param(true); // mapMode
        }
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            try {
                LockUtil.lock(aid);
                int rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(specStrNameSet), nameIdMap);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            specStrDaoCtrl.closeDao();
        }

        for (Param pdScInfo : addPdScInfoList) {
            String name = (String)pdScInfo.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
            Integer scStrId = nameIdMap.getInt(name);
            if(scStrId == null){
                return Errno.ERROR;
            }
            pdScInfo.setInt(ProductSpecEntity.Info.SC_STR_ID, scStrId);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = (String)inPdScValInfo.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                    scStrId = nameIdMap.getInt(name);
                    if(scStrId == null){
                        return Errno.ERROR;
                    }
                    inPdScValInfo.setInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID, scStrId);
                }
            }
        }
        return Errno.OK;
    }

    /**
     * 根据规格值生成sku列表
     * 生成规则如下：
     * 规格1的规格值：1、2、3
     * 规格2的规格值：4、5
     * 规格3的规格值：6、7、8
     * =================
     * 生成的sku组合：
     * 1-4-6
     * 1-4-7
     * 1-4-8
     * 1-5-6
     * 1-5-7
     * 1-5-8
     * 2-4-6
     * 2-4-7
     * 2-4-8
     * 2-5-6
     * 2-5-7
     * 2-5-8
     * 3-4-6
     * 3-4-7
     * 3-4-8
     * 3-5-6
     * 3-5-7
     * 3-5-8
     */
    private FaiList<FaiList<Integer>> genSkuList(FaiList<Param> pdScInfoList, Map<Integer, Integer> idxPdScIdMap, int limit, int checkedListLimit, boolean allowInPdScValListIsEmpty){
        if(idxPdScIdMap == null){
            idxPdScIdMap = new HashMap<>();
        }
        ParamComparator comparator = new ParamComparator(ProductSpecEntity.Info.SORT);
        comparator.addKey(ProductSpecEntity.Info.PD_SC_ID);
        comparator.sort(pdScInfoList);
        // 1 提取出所有勾选的规格字符串id
        FaiList<FaiList<Integer>> allCheckScStrIds = new FaiList<>();
        int idx = 0;
        for (int i = 0; i < pdScInfoList.size(); i++) {
            Param inPdScInfo = pdScInfoList.get(i);
            Integer pdScId = inPdScInfo.getInt(ProductSpecEntity.Info.PD_SC_ID);
            FaiList<Param> inPdScValList = inPdScInfo.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            boolean hasCheck = false;
            for (int j = 0; j < inPdScValList.size(); j++) {
                Param inPdScVal = inPdScValList.get(j);
                boolean check = inPdScVal.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false);
                hasCheck |= check;
                if(check){
                    FaiList<Integer> scStrIdList = null;
                    if(idx >= allCheckScStrIds.size()){
                        scStrIdList = new FaiList<>();
                        allCheckScStrIds.add(idx, scStrIdList);
                    }else{
                        scStrIdList = allCheckScStrIds.get(idx);
                    }
                    scStrIdList.add(inPdScVal.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
                }
            }
            if(hasCheck){
                idxPdScIdMap.put(idx, pdScId);
                idx++;
            }
        }
        if(allCheckScStrIds.size() == 0){
            FaiList<FaiList<Integer>> result = new FaiList<>();
            if(allowInPdScValListIsEmpty){
                result.add(new FaiList<>());
            }
            return result;
        }
        if(allCheckScStrIds.size() > checkedListLimit){
            return null;
        }
        Log.logDbg("whalelog  allCheckScStrIds=%s", allCheckScStrIds);
        // 2 计算有效的sku数量
        int skuTotal = 1;
        for (FaiList<Integer> ScStrIds : allCheckScStrIds) {
            skuTotal *= ScStrIds.size();
        }
        if(skuTotal > limit){
            return null;
        }
        // 3 初始化所有sku容器大小
        FaiList<FaiList<Integer>> skuList = new FaiList<>(skuTotal);
        for (int i = 0; i < skuTotal; i++) {
            skuList.add(new FaiList<>(allCheckScStrIds.size()));
        }
        // 4 计算并填充容器相应位置的数据
        int seqFillCount = skuTotal; // 规格值连续填充的个数
        for (int i = 0; i < allCheckScStrIds.size(); i++) { // 时间复杂度 = skuTotal
            FaiList<Integer> scStrIdList = allCheckScStrIds.get(i);
            int length = scStrIdList.size(); // 当前规格的规格值数量
            seqFillCount/=length; // 规格值连续填充的个数
            for (int j = 0; j < scStrIdList.size(); j++) {
                Integer scStrId = scStrIdList.get(j);
                int k = j * seqFillCount; // 连续填充的起始位置
                while (skuTotal > k){
                    // 填充规格值到sku指定位置
                    skuList.get(k++).add(i, scStrId);
                    if(k%seqFillCount == 0){ // 达到连续填充数量
                        // 计算下一个连续填充的起始位置
                        k += seqFillCount*(length-1);
                    }
                }
            }
        }
        return skuList;
    }

    public static void trimProductSpecStrName(Param pdScInfo){
        if(pdScInfo.containsKey(SpecStrEntity.Info.NAME)){
            pdScInfo.setString(SpecStrEntity.Info.NAME, Str.trim(pdScInfo.getString(SpecStrEntity.Info.NAME)));
        }
        if(pdScInfo.containsKey(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST)){
            FaiList<Param> list = pdScInfo.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            for (Param inPdScStrInfo : list) {
                String name = inPdScStrInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                if (name == null){
                    continue;
                }
                inPdScStrInfo.setString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME, Str.trim(name));
            }
        }
    }

}
