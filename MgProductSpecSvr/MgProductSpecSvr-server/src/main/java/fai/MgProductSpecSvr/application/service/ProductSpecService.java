package fai.MgProductSpecSvr.application.service;


import fai.MgProductSpecSvr.domain.comm.*;
import fai.MgProductSpecSvr.domain.entity.*;
import fai.MgProductSpecSvr.domain.repository.*;
import fai.MgProductSpecSvr.domain.serviceProc.*;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuCodeDao;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.entity.Condition;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
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
    /**
     * 联合修改，包含添加、删除、修改
     */
    public int unionSetPdScInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, String xid, FaiList<Param> addPdScInfoList,
                                    FaiList<Integer> delPdScIdList, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || pdId <= 0 || unionPriId<=0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;pdId=%s;", flow, aid, tid, pdId);
                return rt;
            }
            boolean isSaga = !Str.isEmpty(xid);
            boolean hasAdd = addPdScInfoList != null && !addPdScInfoList.isEmpty();
            boolean hasDel = delPdScIdList != null && !delPdScIdList.isEmpty();
            boolean hasSet = updaterList != null && !updaterList.isEmpty();
            if(!hasAdd && !hasDel && !hasSet){
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList);
                return rt;
            }

            if(hasAdd){
                rt = checkAndReplaceAddPdScInfoList(flow, aid, tid, unionPriId, pdId, addPdScInfoList, null, isSaga);
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
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, transactionCtrl);
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, transactionCtrl);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, transactionCtrl);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        // 用于判断是否要刷新sku
                        Ref<Boolean> needReFreshSkuRef = new Ref<>(false);
                        if (hasDel) {
                            rt = productSpecProc.batchDel(aid, pdId, delPdScIdList, needReFreshSkuRef, isSaga);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (hasAdd) {
                            rt = productSpecProc.batchAdd(aid, pdId, addPdScInfoList, needReFreshSkuRef, isSaga);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (hasSet) {
                            rt = productSpecProc.batchSet(aid, pdId, updaterList, needReFreshSkuRef, isSaga);
                            if (rt != Errno.OK) {
                                return rt;
                            }
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
                        // 记录修改后的商品规格id和它所在的下标
                        Map<Integer/*index*/, Integer/*pdScId*/> idxPdScIdMap = new HashMap<>();
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
                            rt = productSpecSkuProc.updateAllowEmptySku(aid, tid, unionPriId, pdId, skuList.get(0), isSaga);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }else{
                            if (!needReFreshSkuRef.value) {
                                HashMap<String, FaiList<Integer>> newSkuMap = new HashMap<>( skuList.size() * 4 / 3 + 1);
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
                                    rt = productSpecSkuProc.batchSoftDel(aid, pdId, delSkuIdList, isSaga);
                                    if (rt != Errno.OK) {
                                        return rt;
                                    }
                                }

                                if(skuUpdaterList.size() > 0){
                                    // 修改替换名称的sku
                                    rt = productSpecSkuProc.batchSet(aid, pdId, skuUpdaterList, isSaga);
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
                                    rt = productSpecSkuProc.batchAdd(aid, pdId, addPdScSkuInfoList, isSaga);
                                    if (rt != Errno.OK) {
                                        return rt;
                                    }
                                }

                            } else {
                                rt = productSpecSkuProc.refreshSku(aid, tid, unionPriId, pdId, skuList, isSaga);
                                if (rt != Errno.OK) {
                                    Log.logDbg(rt,"refreshSku err aid=%s;pdId=%s;", aid, pdId);
                                    return rt;
                                }
                            }
                        }

                        rt = productSpecSkuCodeProc.batchDel(aid, unionPriId, productSpecSkuProc.getDeletedSkuIdList(), isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 记录分布式事务状态
                        if (isSaga) {
                            SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, transactionCtrl);
                            rt = specSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
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
                    productSpecSkuCodeProc.deleteDirtyCache(aid);
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

    public int unionSetPdScInfoListRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            LockUtil.lock(aid);
            try {
                TransactionCtrl tc = new TransactionCtrl();
                boolean commit = false;
                SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, tc);
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, tc);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, tc);
                try {
                    tc.setAutoCommit(false);

                    // 1、查询 Saga 状态
                    Ref<Param> sagaInfoRef = new Ref<>();
                    rt = specSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                    if (rt != Errno.OK) {
                        // 如果rt=NOT_FOUND，说明出现空补偿或悬挂现象，插入saga记录占位后return OK告知分布式事务组件回滚成功
                        if (rt == Errno.NOT_FOUND) {
                            commit = true;
                            rt = Errno.OK;
                            Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                        }
                        return rt;
                    }

                    Param sagaInfo = sagaInfoRef.value;
                    Integer status = sagaInfo.getInt(SagaEntity.Info.STATUS);
                    // 幂等性保证
                    if (status == SagaValObj.Status.ROLLBACK_OK) {
                        commit = true;
                        Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                        return rt = Errno.OK;
                    }
                    // 获取 Saga 操作记录
                    Ref<FaiList<Param>> specSagaOpListRef = new Ref<>();
                    Ref<FaiList<Param>> specSkuSagaOpListRef = new Ref<>();
                    Ref<FaiList<Param>> specSkuCodeSagaOpListRef = new Ref<>();
                    rt = getSagaOpList(xid, branchId, productSpecProc, productSpecSkuProc, productSpecSkuCodeProc, null, specSagaOpListRef, specSkuSagaOpListRef, specSkuCodeSagaOpListRef, null);
                    if (rt != Errno.OK) {
                        if (rt == Errno.NOT_FOUND) {
                            commit = true;
                            Log.logStd("sagaOpList is empty");
                            rt = Errno.OK;
                        }
                        return rt;
                    }
                    FaiList<Param> specSagaOpList = specSagaOpListRef.value;
                    FaiList<Param> specSkuSagaOpList = specSkuSagaOpListRef.value;
                    FaiList<Param> specSkuCodeSagaOpList = specSkuCodeSagaOpListRef.value;
                    // -------------------------------------- 补偿 start --------------------------------------------
                    // 1、补偿 规格 skuCode
                    if (!Util.isEmptyList(specSkuCodeSagaOpList)) {
                        rt = productSpecSkuCodeProc.batchDelRollback(aid, specSkuCodeSagaOpList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 2、补偿 规格 sku
                    if (!Util.isEmptyList(specSkuSagaOpList)) {
                        FaiList<Param> addSagaOpList = new FaiList<>();
                        FaiList<Param> delSagaOpList = new FaiList<>();
                        FaiList<Param> modifySagaOpList = new FaiList<>();
                        // 做数据区分，之前的操作可能有软删除，修改，以及新增
                        diffSagaOpList(specSagaOpList, addSagaOpList, delSagaOpList, modifySagaOpList);
                        // 补偿添加的数据
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(addSagaOpList)) {
                            rt = productSpecSkuProc.batchAddRollback(aid, addSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 补偿修改的数据
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(modifySagaOpList)) {
                            rt = productSpecSkuProc.batchSetRollback(aid, modifySagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 补偿软删除的数据
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(delSagaOpList)) {
                            rt = productSpecSkuProc.batchSoftDelRollback(aid, delSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                    }
                    // 3、补偿 规格
                    if (!Util.isEmptyList(specSagaOpList)) {
                        FaiList<Param> addSagaOpList = new FaiList<>();
                        FaiList<Param> delSagaOpList = new FaiList<>();
                        FaiList<Param> modifySagaOpList = new FaiList<>();
                        // 区分数据操作
                        diffSagaOpList(specSagaOpList, addSagaOpList, delSagaOpList, modifySagaOpList);
                        // 补偿修改
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(modifySagaOpList)) {
                            rt = productSpecProc.batchSetRollback(aid, modifySagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 补偿添加
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(addSagaOpList)) {
                            rt = productSpecProc.batchAddRollback(aid, addSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 补偿删除
                        if (!fai.middleground.svrutil.misc.Utils.isEmptyList(delSagaOpList)) {
                            rt = productSpecProc.batchDelRollback(aid, delSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                    }
                    // 4、修改 saga 记录状态
                    rt = specSagaProc.setStatus(xid, branchId, SagaValObj.Status.ROLLBACK_OK);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    // -------------------------------------- 补偿 end ----------------------------------------------
                    commit = true;
                    tc.commit();
                } finally {
                    if (!commit) {
                        tc.rollback();
                    }
                    tc.closeDao();
                }
            } finally {
                LockUtil.unlock(aid);
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量删除
     */
    public int batchDelPdAllSc(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList, String xid, boolean softDel) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;pdIdList=%s;", flow, aid, tid, pdIdList);
                return rt;
            }

            boolean isSaga = !Str.isEmpty(xid);
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, transactionCtrl);
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, transactionCtrl);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, transactionCtrl);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        if(!softDel){
                            rt = productSpecProc.batchDel(aid, pdIdList, isSaga);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        rt = productSpecSkuProc.batchDel(aid, pdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecSkuCodeProc.batchDel(aid, null, productSpecSkuProc.getDeletedSkuIdList(), isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if (isSaga) {
                            // 记录 Saga 状态
                            SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, transactionCtrl);
                            rt = specSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
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
                    productSpecSkuCodeProc.deleteDirtyCache(aid);
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

    /**
     * batchDelPdAllSc 的补偿方法
     */
    public int batchDelPdAllScRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            LockUtil.lock(aid);
            try {
                TransactionCtrl tc = new TransactionCtrl();
                boolean commit = false;
                SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, tc);
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, tc);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, tc);
                try {
                    tc.setAutoCommit(false);

                    // 1、查询 Saga 状态
                    Ref<Param> sagaInfoRef = new Ref<>();
                    rt = specSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                    if (rt != Errno.OK) {
                        // 如果rt=NOT_FOUND，说明出现空补偿或悬挂现象，插入saga记录占位后return OK告知分布式事务组件回滚成功
                        if (rt == Errno.NOT_FOUND) {
                            commit = true;
                            rt = Errno.OK;
                            Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                        }
                        return rt;
                    }

                    Param sagaInfo = sagaInfoRef.value;
                    Integer status = sagaInfo.getInt(SagaEntity.Info.STATUS);
                    // 幂等性保证
                    if (status == SagaValObj.Status.ROLLBACK_OK) {
                        commit = true;
                        Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                        return rt = Errno.OK;
                    }
                    // 获取 Saga 操作记录
                    Ref<FaiList<Param>> specSagaOpListRef = new Ref<>();
                    Ref<FaiList<Param>> specSkuSagaOpListRef = new Ref<>();
                    Ref<FaiList<Param>> specSkuCodeSagaOpListRef = new Ref<>();
                    rt = getSagaOpList(xid, branchId, productSpecProc, productSpecSkuProc, productSpecSkuCodeProc, null, specSagaOpListRef, specSkuSagaOpListRef, specSkuCodeSagaOpListRef, null);
                    if (rt != Errno.OK) {
                        if (rt == Errno.NOT_FOUND) {
                            commit = true;
                            Log.logStd("sagaOpList is empty");
                            rt = Errno.OK;
                        }
                        return rt;
                    }
                    FaiList<Param> specSagaOpList = specSagaOpListRef.value;
                    FaiList<Param> specSkuSagaOpList = specSkuSagaOpListRef.value;
                    FaiList<Param> specSkuCodeSagaOpList = specSkuCodeSagaOpListRef.value;
                    // -------------------------------------- 补偿 start --------------------------------------------
                    // 1、补偿 规格 skuCode
                    if (!Util.isEmptyList(specSkuCodeSagaOpList)) {
                        rt = productSpecSkuCodeProc.batchDelRollback(aid, specSkuCodeSagaOpList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 2、补偿 规格 sku
                    if (!Util.isEmptyList(specSkuSagaOpList)) {
                        rt = productSpecSkuProc.batchDelRollback(aid, specSkuSagaOpList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 3、补偿 规格
                    if (!Util.isEmptyList(specSagaOpList)) {
                        rt = productSpecProc.batchDelRollback(aid, specSagaOpList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 4、修改 saga 记录状态
                    rt = specSagaProc.setStatus(xid, branchId, SagaValObj.Status.ROLLBACK_OK);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    // -------------------------------------- 补偿 end ----------------------------------------------
                    commit = true;
                    tc.commit();
                } finally {
                    if (!commit) {
                        tc.rollback();
                    }
                    tc.closeDao();
                }
            } finally {
                LockUtil.unlock(aid);
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取某个商品所关联的所有商品规格集
     */
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

    /**
     * 只获取勾选上的商品规格集合
     */
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
     * 根据商品idList获取商品规格集合，管理态调用
     */
    public int getPdScInfoList4Adm(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> pdIds, boolean onlyGetChecked) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(pdIds) || unionPriId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdIds=%s;", flow, aid, pdIds);
                return rt;
            }
            // 查db的，不能pdIds的数量不能过多
            if(pdIds.size() > 100) {
                rt = Errno.LEN_LIMIT;
                Log.logErr("pdIds size more than 100;flow=%d;aid=%d;pdIds=%s;", flow, aid, pdIds);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                rt = productSpecProc.getListFromDao(aid, pdIds, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecDaoCtrl.closeDao();
            }

            FaiList<Param> pdScInfoList = listRef.value;
            // 筛选勾选的数据
            if(onlyGetChecked) {
                pdScInfoList.forEach(pdSpInfo->{
                    FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                    for (int i = inPdScValList.size() - 1; i >= 0; i--) {
                        Param inpdscValInfo = inPdScValList.get(i);
                        if(!inpdscValInfo.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                            inPdScValList.remove(i);
                        }
                    }
                });
            }

            rt = initPdScSpecStr(flow, aid, pdScInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSpecStr err;aid=%d;pdIds=%s;", aid, pdIds);
                return rt;
            }
            sendPdScInfoList(session, pdScInfoList);
            Log.logDbg("ok;flow=%d;aid=%d;pdId=%s;", flow, aid, pdIds);
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

    public int setPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, String xid, int pdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;updaterList=%s", flow, aid, pdId, updaterList);
                return rt;
            }

            boolean needGenSpuInfo = false; // 是否需要生成spu数据
            ParamUpdater spuUpdater = null;
            Set<String> inPdScStrNameSet = new HashSet<>();
            boolean isSaga = !Str.isEmpty(xid);
            Map<FaiList<String>/*inPdScStrNameList*/, Param/*update.data*/> inPdScStrNameListInfoMap = new HashMap<>();
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
                FaiList<String> skuCodeList = data.getList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                if(skuCodeList != null){
                    for (String skuCode : skuCodeList) {
                        if(!ProductSpecSkuArgCheck.isValidSkuCode(skuCode)){
                            rt = Errno.ARGS_ERROR;
                            Log.logErr("skuCodeList skuCode err;flow=%d;aid=%d;pdId=%s;updater=%s", flow, aid, pdId, updater);
                            return rt;
                        }
                    }
                }
                String skuCode = data.getString(ProductSpecSkuEntity.Info.SKU_CODE);
                if(skuCode != null){
                    if(!ProductSpecSkuArgCheck.isValidSkuCode(skuCode)){
                        rt = Errno.ARGS_ERROR;
                        Log.logErr("skuCode err;flow=%d;aid=%d;pdId=%s;updater=%s", flow, aid, pdId, updater);
                        return rt;
                    }
                    if(skuCodeList == null){
                        skuCodeList = new FaiList<>(Arrays.asList(skuCode));
                        data.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST, skuCodeList);
                    }
                }
            }
            // 获取skuId 并移除 ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST 字段
            if(inPdScStrNameSet.size() != 0){
                Param nameIdMap = new Param(true);
                TransactionCtrl tc = new TransactionCtrl();
                try {
                    SpecStrProc specStrProc = new SpecStrProc(flow, aid, tc);
                    // 查找规格字符串，如果没有就添加
                    rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(inPdScStrNameSet), nameIdMap, isSaga);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    tc.closeDao();
                }
                Map<String, Param> inPdScStrIdListJsonInfoMap = new HashMap<>(inPdScStrNameListInfoMap.size()*4/3+1);
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
                // 添加 spu 数据
                rt = genSkuRepresentSpuInfo(flow, aid, tid, unionPriId, pdId, skuIdRef, isSaga);
                if(rt != Errno.OK){
                    return rt;
                }
                skuIdRepresentSpu = skuIdRef.value;
            }
            FaiList<Param> skuCodeSortList = new FaiList<>();
            HashSet<Long> changeSkuCodeSkuIdSet = new HashSet<>();
            Map<String/*skuCode*/, Long/*skuId*/> newSkuCodeSkuIdMap = new HashMap<>();
            FaiList<Long> needDelSkuCodeSkuIdList = new FaiList<>();
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
                FaiList<String> skuCodeList = (FaiList<String>)data.remove(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                if(skuCodeList != null){
                    changeSkuCodeSkuIdSet.add(skuId);
                    if(skuCodeList.size() > 0){
                        if(skuCodeList.size() > ProductSpecSkuCodeValObj.Limit.MAX_SIZE){
                            rt = Errno.SIZE_LIMIT;
                            Log.logErr(rt,"found not exist sku;flow=%s;aid=%s;pdId=%s;updater=%s;", flow, aid, pdId, updater);
                            return rt;
                        }
                        for (int j = 0; j < skuCodeList.size(); j++) {
                            String skuCode = skuCodeList.get(j);
                            Long ifAbsent = newSkuCodeSkuIdMap.putIfAbsent(skuCode, skuId);
                            if(ifAbsent != null){
                                rt = Errno.ALREADY_EXISTED;
                                Log.logErr(rt,"skuCode repeat;flow=%s;aid=%s;pdId=%s;updater=%s;skuCode=%s;ifAbsent=%s;", flow, aid, pdId, updater, skuCode, ifAbsent);
                                return rt;
                            }
                            skuCodeSortList.add(
                                    new Param()
                                            .setInt(ProductSpecSkuCodeEntity.Info.SORT, j)
                                            .setInt(ProductSpecSkuCodeEntity.Info.AID, aid)
                                            .setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId)
                                            .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                            );
                        }
                    }else{
                        needDelSkuCodeSkuIdList.add(skuId);
                    }
                }
            }
            Log.logDbg("whalelog  newSkuCodeSkuIdMap=%s;needDelSkuCodeSkuIdList=%s;", newSkuCodeSkuIdMap, needDelSkuCodeSkuIdList);

            TransactionCtrl tc = new TransactionCtrl();
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, tc);
                try {
                    LockUtil.lock(aid);
                    try {
                        tc.setAutoCommit(false);
                        // 批量修改 商品规格SKU表
                        rt = productSpecSkuProc.batchSet(aid, pdId, updaterList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 刷新 skuCode
                        rt = productSpecSkuCodeProc.refresh(aid, unionPriId, pdId, newSkuCodeSkuIdMap, needDelSkuCodeSkuIdList, skuCodeSortList, changeSkuCodeSkuIdSet, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 记录 Saga 状态
                        if (isSaga) {
                            SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, tc);
                            rt = specSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                    }finally {
                        if(rt != Errno.OK){
                            tc.rollback();
                            return rt;
                        }
                        tc.commit();
                    }
                    productSpecSkuProc.deleteDirtyCache(aid);
                    productSpecSkuCodeProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                tc.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            FaiList<Param> pdScSkuList = new FaiList<>(); // TODO
            pdScSkuList.toBuffer(sendBuf, ProductSpecSkuDto.Key.INFO_LIST, ProductSpecSkuDto.getInfoDto());
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;skuIdRepresentSpu=%s;", flow, aid, pdId, skuIdRepresentSpu);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * setPdSkuScInfoList 的补偿方法
     */
    public int setPdSkuScInfoListRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            try {
                boolean commit = false;
                LockUtil.lock(aid);
                try {
                    tc.setAutoCommit(false);
                    SpecStrProc specStrProc = new SpecStrProc(flow, aid, tc);
                    ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
                    ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, tc);
                    SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, tc);
                    try {
                        Ref<Param> sagaInfoRef = new Ref<>();
                        // 获取 Saga 状态
                        rt = specSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }
                        Param sagaInfo = sagaInfoRef.value;
                        Integer status = sagaInfo.getInt(SagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == SagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // 查询 Saga 操作记录表的数据
                        Ref<FaiList<Param>> specStrSagaOpListRef = new Ref<>();
                        Ref<FaiList<Param>> specSkuSagaOpListRef = new Ref<>();
                        Ref<FaiList<Param>> specSkuCodeSagaOpListRef = new Ref<>();
                        rt = getSagaOpList(xid, branchId, null, productSpecSkuProc, productSpecSkuCodeProc, specStrProc, null, specSkuSagaOpListRef, specSkuCodeSagaOpListRef, specStrSagaOpListRef);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                Log.logStd("sagaOpList is empty");
                                rt = Errno.OK;
                            }
                            return rt;
                        }
                        FaiList<Param> specStrSagaOpList = specStrSagaOpListRef.value;
                        FaiList<Param> specSkuSagaOpList = specSkuSagaOpListRef.value;
                        FaiList<Param> specSkuCodeSagaOpList = specSkuCodeSagaOpListRef.value;

                        // ----------------------------------------- 补偿 start --------------------------------------------
                        // 1、补偿 规格skuCode
                        if (!Util.isEmptyList(specSkuCodeSagaOpList)) {
                            rt = productSpecSkuCodeProc.refreshRollback(aid, specSkuCodeSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 2、补偿 规格Sku
                        if (!Util.isEmptyList(specSkuSagaOpList)) {
                            FaiList<Param> addSpecSkuSagaOpList = new FaiList<>();
                            FaiList<Param> modifySpecSkuSagaOpList = new FaiList<>();
                            // 做数据区分：之前可能会有添加 spu 的数据，和修改 sku 的数据
                            diffSagaOpList(specSkuSagaOpList, addSpecSkuSagaOpList, null, modifySpecSkuSagaOpList);
                            // 补偿被修改的数据
                            if (!fai.middleground.svrutil.misc.Utils.isEmptyList(modifySpecSkuSagaOpList)) {
                                rt = productSpecSkuProc.batchSetRollback(aid, modifySpecSkuSagaOpList);
                                if (rt != Errno.OK) {
                                    return rt;
                                }
                            }
                            if (!fai.middleground.svrutil.misc.Utils.isEmptyList(addSpecSkuSagaOpList)) {
                                // 补偿新增的spu
                                rt = productSpecSkuProc.genSpuRollback(aid, addSpecSkuSagaOpList);
                                if (rt != Errno.OK) {
                                    return rt;
                                }
                                // 恢复 skuId
                                productSpecSkuProc.restoreMaxId(aid, false);
                            }
                        }
                        // 3、补偿 规格字符串
                        if (!Util.isEmptyList(specStrSagaOpList)) {
                            rt = specStrProc.batchAddRollback(aid, specStrSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 恢复 pdScId
                            specStrProc.restoreMaxId(aid, false);
                        }
                        // 4、修改 Saga 的状态
                        rt = specSagaProc.setStatus(xid, branchId, SagaValObj.Status.ROLLBACK_OK);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // ----------------------------------------- 补偿 end ----------------------------------------------
                        commit = true;
                        tc.commit();
                    } finally {
                        if (!commit) {
                            tc.rollback();
                        }
                    }
                } finally {
                    LockUtil.unlock(aid);
                }
            } finally {
                tc.closeDao();
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;

    }

    /**
     * 生成代表spu的sku数据
     */
    private int genSkuRepresentSpuInfo(int flow, int aid, int tid, int unionPriId, int pdId, Ref<Long> skuIdRef, boolean isSaga){
        int rt;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
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
                    tc.setAutoCommit(false);
                    rt = productSpecSkuProc.genSkuRepresentSpuInfo(aid, unionPriId, tid, pdId, skuIdRef, isSaga);
                }finally {
                    if(rt != Errno.OK){
                        productSpecSkuProc.clearIdBuilderCache(aid);
                        tc.rollback();
                        return rt;
                    }
                    tc.commit();
                    productSpecSkuProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            tc.closeDao();
        }
        return rt;
    }

    /**
     * 跟据商品id获取相关的sku信息
     * @param withSpuInfo 是否顺带获取表示spu的数据
     */
    public int getPdSkuScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId, boolean withSpuInfo) throws IOException {
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
                rt = productSpecSkuProc.getList(aid, pdId, withSpuInfo, pdScSkuInfoListRef);
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
            rt = initPdScSkuCodeList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuCodeList err;aid=%d;pdId=%d;", aid, pdId);
                return rt;
            }
            sendPdSkuScInfoList(session, psScSkuInfoList, true);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 跟据skuId集获取数据
     */
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
            rt = initPdScSkuCodeList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuCodeList err;aid=%d;skuIdList=%s;", aid, skuIdList);
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
     * 获取skuId信息集
     */
    public int getPdSkuIdInfoList(FaiSession session, int flow, int aid, FaiList<Integer> pdIdList, boolean withSpuInfo) throws IOException {
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
                String[] fields = {ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST};
                if(withSpuInfo){
                    ArrayList<String> tmp = new ArrayList<>(Arrays.asList(fields));
                    tmp.add(ProductSpecSkuEntity.Info.FLAG);
                    fields = tmp.toArray(fields);
                }
                Log.logDbg("whalelog  withSpuInfo=%s;fields=%s", withSpuInfo, fields);
                rt = productSpecSkuProc.getListFromDaoByPdIdList(aid, pdIdList, withSpuInfo, pdScSkuInfoListRef, fields);
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
     * 根据pdId集合 获取skuId信息集，查db，管理态用
     */
    public int getPdSkuInfoList4Adm(FaiSession session, int flow, int aid, FaiList<Integer> pdIdList, boolean withSpuInfo) throws IOException {
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
                String[] fields = {ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, ProductSpecSkuEntity.Info.SKU_CODE};
                if(withSpuInfo){
                    ArrayList<String> tmp = new ArrayList<>(Arrays.asList(fields));
                    tmp.add(ProductSpecSkuEntity.Info.FLAG);
                    fields = tmp.toArray(fields);
                }
                Log.logDbg("whalelog  withSpuInfo=%s;fields=%s", withSpuInfo, fields);
                rt = productSpecSkuProc.getListFromDaoByPdIdList(aid, pdIdList, withSpuInfo, pdScSkuInfoListRef, fields);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;pdIdList=%s;", aid, pdIdList);
                return rt;
            }
            rt = initPdScSkuCodeList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuCodeList err;aid=%d;pdIdList=%s;", aid, pdIdList);
                return rt;
            }
            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取只有表示为 spu 的sku数据集
     */
    public int getOnlySpuInfoList(FaiSession session, int flow, int aid, FaiList<Integer> pdIdList) throws IOException {
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
                rt = productSpecSkuProc.getSpuInfoList(aid, pdIdList, pdScSkuInfoListRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }

            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;

            rt = initPdScSkuCodeList(flow, aid, psScSkuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;pdIdList=%s;", aid, pdIdList);
                return rt;
            }

            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取已经存在的 skuCodeList
     */
    public int getExistsSkuCodeList(FaiSession session, int flow, int aid, int unionPriId, FaiList<String> skuCodeList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || skuCodeList == null || skuCodeList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuCodeList=%s", flow, aid, skuCodeList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                rt = productSpecSkuCodeProc.getSkuCodeListFromDao(aid, unionPriId, skuCodeList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }
            FaiList<String> alreadyExistsSkuCodeList = Utils.getValList(listRef.value, ProductSpecSkuCodeEntity.Info.SKU_CODE);

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putString(ProductSpecSkuDto.Key.SKU_CODE_LIST, alreadyExistsSkuCodeList.toJson());
            session.write(sendBuf);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;skuCodeList=%s;", flow, aid, unionPriId, skuCodeList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int searchPdSkuIdInfoListBySkuCode(FaiSession session, int flow, int aid, int unionPriId, String skuCode, Param condition) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Str.isEmpty(skuCode)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuCode=%s", flow, aid, skuCode);
                return rt;
            }
            boolean isFuzzySearch = condition.getBoolean(Condition.Info.FUZZY, false);
            boolean returnFullInfo = condition.getBoolean(Condition.Info.RETURN_FULL_INFO, false);
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                rt = productSpecSkuCodeProc.searchBySkuCode(aid, unionPriId, skuCode, isFuzzySearch, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }
            Map<Long, FaiList<String>> skuIdSkuCodeListMap = new HashMap<>();
            for (Param info : listRef.value) {
                Long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
                String tmpSkuCode = info.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE);
                FaiList<String> skuCodeList = skuIdSkuCodeListMap.getOrDefault(skuId, new FaiList<>());
                skuCodeList.add(tmpSkuCode);
                if(skuCodeList.size() == 1){
                    skuIdSkuCodeListMap.put(skuId, skuCodeList);
                }
            }
            String[] searchResultFields = {ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.FLAG};
            if(returnFullInfo){
                searchResultFields = null;
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getList(aid, new FaiList<>(skuIdSkuCodeListMap.keySet()), pdScSkuInfoListRef, searchResultFields);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }

            FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
            if(returnFullInfo){
                rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;skuIdSkuCodeListMap=%s;", aid, skuIdSkuCodeListMap);
                    return rt;
                }

                for (Param skuInfo : psScSkuInfoList) {
                    Long skuId = skuInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                    FaiList<String> skuCodeList = skuIdSkuCodeListMap.get(skuId);
                    skuInfo.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST, skuCodeList);
                }
            }
            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;skuCode=%s;skuIdSkuCodeListMap=%s;", flow, aid, unionPriId, skuCode, skuIdSkuCodeListMap);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }


    /**
     * 删除条码
     */
    public int deleteSkuCodeByPdIsList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> pdIdList)throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(pdIdList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s", flow, aid,  unionPriId, pdIdList);
                return rt;
            }
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                try {
                    LockUtil.lock(aid);
                    ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                    Ref<FaiList<Long>> refSkuIdList = new Ref<>();
                    rt = productSpecSkuCodeProc.getSkuIdListFromDao(aid, unionPriId, pdIdList, refSkuIdList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    rt = productSpecSkuCodeProc.batchDel(aid, unionPriId, refSkuIdList.value);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok!;flow=%s;aid=%s;unionPriId=%s;pdIdList=%s;", flow, aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取 skuCode数据状态
     */
    public int getSkuCodeDataStatus(FaiSession session, int flow, int aid, int unionPriId)throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<Param> dataStatusRef = new Ref<>();
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                rt = productSpecSkuCodeProc.getDataStatus(aid, unionPriId, dataStatusRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }

            FaiBuffer sendBody = new FaiBuffer();
            dataStatusRef.value.toBuffer(sendBody, ProductSpecSkuCodeDao.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
    public int getSkuCodeAllData(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                rt = productSpecSkuCodeProc.getAllDataFromDao(aid, unionPriId, listRef, ProductSpecSkuCodeEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSkuCode(session, listRef.value, null);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    /**
     * 搜索全部数据的部分字段
     */
    public int searchSkuCodeFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {

            Ref<FaiList<Param>> listRef = new Ref<>();
            ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                rt = productSpecSkuCodeProc.searchAllDataFromDao(aid, unionPriId, searchArg, listRef, ProductSpecSkuCodeEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                productSpecSkuCodeDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSkuCode(session, listRef.value, searchArg);
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
    public int importPdScWithSku(FaiSession session, int flow, int aid, String xid, int tid, int unionPriId, FaiList<Param> specList, FaiList<Param> specSkuList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Param nameIdMap = new Param(true);
            // 检查并替换商品规格的名称和规格值，例：橙色 -> 15 (scStrId)，nameIdMap 是 名称为 key，规格字符串id为 value (橙色:15)
            rt = checkAndReplaceAddPdScInfoList(flow, aid, tid, unionPriId, null, specList, nameIdMap, false);
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
                    /**
                     * 下方的 lambda 表达
                     * if(inPdScStrIdListJsonSpecSkuMap == null){
                     *                         inPdScStrIdListJsonSpecSkuMap = new HashMap<>();
                     *                         pdIdInPdScStrIdListJsonSpecSkuMap.put(pdId, inPdScStrIdListJsonSpecSkuMap);
                     *                     }
                     */
                    Map<String, Param> inPdScStrIdListJsonSpecSkuMap = pdIdInPdScStrIdListJsonSpecSkuMap.computeIfAbsent(pdId, k -> new HashMap<>());
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
                // 根据规格值生成 sku 列表
                FaiList<FaiList<Integer>> skuList = genSkuList(new FaiList<>(pdScInfoList), null, ProductSpecSkuValObj.Limit.SINGLE_PRODUCT_MAX_SIZE,
                        ProductSpecSkuValObj.Limit.InPdScValIdSkuList.MAX_SIZE, true);
                if (skuList == null) {
                    return rt = Errno.SIZE_LIMIT;
                }
                // 是否允许 inPdScValList 为空，即是否是单规格
                boolean allowInPdScValListIsEmpty = false;

                if(pdScInfoList.size() == 1 && skuList.size() == 1) {
                    allowInPdScValListIsEmpty = Misc.checkBit(pdScInfoList.get(0).getInt(ProductSpecEntity.Info.FLAG, 0), ProductSpecValObj.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY);
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
                    int flag = 0;
                    // 单规格设置允许为空
                    if(allowInPdScValListIsEmpty) {
                        flag |= ProductSpecSkuValObj.FLag.ALLOW_EMPTY;
                    }
                    addSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.FLAG, flag);
                    pdScSkuList.add(addSpecSkuInfo);
                }
                pdIdPdScSkuListMap.put(pdId, pdScSkuList);
            }
            FaiList<Param> rtPdSkuList = new FaiList<>();
            Map<Integer/*pdId*/,Map<String/*InPdScStrIdListJson*/, Long/*skuId*/>> pdIdInPdScStrIdListJsonSkuIdMap = new HashMap<>();
            Ref<FaiList<Long>> skuIdListRef = new Ref<>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, transactionCtrl);
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, transactionCtrl);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, transactionCtrl);
                try {
                    LockUtil.lock(aid);
                    try {
                        // 开启分布式事务标志
                        boolean isSaga = !Str.isEmpty(xid);
                        transactionCtrl.setAutoCommit(false);
                        // 添加 商品规格表数据 mgProductSpec_0xxx
                        rt = productSpecProc.batchAdd(aid, pdIdSpecListMap, null, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 添加 商品规格sku表数据 mgProductSpecSku_0xxx
                        rt = productSpecSkuProc.batchAdd(aid, pdIdPdScSkuListMap, pdIdInPdScStrIdListJsonSkuIdMap, skuIdListRef, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        FaiList<Param> skuCodeInfoList = new FaiList<>();
                        for (Map.Entry<Integer, Map<String, Param>> entryMap : pdIdInPdScStrIdListJsonSpecSkuMap.entrySet()) {
                            Integer pdId = entryMap.getKey();
                            /*
                             * inPdScStrIdListJsonSkuIdMap 结构
                             *      key ((String)inPdScStrIdList) : value (skuId)
                             *                            [18,19] : 24
                             */
                            Map<String, Long> inPdScStrIdListJsonSkuIdMap = pdIdInPdScStrIdListJsonSkuIdMap.get(pdId);
                            /*
                             * inPdScStrIdListJsonSpecSkuMap 结构
                             *      key ((String)inPdScStrIdList) : value (specSkuInfo)
                             *                            [18,19] : {aid:"xxx", pdId:"xxx",.......}
                             */
                            Map<String, Param> inPdScStrIdListJsonSpecSkuMap = entryMap.getValue();
                            for (Map.Entry<String, Param> inPdScStrIdListJsonSpecSkuEntry : inPdScStrIdListJsonSpecSkuMap.entrySet()) {
                                String inPdScStrIdListJson = inPdScStrIdListJsonSpecSkuEntry.getKey();
                                Param specSkuInfo = inPdScStrIdListJsonSpecSkuEntry.getValue();
                                Long skuId = inPdScStrIdListJsonSkuIdMap.get(inPdScStrIdListJson);
                                FaiList<String> skuCodeList = specSkuInfo.getListNullIsEmpty(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                                if(skuCodeList.isEmpty()){
                                    continue;
                                }
                                int sort = 0;
                                for (String skuCode : skuCodeList) {
                                    skuCodeInfoList.add(
                                            new Param()
                                                    .setInt(ProductSpecSkuCodeEntity.Info.PD_ID, pdId)
                                                    .setInt(ProductSpecSkuCodeEntity.Info.SORT, sort++)
                                                    .setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, skuId)
                                                    .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                                    );
                                }
                            }
                        }
                        if(!skuCodeInfoList.isEmpty()){
                            rt = productSpecSkuCodeProc.batchAdd(aid, unionPriId, skuCodeInfoList, isSaga);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        // 记录 Saga 状态
                        if (isSaga) {
                            SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, transactionCtrl);
                            rt = specSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                    }finally {
                        if(rt != Errno.OK){
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            transactionCtrl.rollback();
                        }
                        transactionCtrl.commit();
                        productSpecProc.deleteDirtyCache(aid);
                        productSpecSkuProc.deleteDirtyCache(aid);
                        productSpecSkuCodeProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = productSpecSkuProc.getListFromDao(aid, skuIdListRef.value, listRef, ProductSpecSkuEntity.Info.PD_ID, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, ProductSpecSkuEntity.Info.FLAG);
                if(rt != Errno.OK){
                    return rt;
                }
                rtPdSkuList = listRef.value;
            }finally {
                transactionCtrl.closeDao();
            }
            Log.logDbg("flow=%d;aid=%d;rtPdSkuList=%s", flow, aid, rtPdSkuList);
            rt = initPdScSkuSpecStr(flow, aid, rtPdSkuList);
            if(rt != Errno.OK){
                Log.logErr(rt,"initPdScSkuSpecStr err;aid=%d;rtPdSkuList=%s;", aid, rtPdSkuList);
                return rt;
            }
            rt = Errno.OK;
            sendPdSkuScInfoList(session, rtPdSkuList);
            Log.logStd("ok;flow=%d;aid=%d;", flow, aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * importPdScWithSku 的补偿方法
     */
    public int importPdScWithSkuRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            try {
                boolean commit = false;
                LockUtil.lock(aid);
                try {
                    tc.setAutoCommit(false);
                    ProductSpecProc productSpecProc = new ProductSpecProc(flow, aid, tc);
                    ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(flow, aid, tc);
                    ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(flow, aid, tc);
                    SpecSagaProc specSagaProc = new SpecSagaProc(flow, aid, tc);
                    try {
                        Ref<Param> sagaInfoRef = new Ref<>();
                        // 获取 Saga 状态
                        rt = specSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }
                        Param sagaInfo = sagaInfoRef.value;
                        Integer status = sagaInfo.getInt(SagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == SagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // 查询 Saga 操作记录表的数据
                        Ref<FaiList<Param>> specSagaOpListRef = new Ref<>();
                        Ref<FaiList<Param>> specSkuSagaOpListRef = new Ref<>();
                        Ref<FaiList<Param>> specSkuCodeSagaOpListRef = new Ref<>();
                        rt = getSagaOpList(xid, branchId, productSpecProc, productSpecSkuProc, productSpecSkuCodeProc, null, specSagaOpListRef, specSkuSagaOpListRef, specSkuCodeSagaOpListRef, null);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                Log.logStd("sagaOpList is empty");
                                rt = Errno.OK;
                            }
                            return rt;
                        }
                        FaiList<Param> specSagaOpList = specSagaOpListRef.value;
                        FaiList<Param> specSkuSagaOpList = specSkuSagaOpListRef.value;
                        FaiList<Param> specSkuCodeSagaOpList = specSkuCodeSagaOpListRef.value;

                        // ----------------------------------------- 补偿 start --------------------------------------------
                        // 1、补偿 规格skuCode
                        if (!Util.isEmptyList(specSkuCodeSagaOpList)) {
                            rt = productSpecSkuCodeProc.batchAddRollback(aid, specSkuCodeSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 2、补偿 规格Sku
                        if (!Util.isEmptyList(specSkuSagaOpList)) {
                            rt = productSpecSkuProc.batchAddRollback(aid, specSkuSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 恢复 skuId
                            productSpecSkuProc.restoreMaxId(aid, false);
                        }
                        // 3、补偿 规格
                        if (!Util.isEmptyList(specSagaOpList)) {
                            rt = productSpecProc.batchAddRollback(aid, specSagaOpList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 恢复 pdScId
                            productSpecProc.restoreMaxId(aid, false);
                        }
                        // 4、修改 Saga 的状态
                        rt = specSagaProc.setStatus(xid, branchId, SagaValObj.Status.ROLLBACK_OK);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // ----------------------------------------- 补偿 end ----------------------------------------------
                        commit = true;
                        tc.commit();
                    } finally {
                        if (!commit) {
                            tc.rollback();
                        }
                    }
                } finally {
                    LockUtil.unlock(aid);
                }
            } finally {
                tc.closeDao();
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 清除当前aid的所有缓存
     */
    public int clearAllCache(FaiSession session, int flow, int aid) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            boolean allOption = true;
            try {
                LockUtil.readLock(aid);
                boolean boo = CacheCtrl.clearCacheVersion(aid);
                if(!boo){
                    Log.logErr("CacheCtrl.clearCacheVersion err;flow=%s;aid=%s;", flow, aid);
                }
                allOption &= boo;
                boo = SpecStrCacheCtrl.delAllCache(aid);
                if(!boo){
                    Log.logErr("SpecStrCacheCtrl.delAllCache err;flow=%s;aid=%s;", flow, aid);
                }
                allOption &= boo;
                boo = ProductSpecSkuCacheCtrl.delAllCache(aid);
                if(!boo){
                    Log.logErr("ProductSpecSkuCacheCtrl.delAllCache err;flow=%s;aid=%s;", flow, aid);
                }
                allOption &= boo;
                boo = ProductSpecSkuCodeCacheCtrl.delAllCache(aid);
                if(!boo){
                    Log.logErr("ProductSpecSkuCodeCacheCtrl.delAllCache err;flow=%s;aid=%s;", flow, aid);
                }
                allOption &= boo;
            }finally {
                LockUtil.unReadLock(aid);
            }

            if(allOption){
                rt = Errno.OK;
            }
            session.write(rt);
            Log.logStd("flow=%s;aid=%s;allOption=%s", flow, aid, allOption);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 清空aid+unionPriIds数据
     */
    public int clearAcct(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(unionPriIds)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriIds=%s;", flow, aid, unionPriIds);
                return rt;
            }

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);

                SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
                SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
                SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(productSpecDaoCtrl, productSpecSkuCodeDaoCtrl, productSpecSkuDaoCtrl, specTempDaoCtrl, specTempBizRelDaoCtrl, specTempDetailDaoCtrl)){
                    return rt=Errno.ERROR;
                }

                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        // 删除商品规格数据
                        rt = productSpecProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 删除商品规格sku
                        rt = productSpecSkuProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 删除商品规格skuCode
                        rt = productSpecSkuCodeProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        SearchArg searchArg = new SearchArg();
                        searchArg.matcher = new ParamMatcher(SpecTempEntity.Info.AID, ParamMatcher.EQ, aid);
                        searchArg.matcher.and(SpecTempEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
                        Ref<FaiList<Param>> specListRef = new Ref<>();
                        rt = specTempProc.getListFromDB(aid, searchArg, specListRef, SpecTempEntity.Info.TP_SC_ID);
                        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }

                        FaiList<Integer> tcScIds = Utils.getValList(specListRef.value, SpecTempEntity.Info.TP_SC_ID);

                        // 删除规格模板
                        rt = specTempProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 删除规格模板详情
                        if(!Util.isEmptyList(tcScIds)) {
                            rt = specTempDetailProc.clearData(aid, tcScIds);
                            if(rt != Errno.OK) {
                                return rt;
                            }
                        }

                        // 删除规格模板业务关系
                        rt = specTempBizRelProc.clearAcct(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            // 清除idBuilder缓存
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            specTempProc.clearIdBuilderCache(aid);
                            specTempDetailProc.clearIdBuilderCache(aid);
                            for(int unionPriId : unionPriIds) {
                                specTempBizRelProc.clearIdBuilderCache(aid, unionPriId);
                            }
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                    CacheCtrl.clearCacheVersion(aid);
                    SpecStrCacheCtrl.delAllCache(aid);
                    ProductSpecSkuCacheCtrl.delAllCache(aid);
                    ProductSpecSkuCodeCacheCtrl.delAllCache(aid);
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
            Log.logStd("ok;flow=%d;aid=%d;pdIdList=%s;unionPriIds=%s;", flow, aid, unionPriIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    private void sendSkuCode(FaiSession session, FaiList<Param> infoList, SearchArg searchArg) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, ProductSpecSkuCodeDao.Key.INFO_LIST, ProductSpecSkuCodeDao.getInfoDto());
        if(searchArg != null && searchArg.totalSize != null){
            sendBuf.putInt(ProductSpecSkuCodeDao.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    /**
     * 初始化skuCodeList
     */
    private int initPdScSkuCodeList(int flow, int aid, FaiList<Param> psScSkuInfoList) {
        if(psScSkuInfoList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        FaiList<Long> skuIdList = Utils.getValList(psScSkuInfoList, ProductSpecSkuEntity.Info.SKU_ID);
        Ref<Map<Long, FaiList<String>>> mapRef = new Ref<>();
        ProductSpecSkuCodeDaoCtrl productSpecSkuCodeDaoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
        try {
            ProductSpecSkuCodeProc productSpecSkuCodeProc = new ProductSpecSkuCodeProc(productSpecSkuCodeDaoCtrl, flow);
            rt = productSpecSkuCodeProc.getList(aid, skuIdList, mapRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            rt = Errno.OK;
        }finally {
            productSpecSkuCodeDaoCtrl.closeDao();
        }
        for (Param info : psScSkuInfoList) {
            Long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            info.setList(fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity.Info.SKU_CODE_LIST, mapRef.value.getOrDefault(skuId, new FaiList<>()));
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

    /**
     * 检查并替换商品规格的名称和规格值
     * @param pdId
     * @param addPdScInfoList
     */
    private int checkAndReplaceAddPdScInfoList(int flow, int aid, int tid, int unionPriId, Integer pdId, FaiList<Param> addPdScInfoList, Param nameIdMap, boolean isSaga){
        int rt = Errno.ARGS_ERROR;
        Set<String> specStrNameSet = new HashSet<>();
        for (Param pdScInfo : addPdScInfoList) {
            if(pdId == null){
                pdId = pdScInfo.getInt(ProductSpecEntity.Info.PD_ID);
            }
            // 消除规格名称和规格值名称的空格，避免主键冲突
            trimProductSpecStrName(pdScInfo);
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            String name = pdScInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
            if(!SpecStrArgCheck.isValidName(name)){
                Log.logErr(rt,"arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                return rt;
            }
            specStrNameSet.add(name);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            int flag = pdScInfo.getInt(ProductSpecEntity.Info.FLAG, 0);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr(rt, "arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return rt;
                    }
                    // 将所有的名称都放入Set中
                    specStrNameSet.add(name);
                }
            }else{
                // 单一规格时，允许inPdScValList为空，例如：只有一个规格 - "白色"，这个白色已经体现在之前的Name中
                if(!Misc.checkBit(flag, ProductSpecValObj.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY)){
                    Log.logErr(rt, "arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                    return rt;
                }
            }
        }
        if(nameIdMap == null){
            nameIdMap = new Param(true); // mapMode
        }
        TransactionCtrl tc = new TransactionCtrl();
        try {
            SpecStrProc specStrProc = new SpecStrProc(flow, aid, tc);
            try {
                LockUtil.lock(aid);
                // 根据 规格名称（里面包含了规格值的名称），获取 （规格字符串id - 规格名称）对应的Map
                rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(specStrNameSet), nameIdMap, isSaga);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            tc.closeDao();
        }

        // 将名称都替换成规格字符串id
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

    /**
     * 消除名称前后的空格
     * @param pdScInfo 规格对象
     */
    public static void trimProductSpecStrName(Param pdScInfo){
        if(pdScInfo.containsKey(SpecStrEntity.Info.NAME)){
            pdScInfo.setString(SpecStrEntity.Info.NAME, Utils.trim(pdScInfo.getString(SpecStrEntity.Info.NAME)));
        }
        if(pdScInfo.containsKey(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST)){
            FaiList<Param> list = pdScInfo.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            for (Param inPdScStrInfo : list) {
                String name = inPdScStrInfo.getString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME);
                if (name == null){
                    continue;
                }
                inPdScStrInfo.setString(fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj.InPdScValList.Item.NAME, Utils.trim(name));
            }
        }
    }

    /**
     * 公共 — 获取 Saga 操作记录
     *
     * @param xid
     * @param branchId
     * @param specProc 业务 proc
     * @param specSkuProc 业务 proc
     * @param specSkuCodeProc 业务 proc
     * @param specStrProc 业务 proc
     * @param specSagaOpListRef 接收数据
     * @param specSkuSagaOpListRef 接收数据
     * @param specSkuCodeSagaOpListRef 接收数据
     * @param specStrSagaOpListRef 接收数据
     * @return {@link Errno}
     */
    private int getSagaOpList(String xid, Long branchId, ProductSpecProc specProc, ProductSpecSkuProc specSkuProc, ProductSpecSkuCodeProc specSkuCodeProc, SpecStrProc specStrProc,
                              Ref<FaiList<Param>> specSagaOpListRef, Ref<FaiList<Param>> specSkuSagaOpListRef, Ref<FaiList<Param>> specSkuCodeSagaOpListRef, Ref<FaiList<Param>> specStrSagaOpListRef) {
        int rt = Errno.OK;
        if (specProc != null && specSagaOpListRef != null) {
            rt = specProc.getSagaOpList(xid, branchId, specSagaOpListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
        }
        if (specSkuProc != null && specSkuSagaOpListRef != null) {
            rt = specSkuProc.getSagaOpList(xid, branchId, specSkuSagaOpListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
        }
        if (specSkuCodeProc != null && specSkuCodeSagaOpListRef != null) {
            rt = specSkuCodeProc.getSagaOpList(xid, branchId, specSkuCodeSagaOpListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
        }
        if (specStrProc != null && specStrSagaOpListRef != null) {
            rt = specStrProc.getSagaOpList(xid, branchId, specStrSagaOpListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                return rt;
            }
        }
        return rt;
    }

    /**
     * 区分 Saga 操作数据
     * @param sagaOpList 总操作数据
     * @param addOpList 添加操作数据
     * @param delOpList 删除操作数据
     * @param modifyOpList 修改操作数据
     */
    private void diffSagaOpList(FaiList<Param> sagaOpList, FaiList<Param> addOpList, FaiList<Param> delOpList, FaiList<Param> modifyOpList) {
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(sagaOpList)) {
            return;
        }
        sagaOpList.forEach(sagaOpInfo -> {
            int sagaOp = sagaOpInfo.getInt(SagaEntity.Common.SAGA_OP);
            switch (sagaOp) {
                case SagaValObj.SagaOp.ADD :
                    if (addOpList != null) {
                        addOpList.add(sagaOpInfo);
                    }
                    break;
                case SagaValObj.SagaOp.DEL :
                    if (delOpList != null) {
                        delOpList.add(sagaOpInfo);
                    }
                    break;
                case SagaValObj.SagaOp.UPDATE :
                    if (modifyOpList != null) {
                        modifyOpList.add(sagaOpInfo);
                    }
                    break;
                default:
                    break;
            }
        });
    }
}
