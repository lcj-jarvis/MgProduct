package fai.MgProductSpecSvr.application.service;


import com.sun.corba.se.impl.oa.poa.AOMEntry;
import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Misc2;
import fai.MgProductSpecSvr.domain.comm.ProductSpecSkuArgCheck;
import fai.MgProductSpecSvr.domain.comm.SpecStrArgCheck;
import fai.MgProductSpecSvr.domain.repository.ProductSpecDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.TransactionCrtl;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecProc;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecSkuProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecStrProc;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.entity.*;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.service.ServicePub;
import fai.comm.util.*;

import java.io.IOException;
import java.util.*;


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
                        productSpecDaoCtrl.setAutoCommit(true);
                        rt = productSpecProc.batchDel(aid, pdId, null, null);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecProc.batchAdd(aid, pdId, pdScInfoList, null);
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
            Param nameIdMap = new Param(true);
            if(hasSet){
                rt = checkAndReplaceUpdaterList(flow, aid, pdId, updaterList, nameIdMap);
                if(rt != Errno.OK){
                    return rt;
                }
            }

            Ref<FaiList<Param>> productSpecSkuListRef = new Ref<>();
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(productSpecDaoCtrl)){
                    Log.logErr("registered ProductSpecDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                if(!transactionCrtl.registered(productSpecSkuDaoCtrl)){
                    Log.logErr("registered ProductSpecSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCrtl.setAutoCommit(false);
                        Ref<Boolean> needReFreshSkuRef = new Ref<>(false);
                        if (hasAdd) {
                            rt = productSpecProc.batchAdd(aid, pdId, addPdScInfoList, null, needReFreshSkuRef);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (hasDel) {
                            rt = productSpecProc.batchDel(aid, pdId, delPdScIdList, needReFreshSkuRef);
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
                        FaiList<FaiList<Integer>> skuList = genSkuList(pdScInfoListRef.value, ProductSpecSkuValObj.Limit.SINGLE_PRODUCT_MAX_SIZE);
                        if (skuList == null) {
                            return Errno.SIZE_LIMIT;
                        }
                        if (!needReFreshSkuRef.value) {
                            HashMap<String, FaiList<Integer>> newSkuMap = new HashMap<>( skuList.size() * 4 / 3 +1);
                            skuList.forEach(sku -> {
                                FaiList<Integer> tmpSku = new FaiList<>(sku);
                                Collections.sort(tmpSku);
                                newSkuMap.put(tmpSku.toJson(), sku);
                            });

                            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
                            rt = productSpecSkuProc.getListFromDao(aid, pdId, pdScSkuInfoListRef);
                            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                                return rt;
                            }
                            FaiList<Param> pdScSkuInfoList = pdScSkuInfoListRef.value;
                            // 需要删除的sku
                            FaiList<Long> delSkuIdList = new FaiList<>();
                            pdScSkuInfoList.forEach(pdScSkuInfo -> {
                                FaiList<Integer> inPdScStrIdList = pdScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                                FaiList<Integer> tmpInPdScStrIdList = new FaiList<>(inPdScStrIdList);
                                Collections.sort(tmpInPdScStrIdList);
                                String oldSkuKey = tmpInPdScStrIdList.toJson();
                                if (newSkuMap.remove(oldSkuKey) == null) { // 移除现有的sku，如果不存在,则当前的sku需要删除
                                    delSkuIdList.add(pdScSkuInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID));
                                }
                            });

                            if(delSkuIdList.size() > 0){
                                // 删除没用的sku
                                rt = productSpecSkuProc.batchDel(aid, pdId, delSkuIdList);
                                if (rt != Errno.OK) {
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
                                FaiList<Long> addSkuIdList = new FaiList<>(addPdScSkuInfoList.size());
                                rt = productSpecSkuProc.batchAdd(aid, pdId, addPdScSkuInfoList, addSkuIdList);
                                if (rt != Errno.OK) {
                                    return rt;
                                }
                            }

                            // TODO 修改规格值名称的场景
                        } else {
                            if(!skuList.isEmpty()){
                                FaiList<Long> rtIdList = new FaiList<>(skuList.size());
                                rt = productSpecSkuProc.refreshSku(aid, tid, unionPriId, pdId, skuList, rtIdList);
                                if (rt != Errno.OK) {
                                    Log.logDbg(rt,"refreshSku err aid=%s;pdId=%s;", aid, pdId);
                                    return rt;
                                }
                            }
                        }
                        rt = Errno.OK;
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        Log.logErr(e, "flow=%d;aid=%d;pdId=%s;addPdScInfoList=%s;delPdScIdList=%s;updaterList=%s", flow, aid, pdId, addPdScInfoList, delPdScIdList, updaterList);
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCrtl.rollback();
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCrtl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
                productSpecProc.deleteDirtyCache(aid);
                rt = productSpecSkuProc.getListFromDao(aid, pdId, productSpecSkuListRef, ProductSpecSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                transactionCrtl.closeDao();
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

    public int batchDelPdAllSc(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || tid <= 0 || unionPriId<=0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;unionPriId=%s;pdIdList=%s;", flow, aid, tid, unionPriId, pdIdList);
                return rt;
            }

            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(productSpecDaoCtrl)){
                    Log.logErr("registered ProductSpecDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                if(!transactionCrtl.registered(productSpecSkuDaoCtrl)){
                    Log.logErr("registered ProductSpecSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCrtl.setAutoCommit(false);

                        rt = productSpecProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = productSpecSkuProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCrtl.rollback();
                            productSpecProc.clearIdBuilderCache(aid);
                            productSpecSkuProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCrtl.commit();
                    }
                    productSpecProc.deleteDirtyCache(aid);
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCrtl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            session.write(rt);
            Log.logStd("ok;flow=%d;aid=%d;pdIdList=%d;", flow, aid, pdIdList);
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

    public int setPdSkuScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;updaterList=%s", flow, aid, pdId, updaterList);
                return rt;
            }
            Set<String> inPdScStrNameSet = new HashSet<>();
            Map<FaiList<String>, Param> inPdScStrNameListInfoMap = new HashMap<>();
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    FaiList<String> inPdScStrNameList = data.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        return rt = Errno.ARGS_ERROR;
                    }
                    inPdScStrNameSet.addAll(inPdScStrNameList);
                    inPdScStrNameListInfoMap.put(inPdScStrNameList, data);
                }
                String skuNum = data.getString(ProductSpecSkuEntity.Info.SKU_NUM);
                if(skuNum != null){
                    if(!ProductSpecSkuArgCheck.isValidSkuNum(skuNum)){
                        return rt = Errno.ARGS_ERROR;
                    }
                }
            }
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
                            Log.logStd("err aid=%d;pdId=%s;inPdScStrName=%s;", aid, pdId, inPdScStrName);
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
                        Log.logStd(rt, "err aid=%d;pdId=%s;inPdScStrIdLists=%s;", aid, pdId, inPdScStrIdLists);
                        return rt;
                    }
                }finally {
                    productSpecSkuDaoCtrl.closeDao();
                }
                if(inPdScStrIdLists.size() != pdScSkuInfoListRef.value.size()){
                    return rt = Errno.ERROR;
                }
                for (Param info : pdScSkuInfoListRef.value) {
                    String inPdScStrIdListJson = info.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                    Param data = inPdScStrIdListJsonInfoMap.remove(inPdScStrIdListJson);
                    if(data == null){
                        Log.logStd("err aid=%d;pdId=%s;inPdScStrIdListJson=%s;", aid, pdId, inPdScStrIdListJson);
                        return Errno.ERROR;
                    }
                    data.assign(info, ProductSpecSkuEntity.Info.SKU_ID);
                    data.remove(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                }
            }

            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        productSpecSkuDaoCtrl.setAutoCommit(false);
                        rt = productSpecSkuProc.batchSet(aid, pdId, updaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            productSpecSkuDaoCtrl.rollback();
                        }
                        productSpecSkuDaoCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            FaiList<Object> pdScSkuList = new FaiList<>();
            pdScSkuList.toBuffer(sendBuf, ProductSpecSkuDto.Key.INFO_LIST, ProductSpecSkuDto.getInfoDto());
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getPdSkuScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;updaterList=%s", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                rt = productSpecSkuProc.getListFromDao(aid, pdId, pdScSkuInfoListRef);
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
            sendPdSkuScInfoList(session, psScSkuInfoList);
            Log.logDbg("flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

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
        Map<Integer, Param> idNameMap = Misc2.getMap(listRef.value, SpecStrEntity.Info.SC_STR_ID);
        psScSkuInfoList.forEach(psScSkuInfo->{
            FaiList<Integer> inPdScStrIdList = psScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            FaiList<String> inPdScStrNameList = new FaiList<>(inPdScStrIdList.size());
            psScSkuInfo.setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST, inPdScStrNameList);
            inPdScStrIdList.forEach(scStrId ->{
                Param scStrInfo = idNameMap.get(scStrId);
                inPdScStrNameList.add(scStrInfo.getString(SpecStrEntity.Info.NAME));
            });
        });
        return Errno.OK;
    }

    private void sendPdSkuScInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
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

        Map<Integer, Param> idNameMap = Misc2.getMap(listRef.value, SpecStrEntity.Info.SC_STR_ID);
        pdScInfoList.forEach(pdSpInfo->{
            Param specStrInfo = idNameMap.get(pdSpInfo.getInt(ProductSpecEntity.Info.SC_STR_ID));
            pdSpInfo.setString(ProductSpecEntity.Info.NAME, specStrInfo.getString(SpecStrEntity.Info.NAME));
            FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            inPdScValList.forEach(inpdscValInfo->{
                Param specStrInfo2 = idNameMap.get(inpdscValInfo.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
                inpdscValInfo.setString(ProductSpecValObj.InPdScValList.Item.NAME, specStrInfo2.getString(SpecStrEntity.Info.NAME));
            });
        });
        return Errno.OK;
    }

    private void sendPdScInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.getInfoDto());
        session.write(sendBuf);
    }

    /**
     * 这里只替换 产品规格名称，规格值不替换！
     */
    private int checkAndReplaceUpdaterList(int flow, int aid, int pdId, FaiList<ParamUpdater> updaterList, Param nameIdMap) {
        Set<String> specStrNameSet = new HashSet<>();
        for (ParamUpdater updater : updaterList) {
            Param data = updater.getData();
            String name = data.getString(ProductSpecEntity.Info.NAME);
            if(name != null){
                if(!SpecStrArgCheck.isValidName(name)){
                    Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                    return Errno.ARGS_ERROR;
                }
                specStrNameSet.add(name);
            }
            FaiList<Param> inPdScValList = data.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(inPdScValList != null){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameSet.add(name);
                }
            }
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

        for (ParamUpdater updater : updaterList) {
            Param data = updater.getData();
            String name = (String)data.remove(ProductSpecEntity.Info.NAME);
            if (name != null) {
                Integer scStrId = nameIdMap.getInt(name);
                if(scStrId == null){
                    return Errno.ERROR;
                }
                data.setInt(ProductSpecEntity.Info.SC_STR_ID, scStrId);
                FaiList<Param> inPdScValList = data.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                for (Param inPdScValInfo : inPdScValList) {
                    name = (String)inPdScValInfo.remove(ProductSpecValObj.InPdScValList.Item.NAME);
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

    private int checkAndReplaceAddPdScInfoList(int flow, int aid, int tid, int unionPriId, int pdId, FaiList<Param> addPdScInfoList){
        Set<String> specStrNameSet = new HashSet<>();
        for (Param pdScInfo : addPdScInfoList) {
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
            pdScInfo.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
            String name = pdScInfo.getString(ProductSpecEntity.Info.NAME);
            if(!SpecStrArgCheck.isValidName(name)){
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                return Errno.ARGS_ERROR;
            }
            specStrNameSet.add(name);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameSet.add(name);
                }
            }
        }
        Param nameIdMap = new Param(true); // mapMode
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
            String name = (String)pdScInfo.remove(ProductSpecEntity.Info.NAME);
            Integer scStrId = nameIdMap.getInt(name);
            if(scStrId == null){
                return Errno.ERROR;
            }
            pdScInfo.setInt(ProductSpecEntity.Info.SC_STR_ID, scStrId);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = (String)inPdScValInfo.remove(ProductSpecValObj.InPdScValList.Item.NAME);
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
    private FaiList<FaiList<Integer>> genSkuList(FaiList<Param> inPdScInfoList, int limit){
        // 1 提取出所有勾选的规格字符串id
        FaiList<FaiList<Integer>> allCheckScStrIds = new FaiList<>();
        int idx = 0;
        for (int i = 0; i < inPdScInfoList.size(); i++) {
            Param inPdScInfo = inPdScInfoList.get(i);
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
                idx++;
            }
        }
        if(allCheckScStrIds.size() == 0){
            return new FaiList<>();
        }
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
}
