package fai.MgProductSpecSvr.application.service;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Misc2;
import fai.MgProductSpecSvr.domain.comm.ProductSpecSkuArgCheck;
import fai.MgProductSpecSvr.domain.comm.SpecStrArgCheck;
import fai.MgProductSpecSvr.domain.repository.ProductSpecDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.TransacationCrtl;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecProc;
import fai.MgProductSpecSvr.domain.serviceProc.ProductSpecSkuProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecStrProc;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.entity.*;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;


public class ProductSpecService extends ServicePub{
    /**
     * 导入规格模板
     */
    public int importPdScInfo(FaiSession session, int flow, int aid, int pdId, Param tpScDetailInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || Str.isEmpty(tpScDetailInfo)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;tpScDetailInfo=%s;", flow, aid, pdId, tpScDetailInfo);
                return rt;
            }
            FaiList<Param> tpScDetailInfoList = tpScDetailInfo.getList("detailList");
            if(tpScDetailInfoList == null || tpScDetailInfoList.isEmpty()){
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;tpScDetailInfoList=%s;", flow, aid, pdId, tpScDetailInfoList);
                return rt = Errno.ARGS_ERROR;
            }
            final int sourceTid = tpScDetailInfo.getInt("sourceTid");
            FaiList<Param> pdScInfoList = new FaiList<>(tpScDetailInfoList.size());
            tpScDetailInfoList.forEach((tpScDetail)->{
                Param data = new Param();
                pdScInfoList.add(data);

                data.setInt(ProductSpecEntity.Info.SC_STR_ID, tpScDetail.getInt(SpecTempDetailEntity.Info.SC_STR_ID));
                data.setInt(ProductSpecEntity.Info.SOURCE_TID, sourceTid);
                data.setInt(ProductSpecEntity.Info.SORT, tpScDetail.getInt(SpecTempDetailEntity.Info.SORT));
                data.setInt(ProductSpecEntity.Info.FLAG, tpScDetail.getInt(SpecTempDetailEntity.Info.FLAG));
                data.setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, tpScDetail.getList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST));
            });
            ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    rt = productSpecProc.batchAdd(aid, pdId, pdScInfoList, null);
                    if(rt != Errno.OK){
                        return rt;
                    }
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

    public int unionSetPdScInfoList(FaiSession session, int flow, int aid, int unionPriId, int pdId, FaiList<Param> addPdScInfoList,
                                    FaiList<Integer> delPdScIdList, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0 || unionPriId<=0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
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
                rt = checkAndReplaceAddPdScInfoList(flow, aid, pdId, addPdScInfoList);
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
            TransacationCrtl transacationCrtl = new TransacationCrtl();
            try {
                ProductSpecDaoCtrl productSpecDaoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
                ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
                if(!transacationCrtl.registered(productSpecDaoCtrl)){
                    Log.logErr("registered ProductSpecDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                if(!transacationCrtl.registered(productSpecSkuDaoCtrl)){
                    Log.logErr("registered ProductSpecSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(productSpecDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transacationCrtl.setAutoCommit(false);
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
                        rt = productSpecProc.getList(aid, pdId, pdScInfoListRef);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        FaiList<FaiList<Integer>> skuList = genSkuList(pdScInfoListRef.value);
                        if (skuList.size() > ProductSpecSkuValObj.Limit.SINGLE_PRODUCT_MAX_SIZE) {
                            return Errno.SIZE_LIMIT;
                        }
                        if (!needReFreshSkuRef.value) {
                            HashSet<String> newSkuSet = new HashSet<>( skuList.size() * 4 / 3 +1);
                            skuList.forEach(sku -> {
                                Collections.sort(sku);
                                newSkuSet.add(sku.toJson());
                            });

                            Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
                            rt = productSpecSkuProc.getList(aid, pdId, pdScSkuInfoListRef);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            FaiList<Param> pdScSkuInfoList = pdScSkuInfoListRef.value;
                            // 需要删除的sku
                            FaiList<Long> delSkuIdList = new FaiList<>();
                            pdScSkuInfoList.forEach(pdScSkuInfo -> {
                                String oldSku = pdScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST).toJson();
                                if (!newSkuSet.remove(oldSku)) { // 移除现有的sku，如果不存在,则当前的sku需要删除
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
                            if(newSkuSet.size() > 0){
                                // 需要新增的sku
                                FaiList<Param> addPdScSkuInfoList = new FaiList<>(newSkuSet.size());
                                newSkuSet.forEach(sku -> {
                                    addPdScSkuInfoList.add(new Param()
                                            .setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, FaiList.parseIntList(sku)));
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
                                rt = productSpecSkuProc.refreshSku(aid, pdId, skuList, rtIdList);
                                if (rt != Errno.OK) {
                                    Log.logDbg(rt,"refreshSku err aid=%s;pdId=%s;", aid, pdId);
                                    return rt;
                                }
                            }
                        }
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        Log.logErr(e);
                        throw e;
                    }finally {
                        if(rt == Errno.OK){
                            transacationCrtl.commit();
                        }else{
                            transacationCrtl.rollback();
                            return rt;
                        }
                    }
                    rt = productSpecSkuProc.getList(aid, pdId, productSpecSkuListRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transacationCrtl.closeDao();
            }
            FaiList<Param> productSpecSkuList = productSpecSkuListRef.value;

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
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
                return rt;
            }
            sendPdScInfoList(session, pdScInfoList);
            Log.logDbg("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
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
                return rt;
            }
            sendPdScInfoList(session, pdScInfoList);
            Log.logDbg("ok;flow=%d;aid=%d;pdId=%d;", flow, aid, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
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
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    return rt = Errno.ARGS_ERROR;
                }
                String skuNum = data.getString(ProductSpecSkuEntity.Info.SKU_NUM);
                if(skuNum != null){
                    if(!ProductSpecSkuArgCheck.isValidSkuNum(skuNum)){
                        return rt = Errno.ARGS_ERROR;
                    }
                }
            }

            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    rt = productSpecSkuProc.batchSet(aid, pdId, updaterList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
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

            ProductSpecSkuDaoCtrl productSpecSkuDaoCtrl = ProductSpecSkuDaoCtrl.getInstance(flow, aid);
            try {
                ProductSpecSkuProc productSpecSkuProc = new ProductSpecSkuProc(productSpecSkuDaoCtrl, flow);
                Ref<FaiList<Param>> pdScSkuInfoListRef = new Ref<>();
                rt = productSpecSkuProc.getList(aid, pdId, pdScSkuInfoListRef);
                if(rt != Errno.OK){
                    return rt;
                }
                FaiList<Param> psScSkuInfoList = pdScSkuInfoListRef.value;
                rt = initPdScSkuSpecStr(flow, aid, psScSkuInfoList);
                sendPdSkuScInfoList(session, psScSkuInfoList);
            }finally {
                productSpecSkuDaoCtrl.closeDao();
            }
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    private int initPdScSkuSpecStr(int flow, int aid, FaiList<Param> psScSkuInfoList){
        FaiList<Integer> scStrIdList = new FaiList<>();
        psScSkuInfoList.forEach(psScSkuInfo ->{
            scStrIdList.addAll(psScSkuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST));
        });
        Ref<FaiList<Param>> listRef = new Ref<>();
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            int rt = specStrProc.getList(aid, scStrIdList, listRef);
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
        FaiList<Integer> scStrIdList = new FaiList<>();
        pdScInfoList.forEach(pdSpInfo->{
            scStrIdList.add(pdSpInfo.getInt(ProductSpecEntity.Info.SC_STR_ID));
            FaiList<Param> inPdScValList = pdSpInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            inPdScValList.forEach(inpdscValInfo->{
                scStrIdList.add(inpdscValInfo.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
            });
        });
        Ref<FaiList<Param>> listRef = new Ref<>();
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            int rt = specStrProc.getList(aid, scStrIdList, listRef);
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
     * @param nameIdMap need mapMode
     */
    private int checkAndReplaceUpdaterList(int flow, int aid, int pdId, FaiList<ParamUpdater> updaterList, Param nameIdMap) {
        FaiList<String> specStrNameList = new FaiList<>(updaterList.size());
        for (ParamUpdater updater : updaterList) {
            Param data = updater.getData();
            String name = data.getString(ProductSpecEntity.Info.NAME);
            if(name != null){
                if(!SpecStrArgCheck.isValidName(name)){
                    Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                    return Errno.ARGS_ERROR;
                }
                specStrNameList.add(name);
            }
            FaiList<Param> inPdScValList = data.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(inPdScValList != null){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameList.add(name);
                }
            }
        }
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            nameIdMap = new Param(true); // mapMode
            try {
                LockUtil.lock(aid);
                int rt = specStrProc.getListWithBatchAdd(aid, specStrNameList, nameIdMap);
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
            }
        }
        return Errno.OK;
    }

    private int checkAndReplaceAddPdScInfoList(int flow, int aid, int pdId, FaiList<Param> addPdScInfoList){
        FaiList<String> specStrNameList = new FaiList<>(addPdScInfoList.size());
        for (Param pdScInfo : addPdScInfoList) {
            String name = pdScInfo.getString(ProductSpecEntity.Info.NAME);
            if(!SpecStrArgCheck.isValidName(name)){
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                return Errno.ARGS_ERROR;
            }
            specStrNameList.add(name);
            FaiList<Param> inPdScValList = pdScInfo.getListNullIsEmpty(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            if(!inPdScValList.isEmpty()){
                for (Param inPdScValInfo : inPdScValList) {
                    name = inPdScValInfo.getString(ProductSpecValObj.InPdScValList.Item.NAME);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;name=%s", flow, aid, pdId, name);
                        return Errno.ARGS_ERROR;
                    }
                    specStrNameList.add(name);
                }
            }
        }
        Param nameIdMap = new Param(true); // mapMode
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            try {
                LockUtil.lock(aid);
                int rt = specStrProc.getListWithBatchAdd(aid, specStrNameList, nameIdMap);
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
     */
    private FaiList<FaiList<Integer>> genSkuList(FaiList<Param> inPdScInfoList){
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
