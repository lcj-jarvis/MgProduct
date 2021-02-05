package fai.MgProductInfSvr.application.service;

import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductTempEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecValObj;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 规格服务相关接口实现
 */
public class ProductSpecService extends MgProductInfService {
    /**
     * 批量添加规格模板
     */
    public int addTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> list) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.addTpScInfoList(aid, tid, unionPriId, list);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int delTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlTpScIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.delTpScInfoList(aid, tid, unionPriId, rlTpScIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int setTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.setTpScInfoList(aid, tid, unionPriId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板列表
     */
    public int getTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getTpScInfoList(aid, tid, unionPriId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量添加规格模板详情
     */
    public int addTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> list) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.addTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, list);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板详情
     */
    public int delTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Integer> tpScDtIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.delTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, tpScDtIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量修改规格模板详情
     */
    public int setTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.setTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板列表详情
     */
    public int getTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 导入规格模板
     */
    public int importPdScInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlTpScId, FaiList<Integer> tpScDtIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdIdWithAdd(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;


            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.importPdScInfo(aid, tid, unionPriId, pdId, rlTpScId, tpScDtIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量同步spu 为 sku
     * @param spuInfoList Param见 {@link ProductTempEntity.ProductInfo}
     */
    public int batchSynchronousSPU2SKU(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> spuInfoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(ownerTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, ownerTid is not valid;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int ownerUnionPriId = idRef.value;
            Set<Integer> ownerRlPdIdSet = new HashSet<>();
            for (Param info : spuInfoList) {
                Integer ownerRlPdId = info.getInt(ProductTempEntity.ProductInfo.OWNER_RL_PD_ID);
                ownerRlPdIdSet.add(ownerRlPdId);
            }
            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            FaiList<Param> list = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, ownerUnionPriId, new FaiList<>(ownerRlPdIdSet), list);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            Map<Integer, Integer> ownerRlPdId_pdIdMap = new HashMap<>(ownerRlPdIdSet.size()*4/3+1);
            for (Param info : list) {
                Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
                ownerRlPdIdSet.remove(rlPdId);
                ownerRlPdId_pdIdMap.put(rlPdId, pdId);
            }
            if(!ownerRlPdIdSet.isEmpty()){
                list = new FaiList<>();
                for (Integer ownerPlPdId : ownerRlPdIdSet) {
                    list.add(
                      new Param().setInt(ProductRelEntity.Info.RL_PD_ID, ownerPlPdId)
                              .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                    );
                }
                FaiList<Param> idInfoList = new FaiList<>();
                rt = productBasicProc.batchAddProductAndRel(aid, ownerTid, ownerUnionPriId, list, idInfoList);
                if(rt != Errno.OK){
                    Log.logErr(rt, "productBasicProc.batchAddProductAndRel err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;list=%s;", aid, ownerTid, ownerUnionPriId, list);
                    return rt;
                }
                if(idInfoList.size() != list.size()){
                    Log.logErr("size err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;idInfoList.size=%s;list.size=%s;", aid, ownerTid, ownerUnionPriId, idInfoList.size(), list.size());
                    return rt = Errno.ERROR;
                }
                for (Param info : idInfoList) {
                    Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                    Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
                    ownerRlPdId_pdIdMap.put(rlPdId, pdId);
                }
            }
            FaiList<Param> spuToSkuInfoList = new FaiList<>();
            HashSet<Integer> alreadyHavePdIdSet = new HashSet<>();
            for (Param info : spuInfoList) {
                Integer ownerRlPdId = info.getInt(ProductTempEntity.ProductInfo.OWNER_RL_PD_ID);
                Integer pdId = ownerRlPdId_pdIdMap.get(ownerRlPdId);
                info.setInt(ProductTempEntity.ProductInfo.Internal.PD_ID, pdId);
                String specName = info.getString(ProductTempEntity.ProductInfo.SPEC_NAME);
                String specValName = info.getString(ProductTempEntity.ProductInfo.SPEC_VAL_NAME);
                FaiList<Param> inPdScValList = new FaiList<>();
                inPdScValList.add(
                        new Param()
                                .setString(ProductSpecValObj.InPdScValList.Item.NAME, specValName)
                                .setBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, true)
                );
                if(!alreadyHavePdIdSet.contains(pdId)){
                    spuToSkuInfoList.add(
                            new Param()
                                    .setInt(ProductSpecEntity.Info.PD_ID, pdId)
                                    .setString(ProductSpecEntity.Info.NAME, specName)
                                    .setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, inPdScValList)
                    );
                    alreadyHavePdIdSet.add(pdId);
                }
            }

            FaiList<Param> simplePdScSkuInfoList = new FaiList<>();
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.batchSynchronousSPU2SKU(aid, ownerTid, ownerUnionPriId, spuToSkuInfoList, simplePdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }
            Map<Integer, Long> pdId_skuIdMap = new HashMap<>(simplePdScSkuInfoList.size());
            for (Param simplePdScSkuInfo : simplePdScSkuInfoList) {
                pdId_skuIdMap.put(simplePdScSkuInfo.getInt(ProductSpecSkuEntity.Info.PD_ID), simplePdScSkuInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID));
            }
            for (Param info : spuInfoList) {
                Integer pdId = info.getInt(ProductTempEntity.ProductInfo.Internal.PD_ID);
                info.setLong(ProductTempEntity.ProductInfo.Internal.SKU_ID, pdId_skuIdMap.get(pdId));
            }

            rt = Errno.OK;
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改产品规格总接口
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     */
    public int unionSetPdScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdIdWithAdd(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            Log.logDbg("whalelog updaterList=%s", updaterList);
            rt = productSpecProc.unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, pdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.refreshSkuStoreSales(aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取产品规格列表
     */
    public int getPdScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, boolean onlyGetChecked) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getPdScInfoList(aid, tid, unionPriId, pdId, infoList, onlyGetChecked);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.Spec.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量修改产品规格SKU
     */
    public int setPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            rt = productSpecProc.setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取产品规格SKU列表
     */
    public int getPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecSku.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 根据 skuIdList 获取产品规格SKU列表
     */
    public int getPdSkuScInfoListBySkuIdList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1,  FaiList<Long> skuIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getPdSkuScInfoListBySkuIdList(aid, tid, skuIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }
            Set<Integer> pdIdSet = new HashSet<>();
            for (Param info : infoList) {
                pdIdSet.add(info.getInt(ProductSpecEntity.Info.PD_ID));
            }
            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            FaiList<Param> relList = new FaiList<>();
            rt = productBasicProc.getReducedRelsByPdIds(aid, unionPriId, new FaiList<>(pdIdSet), relList);
            if(rt != Errno.OK){
                return rt;
            }
            Map<Integer, Integer> pdIdRlPdIdMap = new HashMap<>(relList.size()*4/3+1);
            for (Param info : relList) {
                pdIdRlPdIdMap.put(info.getInt(ProductRelEntity.Info.PD_ID), info.getInt(ProductRelEntity.Info.RL_PD_ID));
            }
            for (Param info : infoList) {
                int pdId = info.getInt(ProductSpecEntity.Info.PD_ID);
                Integer rlPdId = pdIdRlPdIdMap.get(pdId);
                info.setInt(fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity.SpecSkuInfo.RL_PD_ID, rlPdId);
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecSku.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 根据 rlPdIdList 获取 rlPdId-skuId 集
     */
    public int getPdSkuIdInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            FaiList<Param> list = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, rlPdIdList, list);
            if(rt != Errno.OK) {
                return rt;
            }
            Map<Integer, Integer> pdIdRlPdIdMap = new HashMap<>(list.size()*4/3+1);
            FaiList<Integer> pdIdList = new FaiList<>();
            for (Param info : list) {
                int pdId = info.getInt(ProductBasicEntity.ProductRelInfo.PD_ID);
                int rlPdId = info.getInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID);
                pdIdRlPdIdMap.put(pdId, rlPdId);
                pdIdList.add(pdId);
            }

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productSpecProc.getPdSkuIdInfoList(aid, tid, pdIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }
            for (Param info : infoList) {
                Integer pdId = info.getInt(ProductSpecSkuEntity.Info.PD_ID);
                Integer rlPdId = pdIdRlPdIdMap.get(pdId);
                info.setInt(fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity.SpecInfo.RL_PD_ID, rlPdId);
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecSku.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
