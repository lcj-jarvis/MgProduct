package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.domain.serviceproc.SpecificationProc;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.service.ServicePub;
import fai.comm.util.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 维护接口服务各个service共用的方法
 */
public class MgProductInfService extends ServicePub {

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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.addTpScInfoList(aid, tid, unionPriId, list);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.delTpScInfoList(aid, tid, unionPriId, rlTpScIdList);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.setTpScInfoList(aid, tid, unionPriId, updaterList);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationProc.getTpScInfoList(aid, tid, unionPriId, infoList);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.addTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, list);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.delTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, tpScDtIdList);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.setTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, updaterList);
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
            SpecificationProc specificationProc = new SpecificationProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationProc.getTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, infoList);
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
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;


            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.importPdScInfo(aid, tid, unionPriId, pdId, rlTpScId, tpScDtIdList);
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
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            SpecificationProc specificationProc = new SpecificationProc(flow);
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = specificationProc.unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, pdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.refreshPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
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

            SpecificationProc specificationProc = new SpecificationProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationProc.getPdScInfoList(aid, tid, unionPriId, pdId, infoList, onlyGetChecked);
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

            SpecificationProc specificationProc = new SpecificationProc(flow);
            rt = specificationProc.setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList);
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

            SpecificationProc specificationProc = new SpecificationProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
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
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException  {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(updaterList == null || updaterList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, updaterList is err;flow=%d;aid=%d;tid=%d;updaterList=%s;", flow, aid, tid, updaterList);
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
            Log.logDbg("whalelog  updaterList=%s", updaterList);

            Map<FaiList<String>, Param> inPdScStrNameInfoMap = new HashMap<>();
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                if(skuId == null){
                    FaiList<String> inPdScStrNameList = data.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        return rt = Errno.ARGS_ERROR;
                    }
                    inPdScStrNameInfoMap.put(inPdScStrNameList, data);
                }
            }
            Log.logDbg("whalelog inPdScStrNameInfoMap=%s", inPdScStrNameInfoMap);
            if(inPdScStrNameInfoMap.size() > 0){
                SpecificationProc specificationProc = new SpecificationProc(flow);
                FaiList<Param> infoList = new FaiList<Param>();
                rt = specificationProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
                if(rt != Errno.OK) {
                    return rt;
                }
                for (Param info : infoList) {
                    Param data = inPdScStrNameInfoMap.get(info.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST));
                    if(data == null){
                        continue;
                    }
                    data.assign(info, ProductSpecEntity.SpecSkuInfo.SKU_ID, ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                }
            }

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.setPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, updaterList);
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
     * 扣减库存
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param reduceMode
     *  扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     * @return
     * @throws IOException
     */
    public int reducePdSkuStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count, int reduceMode, int expireTimeSeconds) throws IOException  {
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

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.reducePdSkuStore(aid, tid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
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
     * 预扣模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode#HOLDING} 步骤2
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     */
    public int reducePdSkuHoldingStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count) throws IOException  {
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

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.reducePdSkuHoldingStore(aid, tid, unionPriId, skuId, rlOrderId, count);
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
     * 扣减库存
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param reduceMode
     *  扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @return
     * @throws IOException
     */
    public int makeUpStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count, int reduceMode) throws IOException  {
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

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.makeUpStore(aid, unionPriId, skuId, rlOrderId, count, reduceMode);
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
     * 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId)  throws IOException {
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

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getStoreSalesSkuDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 添加库存出入库记录
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param infoList
     * @return
     */
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(infoList == null || infoList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, infoList is not valid;flow=%d;aid=%d;tid=%d;infoList=%s;", flow, aid, tid, infoList);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            /*FaiList<Param> searchArgList = new FaiList<>();
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 0);
                // 获取unionPriId
                Ref<Integer> idRef = new Ref<Integer>();
                rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
                if(rt != Errno.OK) {
                    return rt;
                }
                info.setInt(ProductStoreEntity.InOutStoreRecordInfo.UNION_PRI_ID, idRef.value);
                searchArgList.add(
                        new Param().setInt(MgPrimaryKeyEntity.Info.TID, tid)
                        .setInt(MgPrimaryKeyEntity.Info.SITE_ID, siteId)
                        .setInt(MgPrimaryKeyEntity.Info.LGID, lgId)
                        .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, keepPriId1)
                );
            }
            FaiList<Param> primaryKeyList = new FaiList<>();
            rt = getPrimaryKeyList(flow, aid, searchArgList, primaryKeyList);
            if(rt != Errno.OK) {
                return rt;
            }
            Map<String, Integer> primaryKeyMap = new HashMap<>();
            for (Param primaryKey : primaryKeyList) {
                Integer siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                Integer lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
                Integer keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                Integer unionPriId = primaryKey.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                primaryKeyMap.put(siteId + "-" + lgId + "-" + keepPriId1, unionPriId);
            }
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 0);
                info.setInt(ProductStoreEntity.InOutStoreRecordInfo.UNION_PRI_ID, primaryKeyMap.get(siteId + "-" + lgId + "-" + keepPriId1));
            }*/

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.addInOutStoreRecordInfoList(aid, tid, unionPriId, infoList);
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
     * 获取商品业务销售信息
     */
    public int getBizSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId)  throws IOException {
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

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getBizSalesSummaryInfoList(aid, tid, unionPriId, pdId, rlPdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getBizSalesSummaryDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取商品销售信息
     */
    public int getSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList)  throws IOException {
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

            // unionPriId + rlPdIdList
            FaiList<Integer> pdIdList = new FaiList<>(rlPdIdList.size());
            for (Integer rlPdId : rlPdIdList) { // TODO 批量
                // 获取pdId
                idRef.value = null;
                rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
                if(rt != Errno.OK) {
                    return rt;
                }
                int pdId = idRef.value;
                pdIdList.add(pdId);
            }


            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getSalesSummaryInfoList(aid, tid, unionPriId, pdIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getSalesSummaryDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取unionPriId
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Ref<Integer> idRef) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error");
            return rt;
        }

        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, idRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        return rt;
    }

    protected int getPrimaryKeyList(int flow, int aid, FaiList<Param> searchArgList, FaiList<Param> list) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        rt = cli.getPrimaryKeyList(aid, searchArgList, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPrimaryKeyList error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 获取PdId
     */
    private int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        int rt = Errno.ERROR;
        MgProductBasicCli mgProductBasicCli = new MgProductBasicCli(flow);
        if(!mgProductBasicCli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgProductBasicCli error");
            return rt;
        }

        Param pdRelInfo = new Param();
        rt = mgProductBasicCli.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
            return rt;
        }
        idRef.value = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
        return rt;
    }
}
