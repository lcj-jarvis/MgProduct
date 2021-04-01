package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.comm.BizPriKey;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuValObj;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;

/**
 * 维护接口服务各个service共用的方法
 */
public class MgProductInfService extends ServicePub {

    /**
     * 获取unionPriId，返回rt
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Ref<Integer> idRef) {
        int rt = Errno.OK;
        try {
            int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
            idRef.value = unionPriId;
        }catch (MgException e) {
            e.log();
            rt = e.getRt();
        }
        return rt;
    }

    /**
     * 获取unionPriId, 返回值为unionPriId, 出错直接抛异常
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error");
        }

        Ref<Integer> idRef = new Ref<>();
        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, idRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        }
        return idRef.value;
    }

    /**
     * 根据unionPriIdList 获取主键信息
     * @param tid
     * @param unionPriIds
     * @param list
     */
    protected int getPrimaryKeyListByUnionPriIds(int flow, int aid, int tid, FaiList<Integer> unionPriIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error");
            return rt;
        }

        rt = cli.getListByUnionPriIds(unionPriIds, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getListByUnionPriIds error;flow=%d;aid=%d;tid=%d;unionPriIds=%s;", flow, aid, tid, unionPriIds);
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

    protected int getPdIdWithAdd(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, true);
    }
    /**
     * 获取PdId
     */
    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, false);
    }

    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef, boolean withAdd) {
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
            if(withAdd && (rt == Errno.NOT_FOUND)){
                rt = mgProductBasicCli.addProductAndRel(aid, tid, unionPriId, new Param()
                                .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                        , idRef, new Ref<>());
                if(rt != Errno.OK) {
                    Log.logErr(rt, "addProductAndRel error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
                    return rt;
                }
            }
            Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
            return rt;
        }
        idRef.value = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
        return rt;
    }

    /**
     * 获取的商品全部组合信息
     * @param tid 创建商品的tid
     * @param siteId 创建商品的siteId
     * @param lgId 创建商品的lgId
     * @param keepPriId1 创建商品的keepPriId1
     * @param rlPdId 业务商品id
     */
    public int getProductFullInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId) throws IOException {
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

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 获取商品关联信息
            Param pdRelInfo = new Param();
            rt = productBasicProc.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
            if(rt != Errno.OK){
                return rt;
            }
            int pdId = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
            // 2 获取商品基础信息
            // 3 ... 获取商品参数啥的 ... ↓
            // 3.1 获取规格相关
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // 获取商品规格
            FaiList<Param> pdScInfoList = new FaiList<>();
            rt = productSpecProc.getPdScInfoList(aid, tid, unionPriId, pdId, pdScInfoList, false);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // 获取商品规格sku
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, pdScSkuInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // 3.2 获取销售库存相关
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSkuStoreSalesByPdId(aid, tid, pdId, pdScSkuSalesStoreInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(!pdScSkuSalesStoreInfoList.isEmpty()){
                FaiList<Integer> unionPriIdList = new FaiList<>();
                for (Param pdScSkuSalesStoreInfo : pdScSkuSalesStoreInfoList) {
                    Integer _unionPriId = pdScSkuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                    unionPriIdList.add(_unionPriId);
                }
                FaiList<Param> primaryKeyList = new FaiList<>();
                rt = getPrimaryKeyListByUnionPriIds(flow, aid, tid, unionPriIdList, primaryKeyList);
                if(rt != Errno.OK){
                    return rt;
                }
                Map<Integer, BizPriKey> unionPriIdBizPriKeyMap = toUnionPriIdBizPriKeyMap(primaryKeyList);
                ProductStoreService.initSkuStoreSalesPrimaryInfo(unionPriIdBizPriKeyMap, pdScSkuSalesStoreInfoList);
            }
            Param productInfo = new Param();
            productInfo.setList(MgProductEntity.Info.SPEC, pdScInfoList);
            productInfo.setList(MgProductEntity.Info.SPEC_SKU, pdScSkuInfoList);
            productInfo.setList(MgProductEntity.Info.STORE_SALES, pdScSkuSalesStoreInfoList);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            productInfo.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    /**
     * 导入商品
     * @param tid 创建商品的 tid
     * @param siteId 创建商品的 siteId
     * @param lgId 创建商品的 lgId
     * @param keepPriId1 创建商品的 keepPriId1
     * @param productList 商品信息集合
     * @param inStoreRecordInfo 入库记录信息
     * @param useMgProductBasicInfo 是否使用商品中台的基础
     */
    public int importProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, boolean useMgProductBasicInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(Util.isEmptyList(productList)){
                rt = Errno.ARGS_ERROR;
                Log.logErr("productList error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(productList.size() > MgProductInfSvr.SVR_OPTION.getImportProductMaxSize()){
                rt = Errno.SIZE_LIMIT;
                Log.logErr("productList size limit, tid is not valid;flow=%d;aid=%d;tid=%d;size=%s;", flow, aid, tid, productList.size());
                return rt;
            }
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
            HashSet<String> skuNumSet = new HashSet<>();
            FaiList<Param> errProductList = new FaiList<>();
            // 检查导入的数据，并分开正确的数据和错误的数据
            checkImportProductList(productList, skuNumSet, errProductList, useMgProductBasicInfo);

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            skuNumSet.removeAll(Collections.singletonList(null)); // 移除所有null值
            if(!skuNumSet.isEmpty()){ // 校验 skuNum 是否已经存在
                Ref<FaiList<String>> existsSkuNumListRef = new Ref<>();
                rt = productSpecProc.getExistsSkuNumList(aid, tid, unionPriId, new FaiList<>(skuNumSet), existsSkuNumListRef);
                if(rt != Errno.OK){
                    return rt;
                }
                HashSet<String> existsSkuNumSet = new HashSet<>(existsSkuNumListRef.value);
                // 过滤掉已经存在skuNum的商品数据
                filterExistsSkuNum(productList, existsSkuNumSet, errProductList);
            }
            // 组装批量添加的商品基础信息
            FaiList<Param> batchAddBasicInfoList = new FaiList<>(productList.size());
            for (Param productInfo : productList) {
                Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                if(!useMgProductBasicInfo){
                    int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                    batchAddBasicInfoList.add(
                            new Param().setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                    .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                    );
                }else{
                    // TODO 业务方接入商品基础信息到中台时需要删除下面报错同时实现组装逻辑
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt,"args error, tid is not valid;flow=%d;aid=%d;tid=%d;productInfo=%s;", flow, aid, tid, productInfo);
                    return rt;
                }
            }
            // 批量添加商品数据
            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            FaiList<Param> idInfoList = new FaiList<>();
            rt = productBasicProc.batchAddProductAndRel(aid, tid, unionPriId, batchAddBasicInfoList, idInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt, "productBasicProc.batchAddProductAndRel err;aid=%s;tid=%s;unionPriId=%s;batchAddBasicInfoList=%s;", aid, tid, unionPriId, batchAddBasicInfoList);
                return rt;
            }
            if(idInfoList.size() != batchAddBasicInfoList.size()){
                rt = Errno.ERROR;
                Log.logErr(rt,"size err;aid=%s;tid=%s;unionPriId=%s;idInfoList.size=%s;batchAddBasicInfoList.size=%s;", aid, tid, unionPriId, idInfoList.size(), batchAddBasicInfoList.size());
                return rt;
            }
            Set<Integer> addedPdIdSet = new HashSet<>();
            if(!useMgProductBasicInfo){
                Map<Integer, Integer> ownerRlPdId_pdIdMap = new HashMap<>(idInfoList.size()*4/3+1);
                for (Param info : idInfoList) {
                    Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                    Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
                    addedPdIdSet.add(pdId);
                    ownerRlPdId_pdIdMap.put(rlPdId, pdId);
                }
                for (Param productInfo : productList) {
                    Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                    int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                    int pdId = ownerRlPdId_pdIdMap.getOrDefault(rlPdId, 0);
                    if(pdId == 0){
                        rt = Errno.ERROR;
                        Log.logErr(rt,"basic add err;aid=%s;tid=%s;unionPriId=%s;rlPdId=%s;", aid, tid, unionPriId, rlPdId);
                        return rt;
                    }
                    productInfo.setInt(MgProductEntity.Info.PD_ID, pdId);
                    productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                }
            }else{
                // TODO 业务方接入商品基础信息到中台时需要实现
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt,"args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            Log.logStd("begin;flow=%s;aid=%s;tid=%s;unionPrId=%s;addedPdIdSet=%s;",flow, aid, tid, unionPriId, addedPdIdSet);
            Map<Integer/*pdId*/, Map<String/*InPdScStrNameListJson*/, Long/*skuId*/>> pdIdInPdScStrNameListJsonSkuIdMap = new HashMap<>();
            // 导入商品规格和商品规格sku
            {
                // 组装批量导入的数据
                FaiList<Param> importSpecList = new FaiList<>();
                FaiList<Param> importSpecSkuList = new FaiList<>();
                for (Param productInfo : productList) {
                    FaiList<Param> specList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC);
                    if(specList.isEmpty()){
                        continue;
                    }
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    for (Param specInfo : specList) {
                        Param importSpecInfo = new Param();
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.NAME, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.SORT, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.SORT);
                        importSpecInfo.setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.PD_ID, pdId);
                        importSpecList.add(importSpecInfo);
                    }
                    FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
                    for (Param specSkuInfo : specSkuList) {
                        Param importSpecSkuInfo = new Param();
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SKU_NUM_LIST, ProductSpecSkuEntity.Info.SKU_NUM_LIST);
                        importSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                        importSpecSkuList.add(importSpecSkuInfo);
                    }
                }
                FaiList<Param> skuIdInfoList = new FaiList<>();
                rt = productSpecProc.importPdScWithSku(aid, tid, unionPriId, importSpecList, importSpecSkuList, skuIdInfoList);
                if(rt != Errno.OK){
                    return rt;
                }
                for (Param skuIdInfo : skuIdInfoList) {
                    int pdId = skuIdInfo.getInt(ProductSpecSkuEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    if(inPdScStrNameListJsonSkuIdMap == null){
                        inPdScStrNameListJsonSkuIdMap = new HashMap<>();
                        pdIdInPdScStrNameListJsonSkuIdMap.put(pdId, inPdScStrNameListJsonSkuIdMap);
                    }
                    int flag = skuIdInfo.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                    if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){ // spu的数据跳过
                        continue;
                    }

                }
            }



        }finally {
            stat.end((rt != Errno.OK), rt);
        }
        return rt;
    }
    private void filterExistsSkuNum(FaiList<Param> productList, HashSet<String> existsSkuNumSet, FaiList<Param> errProductList){
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            for (Param specSkuInfo : specSkuList) {
                if (existsSkuNumSet.contains(specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_NUM))) {
                    errProductList.add(productInfo);
                    iterator.remove();
                    continue;
                }
                FaiList<String> skuNumList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_NUM_LIST);
                for (String skuNum : skuNumList) {
                    if(existsSkuNumSet.contains(skuNum)){
                        errProductList.add(productInfo);
                        iterator.remove();
                        break;
                    }
                }
            }
        }
    }
    private void checkImportProductList(FaiList<Param> productList, HashSet<String> skuNumSet, FaiList<Param> errProductList, boolean useMgProductBasicInfo) {
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
            if(Str.isEmpty(basicInfo)){
                errProductList.add(productInfo);
                iterator.remove();
                continue;
            }
            if(!useMgProductBasicInfo){
                int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                if(rlPdId <= 0){
                    errProductList.add(productInfo);
                    iterator.remove();
                    continue;
                }
            }

            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            for (Param specSkuInfo : specSkuList) {
                skuNumSet.add(specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_NUM));
                FaiList<String> skuNumList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_NUM_LIST);
                if(skuNumList.size() > ProductSpecValObj.SpecSku.Limit.SKU_NUM_MAX_SIZE){
                    errProductList.add(productInfo);
                    iterator.remove();
                    continue;
                }
                skuNumSet.addAll(skuNumList);
            }
        }
    }

    protected Map<Integer, BizPriKey> toUnionPriIdBizPriKeyMap(FaiList<Param> primaryKeyList) {
        Map<Integer, BizPriKey> unionPriIdBizPriKeyMap = new HashMap<>(primaryKeyList.size()*4/3+1);
        toUnionPriIdBizPriKeyMap(primaryKeyList, unionPriIdBizPriKeyMap);
        return unionPriIdBizPriKeyMap;
    }

    protected void toUnionPriIdBizPriKeyMap(FaiList<Param> primaryKeyList, Map<Integer, BizPriKey> unionPriIdBizPriKeyMap) {
        for (Param primaryKeyInfo : primaryKeyList) {
            Integer tmpUnionPriId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
            Integer tmpTid = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.TID);
            Integer tmpSiteId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            Integer tmpLgId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.LGID);
            Integer tmpKeepPriId1 = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            unionPriIdBizPriKeyMap.put(tmpUnionPriId, new BizPriKey(tmpTid, tmpSiteId, tmpLgId, tmpKeepPriId1));
        }
    }
}
