package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.comm.BizPriKey;
import fai.MgProductInfSvr.domain.comm.ProductSpecCheck;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.entity.*;
import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuValObj;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
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

    public int clearCache(FaiSession session, int flow, int aid) throws IOException {
        int returnRt = Errno.OK;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            MgProductBasicCli mgProductBasicCli = FaiCliFactory.createCli(MgProductBasicCli.class, flow);
            int rt = mgProductBasicCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("basic clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }
            MgProductGroupCli mgProductGroupCli = FaiCliFactory.createCli(MgProductGroupCli.class, flow);
            rt = mgProductGroupCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("group clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductPropCli mgProductPropCli = FaiCliFactory.createCli(MgProductPropCli.class, flow);
            rt = mgProductPropCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("prop clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductSpecCli mgProductSpecCli = FaiCliFactory.createCli(MgProductSpecCli.class, flow);
            rt = mgProductSpecCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("spec clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductStoreCli mgProductStoreCli = FaiCliFactory.createCli(MgProductStoreCli.class, flow);
            rt = mgProductStoreCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("store clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(returnRt != Errno.OK, returnRt);
        }
        return returnRt;
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
            rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, true, pdScSkuInfoList);
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
     * @param ownerTid 创建商品的 ownerTid
     * @param ownerSiteId 创建商品的 ownerSiteId
     * @param ownerLgId 创建商品的 ownerLgId
     * @param ownerKeepPriId1 创建商品的 ownerKeepPriId1
     * @param productList 商品信息集合
     * @param inStoreRecordInfo 入库记录信息
     * @param useMgProductBasicInfo 是否接入使用商品中台基础信息
     */
    public int importProduct(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, boolean useMgProductBasicInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        FaiList<Param> errProductList = new FaiList<>();
        try {
            if(Util.isEmptyList(productList)){
                rt = Errno.ARGS_ERROR;
                Log.logErr("productList error;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }
            if(productList.size() > MgProductInfSvr.SVR_OPTION.getImportProductMaxSize()){
                rt = Errno.SIZE_LIMIT;
                Log.logErr("productList size limit;flow=%d;aid=%d;ownerTid=%d;size=%s;", flow, aid, ownerTid, productList.size());
                return rt;
            }
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
            HashSet<String> skuCodeSet = new HashSet<>();
            // 检查导入的数据，并分开正确的数据和错误的数据
            checkImportProductList(productList, skuCodeSet, errProductList, useMgProductBasicInfo);

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            skuCodeSet.removeAll(Collections.singletonList(null)); // 移除所有null值
            if(!skuCodeSet.isEmpty()){ // 校验 skuCode 是否已经存在
                Ref<FaiList<String>> existsSkuCodeListRef = new Ref<>();
                rt = productSpecProc.getExistsSkuCodeList(aid, ownerTid, ownerUnionPriId, new FaiList<>(skuCodeSet), existsSkuCodeListRef);
                if(rt != Errno.OK){
                    return rt;
                }
                HashSet<String> existsSkuCodeSet = new HashSet<>(existsSkuCodeListRef.value);
                // 过滤掉已经存在skuCode的商品数据
                filterExistsSkuCode(productList, existsSkuCodeSet, errProductList);
            }
            // 组装批量添加的商品基础信息
            FaiList<Param> batchAddBasicInfoList = new FaiList<>(productList.size());
            FaiList<Integer> rlPdIdList = new FaiList<>();
            for (Param productInfo : productList) {
                Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                if(!useMgProductBasicInfo){
                    int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                    batchAddBasicInfoList.add(
                            new Param().setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                    .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                    );
                    productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                    rlPdIdList.add(rlPdId);
                }else{
                    // TODO 业务方接入商品基础信息到中台时需要删除下面报错同时实现组装逻辑
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt,"args error;flow=%d;aid=%d;ownerTid=%d;productInfo=%s;", flow, aid, ownerTid, productInfo);
                    return rt;
                }
            }

            if(batchAddBasicInfoList.isEmpty()){
                rt = Errno.ERROR;
                Log.logErr(rt,"batchAddBasicInfoList empty;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                Log.logDbg("whalelog;flow=%d;aid=%d;ownerTid=%d;errProductList=%s;", flow, aid, ownerTid, errProductList);
                return rt;
            }

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            Set<Integer> alreadyExistsRlPdIdSet = new HashSet<>();
            if(!rlPdIdList.isEmpty()){
                FaiList<Param> alreadyExistsList = new FaiList<>();
                rt = productBasicProc.getRelListByRlIds(aid, ownerUnionPriId, rlPdIdList, alreadyExistsList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    Log.logErr(rt, "productBasicProc.getRelListByRlIds err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;rlPdIdList=%s;", aid, ownerTid, ownerUnionPriId, rlPdIdList);
                    return rt;
                }
                alreadyExistsRlPdIdSet = new HashSet<>(alreadyExistsList.size()*4/3+1);
                for (Param info : alreadyExistsList) {
                    int alreadyRlPdId = info.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID);
                    alreadyExistsRlPdIdSet.add(alreadyRlPdId);
                }
            }
            // 移除已经存在的商品
            if(!alreadyExistsRlPdIdSet.isEmpty()){
                for(Iterator<Param> iterator = batchAddBasicInfoList.iterator();iterator.hasNext();){
                    Param info = iterator.next();
                    if(alreadyExistsRlPdIdSet.contains(info.getInt(ProductRelEntity.Info.RL_PD_ID))){
                        iterator.remove();
                    }
                }
                for(Iterator<Param> iterator = productList.iterator();iterator.hasNext();){
                    Param product = iterator.next();
                    if(alreadyExistsRlPdIdSet.contains(product.getInt(MgProductEntity.Info.RL_PD_ID))){
                        product.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.REPEAT_IMPORT);
                        errProductList.add(product);
                        iterator.remove();
                    }
                }
            }

            if(batchAddBasicInfoList.isEmpty()){
                rt = Errno.ERROR;
                Log.logErr(rt,"batchAddBasicInfoList 2 empty;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                Log.logDbg("whalelog;flow=%d;aid=%d;ownerTid=%d;errProductList=%s;", flow, aid, ownerTid, errProductList);
                return rt;
            }

            FaiList<Param> idInfoList = new FaiList<>();
            // 批量添加商品数据
            rt = productBasicProc.batchAddProductAndRel(aid, ownerTid, ownerUnionPriId, batchAddBasicInfoList, idInfoList);
            if(rt != Errno.OK){
                for (Param product : productList) {
                    product.setInt(MgProductEntity.Info.ERRNO, rt);
                }
                errProductList.addAll(productList);
                Log.logErr(rt, "productBasicProc.batchAddProductAndRel err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;batchAddBasicInfoList=%s;", aid, ownerTid, ownerUnionPriId, batchAddBasicInfoList);
                return rt;
            }
            if(idInfoList.size() != batchAddBasicInfoList.size()){
                rt = Errno.ERROR;
                Log.logErr(rt,"size err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;idInfoList.size=%s;batchAddBasicInfoList.size=%s;", aid, ownerTid, ownerUnionPriId, idInfoList.size(), batchAddBasicInfoList.size());
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
                        Log.logErr(rt,"basic add err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;rlPdId=%s;", aid, ownerTid, ownerUnionPriId, rlPdId);
                        return rt;
                    }
                    productInfo.setInt(MgProductEntity.Info.PD_ID, pdId);
                    productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                }
            }else{
                // TODO 业务方接入商品基础信息到中台时需要实现
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt,"args error;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }
            Map<BizPriKey, Integer> bizPriKeyMap = new HashMap<>();
            // 绑定业务关联
            {
                FaiList<Param> batchBindPdRelList = new FaiList<>();
                for (Param productInfo : productList) {
                    FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                    if(Util.isEmptyList(storeSales)){
                        continue;
                    }
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    int ownerRlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);

                    Map<Integer, Integer> unionPriIdRlPdIdMap = new HashMap<>();
                    // 先批量绑定关联
                    for (Param storeSale : storeSales) {
                        Integer tid = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                        Integer siteId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                        Integer lgId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                        Integer keepPriId1 = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                        int rlPdId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.RL_PD_ID, ownerRlPdId);
                        BizPriKey bizPriKey = new BizPriKey(tid, siteId, lgId, keepPriId1);
                        Integer unionPriId = bizPriKeyMap.get(bizPriKey);
                        if(unionPriId == null){
                            unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
                            bizPriKeyMap.put(bizPriKey, unionPriId);
                        }
                        if(unionPriId != ownerUnionPriId){
                            unionPriIdRlPdIdMap.put(unionPriId, rlPdId);
                        }
                    }
                    if(!unionPriIdRlPdIdMap.isEmpty()){
                        FaiList<Param> bindPdRelList = new FaiList<>(unionPriIdRlPdIdMap.size());
                        for (Map.Entry<Integer, Integer> unionPriIdRlPdIdEntry : unionPriIdRlPdIdMap.entrySet()) {
                            int unionPriId = unionPriIdRlPdIdEntry.getKey();
                            int rlPdId = unionPriIdRlPdIdEntry.getValue();
                            bindPdRelList.add(
                                    new Param()
                                            .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                            .setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId)
                                            .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                            );
                        }
                        batchBindPdRelList.add(
                                new Param()
                                        .setInt(ProductRelEntity.Info.PD_ID, pdId)
                                        .setList(ProductRelEntity.Info.BIND_LIST, bindPdRelList)
                        );
                    }
                }
                if(!batchBindPdRelList.isEmpty()){
                    rt = productBasicProc.batchBindProductsRel(aid, ownerTid, batchBindPdRelList);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "batchBindProductsRel err;aid=%s;ownerTid=%s;batchBindPdRelList=%s", aid, ownerTid, batchBindPdRelList);
                        return rt;
                    }
                }
            }
            Log.logStd("begin;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;addedPdIdSet=%s;",flow, aid, ownerTid, ownerUnionPriId, addedPdIdSet);
            Map<Integer/*pdId*/, Map<String/*InPdScStrNameListJson*/, Long/*skuId*/>> pdIdInPdScStrNameListJsonSkuIdMap = new HashMap<>();
            Map<Integer/*pdId*/, Map<Long/*skuId*/, FaiList<Integer>/*inPdScStrIdList*/>> pdIdSkuIdInPdScStrIdMap = new HashMap<>();
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
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.FLAG, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.FLAG);
                        importSpecInfo.setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.PD_ID, pdId);
                        importSpecList.add(importSpecInfo);
                    }
                    FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
                    for (Param specSkuInfo : specSkuList) {
                        Param importSpecSkuInfo = new Param();
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST, ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                        importSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                        // spu数据
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SPU, ProductSpecSkuEntity.Info.SPU);
                        importSpecSkuList.add(importSpecSkuInfo);
                    }
                }
                FaiList<Param> skuIdInfoList = new FaiList<>();
                // 导入
                if(!importSpecList.isEmpty()){
                    rt = productSpecProc.importPdScWithSku(aid, ownerTid, ownerUnionPriId, importSpecList, importSpecSkuList, skuIdInfoList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
                for (Param skuIdInfo : skuIdInfoList) {
                    int pdId = skuIdInfo.getInt(ProductSpecSkuEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(pdId);
                    if(inPdScStrNameListJsonSkuIdMap == null){
                        inPdScStrNameListJsonSkuIdMap = new HashMap<>();
                        pdIdInPdScStrNameListJsonSkuIdMap.put(pdId, inPdScStrNameListJsonSkuIdMap);
                        skuIdInPdScStrIdMap = new HashMap<>();
                        pdIdSkuIdInPdScStrIdMap.put(pdId, skuIdInPdScStrIdMap);
                    }
                    int flag = skuIdInfo.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                    if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){ // spu的数据跳过
                        continue;
                    }
                    long skuId = skuIdInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                    FaiList<String> inPdScStrNameList = skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                    String inPdScStrNameListJson = inPdScStrNameList.toJson();
                    inPdScStrNameListJsonSkuIdMap.put(inPdScStrNameListJson, skuId);

                    skuIdInPdScStrIdMap.put(skuId, skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST));
                }
            }
            // 导入库存销售sku信息并初始化库存
            {
                FaiList<Param> storeSaleSkuList = new FaiList<>();

                for (Param productInfo : productList) {
                    FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                    if(Util.isEmptyList(storeSales)){
                        continue;
                    }
                    int rlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(pdId);
                    if(inPdScStrNameListJsonSkuIdMap == null){
                        Log.logStd("inPdScStrNameListJsonSkuIdMap empty;flow=%s;aid=%s;productInfo=%s;",flow, aid, productInfo);
                        continue;
                    }
                    Set<String> unionPriIdSkuIdSet = new HashSet<>();
                    for (Param storeSale : storeSales) {
                        FaiList<String> inPdScStrNameList = storeSale.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                        Long skuId = inPdScStrNameListJsonSkuIdMap.get(inPdScStrNameList.toJson());
                        if(skuId == null){
                            Log.logStd("skuId empty;flow=%s;aid=%s;productInfo=%s;inPdScStrNameList=%s;",flow, aid, productInfo, inPdScStrNameList);
                            continue;
                        }

                        FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdMap.get(skuId);

                        Integer tid = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                        Integer siteId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                        Integer lgId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                        Integer keepPriId1 = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                        Integer unionPriId = bizPriKeyMap.get(new BizPriKey(tid, siteId, lgId, keepPriId1));

                        if(!unionPriIdSkuIdSet.add(unionPriId+"-"+skuId)){
                            Log.logStd("skuId already;flow=%s;aid=%s;unionPriId=%s;skuId=%s;rlPdId=%s;storeSale=%s;",flow, aid, unionPriId, skuId, rlPdId, storeSale);
                            continue;
                        }

                        Param importStoreSaleSkuInfo = new Param();
                        importStoreSaleSkuInfo.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.SKU_TYPE, StoreSalesSkuEntity.Info.SKU_TYPE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.SORT, StoreSalesSkuEntity.Info.SORT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.COUNT, StoreSalesSkuEntity.Info.COUNT);
//                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.REMAIN_COUNT, StoreSalesSkuEntity.Info.REMAIN_COUNT);
//                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.HOLDING_COUNT, StoreSalesSkuEntity.Info.HOLDING_COUNT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.PRICE, StoreSalesSkuEntity.Info.PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.DURATION, StoreSalesSkuEntity.Info.DURATION);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.FLAG, StoreSalesSkuEntity.Info.FLAG);
                        importStoreSaleSkuInfo.setList(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                        storeSaleSkuList.add(importStoreSaleSkuInfo);
                    }
                }
                // 导入
                if(!storeSaleSkuList.isEmpty()){
                    ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                    rt = productStoreProc.importStoreSales(aid, ownerTid, ownerUnionPriId, storeSaleSkuList, inStoreRecordInfo);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
            }
            Log.logStd("end;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;",flow, aid, ownerTid, ownerUnionPriId);
            rt = Errno.OK;
        }finally {
            try {
                FaiBuffer sendBuf = new FaiBuffer(true);
                errProductList.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
                session.write(rt, sendBuf);
            }finally {
                stat.end((rt != Errno.OK), rt);
            }
        }
        return rt;
    }
    private void filterExistsSkuCode(FaiList<Param> productList, HashSet<String> existsSkuCodeSet, FaiList<Param> errProductList){
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            boolean remove = false;
            tag:
            for (Param specSkuInfo : specSkuList) {
                if (existsSkuCodeSet.contains(specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_CODE))) {
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                    remove = true;
                    break tag;
                }
                FaiList<String> skuCodeList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST);
                for (String skuCode : skuCodeList) {
                    if(existsSkuCodeSet.contains(skuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                        remove = true;
                        break tag;
                    }
                }
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
            }
        }
    }
    private void checkImportProductList(FaiList<Param> productList, HashSet<String> skuCodeSet, FaiList<Param> errProductList, boolean useMgProductBasicInfo) {
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
            if(Str.isEmpty(basicInfo)){
                errProductList.add(productInfo);
                productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                iterator.remove();
                continue;
            }
            if(!useMgProductBasicInfo){
                int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                if(rlPdId <= 0){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                    errProductList.add(productInfo);
                    iterator.remove();
                    continue;
                }
            }
            boolean remove = false;
            FaiList<Param> specList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC);
            tag:
            for (Param spec : specList) {
                String specName = spec.getString(ProductSpecEntity.SpecInfo.NAME);
                if(!ProductSpecCheck.Spec.checkName(specName)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_NAME_LEN_LIMIT);
                    remove = true;
                    break tag;
                }
                int flag = spec.getInt(ProductSpecEntity.SpecInfo.FLAG, 0);
                FaiList<Param> inPdScValList = spec.getListNullIsEmpty(ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST);
                if(inPdScValList.isEmpty()){
                    if(!Misc.checkBit(flag, ProductSpecValObj.Spec.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.IN_PD_SC_VAL_LIST_IS_EMPTY);
                        remove = true;
                        break tag;
                    }
                }else{
                    Set<String> valNameSet = new HashSet<>(inPdScValList.size()*4/3+1);
                    for (Param inPdScVal : inPdScValList) {
                        String name = inPdScVal.getString(ProductSpecValObj.Spec.InPdScValList.Item.NAME);
                        if(!ProductSpecCheck.Spec.checkName(name)){
                            productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_VAL_NAME_LEN_LIMIT);
                            remove = true;
                            break tag;
                        }
                        if(valNameSet.contains(name)){
                            productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_VAL_NAME_REPEAT);
                            remove = true;
                            break tag;
                        }
                        valNameSet.add(name);
                    }
                }
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
                continue;
            }

            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            remove = false;
            tag:
            for (Param specSkuInfo : specSkuList) {
                String skuCode = specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_CODE);
                if(skuCodeSet.contains(skuCode)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                    remove = true;
                    break tag;
                }
                if(!ProductSpecCheck.SpecSku.checkSkuCode(skuCode)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_LEN_LIMIT);
                    remove = true;
                    break tag;
                }
                if(skuCode != null){
                    skuCodeSet.add(skuCode);
                }
                FaiList<String> skuCodeList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST);
                if(skuCodeList.size() > ProductSpecValObj.SpecSku.Limit.SKU_CODE_MAX_SIZE){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_SIZE_LIMIT);
                    remove = true;
                    break tag;
                }
                for (String tmpSkuCode : skuCodeList) {
                    if(skuCodeSet.contains(tmpSkuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                        remove = true;
                        break tag;
                    }
                    if(!ProductSpecCheck.SpecSku.checkSkuCode(tmpSkuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_LEN_LIMIT);
                        remove = true;
                        break tag;
                    }
                }
                skuCodeSet.addAll(skuCodeList);
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
                continue;
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
