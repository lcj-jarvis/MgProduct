package fai.MgProductInfSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.async.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.interfaces.cli.MgProductSearchCli;
import fai.MgProductSpecSvr.interfaces.cli.async.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.interfaces.cli.async.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.RemoteStandResult;
import fai.comm.middleground.FaiValObj;
import fai.comm.rpc.client.DefaultFuture;
import fai.comm.rpc.client.FaiClientProxyFactory;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


public class ProductSearchService extends MgProductInfService {

    @SuccessRt({Errno.OK})
    public int searchList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, String esSearchParamString, String dbSearchParamString, String pageInfoString) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // ??????unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
        Param searchResult = searchPd(flow, aid, unionPriId, tid, esSearchParamString, dbSearchParamString, pageInfoString);

        FaiBuffer sendBuf = new FaiBuffer(true);
        searchResult.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
        session.write(sendBuf);

        rt = Errno.OK;
        return rt;
    }

    /**
     * ????????????
     */
    @SuccessRt({Errno.OK, Errno.NOT_FOUND})
    public int asyncSearchProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1,
                             String esSearchParamString, String dbSearchParamString, String pageInfoString, Param combined) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        // ??????unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // ??????????????????
        Param searchResult = searchPd(flow, aid, unionPriId, tid, esSearchParamString, dbSearchParamString, pageInfoString);
        FaiList<Integer> pdIds = searchResult.getList(MgProductSearchResult.Info.ID_LIST);
        if(Utils.isEmptyList(pdIds)) {
            Log.logDbg("not found;aid=%d;uid=%d;esSearchParamString=%s;dbSearchParamString=%s", aid, unionPriId, esSearchParamString, dbSearchParamString);
            return Errno.NOT_FOUND;
        }
        Integer total = searchResult.getInt(MgProductSearchResult.Info.TOTAL);

        // ???????????????????????????????????????,????????????
        // FaiList<Integer> pdIds = Stream.of(18, 28, 29, 30, 31, 32).collect(Collectors.toCollection(FaiList::new));
        // Integer total = pdIds.size();

        boolean getSpec = combined.getBoolean(MgProductEntity.Info.SPEC, false);
        boolean getSpecSku = combined.getBoolean(MgProductEntity.Info.SPEC_SKU, false);
        boolean getStoreSales = combined.getBoolean(MgProductEntity.Info.STORE_SALES, false);
        boolean getSpuSales = combined.getBoolean(MgProductEntity.Info.SPU_SALES, false);
        // TODO ?????????
        int count = 1;
        if (getSpec)  {
            count++;
        }
        if (getSpecSku) {
            count++;
        }
        if (getStoreSales) {
            count++;
        }
        if (getSpuSales) {
            count++;
        }
        CountDownLatch countDownLatch = new CountDownLatch(count);

        // 1???????????????????????????
        MgProductBasicCli mgProductBasicCli = FaiClientProxyFactory.createProxy(MgProductBasicCli.class);
        DefaultFuture getPdTask = mgProductBasicCli.getPdListByPdIds(flow, aid, unionPriId, pdIds);
        FaiList<Param> pdList = new FaiList<>();
        getPdTask.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
            if (result.isSuccess() || result.getRt() == Errno.NOT_FOUND) {
                FaiList<Param> tempPdList = result.getObject(ProductRelDto.Key.INFO_LIST, FaiList.class);
                if (Objects.isNull(tempPdList)) {
                    // NOT_FOUND??????????????????null???
                    tempPdList = new FaiList<>();
                }
                pdList.addAll(tempPdList);
                Log.logStd("finish getting pd info;flow=%d;aid=%d;unionPriId=%d;pdList=%s;", flow, aid, unionPriId, pdList);
            } else {
                // ??????
                int tempRt = result.getRt();
                throw new MgException(tempRt, "error;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
            }
            countDownLatch.countDown();
        });


        // 2 ... ?????????????????????????????? ... ???
        // 2.1 ??????????????????
        Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
        MgProductSpecCli mgProductSpecCli = FaiClientProxyFactory.createProxy(MgProductSpecCli.class);
        if (getSpec) {
            DefaultFuture getSpecTask = mgProductSpecCli.getPdScInfoList4Adm(flow, aid, unionPriId, pdIds, false);
            getSpecTask.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess() || result.getRt() == Errno.NOT_FOUND) {
                    FaiList<Param> pdScInfoList = result.getObject(ProductSpecDto.Key.INFO_LIST, FaiList.class);
                    if (Objects.isNull(pdScInfoList)) {
                        // NOT_FOUND??????????????????null???
                        pdScInfoList = new FaiList<>();
                    }
                    pdScInfoMap.putAll(pdScInfoList.stream().collect(Collectors.groupingBy(info -> info.getInt(ProductSpecEntity.Info.PD_ID))));
                    Log.logStd("finish getting spec info;flow=%d;aid=%d;unionPriId=%d;pdScInfoList=%s;", flow, aid, unionPriId, pdScInfoList);
                } else {
                    // ??????
                    int tempRt = result.getRt();
                    throw new MgException(tempRt, "error;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
                }
                countDownLatch.countDown();
            });
        }

        // ??????????????????sku
        Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
        if(getSpecSku) {
            DefaultFuture getSpecSkuTask = mgProductSpecCli.getPdSkuInfoList4Adm(flow, aid, pdIds, true);
            getSpecSkuTask.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess() || result.getRt() == Errno.NOT_FOUND) {
                    FaiList<Param> pdScSkuInfoList = result.getObject(ProductSpecSkuDto.Key.INFO_LIST, FaiList.class);
                    if (Objects.isNull(pdScSkuInfoList)) {
                        // NOT_FOUND??????????????????null???
                        pdScSkuInfoList = new FaiList<>();
                    }
                    pdScSkuInfoMap.putAll(pdScSkuInfoList.stream().collect(Collectors.groupingBy(info -> info.getInt(ProductSpecSkuEntity.Info.PD_ID))));
                    Log.logStd("finish getting SpecSku info;flow=%d;aid=%d;pdScSkuInfoList=%s;", flow, aid, pdScSkuInfoList);
                } else {
                    // ??????
                    int tempRt = result.getRt();
                    throw new MgException(tempRt, "error;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
                }
                countDownLatch.countDown();
            });
        }

        // 2.2 ????????????????????????
        MgProductStoreCli mgProductStoreCli = FaiClientProxyFactory.createProxy(MgProductStoreCli.class);
        Map<Integer, List<Param>> pdScSkuSalesStoreInfoMap = new HashMap<>();
        if(getStoreSales) {
            DefaultFuture getStoreSalesTask = mgProductStoreCli.batchGetSkuStoreSalesByUidAndPdId(flow, aid, tid, new FaiList<>(Collections.singletonList(unionPriId)), pdIds);
            getStoreSalesTask.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess() || result.getRt() == Errno.NOT_FOUND) {
                    FaiList<Param> pdScSkuSalesStoreInfoList = result.getObject(StoreSalesSkuDto.Key.INFO_LIST, FaiList.class);
                    if (Objects.isNull(pdScSkuSalesStoreInfoList)) {
                        // NOT_FOUND??????????????????null???
                        pdScSkuSalesStoreInfoList = new FaiList<>();
                    }
                    pdScSkuSalesStoreInfoMap.putAll(pdScSkuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info -> info.getInt(StoreSalesSkuEntity.Info.PD_ID))));
                    Log.logStd("finish getting StoreSales info;flow=%d;aid=%d;pdScSkuInfoList=%s;", flow, aid, pdScSkuSalesStoreInfoList);
                } else {
                    // ??????
                    int tempRt = result.getRt();
                    throw new MgException(tempRt, "error;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
                }
                countDownLatch.countDown();
            });
        }

        // 2.2.2 spu??????????????????
        Map<Integer, Param> spuSalesStoreInfoMap = new HashMap<>();
        if(getSpuSales) {
            FaiList<String> useSourceFieldList = new FaiList<>();
            DefaultFuture getSpuSalesTask = mgProductStoreCli.getSpuBizSummaryInfoList(flow, aid, tid, unionPriId, pdIds, useSourceFieldList);
            getSpuSalesTask.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess() || result.getRt() == Errno.NOT_FOUND) {
                    FaiList<Param> spuSalesStoreInfoList = result.getObject(SpuBizSummaryDto.Key.INFO_LIST, FaiList.class);
                    if (Objects.isNull(spuSalesStoreInfoList)) {
                        // NOT_FOUND??????????????????null???
                        spuSalesStoreInfoList = new FaiList<>();
                    }
                    spuSalesStoreInfoMap.putAll(Utils.getMap(spuSalesStoreInfoList, SpuBizSummaryEntity.Info.PD_ID));
                    Log.logStd("finish getting StoreSales info;flow=%d;aid=%d;pdScSkuInfoList=%s;", flow, aid, spuSalesStoreInfoList);
                } else {
                    // ??????
                    int tempRt = result.getRt();
                    throw new MgException(tempRt, "error;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
                }
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            rt = Errno.ERROR;
            throw new MgException(rt, "waiting get result info time out;flow=%d;aid=%d;unionPriId=%d;pdIds=%s", flow, aid, unionPriId, pdIds);
        }

        // ?????????????????????????????????????????????????????????????????????????????????
        /*ParamComparator customComparatorOfPdIds = new ParamComparator();
        customComparatorOfPdIds.addKey(ProductEntity.Info.PD_ID, pdIds);
        pdList.sort(customComparatorOfPdIds);*/

        // ?????????????????????
        FaiList<Param> result = new FaiList<>();
        for (Param productInfo : pdList) {
            Integer pdId = productInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
            Param info = new Param();
            info.setParam(MgProductEntity.Info.BASIC, productInfo);
            if (getSpec) {
                FaiList<Param> specList = new FaiList<>();
                if (pdScInfoMap.containsKey(pdId)) {
                    specList.addAll(pdScInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC, specList);
            }
            if (getSpecSku) {
                FaiList<Param> specSkuList = new FaiList<>();
                if (pdScSkuInfoMap.containsKey(pdId)) {
                    specSkuList.addAll(pdScSkuInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC_SKU, specSkuList);
            }
            if (getStoreSales) {
                FaiList<Param> storeSalesList = new FaiList<>();
                if (pdScSkuSalesStoreInfoMap.containsKey(pdId)) {
                    storeSalesList.addAll(pdScSkuSalesStoreInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.STORE_SALES, storeSalesList);
            }
            if (getSpuSales) {
                info.setParam(MgProductEntity.Info.SPU_SALES, spuSalesStoreInfoMap.get(pdId));
            }
            result.add(info);
        }
        Log.logStd("flow=%d,aid=%d,unionPriId=%d;final result=%s", flow, aid, unionPriId, result);
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        result.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
        if(total != null) {
            sendBuf.putInt(MgProductDto.Key.TOTAL, total);
        }
        session.write(sendBuf);
        return rt;

    }


    // TODO : ???????????????????????????????????????
    @SuccessRt({Errno.OK, Errno.NOT_FOUND})
    public int searchProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1,
                             String esSearchParamString, String dbSearchParamString, String pageInfoString, Param combined) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // ??????unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // ??????????????????
        Param searchResult = searchPd(flow, aid, unionPriId, tid, esSearchParamString, dbSearchParamString, pageInfoString);
        FaiList<Param> list = new FaiList<>();
        Integer total = searchResult.getInt(MgProductSearchResult.Info.TOTAL);
        FaiList<Integer> pdIds = searchResult.getList(MgProductSearchResult.Info.ID_LIST);
        if(Utils.isEmptyList(pdIds)) {
            Log.logDbg("not found;aid=%d;uid=%d;esSearchParamString=%s;dbSearchParamString=%s", aid, unionPriId, esSearchParamString, dbSearchParamString);

            // not found ??????total???
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
            if(total != null) {
                sendBuf.putInt(MgProductDto.Key.TOTAL, total);
            }
            session.write(sendBuf);
            return Errno.NOT_FOUND;
        }

        // 1 ????????????????????????????????????????????????????????????????????????????????????????????????
        ProductBasicProc productBasicProc = new ProductBasicProc(flow);
        FaiList<Param> pdList = new FaiList<>();
        rt = productBasicProc.getPdListByPdIds(aid, unionPriId, pdIds, pdList);
        if(rt != Errno.OK){
            return rt;
        }

        boolean getSpec = combined.getBoolean(MgProductEntity.Info.SPEC, false);
        boolean getSpecSku = combined.getBoolean(MgProductEntity.Info.SPEC_SKU, false);
        boolean getStoreSales = combined.getBoolean(MgProductEntity.Info.STORE_SALES, false);
        boolean getSpuSales = combined.getBoolean(MgProductEntity.Info.SPU_SALES, false);
        // 2 ... ???????????????????????? ... ???
        // 2.1 ??????????????????
        ProductSpecProc productSpecProc = new ProductSpecProc(flow);
        // ??????????????????
        Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
        if(getSpec) {
            FaiList<Param> pdScInfoList = new FaiList<>();
            rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.Info.PD_ID)));
        }

        // ??????????????????sku
        Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
        if(getSpecSku) {
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
        }

        // 2.2 ????????????????????????
        ProductStoreProc productStoreProc = new ProductStoreProc(flow);
        Map<Integer, List<Param>> pdScSkuSalesStoreInfoMap = new HashMap<>();
        if(getStoreSales) {
            FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getStoreSalesByPdIdsAndUIdList(aid, tid, pdIds, new FaiList<>(Arrays.asList(unionPriId)), pdScSkuSalesStoreInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            pdScSkuSalesStoreInfoMap = pdScSkuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(StoreSalesSkuEntity.Info.PD_ID)));
        }
        // 2.2.2 spu??????????????????
        Map<Integer, Param> spuSalesStoreInfoMap = new HashMap<>();
        if(getSpuSales) {
            FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSpuBizSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIds, spuSalesStoreInfoList, null);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            spuSalesStoreInfoMap = Utils.getMap(spuSalesStoreInfoList, SpuBizSummaryEntity.Info.PD_ID);
        }

        // ?????????????????????????????????????????????????????????????????????????????????
       /* ParamComparator customComparatorOfPdIds = new ParamComparator();
        customComparatorOfPdIds.addKey(ProductEntity.Info.PD_ID, pdIds);
        pdList.sort(customComparatorOfPdIds);*/

        Log.logStd("flow=%d;aid=%d;unionPriId=%d;pdList=%s;", flow, aid, unionPriId, pdList);
        for (Param productInfo : pdList) {
            Integer pdId = productInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
            Param info = new Param();
            info.setParam(MgProductEntity.Info.BASIC, productInfo);
            if (getSpec) {
                FaiList<Param> specList = new FaiList<>();
                if (pdScInfoMap.containsKey(pdId)) {
                    specList.addAll(pdScInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC, specList);
            }
            if (getSpecSku) {
                FaiList<Param> specSkuList = new FaiList<>();
                if (pdScSkuInfoMap.containsKey(pdId)) {
                    specSkuList.addAll(pdScSkuInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC_SKU, specSkuList);
            }
            if (getStoreSales) {
                FaiList<Param> storeSalesList = new FaiList<>();
                if (pdScSkuSalesStoreInfoMap.containsKey(pdId)) {
                    storeSalesList.addAll(pdScSkuSalesStoreInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.STORE_SALES, storeSalesList);
            }
            if (getSpuSales) {
                info.setParam(MgProductEntity.Info.SPU_SALES, spuSalesStoreInfoMap.get(pdId));
            }
            list.add(info);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
        if(total != null) {
            sendBuf.putInt(MgProductDto.Key.TOTAL, total);
        }
        session.write(sendBuf);
        return rt;
    }

    protected Param searchPd(int flow, int aid, int unionPriId, int tid,String esSearchParamString, String dbSearchParamString, String pageInfoString) {
        MgProductSearchCli mgProductSearchCli = FaiClientProxyFactory.createProxy(MgProductSearchCli.class);
        // ??????????????????????????????????????????????????? ???????????? ??????????????????
        int productCount = 100;
        RemoteStandResult remoteStandResult = mgProductSearchCli.searchList(flow, aid, unionPriId, tid, productCount, esSearchParamString, dbSearchParamString, pageInfoString);
        if (remoteStandResult.isSuccess()) {
            return remoteStandResult.getObject(MgProductSearchDto.Key.RESULT_INFO, Param.class);
        } else {
            int rt = remoteStandResult.getRt();
            throw new MgException(rt, "searchList error;flow=%d;aid=%d;uid=%d;esSearchParamString=%s;dbSearchParamString=%s", flow, aid, unionPriId, esSearchParamString, dbSearchParamString);

        }
    }

}
