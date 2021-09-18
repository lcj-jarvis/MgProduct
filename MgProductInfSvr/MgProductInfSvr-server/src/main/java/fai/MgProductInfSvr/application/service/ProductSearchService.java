package fai.MgProductInfSvr.application.service;

import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.interfaces.cli.MgProductSearchCli;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ProductSearchService extends MgProductInfService {

    @SuccessRt({Errno.OK})
    public int searchList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, String esSearchParamString, String dbSearchParamString) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        Param searchResult = searchPd(flow, aid, unionPriId, tid, esSearchParamString, dbSearchParamString);

        FaiBuffer sendBuf = new FaiBuffer(true);
        searchResult.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
        session.write(sendBuf);

        rt = Errno.OK;
        return rt;
    }

    // TODO : 是否考虑加多一层缓存
    @SuccessRt({Errno.OK, Errno.NOT_FOUND})
    public int searchProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1,
                             String esSearchParamString, String dbSearchParamString, Param combined) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // 先查搜索服务
        Param searchResult = searchPd(flow, aid, unionPriId, tid, esSearchParamString, dbSearchParamString);
        FaiList<Integer> pdIds = searchResult.getList(MgProductSearchResult.Info.ID_LIST);
        if(pdIds == null) {
            Log.logDbg("not found;aid=%d;uid=%d;esSearchParamString=%s;dbSearchParamString=%s", aid, unionPriId, esSearchParamString, dbSearchParamString);
            return Errno.NOT_FOUND;
        }
        Integer total = searchResult.getInt(MgProductSearchResult.Info.TOTAL);

        // 1 获取商品信息
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
        // 2 ... 获取商品参数啥的 ... ↓
        // 2.1 获取规格相关
        ProductSpecProc productSpecProc = new ProductSpecProc(flow);
        // 获取商品规格
        Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
        if(getSpec) {
            FaiList<Param> pdScInfoList = new FaiList<>();
            rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.SpecInfo.PD_ID)));
        }

        // 获取商品规格sku
        Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
        if(getSpecSku) {
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
        }

        // 2.2 获取销售库存相关
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
        // 2.2.2 spu销售库存相关
        Map<Integer, List<Param>> spuSalesStoreInfoMap = new HashMap<>();
        if(getSpuSales) {
            FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSpuBizSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIds, spuSalesStoreInfoList, null);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            spuSalesStoreInfoMap = spuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(SpuBizSummaryEntity.Info.PD_ID)));
        }

        FaiList<Param> list = new FaiList<>();
        for(int i = 0; i < pdList.size(); i++) {
            Param productInfo = pdList.get(i);
            Integer pdId = productInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
            Param info = new Param();
            info.setParam(MgProductEntity.Info.BASIC, pdList.get(i));
            if(getSpec) {
                FaiList<Param> specList = new FaiList<>();
                if(pdScInfoMap.containsKey(pdId)) {
                    specList.addAll(pdScInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC, specList);
            }
            if(getSpecSku) {
                FaiList<Param> specSkuList = new FaiList<>();
                if(pdScSkuInfoMap.containsKey(pdId)) {
                    specSkuList.addAll(pdScSkuInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPEC_SKU, specSkuList);
            }
            if(getStoreSales) {
                FaiList<Param> storeSalesList = new FaiList<>();
                if(pdScSkuSalesStoreInfoMap.containsKey(pdId)) {
                    storeSalesList.addAll(pdScSkuSalesStoreInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.STORE_SALES, storeSalesList);
            }
            if(getSpuSales) {
                FaiList<Param> spuSalesList = new FaiList<>();
                if(spuSalesStoreInfoMap.containsKey(pdId)) {
                    spuSalesList.addAll(spuSalesStoreInfoMap.get(pdId));
                }
                info.setList(MgProductEntity.Info.SPU_SALES, spuSalesList);
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

    protected Param searchPd(int flow, int aid, int unionPriId, int tid,String esSearchParamString, String searchParamString) {
        // 先查搜索服务
        MgProductSearchCli searchCli = createSearchCli(flow);
        Param searchRes = new Param();
        int productCount = 100;    // 可以发包拿数据，然后根据商品的数据 走不同的 集群，待实现
        int rt = searchCli.searchList(aid, tid, unionPriId, productCount, esSearchParamString, searchParamString, searchRes);
        if(rt != Errno.OK) {
            throw new MgException(rt, "searchList error;aid=%d;uid=%d;search=%s;", aid, unionPriId, searchParamString);
        }
        return searchRes;
    }

    private MgProductSearchCli createSearchCli(int flow) {
        MgProductSearchCli cli = new MgProductSearchCli(flow);
        if (!cli.init()) {
            throw new RuntimeException("MgProductBasicCli init error, flow="+ flow);
        }
        return cli;
    }

}
