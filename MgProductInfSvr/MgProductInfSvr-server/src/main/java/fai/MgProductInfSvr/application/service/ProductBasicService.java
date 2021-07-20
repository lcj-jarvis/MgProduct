package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductPropProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuValObj;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.fseata.client.core.exception.TransactionException;
import fai.comm.fseata.client.tm.GlobalTransactionContext;
import fai.comm.fseata.client.tm.api.GlobalTransaction;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 商品基础服务相关接口
 */
public class ProductBasicService extends MgProductInfService {

    public int getBindPropInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlLibId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicProc = new ProductBasicProc(flow);
            FaiList<Param> bindPropList = new FaiList<Param>();
            rt = basicProc.getPdBindPropInfo(aid, tid, unionPriId, rlPdId, bindPropList);
            if (rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPropIds = new FaiList<Integer>();
            for (Param tmpInfo : bindPropList) {
                int rlPropId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.RL_PROP_ID);
                if (!rlPropIds.contains(rlPropId)) {
                    rlPropIds.add(rlPropId);
                }
            }
            if (bindPropList.isEmpty()) {
                rt = Errno.NOT_FOUND;
                return rt;
            }
            ProductPropProc productPropProc = new ProductPropProc(flow);
            FaiList<Param> propValList = new FaiList<Param>();
            // 根据参数id集合，获取参数值id集合
            rt = productPropProc.getPropValList(aid, tid, unionPriId, rlLibId, rlPropIds, propValList);
            if (rt != Errno.OK) {
                return rt;
            }
            // 组装数据
            Param resultInfo = new Param();
            for (Param tmpInfo : bindPropList) {
                int rlPropId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.RL_PROP_ID);
                int propValId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.PROP_VAL_ID);
                // 根据参数id，参数值id，获取参数值信息
                ParamMatcher matcher = new ParamMatcher(ProductBasicEntity.BindPropInfo.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
                matcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.EQ, propValId);
                Param propValInfo = Misc.getFirst(propValList, matcher);
                if (!Str.isEmpty(propValInfo)) {
                    String val = propValInfo.getString(ProductBasicEntity.BindPropInfo.VAL);
                    FaiList<Param> props = resultInfo.getList(ProductBasicDto.BIND_PROP_DTO_PREFIX + rlPropId);
                    if (props == null) {
                        props = new FaiList<Param>();
                        resultInfo.setList(ProductBasicDto.BIND_PROP_DTO_PREFIX + rlPropId, props);
                    }
                    Param info = new Param();
                    info.setInt(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, propValId);
                    info.setString(ProductBasicEntity.BindPropInfo.VAL, val);
                    props.add(info);
                }
            }

            // 将def序列化
            ParamDef def = ProductBasicDto.getBindPropDto(rlPropIds);
            ByteBuffer buf = Parser.paramDefToByteBuffer(def);
            if (buf == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "serialize ParamDef err;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putBuffer(ProductBasicDto.Key.SERIALIZE_TMP_DEF, buf);
            resultInfo.toBuffer(sendBuf, ProductBasicDto.Key.BIND_PROP_INFO, def);
            session.write(sendBuf);
        } finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int setProductBindPropInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addPropList, FaiList<Param> delPropList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setPdBindPropInfo(aid, tid, unionPriId, rlPdId, addPropList, delPropList);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getRlPdByPropVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> proIdsAndValIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            FaiList<Integer> rlPdIds = new FaiList<Integer>();
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.getRlPdByPropVal(aid, tid, unionPriId, proIdsAndValIds, rlPdIds);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            rlPdIds.toBuffer(sendBuf, ProductBasicDto.Key.RL_PD_IDS);
            session.write(sendBuf);
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取商品数据（商品表+业务表）
     */
    public int getProductList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            FaiList<Param> list = new FaiList<>();
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.getProductList(aid, unionPriId, rlPdIds, list);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductBasicDto.Key.PD_LIST, ProductBasicDto.getProductDto());
            session.write(sendBuf);
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        } finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     */
    public int addProductAndRel(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Param info) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (Str.isEmpty(info)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, info is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            Ref<Integer> rlPdIdRef = new Ref<Integer>();
            Ref<Integer> pdIdRef = new Ref<Integer>();
            rt = basicService.addProductAndRel(aid, tid, unionPriId, info, pdIdRef, rlPdIdRef);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdIdRef.value);
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;pdId=%d;", flow, aid, unionPriId, rlPdIdRef.value, pdIdRef.value);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 新增商品业务关联
     */
    public int bindProductRel(FaiSession session, int flow, int aid, String xid, int tid, int siteId, int lgId, int keepPriId1, Param addInfo, Param bindPdInfo, Param inStoreRecordInfo) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (addInfo == null || addInfo.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg error;addInfo is empty;flow=%d;aid=%d;tid=%d;siteId=%d;keepPriId1=%d;", flow, aid, tid, siteId, lgId, keepPriId1);
                return rt;
            }
            if (bindPdInfo == null || bindPdInfo.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg error;bindPdInfo is empty;flow=%d;aid=%d;tid=%d;siteId=%d;keepPriId1=%d;", flow, aid, tid, siteId, lgId, keepPriId1);
                return rt;
            }

            // 获取 unionPriId
            Ref<Integer> idRef = new Ref<>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            int ownerTid = bindPdInfo.getInt(ProductBasicEntity.ProductInfo.TID);
            int ownerSiteId = bindPdInfo.getInt(ProductBasicEntity.ProductInfo.SITE_ID);
            int ownerLgId = bindPdInfo.getInt(ProductBasicEntity.ProductInfo.LGID);
            int ownerKeepPriId1 = bindPdInfo.getInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1);
            // 获取 ownerUnionPriId
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int ownerUnionPriId = idRef.value;
            bindPdInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, ownerUnionPriId);

            Param basicInfo = addInfo.getParam(MgProductEntity.Info.BASIC);
            if (!useBasicInfo(tid)) {
                basicInfo.setBoolean(ProductRelEntity.Info.INFO_CHECK, false);
                Integer rlPdId = basicInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
                if(rlPdId == null) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("arg error;rlPdId is null;flow=%d;aid=%d;tid=%d;siteId=%d;keepPriId1=%d;", flow, aid, tid, siteId, lgId, keepPriId1);
                    return rt;
                }
            }

            Ref<Integer> pdIdRef = new Ref<>();
            Ref<Integer> rlPdIdRef = new Ref<>();

            boolean commit = false;
            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            tx.begin(aid, 60000, "mgProduct-bindProductRel", flow);
            xid = tx.getXid();
            try {
                // 添加商品业务绑定数据
                ProductBasicProc basicProc = new ProductBasicProc(flow);
                rt = basicProc.bindProductRel(aid, tid, unionPriId, xid, bindPdInfo, basicInfo, rlPdIdRef, pdIdRef);
                if (rt != Errno.OK) {
                    return rt;
                }

                addInfo.setInt(MgProductEntity.Info.PD_ID, pdIdRef.value);
                addInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdIdRef.value);

                // 新增库存销售sku信息并初始化库存
                FaiList<Param> storeSaleSkuList = new FaiList<>();
                FaiList<Param> storeSales = addInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                if (!Util.isEmptyList(storeSales)) {
                    int rlPdId = addInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                    int pdId = addInfo.getInt(MgProductEntity.Info.PD_ID);

                    Set<Long> skuIds = new HashSet<>();
                    for (Param storeSale : storeSales) {
                        Long skuId = storeSale.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                        if (skuId == null) {
                            rt = Errno.ARGS_ERROR;
                            Log.logErr(rt, "skuId null;flow=%s;aid=%s;addInfo=%s;", flow, aid, addInfo);
                            return rt;
                        }
                        skuIds.add(skuId);
                    }
                    ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                    FaiList<Param> skuList = new FaiList<>();
                    // 根据 SKU 列表获取信息
                    rt = productSpecProc.getPdSkuScInfoListBySkuIdList(aid, tid, new FaiList<>(skuIds), skuList);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "getPdSkuScInfoListBySkuIdList err;flow=%s;aid=%s;addInfo=%s;", flow, aid, addInfo);
                        return rt;
                    }

                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = new HashMap<>();
                    for(Param skuInfo : skuList) {
                        Long skuId = skuInfo.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                        FaiList<Integer> inPdScStrIdList = skuInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                        skuIdInPdScStrIdMap.put(skuId, inPdScStrIdList);
                    }

                    Set<String> unionPriIdSkuIdSet = new HashSet<>();
                    for (Param storeSale : storeSales) {
                        Long skuId = storeSale.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);

                        FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdMap.get(skuId);
                        if (!unionPriIdSkuIdSet.add(unionPriId + "-" + skuId)) {
                            Log.logStd("skuId already;flow=%s;aid=%s;unionPriId=%s;skuId=%s;rlPdId=%s;storeSale=%s;", flow, aid, unionPriId, skuId, rlPdId, storeSale);
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
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.PRICE, StoreSalesSkuEntity.Info.PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.DURATION, StoreSalesSkuEntity.Info.DURATION);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.FLAG, StoreSalesSkuEntity.Info.FLAG);
                        importStoreSaleSkuInfo.setList(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                        storeSaleSkuList.add(importStoreSaleSkuInfo);
                    }
                }
                if (!storeSaleSkuList.isEmpty()) {
                    ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                    rt = productStoreProc.importStoreSales(aid, tid, unionPriId, tx.getXid(), storeSaleSkuList, inStoreRecordInfo);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                }
                commit = true;
                tx.commit();
            }finally {
                if(!commit) {
                    tx.rollback();
                }
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdIdRef.value);
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;pdId=%d;", flow, aid, ownerUnionPriId, rlPdIdRef.value, pdIdRef.value);
            return rt;
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
    }

    /**
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(FaiSession session, int flow, int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, infoList is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            Integer bindRlPdId = bindRlPdInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID);
            if (bindRlPdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, bindRlPdId is not exist;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            int bindTid = bindRlPdInfo.getInt(ProductBasicEntity.ProductInfo.TID, 0);
            if (!FaiValObj.TermId.isValidTid(bindTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, bindTid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, bindTid);
                return rt;
            }
            int bindSiteId = bindRlPdInfo.getInt(ProductBasicEntity.ProductInfo.SITE_ID, 0);
            int bindLgid = bindRlPdInfo.getInt(ProductBasicEntity.ProductInfo.LGID, 0);
            int bindKeepPriId1 = bindRlPdInfo.getInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, 0);

            FaiList<Param> searchArgList = new FaiList<Param>();
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductBasicEntity.ProductInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductBasicEntity.ProductInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, 0);
                searchArgList.add(new Param().setInt(MgPrimaryKeyEntity.Info.TID, tid)
                        .setInt(MgPrimaryKeyEntity.Info.SITE_ID, siteId)
                        .setInt(MgPrimaryKeyEntity.Info.LGID, lgId)
                        .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, keepPriId1)
                );
            }
            searchArgList.add(new Param().setInt(MgPrimaryKeyEntity.Info.TID, bindTid)
                    .setInt(MgPrimaryKeyEntity.Info.SITE_ID, bindSiteId)
                    .setInt(MgPrimaryKeyEntity.Info.LGID, bindLgid)
                    .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, bindKeepPriId1)
            );
            FaiList<Param> primaryKeyList = new FaiList<Param>();
            rt = getPrimaryKeyList(flow, aid, searchArgList, primaryKeyList);
            if (rt != Errno.OK) {
                return rt;
            }
            Map<String, Integer> primaryKeyMap = new HashMap<String, Integer>();
            for (Param primaryKey : primaryKeyList) {
                Integer resTid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
                Integer siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                Integer lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
                Integer keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                Integer unionPriId = primaryKey.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                primaryKeyMap.put(resTid + "-" + siteId + "-" + lgId + "-" + keepPriId1, unionPriId);
            }
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductBasicEntity.ProductInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductBasicEntity.ProductInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, 0);
                info.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, primaryKeyMap.get(tid + "-" + siteId + "-" + lgId + "-" + keepPriId1));
            }

            Integer bindUnionPriId = primaryKeyMap.get(bindTid + "-" + bindSiteId + "-" + bindLgid + "-" + bindKeepPriId1);
            if (bindUnionPriId == null) {
                rt = Errno.ERROR;
                Log.logErr("get unionPriId error;flow=%d;aid=%d;bindRlPdInfo=%d;", flow, aid, bindRlPdInfo);
                return rt;
            }

            // 重组 bindRlPdInfo
            bindRlPdInfo.clear();
            bindRlPdInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, bindUnionPriId);
            bindRlPdInfo.setInt(ProductBasicEntity.ProductInfo.RL_PD_ID, bindRlPdId);

            ProductBasicProc basicService = new ProductBasicProc(flow);
            Ref<FaiList<Integer>> rlPdIdsRef = new Ref<FaiList<Integer>>();
            rt = basicService.batchBindProductRel(aid, tid, bindRlPdInfo, infoList, rlPdIdsRef);
            if (rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPdIds = rlPdIdsRef.value;
            if (rlPdIds == null) {
                rlPdIds = new FaiList<Integer>();
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            rlPdIds.toBuffer(sendBuf, ProductBasicDto.Key.RL_PD_IDS);
            session.write(sendBuf);
            Log.logStd("batch bind ok;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改单个商品数据
     */
    public int setSinglePd(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Integer rlPdId, ParamUpdater updater) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdId < 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdId < 0;flow=%d;aid=%d;rlPdIds=%d;", flow, aid, rlPdId);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setSinglePd(aid, unionPriId, rlPdId, updater);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set pd ok;flow=%d;aid=%d;tid=%d;rlPdId=%s;", flow, aid, tid, rlPdId);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     *  修改商品数据 包括 规格、库存、分类、标签
     */
    public int setProductInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Integer rlPdId, ParamUpdater recvUpdater) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdId < 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdId < 0;flow=%d;aid=%d;rlPdIds=%d;", flow, aid, rlPdId);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取全局事务
            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            // 开启事务
            try {
                tx.begin(aid, 60000, "mgProduct-setProductInfo", flow);
                // 分配修改内容
                Param updaterData = recvUpdater.getData();
                /** 基础信息修改 start */
                Param basicData = updaterData.getParam(MgProductEntity.Info.BASIC);
                if (!Str.isEmpty(basicData)) {
                    // 将basic表和rel表的修改内容 与 分类关联表 和 参数关联表 分开
                    // 1、分类关联表
                    FaiList<Integer> delRlGroupIds = basicData.getList(ProductBasicEntity.BindGroupInfo.DEL_RL_GROUP_IDS);
                    FaiList<Integer> addRlGroupIds = basicData.getList(ProductBasicEntity.BindGroupInfo.ADD_RL_GROUP_IDS);
                    if (!Util.isEmptyList(delRlGroupIds) || !Util.isEmptyList(addRlGroupIds)) {
                        ProductBasicProc basicProc = new ProductBasicProc(flow);
                        rt = basicProc.setPdBindGroup(aid, unionPriId, rlPdId, addRlGroupIds, delRlGroupIds, tx.getXid());
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        basicData.remove(ProductBasicEntity.BindGroupInfo.DEL_RL_GROUP_IDS);
                        basicData.remove(ProductBasicEntity.BindGroupInfo.ADD_RL_GROUP_IDS);
                    }
                    // 2、参数关联表
                    FaiList<Param> addPropList = basicData.getList(ProductBasicEntity.BindPropInfo.ADD_PROP_LIST);
                    FaiList<Param> delPropList = basicData.getList(ProductBasicEntity.BindPropInfo.DEL_PROP_LIST);
                    if (!Util.isEmptyList(addPropList) || !Util.isEmptyList(delPropList)) {
                        ProductBasicProc basicProc = new ProductBasicProc(flow);
                        rt = basicProc.setPdBindPropInfo(aid, tid, unionPriId, rlPdId, addPropList, delPropList, tx.getXid());
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        basicData.remove(ProductBasicEntity.BindPropInfo.ADD_PROP_LIST);
                        basicData.remove(ProductBasicEntity.BindPropInfo.DEL_PROP_LIST);
                    }
                    //3、标签关联表
                    FaiList<Integer> addRlTagIds = basicData.getList(ProductBasicEntity.BindTagInfo.ADD_RL_TAG_IDS);
                    FaiList<Integer> delRlTagIds = basicData.getList(ProductBasicEntity.BindTagInfo.DEL_RL_TAG_IDS);
                    if (!Util.isEmptyList(addRlTagIds) || !Util.isEmptyList(delRlTagIds)) {
                        ProductBasicProc basicProc = new ProductBasicProc(flow);
                        rt = basicProc.setPdBindTag(aid, unionPriId, rlPdId, addRlTagIds, delRlTagIds, tx.getXid());
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        basicData.remove(ProductBasicEntity.BindTagInfo.ADD_RL_TAG_IDS);
                        basicData.remove(ProductBasicEntity.BindTagInfo.DEL_RL_TAG_IDS);
                    }

                    // check again
                    if (!Str.isEmpty(basicData)) {
                        ProductBasicProc basicProc = new ProductBasicProc(flow);
                        ParamUpdater updater = new ParamUpdater(basicData);
                        rt = basicProc.setSinglePd(aid, unionPriId, rlPdId, updater);
                        if (rt != Errno.OK) {
                            // TODO 分布式事务
                            Oss.logAlarm("setProductInfo error;an error occurred while modifying the basic info");
                            return rt;
                        }
                    }
                }
                /** 基础信息修改 end */

                /** 规格SKU 修改 start */
                FaiList<Param> specSkuList = updaterData.getList(MgProductEntity.Info.SPEC_SKU);
                if (!Util.isEmptyList(specSkuList)) {
                    // 获取 pdId
                    idRef.value = null;
                    rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    int pdId = idRef.value;

                    FaiList<ParamUpdater> specSkuUpdaterList = new FaiList<>();
                    for (Param specSkuInfo : specSkuList) {
                        specSkuUpdaterList.add(new ParamUpdater(specSkuInfo));
                    }
                    ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                    rt = productSpecProc.setPdSkuScInfoList(aid, tid, unionPriId, pdId, specSkuUpdaterList);
                    if(rt != Errno.OK) {
                        // TODO 分布式事务
                        if (!Str.isEmpty(basicData)) {
                            Oss.logAlarm("setProductInfo error;an error occurred while modifying the specSku info");
                        }
                        return rt;
                    }
                }
                /** 规格SKU 修改 end */

                /** 库存信息修改 start */
                FaiList<Param> storeData = updaterData.getList(MgProductEntity.Info.STORE_SALES);
                if (!Util.isEmptyList(storeData)) {
                    // 获取 pdId
                    idRef.value = null;
                    rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    int pdId = idRef.value;

                    Map<FaiList<String>, Param> inPdScStrNameInfoMap = new HashMap<>();
                    for (Param storeInfo : storeData) {
                        Long skuId = storeInfo.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                        if(skuId == null){
                            FaiList<String> inPdScStrNameList = storeInfo.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                            if(inPdScStrNameList == null){
                                return rt = Errno.ARGS_ERROR;
                            }
                            Collections.sort(inPdScStrNameList);
                            inPdScStrNameInfoMap.put(inPdScStrNameList, storeInfo);
                        }
                    }
                    if(inPdScStrNameInfoMap.size() > 0){
                        ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                        FaiList<Param> infoList = new FaiList<Param>();
                        rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, false, infoList);
                        if(rt != Errno.OK) {
                            return rt;
                        }
                        for (Param info : infoList) {
                            FaiList<String> inPdScStrNameList = info.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST);
                            Collections.sort(inPdScStrNameList);
                            Param storeInfo = inPdScStrNameInfoMap.remove(inPdScStrNameList);
                            if(storeInfo == null){
                                continue;
                            }
                            storeInfo.assign(info, ProductSpecEntity.SpecSkuInfo.SKU_ID, ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                        }
                        if(inPdScStrNameInfoMap.size() > 0){
                            Log.logErr("args error, updaterList is err;flow=%d;aid=%d;tid=%d;inPdScStrNameInfoMap=%s;", flow, aid, tid, inPdScStrNameInfoMap);
                            return rt = Errno.ARGS_ERROR;
                        }
                    }

                    FaiList<ParamUpdater> updaterList = new FaiList<>();
                    for (Param storeInfo : storeData) {
                        updaterList.add(new ParamUpdater(storeInfo));
                    }
                    ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                    rt = productStoreProc.setSkuStoreSales(aid, tid, unionPriId, pdId, rlPdId, updaterList);
                    if(rt != Errno.OK) {
                        // TODO 分布式事务
                        if (!Str.isEmpty(basicData) || !Util.isEmptyList(specSkuList)) {
                            Oss.logAlarm("setProductInfo error;an error occurred while modifying the storeSaleSku info");
                        }
                        return rt;
                    }
                }
                /** 库存信息修改 end */

                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                Log.logStd("set productInfo ok;flow=%d;aid=%d;tid=%d;rlPdId=%s;", flow, aid, tid, rlPdId);
            } catch (TransactionException e) {
                rt = Errno.ERROR;
                e.printStackTrace();
            } finally {
                if (rt != Errno.OK) {
                    tx.rollback();
                } else {
                    tx.commit();
                }
            }
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int setProducts(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, ParamUpdater updater) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setProducts(aid, unionPriId, rlPdIds, updater);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set pds ok;flow=%d;aid=%d;tid=%d;rlPdIds=%s;", flow, aid, tid, rlPdIds);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 取消 rlPdIds 的商品业务关联
     *
     * @return
     */
    public int batchDelPdRelBind(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, boolean softDel) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.batchDelPdRelBind(aid, unionPriId, rlPdIds, softDel);
            if (rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("batch del bind ok;flow=%d;aid=%d;tid=%d;rlPdIds=%s;", flow, aid, tid, rlPdIds);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 删除 rlPdIds 的商品数据及业务关联
     *
     * @return
     */
    public int batchDelProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, String xid, FaiList<Integer> rlPdIds, boolean softDel) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            boolean commit = false;
            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            tx.begin(aid, 60000, "mgProduct-batchDelProduct", flow);
            try {
                ProductStoreProc storeProc = new ProductStoreProc(flow);
                ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                ProductBasicProc basicService = new ProductBasicProc(flow);
                FaiList<Param> list = new FaiList<>();
                rt = basicService.getRelListByRlIds(aid, unionPriId, rlPdIds, list);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "batchDelProduct err;aid=%s;tid=%s;uid=%d;rlPdIds=%s;", aid, tid, unionPriId, rlPdIds);
                    return rt;
                }
                FaiList<Integer> pdIdList = OptMisc.getValList(list, ProductBasicEntity.ProductInfo.PD_ID);
                StringBuilder msg = new StringBuilder();
                msg.append(String.format("del products start;flow=%d;aid=%d;tid=%d;rlPdIds=%s;pdIdList=%s;", flow, aid, tid, rlPdIds, pdIdList));
                Log.logStdCusLen(msg.length(), msg.toString());// 输出全日志
                // TODO 分布式事务

                // 删除商品基础信息
                rt = basicService.batchDelProduct(aid, tid, unionPriId, rlPdIds, softDel);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "batchDelProduct err;aid=%s;tid=%s;pdIdList=%s;", aid, tid, pdIdList);
                    return rt;
                }
                // 删除库存销售相关信息
                rt = storeProc.batchDelPdAllStoreSales(aid, tid, pdIdList, tx.getXid(), softDel);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "batchDelPdAllStoreSales err;aid=%s;tid=%s;pdIdList=%s;", aid, tid, pdIdList);
                    return rt;
                }
                // 删除商品规格相关信息
                rt = productSpecProc.batchDelPdAllSc(aid, tid, pdIdList, softDel);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "batchDelPdAllStoreSales err;aid=%s;tid=%s;pdIdList=%s;", aid, tid, pdIdList);
                    return rt;
                }

                commit = true;
                tx.commit();
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                Log.logStd("del products ok;flow=%d;aid=%d;tid=%d;softDel=%s;pdIdList=%s;", flow, aid, tid, softDel, pdIdList);
            } finally {
                if (!commit) {
                    tx.rollback();
                }
            }
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getPdBindGroupList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            FaiList<Param> list = new FaiList<>();
            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.getPdBindGroups(aid, unionPriId, rlPdIds, list);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductBasicDto.Key.BIND_GROUP_LIST, ProductBasicDto.getBindGroupDto());
            session.write(sendBuf);
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int setPdBindGroup(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setPdBindGroup(aid, unionPriId, rlPdId, addGroupIds, delGroupIds);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int addProductInfo(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, String xid, Param addInfo, Param inStoreRecordInfo) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            // 获取 unionPriId
            Ref<Integer> idRef = new Ref<>();
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int ownerUnionPriId = idRef.value;
            // TODO 后期考虑交给业务控制，是否使用中台商品基础信息
            boolean useMgProductBasicInfo = false;
            Ref<Integer> pdIdRef = new Ref<>();
            Ref<Integer> rlPdIdRef = new Ref<>();
            boolean commit = false;
            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            tx.begin(aid, 60000, "mgProduct-addProduct", flow);
            try {
                if (addInfo.isEmpty()) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("arg error;addInfo is empty;flow=%d;aid=%d;uid=%d;", flow, aid, ownerUnionPriId);
                }
                rt = checkAddProductInfo(addInfo, useMgProductBasicInfo);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "check fail;flow=%d;aid=%d;uid=%d;", flow, aid, ownerUnionPriId);
                    return rt;
                }
                Param basicInfo = addInfo.getParam(MgProductEntity.Info.BASIC);
                FaiList<Integer> rlPdIdList = new FaiList<>();
                if (!useMgProductBasicInfo) {
                    basicInfo.setBoolean(ProductRelEntity.Info.INFO_CHECK, false);
                    rlPdIdList.add(basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0));
                } else {
                    // TODO 业务方接入商品基础信息到中台时需要删除下面报错同时实现组装逻辑
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt, "args error;flow=%d;aid=%d;tid=%d;addInfo=%s;", flow, aid, ownerTid, addInfo);
                    return rt;
                }

                ProductBasicProc basicProc = new ProductBasicProc(flow);
                if (!rlPdIdList.isEmpty()) {
                    FaiList<Param> alreadyExistsList = new FaiList<>();
                    rt = basicProc.getRelListByRlIds(aid, ownerUnionPriId, rlPdIdList, alreadyExistsList);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "productBasicProc.getRelListByRlIds err;aid=%d;tid=%d;unionPriId=%d;rlPdIdList=%s;", aid, ownerTid, ownerUnionPriId, rlPdIdList);
                        return rt;
                    }
                    if (!alreadyExistsList.isEmpty()) {
                        rt = Errno.ARGS_ERROR;
                        Log.logErr(rt, "rlPdId is exist;aid=%d;tid=%d;unionPriId=%d;rlPdId=%s;", aid, ownerTid, ownerUnionPriId, rlPdIdList);
                        return rt;
                    }
                }

                // 添加商品数据
                rt = basicProc.addProductAndRel(aid, ownerTid, ownerUnionPriId, basicInfo, pdIdRef, rlPdIdRef);
                if (rt != Errno.OK) {
                    return rt;
                }
                addInfo.setInt(MgProductEntity.Info.PD_ID, pdIdRef.value);
                addInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdIdRef.value);

                // 添加商品与分类的绑定
                FaiList<Integer> rlGroupIds = basicInfo.getList(ProductBasicEntity.ProductInfo.RL_GROUP_IDS);
                if (!Util.isEmptyList(rlGroupIds)) {
                    rt = basicProc.setPdBindGroup(aid, ownerUnionPriId, rlPdIdRef.value, rlGroupIds, null);
                    if (rt != Errno.OK) {
                        // TODO 因为未加入分布式事务，只能先告警
                        Oss.logAlarm("addProductInfo error;there is an error in the binding groupInfo");
                        return rt;
                    }
                }

                // 添加商品与参数的绑定
                FaiList<Integer> rlPropIds = basicInfo.getList(ProductBasicEntity.ProductInfo.RL_PROP_IDS);
                FaiList<Integer> propValIds = basicInfo.getList(ProductBasicEntity.ProductInfo.PROP_VAL_IDS);
                if (!Util.isEmptyList(rlPropIds) && !Util.isEmptyList(propValIds) && rlPropIds.size() == propValIds.size()) {
                    FaiList<Param> addList = new FaiList<>();
                    // 构造添加的参数
                    for (int i = 0; i < propValIds.size(); i++) {
                        Param info = new Param();
                        info.setInt(ProductBindPropEntity.Info.AID, aid);
                        info.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdIdRef.value);
                        info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropIds.get(i));
                        info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValIds.get(i));
                        info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, ownerUnionPriId);
                        addList.add(info);
                    }
                    rt = basicProc.setPdBindPropInfo(aid, ownerTid, ownerUnionPriId, rlPdIdRef.value, addList, null);
                    if (rt != Errno.OK) {
                        // TODO 因为未加入分布式事务，只能先告警
                        Oss.logAlarm("addProductInfo error;there is an error in the binding propInfo");
                        return rt;
                    }
                }

                // 商品规格和商品规格sku
                Map<Integer/*pdId*/, Map<String/*InPdScStrNameListJson*/, Long/*skuId*/>> pdIdInPdScStrNameListJsonSkuIdMap = new HashMap<>();
                Map<Integer/*pdId*/, Map<Long/*skuId*/, FaiList<Integer>/*inPdScStrIdList*/>> pdIdSkuIdInPdScStrIdMap = new HashMap<>();
                FaiList<Param> specList = addInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC);
                if (!specList.isEmpty()) {
                    ProductSpecProc specProc = new ProductSpecProc(flow);
                    int pdId = addInfo.getInt(MgProductEntity.Info.PD_ID);
                    FaiList<Param> importSpecList = new FaiList<>();
                    FaiList<Param> importSpecSkuList = new FaiList<>();
                    for (Param specInfo : specList) {
                        Param importSpecInfo = new Param();
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.NAME, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.SORT, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.SORT);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.FLAG, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.FLAG);
                        importSpecInfo.setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.PD_ID, pdId);
                        importSpecList.add(importSpecInfo);
                    }
                    FaiList<Param> specSkuList = addInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
                    for (Param specSkuInfo : specSkuList) {
                        Param importSpecSkuInfo = new Param();
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST, ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                        importSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                        // spu数据
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SPU, ProductSpecSkuEntity.Info.SPU);
                        importSpecSkuList.add(importSpecSkuInfo);
                    }
                    FaiList<Param> skuIdInfoList = new FaiList<>();
                    // 导入
                    if (!importSpecList.isEmpty()) {
                        rt = specProc.importPdScWithSku(aid, ownerTid, ownerUnionPriId, importSpecList, importSpecSkuList, skuIdInfoList);
                        if (rt != Errno.OK) {
                            // TODO 因为未加入分布式事务，只能先告警
                            Oss.logAlarm("addProductInfo error;an error occurred in adding the product specification");
                            return rt;
                        }
                    }
                    for (Param skuIdInfo : skuIdInfoList) {
                        int curPdId = skuIdInfo.getInt(ProductSpecSkuEntity.Info.PD_ID);
                        Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(curPdId);
                        Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(curPdId);
                        if (inPdScStrNameListJsonSkuIdMap == null) {
                            inPdScStrNameListJsonSkuIdMap = new HashMap<>();
                            pdIdInPdScStrNameListJsonSkuIdMap.put(curPdId, inPdScStrNameListJsonSkuIdMap);
                            skuIdInPdScStrIdMap = new HashMap<>();
                            pdIdSkuIdInPdScStrIdMap.put(curPdId, skuIdInPdScStrIdMap);
                        }
                        int flag = skuIdInfo.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                        if (Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)) { // spu的数据跳过
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
                FaiList<Param> storeSaleSkuList = new FaiList<>();
                FaiList<Param> storeSales = addInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                if (!Util.isEmptyList(storeSales)) {
                    int rlPdId = addInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                    int pdId = addInfo.getInt(MgProductEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(pdId);
                    if (!inPdScStrNameListJsonSkuIdMap.isEmpty()) {
                        Set<String> unionPriIdSkuIdSet = new HashSet<>();
                        for (Param storeSale : storeSales) {
                            FaiList<String> inPdScStrNameList = storeSale.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                            Long skuId = inPdScStrNameListJsonSkuIdMap.get(inPdScStrNameList.toJson());
                            if (skuId == null) {
                                Log.logStd("skuId empty;flow=%s;aid=%s;addInfo=%s;inPdScStrNameList=%s;", flow, aid, addInfo, inPdScStrNameList);
                                continue;
                            }

                            FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdMap.get(skuId);
                            Integer unionPriId = ownerUnionPriId;
                            if (!unionPriIdSkuIdSet.add(unionPriId + "-" + skuId)) {
                                Log.logStd("skuId already;flow=%s;aid=%s;unionPriId=%s;skuId=%s;rlPdId=%s;storeSale=%s;", flow, aid, unionPriId, skuId, rlPdId, storeSale);
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
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.PRICE, StoreSalesSkuEntity.Info.PRICE);
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.DURATION, StoreSalesSkuEntity.Info.DURATION);
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.FLAG, StoreSalesSkuEntity.Info.FLAG);
                            importStoreSaleSkuInfo.setList(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                            storeSaleSkuList.add(importStoreSaleSkuInfo);
                        }
                    }
                }
                // 导入
                if (!storeSaleSkuList.isEmpty()) {
                    ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                    rt = productStoreProc.importStoreSales(aid, ownerTid, ownerUnionPriId, tx.getXid(), storeSaleSkuList, inStoreRecordInfo);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                }
                commit = true;
                tx.commit();
            } finally {
                if (!commit) {
                    tx.rollback();
                }
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rlPdIdRef.value != null) {
                sendBuf.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdIdRef.value);
            }
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;pdId=%d;", flow, aid, ownerUnionPriId, rlPdIdRef.value, pdIdRef.value);
            return rt;
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
    }

    private static boolean useBasicInfo(int tid) {
        if(tid == FaiValObj.TermId.YK) {
            return false;
        }
        return true;
    }

    private int checkAddProductInfo(Param addInfo, boolean useMgProductBasicInfo) {
        Param basicInfo = addInfo.getParam(MgProductEntity.Info.BASIC);
        if (Str.isEmpty(basicInfo)) {
            Log.logErr("args err;basicInfo is empty");
            return Errno.ARGS_ERROR;
        }
        if (!useMgProductBasicInfo) {
            int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
            if (rlPdId <= 0) {
                Log.logErr("rlPdId is not valid;rlPdId=%d", rlPdId);
                return Errno.ARGS_ERROR;
            }
        }
        return Errno.OK;
    }

    /********************************************商品和标签的关联开始********************************************************/
    @SuccessRt(value = Errno.OK)
    public int getPdBindTagList(FaiSession session, int flow, int aid, int tid, int siteId,
                                int lgId, int keepPriId1, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            FaiList<Param> list = new FaiList<>();
            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.getPdBindTags(aid, unionPriId, rlPdIds, list);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductBasicDto.Key.BIND_TAG_LIST, ProductBasicDto.getBindTagDto());
            session.write(sendBuf);
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setPdBindTag(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1,
                            int rlPdId, FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setPdBindTag(aid, unionPriId, rlPdId, addRlTagIds, delRlTagIds);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delPdBindTag(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> delRlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if (rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.delPdBindTag(aid, unionPriId, delRlPdIds);
            if (rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("delete ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, delRlPdIds);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /********************************************商品和标签的关联结束********************************************************/

}