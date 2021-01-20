package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.domain.comm.BizPriKey;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.domain.serviceproc.SpecificationProc;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.MgProductStoreSvr.domain.entity.StoreSkuSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.interfaces.entity.SalesSummaryEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;

import java.io.IOException;
import java.util.*;

/**
 * 库存服务相关接口实现
 */
public class ProductStoreService extends MgProductInfService {

    /**
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException {
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

            Map<FaiList<String>, Param> inPdScStrNameInfoMap = new HashMap<>();
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                if(skuId == null){
                    FaiList<String> inPdScStrNameList = data.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        return rt = Errno.ARGS_ERROR;
                    }
                    Collections.sort(inPdScStrNameList);
                    inPdScStrNameInfoMap.put(inPdScStrNameList, data);
                }
            }
            if(inPdScStrNameInfoMap.size() > 0){
                Log.logDbg("whalelog  updaterList=%s", updaterList);
                Log.logDbg("whalelog inPdScStrNameInfoMap=%s", inPdScStrNameInfoMap);
                SpecificationProc specificationProc = new SpecificationProc(flow);
                FaiList<Param> infoList = new FaiList<Param>();
                rt = specificationProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
                if(rt != Errno.OK) {
                    return rt;
                }
                Log.logDbg("whalelog infoList=%s", infoList);
                for (Param info : infoList) {
                    FaiList<String> inPdScStrNameList = info.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST);
                    Collections.sort(inPdScStrNameList);
                    Param data = inPdScStrNameInfoMap.remove(inPdScStrNameList);
                    if(data == null){
                        continue;
                    }
                    data.assign(info, ProductSpecEntity.SpecSkuInfo.SKU_ID, ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                }
                if(inPdScStrNameInfoMap.size() > 0){
                    Log.logErr("args error, updaterList is err;flow=%d;aid=%d;tid=%d;inPdScStrNameInfoMap=%s;", flow, aid, tid, inPdScStrNameInfoMap);
                    return rt = Errno.ARGS_ERROR;
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
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 添加库存出入库记录
     * @param aid
     * @param ownerTid 业务商品所属权的tid
     * @param ownerSiteId 业务商品所属权的 siteId (如:悦客总店的 siteId)
     * @param ownerLgId 业务商品所属权的 ownerLgId (如:悦客总店的 ownerLgId)
     * @param ownerKeepPriId1 业务商品所属权的 ownerKeepPriId1 (如:悦客总店的 ownerKeepPriId1)
     * @param infoList 出入库记录集合，需要包含 tid, siteId, lgId, keepPriId1, rlPdId, ownerRlPdId, skuId|inPdScStrNameList
     * @return
     */
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(ownerTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, ownerTid is not valid;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }
            if(infoList == null || infoList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, infoList is not valid;flow=%d;aid=%d;ownerTid=%d;infoList=%s;", flow, aid, ownerTid, infoList);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            // 添加产品同时初始化库存时，还没生成skuId，只能通过规格值的字符串集间接获取skuId
            Map<Integer, Map<FaiList<String>, Long>> ownerRlPdId_inPdScStrNameSkuIdMapMap = new HashMap<>();

            int ownerUnionPriId = idRef.value;
            Map<Integer, Integer> ownerRlPdIdPdIdMap = new HashMap<>();
            Map<BizPriKey, Integer> bizPriKeyUnionPriIdMap = new HashMap<>();
            Map<BizPriKey, Map<Integer, Integer>> bizPriKey_rlPdIdOwnerRlPdIdMapMap = new HashMap<>();
            FaiList<Param> searchArgList = new FaiList<>();
            for (Param info : infoList) {
                int ownerRlPdId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.OWNER_RL_PD_ID, 0);
                int rlPdId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.RL_PD_ID, 0);
                int tid = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.TID, ownerTid);
                int siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, -1);
                int lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID, -1);
                int keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, -1);
                Long skuId = info.getLong(ProductStoreEntity.InOutStoreRecordInfo.SKU_ID);
                if(ownerRlPdId == 0 || rlPdId == 0 || !FaiValObj.TermId.isValidTid(tid) || siteId < 0 || lgId < 0 || keepPriId1 < 0){
                    Log.logErr("arg ownerRlPdId|rlPdId|siteId|lgId|keepPriId1 err;flow=%d;aid=%d;ownerTid=%d;info=%s;", flow, aid, ownerTid, info);
                    return Errno.ARGS_ERROR;
                }
                if(skuId == null){
                    FaiList<String> inPdScStrNameList = info.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        return rt = Errno.ARGS_ERROR;
                    }
                    Map<FaiList<String>, Long> inPdScStrNameInfoMap = ownerRlPdId_inPdScStrNameSkuIdMapMap.get(ownerRlPdId);
                    if(inPdScStrNameInfoMap == null){
                        inPdScStrNameInfoMap = new HashMap<>();
                        ownerRlPdId_inPdScStrNameSkuIdMapMap.put(ownerRlPdId, inPdScStrNameInfoMap);
                    }
                    Collections.sort(inPdScStrNameList);
                    inPdScStrNameInfoMap.put(inPdScStrNameList, null);
                }
                ownerRlPdIdPdIdMap.put(ownerRlPdId, null);
                {
                    BizPriKey bizPriKey = new BizPriKey(tid, siteId, lgId, keepPriId1);
                    {
                        if(!bizPriKeyUnionPriIdMap.containsKey(bizPriKey)){
                            bizPriKeyUnionPriIdMap.put(bizPriKey, null);
                            searchArgList.add(
                                    new Param().setInt(MgPrimaryKeyEntity.Info.TID, tid)
                                            .setInt(MgPrimaryKeyEntity.Info.SITE_ID, siteId)
                                            .setInt(MgPrimaryKeyEntity.Info.LGID, lgId)
                                            .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, keepPriId1)
                            );
                        }
                    }
                    {
                        Map<Integer, Integer> rlPdIdOwnerRlPdIdMap = bizPriKey_rlPdIdOwnerRlPdIdMapMap.get(bizPriKey);
                        if(rlPdIdOwnerRlPdIdMap == null){
                            rlPdIdOwnerRlPdIdMap = new HashMap<>();
                            bizPriKey_rlPdIdOwnerRlPdIdMapMap.put(bizPriKey, rlPdIdOwnerRlPdIdMap);
                        }
                        rlPdIdOwnerRlPdIdMap.put(rlPdId, ownerRlPdId);
                    }
                }
            }
            // 获取联合主键
            FaiList<Param> primaryKeyList = new FaiList<>();
            rt = getPrimaryKeyList(flow, aid, searchArgList, primaryKeyList);
            if(rt != Errno.OK){
                return rt;
            }
            for (Param primaryKeyInfo : primaryKeyList) {
                int tid = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.TID);
                int siteId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                int lgId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.LGID);
                int keepPriId1 = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                int unionPriId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                bizPriKeyUnionPriIdMap.put(new BizPriKey(tid, siteId, lgId, keepPriId1), unionPriId);
            }
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            {
                FaiList<Param> relPdInfoList = new FaiList<>();
                rt = basicProc.getRelListByRlIds(aid, ownerUnionPriId, new FaiList<>(ownerRlPdIdPdIdMap.keySet()), relPdInfoList);
                if(rt != Errno.OK){
                    return rt;
                }
                if(relPdInfoList.size() != ownerRlPdIdPdIdMap.size()){
                    Log.logErr(rt, "size err;flow=%s;aid=%s;ownerUnionPriId=%s;ownerRlPdIdPdIdMap=%s;", flow, aid, ownerUnionPriId, ownerRlPdIdPdIdMap);
                    return rt = Errno.NOT_FOUND;
                }
                for (Param relPdInfo : relPdInfoList) {
                    ownerRlPdIdPdIdMap.put(relPdInfo.getInt(ProductRelEntity.Info.RL_PD_ID), relPdInfo.getInt(ProductRelEntity.Info.PD_ID));
                }
            }

            // 判断是否关联，没关联则生成关联
            for (Map.Entry<BizPriKey, Map<Integer, Integer>> bizPriKey_rlPdIdOwnerRlPdIdMapEntry : bizPriKey_rlPdIdOwnerRlPdIdMapMap.entrySet()) {
                BizPriKey bizPriKey = bizPriKey_rlPdIdOwnerRlPdIdMapEntry.getKey();
                Map<Integer, Integer> rlPdIdOwnerRlPdIdMap = bizPriKey_rlPdIdOwnerRlPdIdMapEntry.getValue();
                Integer unionPriId = bizPriKeyUnionPriIdMap.get(bizPriKey);
                Set<Integer> rlPdIdSet = rlPdIdOwnerRlPdIdMap.keySet();
                FaiList<Param> list = new FaiList<>();
                rt = basicProc.getRelListByRlIds(aid, unionPriId, new FaiList<>(rlPdIdSet), list);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                // 处理已经关联的商品
                for (Param pdRelInfo : list) {
                    Integer rlPdId = pdRelInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
                    rlPdIdOwnerRlPdIdMap.remove(rlPdId);
                }
                // 处理未关联的商品
                Map<Integer, Set<Integer>> ownerRlPdId_rlPdIdSetMap = new HashMap<>();
                rlPdIdOwnerRlPdIdMap.forEach((rlPdId, ownerRlPdId)->{
                    Set<Integer> tmpRlPdIdSet = ownerRlPdId_rlPdIdSetMap.get(ownerRlPdId);
                    if(tmpRlPdIdSet == null){
                        tmpRlPdIdSet = new HashSet<>();
                        ownerRlPdId_rlPdIdSetMap.put(ownerRlPdId, tmpRlPdIdSet);
                    }
                    tmpRlPdIdSet.add(rlPdId);
                });
                for (Map.Entry<Integer, Set<Integer>> ownerRlPdId_rlPdIdSetEntry : ownerRlPdId_rlPdIdSetMap.entrySet()) {
                    Integer ownerRlPdId = ownerRlPdId_rlPdIdSetEntry.getKey();
                    Set<Integer> tmpRlPdIdSet = ownerRlPdId_rlPdIdSetEntry.getValue();
                    // 被绑定的商品信息
                    Param bindedRlPdInfo = new Param()
                            .setInt(ProductRelEntity.Info.RL_PD_ID, ownerRlPdId)
                            .setInt(ProductRelEntity.Info.UNION_PRI_ID, ownerUnionPriId)
                            ;
                    FaiList<Param> needBindInfoList = new FaiList<>(tmpRlPdIdSet.size());
                    for (Integer rlPdId : tmpRlPdIdSet) {
                        needBindInfoList.add( new Param()
                                .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                .setInt(ProductRelEntity.Info.UNION_PRI_ID,unionPriId)
                                .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                        );
                    }
                    rt = basicProc.batchBindProductRel(aid, bizPriKey.tid, bindedRlPdInfo, needBindInfoList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
            }

            if(ownerRlPdId_inPdScStrNameSkuIdMapMap.size() > 0){
                SpecificationProc specificationProc = new SpecificationProc(flow);
                for (Map.Entry<Integer, Map<FaiList<String>, Long>> ownerRlPdId_inPdScStrNameSkuIdMapEntry : ownerRlPdId_inPdScStrNameSkuIdMapMap.entrySet()) {
                    Integer ownerRlPdId = ownerRlPdId_inPdScStrNameSkuIdMapEntry.getKey();
                    Integer ownerPdId = ownerRlPdIdPdIdMap.get(ownerRlPdId);
                    FaiList<Param> pdSkuScInfoList = new FaiList<Param>();
                    rt = specificationProc.getPdSkuScInfoList(aid, ownerTid, ownerUnionPriId, ownerPdId, pdSkuScInfoList);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    Map<FaiList<String>, Long> inPdScStrNameSkuIdMap = ownerRlPdId_inPdScStrNameSkuIdMapEntry.getValue();
                    for (Param pdSkuScInfo : pdSkuScInfoList) {
                        FaiList<String> inPdScStrNameList = pdSkuScInfo.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST);
                        Collections.sort(inPdScStrNameList);
                        if(!inPdScStrNameSkuIdMap.containsKey(inPdScStrNameList)){
                            continue;
                        }
                        inPdScStrNameSkuIdMap.put(inPdScStrNameList, pdSkuScInfo.getLong(ProductSpecEntity.SpecSkuInfo.SKU_ID));
                    }
                    for (Long skuId : inPdScStrNameSkuIdMap.values()) {
                        if(skuId == null){
                            Log.logErr("skuId not found error;flow=%d;aid=%d;inPdScStrNameSkuIdMap=%s;", flow, aid, inPdScStrNameSkuIdMap);
                            return rt = Errno.ARGS_ERROR;
                        }
                    }
                }
            }

            for (Param info : infoList) { // 组装好数据
                int ownerRlPdId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.OWNER_RL_PD_ID);
                int tid = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.TID, ownerTid);
                int siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID);
                int lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID);
                int keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1);
                int unionPriId = bizPriKeyUnionPriIdMap.get(new BizPriKey(tid, siteId, lgId, keepPriId1));
                int pdId = ownerRlPdIdPdIdMap.get(ownerRlPdId);
                Long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID);
                if (skuId == null) {
                    FaiList<String> inPdScStrNameList = info.getList(ProductStoreEntity.InOutStoreRecordInfo.IN_PD_SC_STR_NAME_LIST);
                    skuId = ownerRlPdId_inPdScStrNameSkuIdMapMap.get(ownerRlPdId).get(inPdScStrNameList);
                }
                info.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                info.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                info.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            }

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            rt = productStoreProc.addInOutStoreRecordInfoList(aid, ownerTid, ownerUnionPriId, infoList);
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
     * 获取指定商品所有的业务销售记录
     */
    public int getAllBizSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId)  throws IOException {
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
            rt = productStoreProc.getAllBizSalesSummaryInfoList(aid, tid, pdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.BizSalesSummary.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    /**
     * 获取指定业务下指定商品id集的业务销售信息
     */
    public int getBizSalesSummaryInfoListByPdIdList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList)  throws IOException {
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

            FaiList<Param> list = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, rlPdIdList, list);
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Integer> pdIdList = OptMisc.getValList(list, ProductBasicEntity.ProductRelInfo.PD_ID);

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getBizSalesSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.BizSalesSummary.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
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
            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            // unionPriId + rlPdIdList
            FaiList<Param> list = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, rlPdIdList, list);
            if(rt != Errno.OK){
                return rt;
            }
            Map<Integer, Integer> pdIdRlPdIdMap = new HashMap<>(list.size()*4/3+1);
            FaiList<Integer> pdIdList = new FaiList<>(list.size());
            list.forEach(info->{
                Integer rlPdId = info.getInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID);
                Integer pdId = info.getInt(ProductBasicEntity.ProductRelInfo.PD_ID);
                pdIdRlPdIdMap.put(pdId, rlPdId);
                pdIdList.add(pdId);
            });

            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getSalesSummaryInfoList(aid, tid, unionPriId, pdIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }
            for (Param info : infoList) {
                Integer pdId = info.getInt(SalesSummaryEntity.Info.PD_ID);
                Integer rlPdId = pdIdRlPdIdMap.get(pdId);
                info.setInt(ProductStoreEntity.SalesSummaryInfo.RL_PD_ID, rlPdId);
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SalesSummary.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    /**
     * 获取库存SKU汇总信息
     * @param tid 商品所属权的 tid
     * @param siteId 商品所属权的 siteId
     * @param lgId 商品所属权的 lgId
     * @param keepPriId1 商品所属权的 keepPriId1
     * @param searchArg 查询条件
     */
    public int getStoreSkuSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, SearchArg searchArg)  throws IOException {
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
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreProc.getStoreSkuSummaryInfoList(aid, tid, searchArg, infoList);
            if(rt != Errno.OK) {
                return rt;
            }
            if(infoList.size() > 0){
                Set<Integer> pdIdSet = new HashSet<>();
                for (Param info : infoList) {
                    Integer pdId = info.getInt(StoreSkuSummaryEntity.Info.PD_ID); //转化为PdId
                    pdIdSet.add(pdId);
                }
                Log.logDbg("whalelog aid=%s;unionPriId=%s;pdIdSet=%s", aid, unionPriId, pdIdSet);
                FaiList<Param> reducedRels = new FaiList<>();
                rt = productBasicProc.getReducedRelsByPdIds(aid, unionPriId, new FaiList<>(pdIdSet), reducedRels);
                if(rt != Errno.OK) {
                    return rt;
                }
                Map<Integer, Integer> pdIdRlPdIdMap = new HashMap<>(reducedRels.size()*4/3+1);
                for (Param reducedRel : reducedRels) {
                    Integer rlPdId = reducedRel.getInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID);
                    Integer pdId = reducedRel.getInt(ProductBasicEntity.ProductRelInfo.PD_ID);
                    pdIdRlPdIdMap.put(pdId, rlPdId);
                }

                for (Param info : infoList) { //转化为rlPdId
                    Integer pdId = info.getInt(StoreSkuSummaryEntity.Info.PD_ID);
                    Integer rlPdId = pdIdRlPdIdMap.get(pdId);
                    info.setInt(ProductStoreEntity.StoreSkuSummaryInfo.RL_PD_ID, rlPdId);
                }
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSkuSummary.getInfoDto());
            if(searchArg.totalSize != null){
                sendBuf.putInt(ProductStoreDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
            }
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

}
