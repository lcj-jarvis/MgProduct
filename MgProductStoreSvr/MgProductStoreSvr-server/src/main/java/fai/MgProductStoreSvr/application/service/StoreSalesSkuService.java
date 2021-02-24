package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.comm.Utils;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;

public class StoreSalesSkuService extends StoreService {
    /**
     * 刷新库存销售sku信息
     */
    public int refreshSkuStoreSales(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || pdId <= 0 || rlPdId <= 0 || pdScSkuInfoList == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;pdScSkuInfoList=%s", flow, aid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
                return rt;
            }

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                LockUtil.lock(aid);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl =SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, skuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);

                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriId, pdId, listRef, StoreSalesSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Param> storeSalesSkuInfoList = listRef.value;
                Map<Long, Param> skuIdPdScSkuInfoMap = Utils.getMap(pdScSkuInfoList, StoreSalesSkuEntity.Info.SKU_ID);
                FaiList<Long> delSkuIdList = new FaiList<>();
                FaiList<Param> addInfoList = new FaiList<>();
                for (Param storeSalesSkuInfo : storeSalesSkuInfoList) {
                    Long skuId = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                    Param pdScSkuInfo = skuIdPdScSkuInfoMap.remove(skuId);
                    if(pdScSkuInfo == null){
                        delSkuIdList.add(skuId);
                        continue;
                    }
                }

                for (Map.Entry<Long, Param> skuIdPdScSkuInfoEntry : skuIdPdScSkuInfoMap.entrySet()) {
                    addInfoList.add(new Param()
                            .setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId)
                            .setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId)
                            .setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId)
                            .setLong(StoreSalesSkuEntity.Info.SKU_ID, skuIdPdScSkuInfoEntry.getKey())
                    );
                }
                try {
                    transactionCtrl.setAutoCommit(false);
                    boolean notModify = true;
                    if(!delSkuIdList.isEmpty()){
                        notModify = false;
                        rt = storeSalesSkuProc.batchDel(aid, pdId, delSkuIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = skuSummaryProc.batchDel(aid, pdId, delSkuIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    if(!addInfoList.isEmpty()){
                        notModify = false;
                        rt = storeSalesSkuProc.batchAdd(aid, pdId, addInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    if(notModify){
                        FaiBuffer sendBuf = new FaiBuffer(true);
                        session.write(sendBuf);
                        Log.logDbg("not modify;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                        return rt = Errno.OK;
                    }
                    rt = reportSummary(aid, new FaiList<>(Arrays.asList(pdId)), SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT|SpuBizStoreSalesReportValObj.Flag.REPORT_PRICE
                            , null, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    transactionCtrl.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;aid=%s;unionPriId=%s;pdId=%s;rlPdId=%s;",aid, unionPriId, pdId, rlPdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量同步 库存销售 spu数据到sku
     */
    public int batchSynchronousStoreSalesSPU2SKU(FaiSession session, int flow, int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> spuStoreSalesInfoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || sourceUnionPriId == 0 || spuStoreSalesInfoList == null || spuStoreSalesInfoList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;sourceUnionPriId=%s;spuStoreSalesInfoList=%s", flow, aid, sourceUnionPriId, spuStoreSalesInfoList);
                return rt;
            }
            long startTime = System.currentTimeMillis();
            Map<Integer, Map<Long, Param>> unionPriId_skuId_salesStoreDataMapMap = new HashMap<>();
            // 需要更新的最多key集
            Set<String> maxUpdateKeySet = new HashSet<>();
            FaiList<Long> skuIdList = new FaiList<>();
            for (Param spuSalesStoreInfo : spuStoreSalesInfoList) {
                int unionPriId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, 0);
                int pdId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.PD_ID, 0);
                int rlPdId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.RL_PD_ID, 0);
                long skuId = spuSalesStoreInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID, 0L);
                if(unionPriId <= 0 || pdId <= 0 || rlPdId <= 0 || skuId <= 0){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;skuId=%s;", flow, aid, unionPriId, pdId, rlPdId, skuId);
                    return rt;
                }
                skuIdList.add(skuId);
                Map<Long, Param> skuId_SalesStoreDataMap = unionPriId_skuId_salesStoreDataMapMap.get(unionPriId);
                if(skuId_SalesStoreDataMap == null){
                    skuId_SalesStoreDataMap = new HashMap<>();
                    unionPriId_skuId_salesStoreDataMapMap.put(unionPriId, skuId_SalesStoreDataMap);
                }
                Param skuSalesStoreData = new Param();
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.AID, aid);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                skuSalesStoreData.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.SKU_TYPE);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.SORT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.COUNT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.REMAIN_COUNT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.HOLDING_COUNT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.PRICE);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.MIN_AMOUNT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.MAX_AMOUNT);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.DURATION);
                skuSalesStoreData.assign(spuSalesStoreInfo, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
                maxUpdateKeySet.addAll(skuSalesStoreData.keySet());
                skuId_SalesStoreDataMap.put(skuId, skuSalesStoreData);
            }
            maxUpdateKeySet.removeAll(Arrays.asList(StoreSalesSkuEntity.Info.AID, StoreSalesSkuEntity.Info.UNION_PRI_ID, StoreSalesSkuEntity.Info.PD_ID, StoreSalesSkuEntity.Info.RL_PD_ID, StoreSalesSkuEntity.Info.SKU_ID));

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                LockUtil.lock(aid);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);

                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, skuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);

                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = storeSalesSkuProc.batchSynchronousSPU2SKU(aid, unionPriId_skuId_salesStoreDataMapMap, maxUpdateKeySet);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = storeSalesSkuProc.getReportList4synSPU2SKU(aid, skuIdList, listRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    FaiList<Param> reportInfoList = listRef.value;
                    Map<Integer, Map<Integer, Param>> unionPriId_pdId_bizSalesSummaryInfoMapMap = new HashMap<>();
                    Map<Integer, Param> pdIdSalesSummaryInfoMap = new HashMap<>();
                    Map<Long, Param> skuIdStoreSkuSummaryInfoMap = new HashMap<>();
                    for (Param reportInfo : reportInfoList) {
                        int pdId = reportInfo.getInt(StoreSalesSkuEntity.Info.PD_ID);
                        int unionPriId = reportInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                        long skuId = reportInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                        {
                            Param spuBizSalesSummaryInfo = new Param();
                            assemblySpuBizSummaryInfo(spuBizSalesSummaryInfo, reportInfo, SpuBizStoreSalesReportValObj.Flag.REPORT_PRICE| SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT);
                            Map<Integer, Param> pdId_bizSalesSummaryInfoMap = unionPriId_pdId_bizSalesSummaryInfoMapMap.get(unionPriId);
                            if(pdId_bizSalesSummaryInfoMap == null){
                                pdId_bizSalesSummaryInfoMap = new HashMap<>();
                                unionPriId_pdId_bizSalesSummaryInfoMapMap.put(unionPriId, pdId_bizSalesSummaryInfoMap);
                            }
                            pdId_bizSalesSummaryInfoMap.put(pdId, spuBizSalesSummaryInfo);
                        }
                        int sumCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_COUNT);
                        int sumRemainCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT);
                        int sumHoldingCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT);
                        long maxPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MAX_PRICE);
                        long minPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MIN_PRICE);
                        {
                            Param spuSummaryInfo = pdIdSalesSummaryInfoMap.get(pdId);
                            if(spuSummaryInfo == null){
                                spuSummaryInfo = new Param();
                                spuSummaryInfo.setInt(SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                pdIdSalesSummaryInfoMap.put(pdId, spuSummaryInfo);
                            }
                            int lastCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.HOLDING_COUNT, 0);
                            long lastMaxPrice =  spuSummaryInfo.getLong(SpuSummaryEntity.Info.MAX_PRICE, Long.MIN_VALUE);
                            long lastMinPrice =  spuSummaryInfo.getLong(SpuSummaryEntity.Info.MIN_PRICE, Long.MAX_VALUE);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.COUNT, lastCount+sumCount);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                            spuSummaryInfo.setLong(SpuSummaryEntity.Info.MAX_PRICE, Math.max(lastMaxPrice, maxPrice));
                            spuSummaryInfo.setLong(SpuSummaryEntity.Info.MIN_PRICE, Math.min(lastMinPrice, minPrice));
                        }
                        {
                            Param skuSummaryInfo = skuIdStoreSkuSummaryInfoMap.get(skuId);
                            if(skuSummaryInfo == null){
                                skuSummaryInfo = new Param();
                                skuSummaryInfo.setInt(SkuSummaryEntity.Info.PD_ID, pdId);
                                skuSummaryInfo.setInt(SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                skuIdStoreSkuSummaryInfoMap.put(skuId, skuSummaryInfo);
                            }
                            int lastCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.HOLDING_COUNT, 0);
                            long lastMaxPrice =  skuSummaryInfo.getLong(SkuSummaryEntity.Info.MAX_PRICE, Long.MIN_VALUE);
                            long lastMinPrice =  skuSummaryInfo.getLong(SkuSummaryEntity.Info.MIN_PRICE, Long.MAX_VALUE);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.COUNT, lastCount+sumCount);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                            skuSummaryInfo.setLong(SkuSummaryEntity.Info.MAX_PRICE, Math.max(lastMaxPrice, maxPrice));
                            skuSummaryInfo.setLong(SkuSummaryEntity.Info.MIN_PRICE, Math.min(lastMinPrice, minPrice));
                        }
                    }

                    rt = spuBizSummaryProc.report4synSPU2SKU(aid, unionPriId_pdId_bizSalesSummaryInfoMapMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = spuSummaryProc.report4synSPU2SKU(aid, pdIdSalesSummaryInfoMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = skuSummaryProc.report4synSPU2SKU(aid, skuIdStoreSkuSummaryInfoMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    transactionCtrl.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            long endTime = System.currentTimeMillis();
            Log.logStd("ok;aid=%s;consume=%s;",aid, (endTime-startTime));
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改库存销售sku信息
     */
    public int setSkuStoreSales(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || pdId <= 0 || rlPdId <= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;updaterList=%s", flow, aid, unionPriId, pdId, rlPdId, updaterList);
                return rt;
            }
            FaiList<Long> skuIdList = new FaiList<>();
            for (ParamUpdater updater : updaterList) {
                Long skuId = updater.getData().getLong(StoreSalesSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    Log.logErr("skuId err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;updater=%s", flow, aid, unionPriId, pdId, rlPdId, updater.toJson());
                    return Errno.ARGS_ERROR;
                }
                skuIdList.add(skuId);
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, skuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = storeSalesSkuProc.batchSet(aid, unionPriId, pdId, updaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = reportSummary(aid, new FaiList<>(Arrays.asList(pdId)), SpuBizStoreSalesReportValObj.Flag.REPORT_PRICE,
                                skuIdList, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        transactionCtrl.closeDao();
                        spuBizSummaryProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;aid=%s;unionPriId=%s;pdId=%s;rlPdId=%s;",aid, unionPriId, pdId, rlPdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量扣减库存
     * @return
     */
    public int batchReducePdSkuStore(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode) || reduceMode <= 0 || expireTimeSeconds < 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;", flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
                return rt;
            }
            TreeMap<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuIdCountMap.put(skuId, count);
            }
            boolean holdingMode = reduceMode == StoreSalesSkuValObj.ReduceMode.HOLDING;
            FaiList<Integer> pdIdList = new FaiList<>();
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreOrderRecordDaoCtrl storeOrderRecordDaoCtrl = StoreOrderRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if (!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, storeOrderRecordDaoCtrl, holdingRecordDaoCtrl)) {
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(storeOrderRecordDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                pdIdList = Utils.getValList(listRef.value, StoreSalesSkuEntity.Info.PD_ID);
                try {
                    transactionCtrl.setAutoCommit(false);
                    if(holdingMode){ // 生成预扣记录
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdCountMap, true, false);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdCountMap.isEmpty()){ // 重复扣减了
                            Log.logStd("find repeat reduce；aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdCountList=%s", aid, unionPriId, rlOrderCode, skuIdCountList);
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            return rt = Errno.OK;
                        }
                        rt = holdingRecordProc.batchAdd(aid, unionPriId, skuIdCountMap, rlOrderCode, expireTimeSeconds);
                    }else { // 生成库存订单关联记录
                        rt = storeOrderRecordProc.batchAdd(aid, unionPriId, skuIdCountMap, rlOrderCode);
                    }
                    if(rt != Errno.OK){
                        return rt;
                    }
                    rt = storeSalesSkuProc.batchReduceStore(aid, unionPriId, skuIdCountMap, holdingMode, false);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        storeSalesSkuProc.deleteRemainCountDirtyCache(aid);
                        return rt;
                    }
                    transactionCtrl.commit();
                }
            }finally {
                transactionCtrl.closeDao();
            }

            // 异步上报数据
            asynchronousReport(flow, aid, unionPriId, skuIdList, pdIdList);
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;;skuIdCountMap=%s", aid, unionPriId, rlOrderCode, reduceMode, expireTimeSeconds, skuIdCountMap);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 检测是否存在预扣记录
     */
    private int checkHoldingRecordExists(HoldingRecordProc holdingRecordProc, int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode, Map<Long, Integer> skuIdCountMap, boolean notJudgeDel, boolean isMakeup){
        int rt = Errno.ERROR;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = holdingRecordProc.getFromDao(aid, unionPriId, skuIdList, rlOrderCode, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            return rt;
        }
        for (Param info : listRef.value) {
            boolean alreadyDel = info.getBoolean(HoldingRecordEntity.Info.ALREADY_DEL);
            if(notJudgeDel || alreadyDel){ // 不判断删除 或者 已经删除
                Long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
                Integer recordCount = info.getInt(HoldingRecordEntity.Info.COUNT);
                Integer count = skuIdCountMap.remove(skuId);
                if(!recordCount.equals(count)){
                    if(!isMakeup){
                        rt = MgProductErrno.Store.REPEAT_REDUCE_COUNT_DIF;
                    }else{
                        rt = MgProductErrno.Store.REPEAT_MAKEUP_COUNT_DIF;
                    }
                    Log.logErr(rt, "count not equals record count;count=%s;dbInfo=%s;notJudgeDel=%s;", count, info, notJudgeDel);
                    return rt;
                }
            }
        }
        return rt = Errno.OK;
    }

    /**
     * 批量扣减预扣库存
     */
    public int batchReducePdSkuHoldingStore(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode) || outStoreRecordInfo == null || outStoreRecordInfo.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;outStoreRecordInfo=%s;", flow, aid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo);
                return rt;
            }

            TreeMap<Long, Integer> skuIdChangeCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            Set<SkuStoreKey> skuStoreKeySet = new HashSet<>();
            FaiList<Integer> pdIdList = new FaiList<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuIdChangeCountMap.put(skuId, count);
                skuStoreKeySet.add(new SkuStoreKey(unionPriId, skuId));
            }

            MgProductSpecCli mgProductSpecCli = createMgProductSpecCli(flow);
            FaiList<Param> skuInfoList = new FaiList<>();
            rt = mgProductSpecCli.getPdSkuScInfoListBySkuIdList(aid, tid, skuIdList, skuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt, "MgProductSpecCli getPdSkuScInfoListBySkuIdList err;flow=%s;aid=%s;skuIdList=%s", flow, aid, skuIdList);
                return rt;
            }
            Map<Long, FaiList<Integer>> skuIdInPdScStrIdListMap = Utils.getMap(skuInfoList, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);

            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, holdingRecordDaoCtrl, holdingRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdChangeCountMap, false, false);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdChangeCountMap.isEmpty()){ // 重复扣减了
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            Log.logStd("find repeat reduceHolding;aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdCountList=%s", aid, unionPriId, rlOrderCode, skuIdCountList);
                            return rt = Errno.OK;
                        }

                        // 删掉预扣记录
                        rt = holdingRecordProc.batchLogicDel(aid, unionPriId, skuIdList, rlOrderCode);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 扣减预扣库存
                        rt = storeSalesSkuProc.batchReduceStore(aid, unionPriId, skuIdChangeCountMap, true, true);
                        if(rt != Errno.OK){
                            return rt;
                        }


                        Map<SkuStoreKey, Param> changeCountAfterSkuStoreSalesInfoMap = new HashMap<>();
                        rt = storeSalesSkuProc.getInfoMap4OutRecordFromDao(aid, skuStoreKeySet, changeCountAfterSkuStoreSalesInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        for (Param skuStoreSalesInfo : changeCountAfterSkuStoreSalesInfoMap.values()) {
                            pdIdList.add(skuStoreSalesInfo.getInt(StoreSalesSkuEntity.Info.PD_ID));
                        }

                        // 添加出库记录
                        outStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.OUT);
                        rt = inOutStoreRecordProc.batchAddOutStoreRecord(aid, unionPriId, skuIdChangeCountMap, changeCountAfterSkuStoreSalesInfoMap, outStoreRecordInfo, skuIdInPdScStrIdListMap);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 更新总成本
                        rt = storeSalesSkuProc.batchUpdateTotalCost(aid, changeCountAfterSkuStoreSalesInfoMap);
                        if(rt != Errno.OK){
                            return rt;
                        }

                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }

            // 异步上报数据
            asynchronousReport(flow, aid, unionPriId, skuIdList, pdIdList);
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdChangeCountMap=%s;", aid, unionPriId, rlOrderCode, skuIdChangeCountMap);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量补偿库存
     */
    public int batchMakeUpStore(FaiSession session, int flow, int aid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;", flow, aid, unionPriId, skuIdCountList, rlOrderCode);
                return rt;
            }

            TreeMap<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuIdCountMap.put(skuId, count);
            }

            boolean holdingMode = reduceMode == StoreSalesSkuValObj.ReduceMode.HOLDING;
            FaiList<Integer> pdIdList = new FaiList<>();
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, holdingRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);

                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                pdIdList = Utils.getValList(listRef.value, StoreSalesSkuEntity.Info.PD_ID);

                try {
                    transactionCtrl.setAutoCommit(false);
                    if(holdingMode){
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdCountMap, true, true);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdCountMap.isEmpty()){ // 重复补偿了
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            Log.logStd("find repeat makeup;aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdCountList=%s", aid, unionPriId, rlOrderCode, skuIdCountList);
                            return rt = Errno.OK;
                        }
                        rt = holdingRecordProc.batchLogicDel(aid, unionPriId, skuIdList, rlOrderCode);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }

                    rt = storeSalesSkuProc.batchMakeUpStore(aid, unionPriId, skuIdCountMap, holdingMode);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        storeSalesSkuProc.deleteRemainCountDirtyCache(aid);
                        return rt;
                    }
                    transactionCtrl.commit();
                }
            }finally {
                transactionCtrl.closeDao();
            }

            // 异步上报数据
            asynchronousReport(flow, aid, unionPriId, skuIdList, pdIdList);
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;rlOrderCode=%s;skuIdCountMap=%s;", flow, aid, unionPriId, rlOrderCode, skuIdCountMap);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 根据uid和pdId获取库存销售sku信息
     */
    public int getSkuStoreSales(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<String> useSourceFieldList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdId <= 0 || rlPdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                return rt;
            }
            FaiList<Param> list;

            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getList(aid, unionPriId, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
                list = listRef.value;
                if(useSourceFieldList != null && !useSourceFieldList.isEmpty()){
                    Set<String> useSourceFieldSet = new HashSet<>(useSourceFieldList);
                    useSourceFieldSet.retainAll(Arrays.asList(StoreSalesSkuEntity.getValidKeys()));
                    if(list.size() > 0 && useSourceFieldSet.size() > 0){
                        useSourceFieldSet.add(StoreSalesSkuEntity.Info.SKU_ID);
                        Param first = list.get(0);
                        int tmpUnionPriId = first.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                        int sourceUnionPriId = first.getInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID);
                        if(tmpUnionPriId != sourceUnionPriId){
                            FaiList<Long> skuIdList = Utils.getValList(list, StoreSalesSkuEntity.Info.SKU_ID);
                            listRef = new Ref<>();
                            rt = storeSalesSkuProc.getListFromDao(aid, sourceUnionPriId, skuIdList, listRef, useSourceFieldSet);
                            if(rt != Errno.OK){
                                if(rt == Errno.NOT_FOUND){
                                    Log.logErr( rt,"source list not found;flow=%s;aid=%s;sourceUnionPriId=%s;skuIdList=%s;", flow, aid, sourceUnionPriId, skuIdList);
                                }
                                return rt;
                            }
                            Map<Long, Param> skuIdFiledMap = new HashMap<>(listRef.value.size()*4/3+1);
                            for (Param sourceFieldInfo : listRef.value) {
                                Long skuId = sourceFieldInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                                skuIdFiledMap.put(skuId, sourceFieldInfo);
                            }
                            for (Param info : list) {
                                Long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                                Param sourceFieldInfo = skuIdFiledMap.get(skuId);
                                if(sourceFieldInfo == null){
                                    rt = Errno.NOT_FOUND;
                                    Log.logErr(rt, "source info not found;flow=%s;aid=%s;sourceUnionPriId=%s;skuId=%s;", flow, aid, sourceUnionPriId, skuId);
                                    return rt;
                                }
                                for (String sourceField : useSourceFieldSet) {
                                    info.assign(sourceFieldInfo, sourceField);
                                }
                            }
                        }
                    }
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, list);
            Log.logDbg("ok;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", aid, unionPriId, pdId, rlPdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据 skuId 和 uid 获取相应的库存销售信息
     */
    public int getStoreSalesBySkuIdAndUIdList(FaiSession session, int flow, int aid, long skuId, FaiList<Integer> unionPriIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || skuId <= 0 || unionPriIdList== null || unionPriIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", flow, aid, skuId, unionPriIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getListBySkuIdAndUnionPriIdList(aid, skuId, unionPriIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, listRef.value);
            Log.logDbg("ok;aid=%d;skuId=%s;unionPriIdList=%s;", aid, skuId, unionPriIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据pdId获取库存销售sku信息
     */
    public int getSkuStoreSalesByPdId(FaiSession session, int flow, int aid, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;skuId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getListByPdId(aid, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, listRef.value);
            Log.logDbg("ok;aid=%d;pdId=%s;", aid, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    private void sendPdScSkuSalesStore(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, StoreSalesSkuDto.Key.INFO_LIST, StoreSalesSkuDto.getInfoDto());
        session.write(sendBuf);
    }

    /**
     * MQ 异步上报
     */
    private void asynchronousReport(int flow, int aid, int unionPriId, FaiList<Long> skuIdList, FaiList<Integer> pdIdList) {
        TransactionCtrl transactionCtrl;
        if (pdIdList.size() > 0) {
            transactionCtrl = new TransactionCtrl();
            try {
                SpuBizStoreSalesReportDaoCtrl spuBizStoreSalesReportDaoCtrl = SpuBizStoreSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizStoreSalesReportProc spuBizStoreSalesReportProc = new SpuBizStoreSalesReportProc(spuBizStoreSalesReportDaoCtrl, flow);

                transactionCtrl.setAutoCommit(false);
                // 提交库存上报任务，提交失败暂不处理
                if (spuBizStoreSalesReportProc.addReportCountTask(aid, unionPriId, pdIdList) != Errno.OK) {
                    transactionCtrl.rollback();
                } else {
                    transactionCtrl.commit();
                }
            } finally {
                transactionCtrl.closeDao();
            }
        }
        if (skuIdList.size() > 0) {
            for (Long skuId : skuIdList) {
                addSkuSummaryReportMq(flow, aid, skuId);
            }
        }
    }
    /**
     * 投递数据到 mq
     */
    private int addSkuSummaryReportMq(int flow, int aid, long skuId){
        Param sendInfo = new Param();
        sendInfo.setInt(SkuSummaryEntity.Info.AID, aid);
        sendInfo.setLong(SkuSummaryEntity.Info.SKU_ID, skuId);
        FaiMqMessage message = new FaiMqMessage();
        // 指定topic
        message.setTopic(MqConfig.SkuReport.TOPIC);
        // 指定tag
        message.setTag(MqConfig.SkuReport.TAG);
        // 添加流水号
        message.setFlow(flow);
        // 使用flow
        message.setKey(aid+"-"+skuId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.SpuBizReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e, MqConfig.SpuBizReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s", message.getFlow(), message.getMsgId());
        }
        return Errno.ERROR;
    }
}
