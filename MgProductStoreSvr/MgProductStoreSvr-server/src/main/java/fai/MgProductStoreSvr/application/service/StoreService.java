package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.Misc2;
import fai.MgProductStoreSvr.domain.comm.PdKey;
import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.BizSalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.SalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSkuSummaryDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;

public class StoreService{

    /**
     * 刷新库存销售sku信息
     */
    public int refreshPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) throws IOException {
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
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, bizSalesSummaryDaoCtrl, salesSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);

                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriId, pdId, listRef, StoreSalesSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Param> storeSalesSkuInfoList = listRef.value;
                Map<Long, Param> skuIdPdScSkuInfoMap = Misc2.getMap(pdScSkuInfoList, StoreSalesSkuEntity.Info.SKU_ID);
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
                    }
                    if(!addInfoList.isEmpty()){
                        notModify = false;
                        rt = storeSalesSkuProc.batchAdd(aid, pdId, addInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    if(notModify){
                        Log.logDbg("not modify;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                        return rt = Errno.OK;
                    }

                    Ref<FaiList<Param>> reportListRef = new Ref<>();
                    rt = storeSalesSkuProc.getReportList(aid, pdId, reportListRef);
                    if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                        return rt;
                    }
                    rt = Errno.OK;

                    if(!reportListRef.value.isEmpty()){
                        FaiList<Param> bizSalesSummaryInfoList = new FaiList<>(reportListRef.value.size());
                        for (Param reportInfo : reportListRef.value) {
                            Param bizSalesSummaryInfo = new Param();
                            assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, BizSalesReportValObj.Flag.REPORT_COUNT);
                            bizSalesSummaryInfoList.add(bizSalesSummaryInfo);
                        }
                        rt = bizSalesSummaryProc.report(aid, pdId, bizSalesSummaryInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = bizSalesSummaryReport(bizSalesSummaryProc, salesSummaryProc, aid, pdId, BizSalesReportValObj.Flag.REPORT_COUNT);
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    transactionCtrl.commit();
                    bizSalesSummaryProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCtrl.closeDao();
            }
            Log.logStd("ok!aid=%s;unionPriId=%s;pdId=%s;rlPdId=%s;",aid, unionPriId, pdId, rlPdId);
        }finally {
            if(rt == Errno.OK){
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
            }
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
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSkuSummaryDaoCtrl storeSkuSummaryDaoCtrl = StoreSkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);

                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, bizSalesSummaryDaoCtrl, salesSummaryDaoCtrl, storeSkuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                StoreSkuSummaryProc storeSkuSummaryProc = new StoreSkuSummaryProc(storeSkuSummaryDaoCtrl, flow);

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
                            Param bizSalesSummaryInfo = new Param();
                            assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, BizSalesReportValObj.Flag.REPORT_PRICE|BizSalesReportValObj.Flag.REPORT_COUNT);
                            Map<Integer, Param> pdId_bizSalesSummaryInfoMap = unionPriId_pdId_bizSalesSummaryInfoMapMap.get(unionPriId);
                            if(pdId_bizSalesSummaryInfoMap == null){
                                pdId_bizSalesSummaryInfoMap = new HashMap<>();
                                unionPriId_pdId_bizSalesSummaryInfoMapMap.put(unionPriId, pdId_bizSalesSummaryInfoMap);
                            }
                            pdId_bizSalesSummaryInfoMap.put(pdId, bizSalesSummaryInfo);
                        }
                        int sumCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_COUNT);
                        int sumRemainCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT);
                        int sumHoldingCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT);
                        long maxPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MAX_PRICE);
                        long minPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MIN_PRICE);
                        {
                            Param salesSummaryInfo = pdIdSalesSummaryInfoMap.get(pdId);
                            if(salesSummaryInfo == null){
                                salesSummaryInfo = new Param();
                                salesSummaryInfo.setInt(SalesSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                pdIdSalesSummaryInfoMap.put(pdId, salesSummaryInfo);
                            }
                            int lastCount = salesSummaryInfo.getInt(SalesSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = salesSummaryInfo.getInt(SalesSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = salesSummaryInfo.getInt(SalesSummaryEntity.Info.HOLDING_COUNT, 0);
                            long lastMaxPrice =  salesSummaryInfo.getLong(SalesSummaryEntity.Info.MAX_PRICE, Long.MIN_VALUE);
                            long lastMinPrice =  salesSummaryInfo.getLong(SalesSummaryEntity.Info.MIN_PRICE, Long.MAX_VALUE);
                            salesSummaryInfo.setInt(SalesSummaryEntity.Info.COUNT, lastCount+sumCount);
                            salesSummaryInfo.setInt(SalesSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            salesSummaryInfo.setInt(SalesSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                            salesSummaryInfo.setLong(SalesSummaryEntity.Info.MAX_PRICE, Math.max(lastMaxPrice, maxPrice));
                            salesSummaryInfo.setLong(SalesSummaryEntity.Info.MIN_PRICE, Math.min(lastMinPrice, minPrice));
                        }
                        {
                            Param storeSkuSummaryInfo = skuIdStoreSkuSummaryInfoMap.get(skuId);
                            if(storeSkuSummaryInfo == null){
                                storeSkuSummaryInfo = new Param();
                                storeSkuSummaryInfo.setInt(StoreSkuSummaryEntity.Info.PD_ID, pdId);
                                storeSkuSummaryInfo.setInt(StoreSkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                skuIdStoreSkuSummaryInfoMap.put(skuId, storeSkuSummaryInfo);
                            }
                            int lastCount = storeSkuSummaryInfo.getInt(StoreSkuSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = storeSkuSummaryInfo.getInt(StoreSkuSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = storeSkuSummaryInfo.getInt(StoreSkuSummaryEntity.Info.HOLDING_COUNT, 0);
                            storeSkuSummaryInfo.setInt(StoreSkuSummaryEntity.Info.COUNT, lastCount+sumCount);
                            storeSkuSummaryInfo.setInt(StoreSkuSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            storeSkuSummaryInfo.setInt(StoreSkuSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                        }
                    }

                    rt = bizSalesSummaryProc.report4synSPU2SKU(aid, unionPriId_pdId_bizSalesSummaryInfoMapMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = salesSummaryProc.report4synSPU2SKU(aid, pdIdSalesSummaryInfoMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = storeSkuSummaryProc.report4synSPU2SKU(aid, skuIdStoreSkuSummaryInfoMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        bizSalesSummaryProc.deleteDirtyCache(aid);
                        return rt;
                    }
                    transactionCtrl.commit();
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok!aid=%s;",aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量同步 出入库记录
     * 不会汇总信息
     */
    public int batchSynchronousInOutStoreRecord(FaiSession session, int flow, int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || infoList == null || infoList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;infoList=%s", flow, aid, infoList);
                return rt;
            }
            FaiList<Param> batchAddDataList = new FaiList<>(infoList.size());
            for (Param info : infoList) {
                int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
                long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
                int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, 0);
                int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
                int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
                int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
                Calendar createTime = info.getCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME);
                if(unionPriId == 0 || skuId == 0 || ioStoreRecId == 0 || pdId == 0 || rlPdId ==0 || optType ==0){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;ioStoreRecId=%s;pdId=%s;rlPdId=%s;optType=%s;", flow, aid, unionPriId, skuId, ioStoreRecId, pdId, rlPdId, optType);
                    return rt;
                }
                String number = InOutStoreRecordValObj.Number.genNumber(createTime, ioStoreRecId);
                Param data = new Param();
                data.setInt(InOutStoreRecordEntity.Info.AID, aid);
                data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
                data.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
                data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);
                data.assign(info, InOutStoreRecordEntity.Info.C_TYPE);
                data.assign(info, InOutStoreRecordEntity.Info.S_TYPE);
                data.assign(info, InOutStoreRecordEntity.Info.CHANGE_COUNT);
                data.assign(info, InOutStoreRecordEntity.Info.CHANGE_COUNT, InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                data.assign(info, InOutStoreRecordEntity.Info.REMAIN_COUNT);
                data.assign(info, InOutStoreRecordEntity.Info.PRICE);
                data.setString(InOutStoreRecordEntity.Info.NUMBER, number);
                data.assign(info, InOutStoreRecordEntity.Info.OPT_SID);
                data.assign(info, InOutStoreRecordEntity.Info.HEAD_SID);
                data.assign(info, InOutStoreRecordEntity.Info.OPT_TIME);
                data.assign(info, InOutStoreRecordEntity.Info.FLAG);
                data.assign(info, InOutStoreRecordEntity.Info.SYS_UPDATE_TIME);
                data.setCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, createTime);
                data.assign(info, InOutStoreRecordEntity.Info.REMARK);
                data.assign(info, InOutStoreRecordEntity.Info.RL_ORDER_CODE);
                data.assign(info, InOutStoreRecordEntity.Info.RL_REFUND_ID);
                batchAddDataList.add(data);
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(inOutStoreRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = inOutStoreRecordProc.synBatchAdd(aid, batchAddDataList);
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
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok!aid=%s;",aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改库存销售sku信息
     */
    public int setPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || pdId <= 0 || rlPdId <= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;updaterList=%s", flow, aid, unionPriId, pdId, rlPdId, updaterList);
                return rt;
            }
            for (ParamUpdater updater : updaterList) {
                Long skuId = updater.getData().getLong(StoreSalesSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    Log.logErr("skuId err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;updater=%s", flow, aid, unionPriId, pdId, rlPdId, updater.toJson());
                    return Errno.ARGS_ERROR;
                }
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, bizSalesSummaryDaoCtrl, salesSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);

                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = storeSalesSkuProc.batchSet(aid, unionPriId, pdId, updaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        Ref<FaiList<Param>> reportListRef = new Ref<>();
                        rt = storeSalesSkuProc.getReportList(aid, pdId, reportListRef);
                        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                            return rt;
                        }
                        if(!reportListRef.value.isEmpty()){
                            FaiList<Param> bizSalesSummaryInfoList = new FaiList<>(reportListRef.value.size());
                            for (Param reportInfo : reportListRef.value) {
                                Param bizSalesSummaryInfo = new Param();
                                assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, BizSalesReportValObj.Flag.REPORT_PRICE);
                                bizSalesSummaryInfoList.add(bizSalesSummaryInfo);
                            }
                            rt = bizSalesSummaryProc.report(aid, pdId, bizSalesSummaryInfoList);
                            if(rt != Errno.OK){
                                return rt;
                            }
                            rt = bizSalesSummaryReport(bizSalesSummaryProc, salesSummaryProc, aid, pdId, BizSalesReportValObj.Flag.REPORT_PRICE);
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        transactionCtrl.closeDao();
                        bizSalesSummaryProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok!aid=%s;unionPriId=%s;pdId=%s;rlPdId=%s;",aid, unionPriId, pdId, rlPdId);
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
            Map<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
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
                SotreOrderRecordDaoCtrl sotreOrderRecordDaoCtrl = SotreOrderRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if (!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, sotreOrderRecordDaoCtrl, holdingRecordDaoCtrl)) {
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(sotreOrderRecordDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                pdIdList = Misc2.getValList(listRef.value, StoreSalesSkuEntity.Info.PD_ID);
                try {
                    transactionCtrl.setAutoCommit(false);
                    if(holdingMode){ // 生成预扣记录
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
                        return rt;
                    }
                    transactionCtrl.commit();
                }

            }finally {
                transactionCtrl.closeDao();
            }
            if(pdIdList.size() > 0){
                transactionCtrl = new TransactionCtrl();
                try {
                    BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                    transactionCtrl.setAutoCommit(false);
                    BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                    for (Integer pdId : pdIdList) { // TODO 先简单处理
                        // 提交库存上报任务，提交失败暂不处理
                        if(bizSalesReportProc.addReportCountTask(aid, unionPriId, pdId) != Errno.OK){
                            transactionCtrl.rollback();
                        }else{
                            transactionCtrl.commit();
                        }
                    }
                }finally {
                    transactionCtrl.closeDao();
                }
            }
            if(skuIdList.size()> 0){ // TODO 先简单处理
                for (Long skuId : skuIdList) {
                    addStoreSkuReportMq(flow, aid, skuId);
                }
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;", aid, unionPriId, skuIdCountMap, rlOrderCode, reduceMode, expireTimeSeconds);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    private int addStoreSkuReportMq(int flow, int aid, long skuId){
        Param sendInfo = new Param();
        sendInfo.setInt(StoreSkuSummaryEntity.Info.AID, aid);
        sendInfo.setLong(StoreSkuSummaryEntity.Info.SKU_ID, skuId);
        FaiMqMessage message = new FaiMqMessage();
        // 指定topic
        message.setTopic(MqConfig.StoreSkuReport.TOPIC);
        // 指定tag
        message.setTag(MqConfig.StoreSkuReport.TAG);
        // 添加流水号
        message.setFlow(flow);
        // aid-skuId 做幂等
        message.setKey(aid+"-"+skuId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.BizSalesReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e,MqConfig.BizSalesReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s", message.getFlow(), message.getMsgId());
        }
        return Errno.ERROR;
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

            Map<Long, Integer> skuIdChangeCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            Set<SkuStoreKey> skuStoreKeySet = new HashSet<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuIdChangeCountMap.put(skuId, count);
                skuStoreKeySet.add(new SkuStoreKey(unionPriId, skuId));
            }

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


                        Map<SkuStoreKey, Param> changeCountAfterSkuStoreCountAndTotalCostMap = new HashMap<>();
                        rt = storeSalesSkuProc.getCountAndTotalCostFromDao(aid, skuStoreKeySet, changeCountAfterSkuStoreCountAndTotalCostMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }

                        // 添加出库记录
                        outStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.OUT);
                        rt = inOutStoreRecordProc.batchAddOutStoreRecord(aid, unionPriId, skuIdChangeCountMap, changeCountAfterSkuStoreCountAndTotalCostMap, outStoreRecordInfo);
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
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;skuIdChangeCountMap=%s;rlOrderCode=%s;", aid, unionPriId, skuIdChangeCountMap, rlOrderCode);
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

            Map<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
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
                pdIdList = Misc2.getValList(listRef.value, StoreSalesSkuEntity.Info.PD_ID);

                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = storeSalesSkuProc.batchMakeUpStore(aid, unionPriId, skuIdCountMap, holdingMode);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    if(holdingMode){
                        rt = holdingRecordProc.batchLogicDel(aid, unionPriId, skuIdList, rlOrderCode);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    transactionCtrl.commit();
                }
            }finally {
                transactionCtrl.closeDao();
            }
            if(pdIdList.size() > 0){
                transactionCtrl = new TransactionCtrl();
                try {
                    BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                    transactionCtrl.setAutoCommit(false);
                    BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                    for (Integer pdId : pdIdList) { // TODO 先简单处理
                        // 提交库存上报任务，提交失败暂不处理
                        if(bizSalesReportProc.addReportCountTask(aid, unionPriId, pdId) != Errno.OK){
                            transactionCtrl.rollback();
                        }else{
                            transactionCtrl.commit();
                        }
                    }
                }finally {
                    transactionCtrl.closeDao();
                }
            }
            if(skuIdList.size()> 0){ // TODO 先简单处理
                for (Long skuId : skuIdList) {
                    addStoreSkuReportMq(flow, aid, skuId);
                }
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;skuIdCountMap=%s;rlOrderCode=%s;", flow, aid, unionPriId, skuIdCountMap, rlOrderCode);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取库存销售sku信息
     */
    public int getPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdId <= 0 || rlPdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getList(aid, unionPriId, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, listRef.value);
            Log.logDbg("aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", aid, unionPriId, pdId, rlPdId);
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
     * 添加出入库记录
     */
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int ownerUnionPriId, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || ownerUnionPriId <= 0|| infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;infoList=%s;ownerUnionPriId=%s;", flow, aid, ownerUnionPriId, infoList);
                return rt;
            }
            Map<SkuStoreKey, Integer> skuStoreChangeCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            Set<Integer> pdIdSet = new HashSet<>();
            Map<SkuStoreKey, PdKey> needCheckSkuStoreKeyPdKeyMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            Set<Long> skuIdSet = new HashSet<>();
            Set<SkuStoreKey> needGetChangeCountAfterSkuStoreKeySet = new HashSet<>();
            for (Param info : infoList) {
                int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
                if (unionPriId <= 0) {
                    Log.logErr("unionPriId err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
                if (skuId <= 0) {
                    Log.logErr("skuId err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
                if (pdId <= 0) {
                    Log.logErr("pdId err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
                if (rlPdId <= 0) {
                    Log.logErr("rlPdId err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
                if (optType <= 0) {
                    Log.logErr("optType err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, 0);
                if (changeCount <= 0) {
                    Log.logErr("changeCount err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                pdIdSet.add(pdId);
                skuIdSet.add(skuId);
                SkuStoreKey skuStoreKey = new SkuStoreKey(unionPriId, skuId);
                if(unionPriId != ownerUnionPriId){
                    needCheckSkuStoreKeyPdKeyMap.put(skuStoreKey, new PdKey(unionPriId, pdId, rlPdId));
                }
                Integer count = skuStoreChangeCountMap.get(skuStoreKey);
                if(count == null){
                    count = 0;
                }
                try {
                    count = InOutStoreRecordValObj.OptType.computeCount(optType, count, changeCount);
                }catch (RuntimeException e){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt, e, "arg err;flow=%d;aid=%d;info=%s;", flow, aid, info);
                    return rt;
                }
                skuStoreChangeCountMap.put(skuStoreKey, count);
                if(!info.containsKey(InOutStoreRecordEntity.Info.REMAIN_COUNT)){ // 兼容数据迁移
                    needGetChangeCountAfterSkuStoreKeySet.add(skuStoreKey);
                }
            }
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSkuSummaryDaoCtrl storeSkuSummaryDaoCtrl = StoreSkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, bizSalesSummaryDaoCtrl, salesSummaryDaoCtrl, storeSkuSummaryDaoCtrl, inOutStoreRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                StoreSkuSummaryProc storeSkuSummaryProc = new StoreSkuSummaryProc(storeSkuSummaryDaoCtrl, flow);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        if (!needCheckSkuStoreKeyPdKeyMap.isEmpty()) {
                            // 检查是否存在sku, 没有则生成
                            rt = storeSalesSkuProc.checkAndAdd(aid, ownerUnionPriId, needCheckSkuStoreKeyPdKeyMap);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 批量更新库存
                        rt = storeSalesSkuProc.batchChangeStore(aid, skuStoreChangeCountMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }

                        Map<SkuStoreKey, Param> changeCountAfterSkuStoreCountAndTotalCostMap = new HashMap<>();
                        rt = storeSalesSkuProc.getCountAndTotalCostFromDao(aid, needGetChangeCountAfterSkuStoreKeySet, changeCountAfterSkuStoreCountAndTotalCostMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // 批量添加记录
                        rt = inOutStoreRecordProc.batchAdd(aid, infoList, changeCountAfterSkuStoreCountAndTotalCostMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }catch (Exception e){
                        Log.logDbg(e, "whalelog");
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                    try {
                        transactionCtrl.setAutoCommit(false);
                        Ref<FaiList<Param>> reportListRef = new Ref<>();
                        for (Integer pdId : pdIdSet) {
                            rt = storeSalesSkuProc.getReportList(aid, pdId, reportListRef);
                            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                                return rt;
                            }
                            if(!reportListRef.value.isEmpty()){
                                FaiList<Param> bizSalesSummaryInfoList = new FaiList<>(reportListRef.value.size());
                                for (Param reportInfo : reportListRef.value) {
                                    Param bizSalesSummaryInfo = new Param();
                                    assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, BizSalesReportValObj.Flag.REPORT_COUNT);
                                    bizSalesSummaryInfoList.add(bizSalesSummaryInfo);
                                }
                                rt = bizSalesSummaryProc.report(aid, pdId, bizSalesSummaryInfoList);
                                if(rt != Errno.OK){
                                    return rt;
                                }
                                rt = bizSalesSummaryReport(bizSalesSummaryProc, salesSummaryProc, aid, pdId, BizSalesReportValObj.Flag.REPORT_COUNT);
                            }
                        }
                        rt = storeSalesSkuProc.getReportList(aid, new FaiList<>(skuIdSet), reportListRef);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        FaiList<Param> storeSkuSummaryInfoList = new FaiList<>(reportListRef.value.size());
                        for (Param reportInfo : reportListRef.value) {
                            Param info = new Param();
                            assemblyStoreSkuSummaryInfo(reportInfo, info);
                            storeSkuSummaryInfoList.add(info);
                        }
                        rt = storeSkuSummaryProc.report(aid, storeSkuSummaryInfoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
                        Log.logDbg(e, "whalelog");
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        bizSalesSummaryProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logDbg("ok!;aid=%d;ownerUnionPriId=%s;", aid, ownerUnionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }

        return rt;
    }



    /**
     * 上报业务销售汇总
     */
    public int reportBizSales(int flow, int aid, int unionPriId, int pdId){
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
                return rt;
            }
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();

            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(bizSalesReportDaoCtrl, bizSalesSummaryDaoCtrl, salesSummaryDaoCtrl, storeSalesSkuDaoCtrl)){
                    return rt = Errno.ERROR;
                }
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);

                Ref<Param> infoRef = new Ref<>();
                rt = bizSalesReportProc.getInfo(aid, unionPriId, pdId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                Param bizSalesReportInfo = infoRef.value;
                int flag = bizSalesReportInfo.getInt(BizSalesReportEntity.Info.FLAG, 0);

                Param reportInfo = null;


                infoRef.value = null;
                rt = storeSalesSkuProc.getReportInfo(aid, pdId, unionPriId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                reportInfo = infoRef.value;

                Param bizSalesSummaryInfo = new Param();
                bizSalesSummaryInfo.setInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                bizSalesSummaryInfo.setInt(BizSalesSummaryEntity.Info.PD_ID, pdId);
                assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, flag);

                try {
                    LockUtil.lock(aid);
                    try { // 上报数据 并 删除上报任务
                        transactionCtrl.setAutoCommit(false);
                        rt = bizSalesSummaryProc.report(aid, pdId, new FaiList<>(Arrays.asList(bizSalesSummaryInfo)));
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = bizSalesReportProc.del(aid, unionPriId, pdId, flag);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = bizSalesSummaryReport(bizSalesSummaryProc, salesSummaryProc, aid, pdId, flag);
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        transactionCtrl.closeDao();
                        bizSalesSummaryProc.deleteDirtyCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 上报sku维度的库存信息
     */
    public int reportStoreSku(int flow, int aid, long skuId) {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            StoreSkuSummaryDaoCtrl storeSkuSummaryDaoCtrl = StoreSkuSummaryDaoCtrl.getInstance(flow, aid);
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSkuSummaryProc storeSkuSummaryProc = new StoreSkuSummaryProc(storeSkuSummaryDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                Ref<Param> infoRef = new Ref<>();
                rt = storeSalesSkuProc.getReportInfo(aid, skuId, infoRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                rt = Errno.OK;
                if(!infoRef.value.isEmpty()){
                    Param info = new Param();
                    assemblyStoreSkuSummaryInfo(infoRef.value, info);
                    rt = storeSkuSummaryProc.report(aid, skuId, info);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
            }finally {
                storeSkuSummaryDaoCtrl.closeDao();
                storeSalesSkuDaoCtrl.closeDao();
            }
            Log.logStd("ok;flow=%s;aid=%s;skuId=%s;", flow, aid, skuId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取某个业务下指定商品的业务销售信息
     */
    public int getBizSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;", flow, aid, unionPriId, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
            try {
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                rt = bizSalesSummaryProc.getInfoListByPdIdListFromDao(aid, unionPriId, pdIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                bizSalesSummaryDaoCtrl.closeDao();
            }
            sendBizSalesSummary(session, listRef.value);
            Log.logDbg("aid=%d;unionPriId=%s;pdIdList=%s;", aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取某个商品所有业务销售信息
     */
    public int getBizSalesSummaryInfoListByPdId(FaiSession session, int flow, int aid, int tid, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
            try {
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                rt = bizSalesSummaryProc.getInfoListByUnionPriIdListFromDao(aid, null, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                bizSalesSummaryDaoCtrl.closeDao();
            }
            sendBizSalesSummary(session, listRef.value);
            Log.logDbg("aid=%d;pdId=%s;", aid, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    private void sendBizSalesSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, BizSalesSummaryDto.Key.INFO_LIST, BizSalesSummaryDto.getInfoDto());
        session.write(sendBuf);
    }
    /**
     * 获取商品销售信息
     */
    public int getSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;", flow, aid, unionPriId, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                rt = salesSummaryProc.getInfoListFromDao(aid, pdIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                salesSummaryDaoCtrl.closeDao();
            }
            sendSalesSummary(session, listRef.value);
            Log.logDbg("aid=%d;unionPriId=%s;pdIdList=%s;", aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    private void sendSalesSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SalesSummaryDto.Key.INFO_LIST, SalesSummaryDto.getInfoDto());
        session.write(sendBuf);
    }


    /**
     * 删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList) throws IOException{
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdIdList=%s;", flow, aid, pdIdList);
                return rt;
            }
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SotreOrderRecordDaoCtrl sotreOrderRecordDaoCtrl = SotreOrderRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSkuSummaryDaoCtrl storeSkuSummaryDaoCtrl = StoreSkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(bizSalesReportDaoCtrl, bizSalesSummaryDaoCtrl, holdingRecordDaoCtrl, inOutStoreRecordDaoCtrl
                        , salesSummaryDaoCtrl, sotreOrderRecordDaoCtrl, storeSalesSkuDaoCtrl, storeSkuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                //BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                //HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                //StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(sotreOrderRecordDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                StoreSkuSummaryProc storeSkuSummaryProc = new StoreSkuSummaryProc(storeSkuSummaryDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = storeSkuSummaryProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = bizSalesSummaryProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = salesSummaryProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = inOutStoreRecordProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = storeSalesSkuProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
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
            Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取 业务 库存SKU信息
     */
    public int getBizStoreSkuSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || searchArg == null || searchArg.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;searchArg=%s;", flow, aid, searchArg);
                return rt;
            }
            if(searchArg.limit > 100){
                Log.logErr("searchArg.limit err;flow=%d;aid=%d;searchArg.limit=%s;", flow, aid, searchArg.limit);
                return rt = Errno.ARGS_ERROR;
            }

            ParamMatcher baseMatcher = new ParamMatcher(StoreSalesSkuEntity.Info.AID, ParamMatcher.EQ, aid);
            baseMatcher.and(StoreSalesSkuEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            baseMatcher.and(searchArg.matcher);
            searchArg.matcher = baseMatcher;

            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.searchStoreSkuSummaryFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendStoreSkuSummary(session, searchArg, listRef);
            Log.logDbg("aid=%d;searchArg.matcher.toJson=%s", aid, searchArg.matcher.toJson());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取库存SKU汇总信息
     */
    public int getStoreSkuSummaryInfoList(FaiSession session, int flow, int aid, int tid, int sourceUnionPriId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || searchArg == null || searchArg.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;searchArg=%s;", flow, aid, searchArg);
                return rt;
            }
            if(searchArg.limit > 100){
                Log.logErr("searchArg.limit err;flow=%d;aid=%d;searchArg.limit=%s;", flow, aid, searchArg.limit);
                return rt = Errno.ARGS_ERROR;
            }

            ParamMatcher baseMatcher = new ParamMatcher(StoreSkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
            baseMatcher.and(StoreSkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.EQ, sourceUnionPriId);
            baseMatcher.and(searchArg.matcher);
            searchArg.matcher = baseMatcher;

            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSkuSummaryDaoCtrl storeSkuSummaryDaoCtrl = StoreSkuSummaryDaoCtrl.getInstance(flow, aid);
            try {
                StoreSkuSummaryProc storeSkuSummaryProc = new StoreSkuSummaryProc(storeSkuSummaryDaoCtrl, flow);
                rt = storeSkuSummaryProc.searchListFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                storeSkuSummaryDaoCtrl.closeDao();
            }
            sendStoreSkuSummary(session, searchArg, listRef);
            Log.logDbg("aid=%d;searchArg.matcher.toJson=%s", aid, searchArg.matcher.toJson());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    private void sendStoreSkuSummary(FaiSession session, SearchArg searchArg, Ref<FaiList<Param>> listRef) throws IOException {
        FaiList<Param> infoList = listRef.value;
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, StoreSkuSummaryDto.Key.INFO_LIST, StoreSkuSummaryDto.getInfoDto());
        if(searchArg.totalSize != null){
            sendBuf.putInt(StoreSkuSummaryDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    /**
     * 商品业务销售总表 汇总到 商品销售总表
     */
    private int bizSalesSummaryReport(BizSalesSummaryProc bizSalesSummaryProc, SalesSummaryProc salesSummaryProc, int aid, int pdId, int flag){
        int rt = Errno.ERROR;
        Ref<Param> infoRef = new Ref<>();
        rt = bizSalesSummaryProc.getReportInfo(aid, pdId, infoRef);
        if(rt != Errno.OK){
            return rt;
        }
        Param salesSummaryInfo = new Param();
        assemblySalesSummaryInfo(salesSummaryInfo, infoRef.value, flag);
        rt = salesSummaryProc.report(aid, pdId, salesSummaryInfo);
        return rt;
    }
    /**
     * 组装 商品业务销售总表 信息
     */
    private void assemblyBizSalesSummaryInfo(Param bizSalesSummaryInfo, Param reportInfo, int flag){
        bizSalesSummaryInfo.assign(reportInfo,StoreSalesSkuEntity.Info.UNION_PRI_ID, BizSalesSummaryEntity.Info.UNION_PRI_ID);
        bizSalesSummaryInfo.assign(reportInfo,StoreSalesSkuEntity.Info.RL_PD_ID, BizSalesSummaryEntity.Info.RL_PD_ID);
        bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, BizSalesSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, BizSalesReportValObj.Flag.REPORT_COUNT)){
            bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_COUNT, BizSalesSummaryEntity.Info.COUNT);
            bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT, BizSalesSummaryEntity.Info.REMAIN_COUNT);
            bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT, BizSalesSummaryEntity.Info.HOLDING_COUNT);
        }
        if(Misc.checkBit(flag, BizSalesReportValObj.Flag.REPORT_PRICE)){
            bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MIN_PRICE, BizSalesSummaryEntity.Info.MIN_PRICE);
            bizSalesSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MAX_PRICE, BizSalesSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 商品销售总表 信息
     */
    private void assemblySalesSummaryInfo(Param salesSummaryInfo, Param reportInfo, int flag){
        salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.SOURCE_UNION_PRI_ID, SalesSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, BizSalesReportValObj.Flag.REPORT_COUNT)){
            salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.SUM_COUNT, SalesSummaryEntity.Info.COUNT);
            salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.SUM_REMAIN_COUNT, SalesSummaryEntity.Info.REMAIN_COUNT);
            salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.SUM_REMAIN_COUNT, SalesSummaryEntity.Info.HOLDING_COUNT);
        }
        if(Misc.checkBit(flag, BizSalesReportValObj.Flag.REPORT_PRICE)){
            salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.MIN_PRICE, SalesSummaryEntity.Info.MIN_PRICE);
            salesSummaryInfo.assign(reportInfo, BizSalesSummaryEntity.ReportInfo.MAX_PRICE, SalesSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 sku库存 信息
     */
    private void assemblyStoreSkuSummaryInfo(Param reportInfo, Param info) {
        info.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, StoreSkuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        info.assign(reportInfo, StoreSalesSkuEntity.Info.PD_ID, StoreSkuSummaryEntity.Info.PD_ID);
        info.assign(reportInfo, StoreSalesSkuEntity.Info.SKU_ID, StoreSkuSummaryEntity.Info.SKU_ID);
        info.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_COUNT, StoreSkuSummaryEntity.Info.COUNT);
        info.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT, StoreSkuSummaryEntity.Info.REMAIN_COUNT);
        info.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT, StoreSkuSummaryEntity.Info.HOLDING_COUNT);
    }



}
