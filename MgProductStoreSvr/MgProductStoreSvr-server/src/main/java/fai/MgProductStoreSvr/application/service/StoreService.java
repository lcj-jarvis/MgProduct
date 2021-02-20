package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.PdKey;
import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.comm.Utils;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.*;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.middleground.svrutil.repository.TransactionCtrl;
import io.netty.util.internal.IntegerHolder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class StoreService{

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
                        spuBizSummaryProc.deleteDirtyCache(aid);
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
            long endTime = System.currentTimeMillis();
            Log.logStd("ok;aid=%s;consume=%s;",aid, (endTime-startTime));
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
            Set<Integer> unionPriIdSet = new HashSet<>();
            Set<Long> skuIdSet = new HashSet<>();
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
                unionPriIdSet.add(unionPriId);
                skuIdSet.add(skuId);
                String number = InOutStoreRecordValObj.Number.genNumber(createTime, ioStoreRecId);
                Param data = new Param();
                data.setInt(InOutStoreRecordEntity.Info.AID, aid);
                data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
                data.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
                data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);
                data.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
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
                if(data.containsKey(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST)){
                    data.setString(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, info.getList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST).toJson());
                }
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
                        rt = inOutStoreRecordProc.synBatchAdd(aid, unionPriIdSet, skuIdSet, batchAddDataList);
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
            Log.logStd("ok;aid=%s;",aid);
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
     * 库存销售数据汇总上报
     */
    private int reportSummary(int aid, FaiList<Integer> pdIdList, int flag, FaiList<Long> skuIdList, StoreSalesSkuProc storeSalesSkuProc, SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, SkuSummaryProc skuSummaryProc){
        int rt = Errno.OK;
        Ref<FaiList<Param>> reportListRef = new Ref<>();
        // pdIdList 不为空就上报spu相关汇总
        if(pdIdList != null && !pdIdList.isEmpty()){
            rt = storeSalesSkuProc.getReportListByPdIdList(aid, pdIdList, reportListRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(!reportListRef.value.isEmpty()){
                Map<Integer, List<Param>> pdIdReportListMap = reportListRef.value.parallelStream().collect(Collectors.groupingBy((info) -> {
                    return info.getInt(StoreSalesSkuEntity.Info.PD_ID);
                }));
                for (Map.Entry<Integer, List<Param>> pdIdReportListEntry : pdIdReportListMap.entrySet()) {
                    Integer pdId = pdIdReportListEntry.getKey();
                    List<Param> reportList = pdIdReportListEntry.getValue();
                    FaiList<Param> bizSalesSummaryInfoList = new FaiList<>(reportList.size());
                    for (Param reportInfo : reportList) {
                        Param bizSalesSummaryInfo = new Param();
                        assemblySpuBizSummaryInfo(bizSalesSummaryInfo, reportInfo, flag);
                        bizSalesSummaryInfoList.add(bizSalesSummaryInfo);
                    }
                    rt = spuBizSummaryProc.report(aid, pdId, bizSalesSummaryInfoList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
                rt = spuSummaryReport(spuBizSummaryProc, spuSummaryProc, aid, new FaiList<>(pdIdReportListMap.keySet()), flag);
                if(rt != Errno.OK){
                    return rt;
                }
            }
        }
        // skuIdList 不为空就上报sku相关汇总
        if(skuIdList != null && !skuIdList.isEmpty()){
            reportListRef = new Ref<>();
            rt = storeSalesSkuProc.getReportListBySkuIdList(aid, skuIdList, reportListRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(!reportListRef.value.isEmpty()){
                FaiList<Param> storeSkuSummaryInfoList = new FaiList<>(reportListRef.value.size());
                for (Param reportInfo : reportListRef.value) {
                    Param skuSummaryInfo = new Param();
                    assemblySkuSummaryInfo(skuSummaryInfo, reportInfo);
                    storeSkuSummaryInfoList.add(skuSummaryInfo);
                }
                rt = skuSummaryProc.report(aid, storeSkuSummaryInfoList);
                if(rt != Errno.OK){
                    return rt;
                }
            }
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

            Map<Long, Integer> skuIdChangeCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
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

    private MgProductSpecCli createMgProductSpecCli(int flow){
        MgProductSpecCli mgProductSpecCli = new MgProductSpecCli(flow);
        if(!mgProductSpecCli.init()){
            Log.logErr(Errno.ERROR, "createMgProductSpecCli error;flow=%d;", flow);
            return null;
        }
        return mgProductSpecCli;
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
     * MQ 异步上报
     */
    private void asynchronousReport(int flow, int aid, int unionPriId, FaiList<Long> skuIdList, FaiList<Integer> pdIdList) {
        TransactionCtrl transactionCtrl;
        if (pdIdList.size() > 0) {
            transactionCtrl = new TransactionCtrl();
            try {
                transactionCtrl.setAutoCommit(false);
                SpuBizStoreSalesReportDaoCtrl spuBizStoreSalesReportDaoCtrl = SpuBizStoreSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizStoreSalesReportProc spuBizStoreSalesReportProc = new SpuBizStoreSalesReportProc(spuBizStoreSalesReportDaoCtrl, flow);
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
                info.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId);
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
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, skuSummaryDaoCtrl, inOutStoreRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
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

                        Map<SkuStoreKey, Param> changeCountAfterSkuStoreInfoMap = new HashMap<>();
                        // 获取更新后的库存量
                        rt = storeSalesSkuProc.getInfoMap4OutRecordFromDao(aid, needGetChangeCountAfterSkuStoreKeySet, changeCountAfterSkuStoreInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // 批量添加记录
                        rt = inOutStoreRecordProc.batchAdd(aid, infoList, changeCountAfterSkuStoreInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }

                        // 更新总成本
                        rt = storeSalesSkuProc.batchUpdateTotalCost(aid, changeCountAfterSkuStoreInfoMap);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
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
                        rt = reportSummary(aid, new FaiList<>(pdIdSet), SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT,
                                new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
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
            Log.logDbg("ok;;aid=%d;ownerUnionPriId=%s;", aid, ownerUnionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }

        return rt;
    }

    /**
     * 获取出入库记录
     */
    public int getInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, boolean isSource, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || searchArg == null || searchArg.isEmpty() || unionPriId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;searchArg=%s;", flow, aid, unionPriId, searchArg);
                return rt;
            }

            if(searchArg.limit > 100){
                Log.logErr("searchArg.limit err;flow=%d;aid=%d;searchArg.limit=%s;", flow, aid, searchArg.limit);
                return rt = Errno.ARGS_ERROR;
            }

            ParamMatcher baseMatcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
            if(isSource){
                baseMatcher.and(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            }else{
                baseMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            }

            baseMatcher.and(searchArg.matcher);
            searchArg.matcher = baseMatcher;

            Ref<FaiList<Param>> listRef = new Ref<>();
            InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstance(flow, aid);
            try {
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                rt = inOutStoreRecordProc.searchFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                inOutStoreRecordDaoCtrl.closeDao();
            }
            sendInOutRecord(session, searchArg, listRef);
            Log.logDbg("ok;aid=%d;searchArg.matcher.toJson=%s", aid, searchArg.matcher.toJson());
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    private void sendInOutRecord(FaiSession session, SearchArg searchArg, Ref<FaiList<Param>> listRef) throws IOException {
        FaiList<Param> infoList = listRef.value;
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, InOutStoreRecordDto.Key.INFO_LIST, InOutStoreRecordDto.getInfoDto());
        if(searchArg.totalSize != null){
            sendBuf.putInt(InOutStoreRecordDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    /**
     * 上报spu业务库存销售汇总信息，同时上报到spu维度
     */
    public int reportSpuBizSummary(int flow, int aid, int unionPriId, int pdId){
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
                SpuBizStoreSalesReportDaoCtrl spuBizStoreSalesReportDaoCtrl = SpuBizStoreSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(spuBizStoreSalesReportDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, storeSalesSkuDaoCtrl)){
                    return rt = Errno.ERROR;
                }
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizStoreSalesReportProc spuBizStoreSalesReportProc = new SpuBizStoreSalesReportProc(spuBizStoreSalesReportDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);

                Ref<Param> infoRef = new Ref<>();
                rt = spuBizStoreSalesReportProc.getInfo(aid, unionPriId, pdId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                Param bizSalesReportInfo = infoRef.value;
                int flag = bizSalesReportInfo.getInt(SpuBizStoreSalesReportEntity.Info.FLAG, 0);

                Param reportInfo = null;


                infoRef.value = null;
                rt = storeSalesSkuProc.getReportInfo(aid, pdId, unionPriId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                reportInfo = infoRef.value;

                Param bizSalesSummaryInfo = new Param();
                bizSalesSummaryInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                bizSalesSummaryInfo.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
                assemblySpuBizSummaryInfo(bizSalesSummaryInfo, reportInfo, flag);

                try {
                    LockUtil.lock(aid);
                    try { // 上报数据 并 删除上报任务
                        transactionCtrl.setAutoCommit(false);
                        rt = spuBizSummaryProc.report(aid, pdId, new FaiList<>(Arrays.asList(bizSalesSummaryInfo)));
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuBizStoreSalesReportProc.del(aid, unionPriId, pdId, flag);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuSummaryReport(spuBizSummaryProc, spuSummaryProc, aid, pdId, flag);
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
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 上报sku维度的库存销售汇总信息
     */
    public int reportSkuSummary(int flow, int aid, long skuId) {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstance(flow, aid);
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                Ref<Param> infoRef = new Ref<>();
                rt = storeSalesSkuProc.getReportInfo(aid, skuId, infoRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                rt = Errno.OK;
                if(!infoRef.value.isEmpty()){
                    Param skuSummaryInfo = new Param();
                    assemblySkuSummaryInfo(skuSummaryInfo, infoRef.value);
                    rt = skuSummaryProc.report(aid, skuId, skuSummaryInfo);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
            }finally {
                skuSummaryDaoCtrl.closeDao();
                storeSalesSkuDaoCtrl.closeDao();
            }
            Log.logStd("ok;flow=%s;aid=%s;skuId=%s;", flow, aid, skuId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取某个业务下spu业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<String> useSourceFieldList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;", flow, aid, unionPriId, pdIdList);
                return rt;
            }
            FaiList<Param> list;
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = spuBizSummaryProc.getInfoListByPdIdListFromDao(aid, unionPriId, pdIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
                list = listRef.value;
                if(useSourceFieldList != null && !useSourceFieldList.isEmpty()) {
                    Set<String> useSourceFieldSet = new HashSet<>(useSourceFieldList);
                    useSourceFieldSet.retainAll(Arrays.asList(SpuBizSummaryEntity.getValidKeys()));
                    if(list.size() > 0 && useSourceFieldSet.size() > 0){
                        useSourceFieldSet.add(SpuBizSummaryEntity.Info.PD_ID);
                        Param first = list.get(0);
                        int tmpUnionPriId = first.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
                        int sourceUnionPriId = first.getInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID);
                        if(tmpUnionPriId != sourceUnionPriId){
                            FaiList<Integer> sourcePdIdList = Utils.getValList(list, SpuBizSummaryEntity.Info.PD_ID);
                            listRef = new Ref<>();
                            rt = spuBizSummaryProc.getInfoListByPdIdListFromDao(aid, sourceUnionPriId, sourcePdIdList, listRef, useSourceFieldSet.toArray(new String[]{}));
                            if(rt != Errno.OK){
                                if(rt == Errno.NOT_FOUND){
                                    Log.logErr( rt,"source list not found;flow=%s;aid=%s;sourcePriId=%s;sourcePdIdList=%s;", flow, aid, sourceUnionPriId, sourcePdIdList);
                                }
                                return rt;
                            }
                            Map<Integer, Param> map = Utils.getMap(listRef.value, SpuBizSummaryEntity.Info.PD_ID);
                            for (Param info : list) {
                                Integer pdId = info.getInt(SpuBizSummaryEntity.Info.PD_ID);
                                Param sourceInfo = map.get(pdId);
                                if(sourceInfo == null){
                                    rt = Errno.NOT_FOUND;
                                    Log.logErr(rt,"source info not found;flow=%s;aid=%s;sourcePriId=%s;pdId=%s;", flow, aid, sourceUnionPriId, pdId);
                                    return rt;
                                }
                                for (String key : useSourceFieldSet) {
                                    info.assign(sourceInfo, key);
                                }
                            }
                        }
                    }
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            sendSpuBizSummary(session, list);
            Log.logDbg("ok;aid=%d;unionPriId=%s;pdIdList=%s;", aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据pdId 获取所关联的 spu 业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoListByPdId(FaiSession session, int flow, int aid, int tid, int pdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                rt = spuBizSummaryProc.getInfoListByUnionPriIdListFromDao(aid, null, pdId, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            sendSpuBizSummary(session, listRef.value);
            Log.logDbg("ok;aid=%d;pdId=%s;", aid, pdId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    private void sendSpuBizSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpuBizSummaryDto.Key.INFO_LIST, SpuBizSummaryDto.getInfoDto());
        session.write(sendBuf);
    }
    /**
     * 获取spu库存销售汇总信息
     */
    public int getSpuSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;", flow, aid, unionPriId, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                rt = spuSummaryProc.getInfoListFromDao(aid, pdIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                spuSummaryDaoCtrl.closeDao();
            }
            sendSpuSummary(session, listRef.value);
            Log.logDbg("ok;aid=%d;unionPriId=%s;pdIdList=%s;", aid, unionPriId, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    private void sendSpuSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpuSummaryDto.Key.INFO_LIST, SpuSummaryDto.getInfoDto());
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
                //BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                //HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                //SotreOrderRecordDaoCtrl sotreOrderRecordDaoCtrl = SotreOrderRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(spuBizSummaryDaoCtrl, inOutStoreRecordDaoCtrl
                        , spuSummaryDaoCtrl,  storeSalesSkuDaoCtrl, skuSummaryDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                //BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                //HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                //StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(sotreOrderRecordDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = skuSummaryProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuBizSummaryProc.batchDel(aid, pdIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuSummaryProc.batchDel(aid, pdIdList);
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
            Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取 SKU业务库存销售汇总信息  -  不用汇总已经是最新粒度
     */
    public int getSkuBizSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, SearchArg searchArg) throws IOException {
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
                rt = storeSalesSkuProc.searchSkuBizSummaryFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            for (Param info : listRef.value) {
                Long price = info.getLong(StoreSalesSkuEntity.Info.PRICE);
                info.setLong(SkuSummaryEntity.Info.MIN_PRICE, price);
                info.setLong(SkuSummaryEntity.Info.MAX_PRICE, price);
            }
            sendSkuSummary(session, searchArg, listRef);
            Log.logDbg("ok;aid=%d;searchArg.matcher.toJson=%s", aid, searchArg.matcher.toJson());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取SKU库存销售汇总信息
     */
    public int getSkuSummaryInfoList(FaiSession session, int flow, int aid, int tid, int sourceUnionPriId, SearchArg searchArg) throws IOException {
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

            ParamMatcher baseMatcher = new ParamMatcher(SkuSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
            baseMatcher.and(SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.EQ, sourceUnionPriId);
            baseMatcher.and(searchArg.matcher);
            searchArg.matcher = baseMatcher;

            Ref<FaiList<Param>> listRef = new Ref<>();
            SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                rt = skuSummaryProc.searchListFromDao(aid, searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                skuSummaryDaoCtrl.closeDao();
            }
            sendSkuSummary(session, searchArg, listRef);
            Log.logDbg("ok;aid=%d;searchArg.matcher.toJson=%s", aid, searchArg.matcher.toJson());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    private void sendSkuSummary(FaiSession session, SearchArg searchArg, Ref<FaiList<Param>> listRef) throws IOException {
        FaiList<Param> infoList = listRef.value;
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SkuSummaryDto.Key.INFO_LIST, SkuSummaryDto.getInfoDto());
        if(searchArg.totalSize != null){
            sendBuf.putInt(SkuSummaryDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    /**
     * 上报 spu“业务”库存销售汇总数据 汇总到 spu库存销售汇总
     */
    private int spuSummaryReport(SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, int aid, int pdId, int flag){
        int rt = Errno.ERROR;
        Ref<Param> infoRef = new Ref<>();
        rt = spuBizSummaryProc.getReportInfo(aid, pdId, infoRef);
        if(rt != Errno.OK){
            return rt;
        }
        Param spuSummaryInfo = new Param();
        assemblySpuSummaryInfo(spuSummaryInfo, infoRef.value, flag);
        rt = spuSummaryProc.report(aid, pdId, spuSummaryInfo);
        return rt;
    }

    /**
     * 批量 上报 spu“业务”库存销售汇总数据 汇总到 spu库存销售汇总
     */
    private int spuSummaryReport(SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, int aid, FaiList<Integer> pdIdList, int flag){
        int rt = Errno.ERROR;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = spuBizSummaryProc.getReportList(aid, pdIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Map<Integer, Param> pdIdInfoMap = new HashMap<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            Integer pdId = info.getInt(SkuSummaryEntity.Info.PD_ID);
            Param spuSummaryInfo = new Param();
            assemblySpuSummaryInfo(spuSummaryInfo, info, flag);
            pdIdInfoMap.put(pdId, spuSummaryInfo);
        }
        rt = spuSummaryProc.batchReport(aid, pdIdInfoMap);
        return rt;
    }

    /**
     * 组装 spu业务库存销售汇总信息
     */
    private void assemblySpuBizSummaryInfo(Param spuBizSummaryInfo, Param reportInfo, int flag){
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.UNION_PRI_ID, SpuBizSummaryEntity.Info.UNION_PRI_ID);
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.RL_PD_ID, SpuBizSummaryEntity.Info.RL_PD_ID);
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT)){
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_COUNT, SpuBizSummaryEntity.Info.COUNT);
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT, SpuBizSummaryEntity.Info.REMAIN_COUNT);
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT, SpuBizSummaryEntity.Info.HOLDING_COUNT);
        }
        if(Misc.checkBit(flag, SpuBizStoreSalesReportValObj.Flag.REPORT_PRICE)){
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MIN_PRICE, SpuBizSummaryEntity.Info.MIN_PRICE);
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MAX_PRICE, SpuBizSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 spu库存销售汇总信息
     */
    private void assemblySpuSummaryInfo(Param spuSummaryInfo, Param reportInfo, int flag){
        spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.Info.PD_ID, SpuSummaryEntity.Info.PD_ID);
        spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SOURCE_UNION_PRI_ID, SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT)){
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_COUNT, SpuSummaryEntity.Info.COUNT);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_REMAIN_COUNT, SpuSummaryEntity.Info.REMAIN_COUNT);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_HOLDING_COUNT, SpuSummaryEntity.Info.HOLDING_COUNT);
        }
        if(Misc.checkBit(flag, SpuBizStoreSalesReportValObj.Flag.REPORT_PRICE)){
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.MIN_PRICE, SpuSummaryEntity.Info.MIN_PRICE);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.MAX_PRICE, SpuSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 sku库存销售汇总信息
     */
    private void assemblySkuSummaryInfo(Param skuSummaryInfo, Param reportInfo) {
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.PD_ID, SkuSummaryEntity.Info.PD_ID);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.SKU_ID, SkuSummaryEntity.Info.SKU_ID);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_COUNT, SkuSummaryEntity.Info.COUNT);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT, SkuSummaryEntity.Info.REMAIN_COUNT);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT, SkuSummaryEntity.Info.HOLDING_COUNT);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MIN_PRICE, SkuSummaryEntity.Info.MIN_PRICE);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MAX_PRICE, SkuSummaryEntity.Info.MAX_PRICE);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_FIFO_TOTAL_COST, SkuSummaryEntity.Info.FIFO_TOTAL_COST);
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SUM_MW_TOTAL_COST, SkuSummaryEntity.Info.MW_TOTAL_COST);
    }



}
