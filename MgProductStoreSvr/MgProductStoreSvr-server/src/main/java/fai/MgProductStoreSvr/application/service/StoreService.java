package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StoreService {
    /**
     * 库存销售数据汇总上报
     */
    protected int reportSummary(int aid, FaiList<Integer> pdIdList, int flag, FaiList<Long> skuIdList, StoreSalesSkuProc storeSalesSkuProc, SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, SkuSummaryProc skuSummaryProc){
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
     * 上报 spu“业务”库存销售汇总数据 汇总到 spu库存销售汇总
     */
    protected int spuSummaryReport(SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, int aid, int pdId, int flag){
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
    protected int spuSummaryReport(SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, int aid, FaiList<Integer> pdIdList, int flag){
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
                        spuBizSummaryProc.setDirtyCacheEx(aid);
                        spuSummaryProc.setDirtyCacheEx(aid);
                        transactionCtrl.commit();
                        spuBizSummaryProc.deleteDirtyCache(aid);
                        spuSummaryProc.deleteDirtyCache(aid);
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
     * 组装 spu业务库存销售汇总信息
     */
    protected void assemblySpuBizSummaryInfo(Param spuBizSummaryInfo, Param reportInfo, int flag){
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.UNION_PRI_ID, SpuBizSummaryEntity.Info.UNION_PRI_ID);
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.RL_PD_ID, SpuBizSummaryEntity.Info.RL_PD_ID);
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, ReportValObj.Flag.REPORT_COUNT)){
            int count = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_COUNT);
            int remainCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT);
            int holdingCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT);
            int sales = count - holdingCount - remainCount;
            spuBizSummaryInfo.setInt(SpuBizSummaryEntity.Info.COUNT, count);
            spuBizSummaryInfo.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, remainCount);
            spuBizSummaryInfo.setInt(SpuBizSummaryEntity.Info.HOLDING_COUNT, holdingCount);
            spuBizSummaryInfo.setInt(SpuBizSummaryEntity.Info.SALES, sales);
        }
        if(Misc.checkBit(flag, ReportValObj.Flag.REPORT_PRICE)){
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MIN_PRICE, SpuBizSummaryEntity.Info.MIN_PRICE);
            spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.MAX_PRICE, SpuBizSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 spu库存销售汇总信息
     */
    protected void assemblySpuSummaryInfo(Param spuSummaryInfo, Param reportInfo, int flag){
        spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.Info.PD_ID, SpuSummaryEntity.Info.PD_ID);
        spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SOURCE_UNION_PRI_ID, SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        if(Misc.checkBit(flag, ReportValObj.Flag.REPORT_COUNT)){
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_COUNT, SpuSummaryEntity.Info.COUNT);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_REMAIN_COUNT, SpuSummaryEntity.Info.REMAIN_COUNT);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.SUM_HOLDING_COUNT, SpuSummaryEntity.Info.HOLDING_COUNT);
        }
        if(Misc.checkBit(flag, ReportValObj.Flag.REPORT_PRICE)){
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.MIN_PRICE, SpuSummaryEntity.Info.MIN_PRICE);
            spuSummaryInfo.assign(reportInfo, SpuBizSummaryEntity.ReportInfo.MAX_PRICE, SpuSummaryEntity.Info.MAX_PRICE);
        }
    }

    /**
     * 组装 sku库存销售汇总信息
     */
    protected void assemblySkuSummaryInfo(Param skuSummaryInfo, Param reportInfo) {
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


    protected MgProductSpecCli createMgProductSpecCli(int flow){
        MgProductSpecCli mgProductSpecCli = new MgProductSpecCli(flow);
        if(!mgProductSpecCli.init()){
            Log.logErr(Errno.ERROR, "createMgProductSpecCli error;flow=%d;", flow);
            return null;
        }
        return mgProductSpecCli;
    }
}
