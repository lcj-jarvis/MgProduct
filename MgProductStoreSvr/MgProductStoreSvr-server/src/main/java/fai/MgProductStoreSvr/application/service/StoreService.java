package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.SagaRollback;
import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 处理既有库存销售sku又有出入库记录的请求
 */
public class StoreService extends StoreParentService {

    protected int reportSummary(int aid, FaiList<Integer> pdIdList, int flag, FaiList<Long> skuIdList, StoreSalesSkuProc storeSalesSkuProc, SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, SkuSummaryProc skuSummaryProc) {
        return reportSummary(aid, pdIdList, flag, skuIdList, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, false);
    }

    /**
     * 库存销售数据汇总上报
     */
    protected int reportSummary(int aid, FaiList<Integer> pdIdList, int flag, FaiList<Long> skuIdList, StoreSalesSkuProc storeSalesSkuProc, SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, SkuSummaryProc skuSummaryProc, boolean isSaga){
        int rt = Errno.OK;
        Ref<FaiList<Param>> reportListRef = new Ref<>();
        // pdIdList 不为空就上报spu相关汇总
        if(pdIdList != null && !pdIdList.isEmpty()){
            // 在mgStoreSaleSKU_0xxx中查找数据
            rt = storeSalesSkuProc.getReportListByPdIdList(aid, pdIdList, reportListRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(!reportListRef.value.isEmpty()){
                Map<Integer, List<Param>> pdIdReportListMap = reportListRef.value.stream().collect(Collectors.groupingBy((info) -> {
                    return info.getInt(StoreSalesSkuEntity.Info.PD_ID);
                }));
                for (Map.Entry<Integer, List<Param>> pdIdReportListEntry : pdIdReportListMap.entrySet()) {
                    Integer pdId = pdIdReportListEntry.getKey();
                    List<Param> reportList = pdIdReportListEntry.getValue();
                    FaiList<Param> bizSalesSummaryInfoList = new FaiList<>(reportList.size());
                    if (pdId == 6544) {
                        Log.logStd("joke:getFromList=%s", reportList);
                    }
                    for (Param reportInfo : reportList) {
                        Param bizSalesSummaryInfo = new Param();
                        assemblySpuBizSummaryInfo(bizSalesSummaryInfo, reportInfo, flag);
                        bizSalesSummaryInfoList.add(bizSalesSummaryInfo);
                    }
                    // 上报 aid + pdId + unionPriId 维度下，既业务维度 操作表 mgSpuBizSummary_0xxx
                    rt = spuBizSummaryProc.report(aid, pdId, bizSalesSummaryInfoList, isSaga);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
                // 上报 aid + pdId 维度下，既商品维度 操作表 mgSpuSummary_0xxx
                rt = spuSummaryReport(spuBizSummaryProc, spuSummaryProc, aid, new FaiList<>(pdIdReportListMap.keySet()), flag, isSaga);
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
                // 操作表 mgSkuSummary_0xxx
                rt = skuSummaryProc.report(aid, storeSkuSummaryInfoList, isSaga);
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
    protected int spuSummaryReport(SpuBizSummaryProc spuBizSummaryProc, SpuSummaryProc spuSummaryProc, int aid, FaiList<Integer> pdIdList, int flag, boolean isSaga){
        int rt;
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
        rt = spuSummaryProc.batchReport(aid, pdIdInfoMap, isSaga);
        return rt;
    }

    public int restoreSoftDelBizPd(FaiSession session, int flow, int aid, String xid, FaiList<Param> restoreList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Utils.isEmptyList(restoreList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;restoreList=%s;", flow, aid, restoreList);
                return rt;
            }
            FaiList<Param> updateList = new FaiList<>();

            TransactionCtrl tc = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                boolean isSaga = !Str.isEmpty(xid);
                LockUtil.lock(aid);
                try {
                    boolean commit = false;
                    tc.setAutoCommit(false);
                    try {
                        Ref<FaiList<Param>> updateListRef = new Ref<>();
                        rt = storeSalesSkuProc.restoreSoftDelBizPds(aid, restoreList, isSaga, updateListRef);
                        if(rt != Errno.OK) {
                            return rt;
                        }
                        updateList = updateListRef.value;
                        // 要更新的都不存在，直接返回ok
                        if(updateList.isEmpty()) {
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            Log.logStd("ok, update is empty;flow=%s;aid=%s;uid=%s;restoreList=%s;", flow, aid, restoreList);
                            return Errno.OK;
                        }
                        rt = spuBizSummaryProc.restoreSoftDelBizPds(aid, restoreList, isSaga);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        if (isSaga) {
                            // 将预记录的修改数据持久化到 db
                            rt = storeSalesSkuProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 记录 Saga 状态
                            StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                            rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        commit = true;
                    }finally {
                        if(commit) {
                            tc.commit();
                        }else {
                            tc.rollback();
                        }
                    }
                    try {
                        FaiList<Integer> updatePdIds = new FaiList<>();
                        FaiList<Long> updateSkuIds = new FaiList<>();
                        for(Param updateInfo : updateList) {
                            int pdId = updateInfo.getInt(StoreSalesSkuEntity.Info.PD_ID);
                            long skuId = updateInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                            updatePdIds.add(pdId);
                            updateSkuIds.add(skuId);
                        }
                        tc.setAutoCommit(false);
                        rt = reportSummary(aid, updatePdIds, ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                                updateSkuIds, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, true);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 上面已经记录过saga状态了，这里直接记录修改到的数据就行
                        if (isSaga) {
                            // 将预记录的修改数据持久化到 db
                            rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = skuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = spuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                    }finally {
                        if(rt != Errno.OK){
                            tc.rollback();
                            return rt;
                        }
                        spuBizSummaryProc.setDirtyCacheEx(aid);
                        spuSummaryProc.setDirtyCacheEx(aid);
                        tc.commit();
                        spuBizSummaryProc.deleteDirtyCache(aid);
                        spuSummaryProc.deleteDirtyCache(aid);
                    }

                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                tc.closeDao();
            }


            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%s;aid=%s;uid=%s;restoreList=%s;", flow, aid, restoreList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     *  restoreSoftDelBizPd 的补偿方法
     */
    @SuccessRt(value = Errno.OK)
    public int restoreSoftDelBizPdRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        SagaRollback sagaRollback = tc -> {
            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            storeSalesSkuProc.rollback4Saga(aid, xid, branchId);

            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            spuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            spuBizSummaryProc.rollback4Saga(aid, xid, branchId);

            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            skuSummaryProc.rollback4Saga(aid, xid, branchId);
        };
        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    public int batchDelBizPdStoreSales(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds, int sysType, FaiList<Integer> rlPdIdList, boolean softDel, String xid) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || rlPdIdList == null || rlPdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;rlPdIdList=%s;", flow, aid, rlPdIdList);
                return rt;
            }
            FaiList<Param> delList = new FaiList<>();

            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);
                try {
                    boolean commit = false;
                    boolean isSaga = !Str.isEmpty(xid);
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        Ref<FaiList<Param>> listRef = new Ref<>();
                        rt = storeSalesSkuProc.batchDelByRlPdIds(aid, unionPriIds, sysType, rlPdIdList, softDel, isSaga, listRef);
                        if(rt != Errno.OK){
                            Log.logErr(rt, "batchDelByRlPdIds err;");
                            return rt;
                        }

                        delList = listRef.value;
                        if(delList == null) {
                            rt = Errno.ERROR;
                            Log.logErr(rt, "get del list err;flow=%s;aid=%s;uids=%s;sysType=%s;delRlPdIds=%s;", flow, aid, unionPriIds, sysType, rlPdIdList);
                            return rt;
                        }

                        rt = spuBizSummaryProc.batchDelByRlPdIds(aid, unionPriIds, sysType, rlPdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        if (isSaga) {
                            // 将预记录的修改数据持久化到 db
                            rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = storeSalesSkuProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 记录 Saga 状态
                            StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                            rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        commit = true;
                    }finally {
                        if(!commit){
                            transactionCtrl.rollback();
                        }
                    }
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    transactionCtrl.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);

                    FaiList<Integer> delPdIds = new FaiList<>();
                    FaiList<Long> delSkuIds = new FaiList<>();
                    for(Param info : delList) {
                        int pdId = info.getInt(StoreSalesSkuEntity.Info.PD_ID);
                        long skuId = info.getInt(StoreSalesSkuEntity.Info.SKU_ID);
                        delPdIds.add(pdId);
                        delSkuIds.add(skuId);
                    }

                    // 重新汇总
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = reportSummary(aid, delPdIds, ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                                delSkuIds, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 上面已经记录过saga状态了，这里直接记录修改到的数据就行
                        if (isSaga) {
                            // 将预记录的修改数据持久化到 db
                            rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = skuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = spuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
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
            Log.logStd("ok;flow=%s;aid=%s;uid=%s;sysType=%s;delRlPdIds=%s;", flow, aid, unionPriIds, sysType, rlPdIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     *  batchDelBizPdStoreSales 的补偿方法
     */
    @SuccessRt(value = Errno.OK)
    public int batchDelBizPdStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        SagaRollback sagaRollback = tc -> {
            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            storeSalesSkuProc.rollback4Saga(aid, xid, branchId);

            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            spuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            spuBizSummaryProc.rollback4Saga(aid, xid, branchId);

            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            skuSummaryProc.rollback4Saga(aid, xid, branchId);
        };
        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }


    /**
     * 删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList, boolean softDel, String xid) throws IOException{
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

                //BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                //HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                //StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(sotreOrderRecordDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);
                try {
                    boolean commit = false;
                    boolean isSaga = !Str.isEmpty(xid);
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = skuSummaryProc.batchDel(aid, pdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuBizSummaryProc.batchDel(aid, pdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuSummaryProc.batchDel(aid, pdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 软删除
                        rt = inOutStoreRecordProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = storeSalesSkuProc.batchDel(aid, pdIdList, softDel, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        if (isSaga) {
                            // 将预记录的修改数据持久化到 db
                            rt = skuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = spuSummaryProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = storeSalesSkuProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            rt = inOutStoreRecordProc.addUpdateSaga2Db(aid);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 记录 Saga 状态
                            StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                            rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        commit = true;
                    }finally {
                        if(!commit){
                            transactionCtrl.rollback();
                        }
                    }
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    spuSummaryProc.setDirtyCacheEx(aid);
                    transactionCtrl.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);
                    spuSummaryProc.deleteDirtyCache(aid);
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
     *  batchDelPdAllStoreSales 的补偿方法
     */
    @SuccessRt(value = Errno.OK)
    public int batchDelPdAllStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        SagaRollback sagaRollback = tc -> {
            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            storeSalesSkuProc.rollback4Saga(aid, xid, branchId);

            InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
            inOutStoreRecordProc.rollback4Saga(aid, xid, branchId);

            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            spuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            spuBizSummaryProc.rollback4Saga(aid, xid, branchId);

            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            skuSummaryProc.rollback4Saga(aid, xid, branchId);
        };
        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    @SuccessRt(Errno.OK)
    public int restoreData(FaiSession session, int flow, int aid, String xid, FaiList<Integer> pdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        if (aid <= 0 || Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;aid=%d;pdIds=%s", aid, pdIds);
        }
        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl(false);
            boolean commit = false;
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);

                boolean isSaga = !Str.isEmpty(xid);

                storeSalesSkuProc.restoreData(aid, pdIds, isSaga);

                inOutStoreRecordProc.restoreData(aid, pdIds, isSaga);

                spuSummaryProc.restoreData(aid, pdIds, isSaga);

                spuBizSummaryProc.restoreData(aid, pdIds, isSaga);

                skuSummaryProc.restoreData(aid, pdIds, isSaga);

                if (isSaga) {
                    // 将预记录的修改数据持久化到 db
                    rt = skuSummaryProc.addUpdateSaga2Db(aid);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    rt = spuSummaryProc.addUpdateSaga2Db(aid);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    rt = storeSalesSkuProc.addUpdateSaga2Db(aid);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    rt = inOutStoreRecordProc.addUpdateSaga2Db(aid);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                    // 记录 Saga 状态
                    StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                    rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                    if (rt != Errno.OK) {
                        return rt;
                    }
                }

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                }
                tc.closeDao();
                CacheCtrl.clearAllCache(aid);
            }
        } finally {
            LockUtil.unlock(aid);
        }


        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", flow, aid, pdIds);
        return rt;
    }

    @SuccessRt(Errno.OK)
    public int restoreDataRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        SagaRollback sagaRollback = tc -> {
            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            skuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            spuBizSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            spuSummaryProc.rollback4Saga(aid, xid, branchId);

            InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
            inOutStoreRecordProc.rollback4Saga(aid, xid, branchId);

            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            storeSalesSkuProc.rollback4Saga(aid, xid, branchId);
        };
        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    /**
     *  导入数据
     */
    public int importStoreSales(FaiSession session, int flow, int aid, int sourceTid, int sourceUnionPriId, int sysType, String xid, FaiList<Param> storeSaleSkuList, Param inStoreRecordInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || sourceTid<=0 || sourceUnionPriId <= 0|| Utils.isEmptyList(storeSaleSkuList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;sourceUnionPriId=%s;storeSaleSkuList=%s;inStoreRecordInfo=%s;", flow, aid, sourceUnionPriId, storeSaleSkuList, inStoreRecordInfo);
                return rt;
            }
            Set<Integer> pdIdSet = new HashSet<>();
            Set<Long> skuIdSet = new HashSet<>();
            Set<SkuBizKey> skuBizKeySet = new HashSet<>();
            FaiList<Param> inStoreRecordList = new FaiList<>();
            boolean isSaga = !Str.isEmpty(xid);
            for (Param storeSaleSku : storeSaleSkuList) {
                int count = storeSaleSku.getInt(StoreSalesSkuEntity.Info.COUNT, 0);
                storeSaleSku.setInt(StoreSalesSkuEntity.Info.COUNT, count);
                storeSaleSku.setInt(StoreSalesSkuEntity.Info.REMAIN_COUNT, count);
                long skuId = storeSaleSku.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int pdId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.PD_ID);
                int rlPdId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.RL_PD_ID);
                int unionPriId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                FaiList<Integer> inPdScStrIdList = (FaiList<Integer>)storeSaleSku.remove(fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
                skuBizKeySet.add(new SkuBizKey(unionPriId, skuId));
                pdIdSet.add(pdId);
                skuIdSet.add(skuId);
                long costPrice = storeSaleSku.getLong(StoreSalesSkuEntity.Info.COST_PRICE, 0L);

                // 初始入库记录
                if(!Str.isEmpty(inStoreRecordInfo)) {
                    Param addInStoreRecordInfo = inStoreRecordInfo.clone();
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                    addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.SYS_TYPE, sysType);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.IN);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, count);
                    addInStoreRecordInfo.setList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                    addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.PRICE, costPrice);
                    addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, costPrice * count);
                    addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.STATUS, storeSaleSku.getInt(StoreSalesSkuEntity.Info.STATUS, 0));
                    inStoreRecordList.add(addInStoreRecordInfo);
                }
            }
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);

                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        // 添加商品规格库存销售SKU表，表：mgStoreSaleSKU_0xxx
                        rt = storeSalesSkuProc.batchAdd(aid, null, storeSaleSkuList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        Map<SkuBizKey, Param> skuBizSkuStoreSalesInfoMap = new HashMap<>();
                        // 通过 aid + unionPriId + skuId(List) 查找有关库存和价格的字段，放入Map[SkuBizKey(unionPriId,skuId)，Param(info)]中，方便初始化出入库使用
                        rt = storeSalesSkuProc.getInfoMap4OutRecordFromDao(aid, skuBizKeySet, skuBizSkuStoreSalesInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        if(!inStoreRecordList.isEmpty()) {
                            // 添加入库记录 及 汇总
                            rt = inOutStoreRecordProc.batchAdd(aid, inStoreRecordList, skuBizSkuStoreSalesInfoMap, isSaga);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 更新总成本 修改表 mgStoreSaleSKU_0xxx 的 mwTotalCost、mwCost、fifoTotalCost字段 (这里不用补偿，因为修改的是刚添加的数据，补偿时删除数据即可)
                        rt = storeSalesSkuProc.batchUpdateTotalCost(aid, skuBizSkuStoreSalesInfoMap);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 需要在第一个事务里就添加 saga 状态记录
                        // 如果在后面记录的话，当后面的事务失败回滚时，会将状态记录回滚掉，这样 saga 补偿进来后查不到记录就会认为是空补偿或悬挂，而无法补偿第一次事务提交的内容
                        StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                        rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
                try {
                    transactionCtrl.setAutoCommit(false);
                    // 汇总
                    rt = reportSummary(aid, new FaiList<>(pdIdSet), ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                            new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, isSaga);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    // 添加补偿记录 & 将之前预记录的 sagaMap 持久化到 db (ps:这里是指 xid + branchId 下，主要记录了 status)
                    if (!Str.isEmpty(xid)) {
                        rt = spuBizSummaryProc.addUpdateSaga2Db(aid);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        rt = spuSummaryProc.addUpdateSaga2Db(aid);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        rt = skuSummaryProc.addUpdateSaga2Db(aid);
                        if (rt != Errno.OK) {
                            return rt;
                        }
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
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%s;aid=%s;pdIdSet=%s;skuIdSet=%s;", flow, aid, pdIdSet, skuIdSet);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 导入库存的补偿方法
     */
    @SuccessRt(value = Errno.OK)
    public int importStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        SagaRollback sagaRollback = tc -> {
            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            skuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            spuSummaryProc.rollback4Saga(aid, xid, branchId);

            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            spuBizSummaryProc.rollback4Saga(aid, xid, branchId);

            InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
            inOutStoreRecordProc.rollback4Saga(aid, xid, branchId);

            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            storeSalesSkuProc.rollback4Saga(aid, xid, branchId);
        };
        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    public int clearRelData(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            // 事务
            TransactionCtrl tc = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);

                Ref<FaiList<Param>> skuIdAndPdIdRef = new Ref<>();
                rt = storeSalesSkuProc.getAllSkuIdAndPdId(aid, unionPriId, skuIdAndPdIdRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    return rt;
                }
                // 没数据直接返回ok
                if(rt == Errno.NOT_FOUND) {
                    rt = Errno.OK;
                    Log.logStd("clearBizData, data not found;flow=%s;aid=%s;unionPriId=%s", flow, aid, unionPriId);
                    session.write(rt);
                    return rt;
                }

                Set<Integer> pdIdSet = new HashSet<>(skuIdAndPdIdRef.value.size());
                FaiList<Long> skuIds = new FaiList<>();
                for(Param info : skuIdAndPdIdRef.value) {
                    long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                    int pdId = info.getInt(StoreSalesSkuEntity.Info.PD_ID);
                    skuIds.add(skuId);
                    pdIdSet.add(pdId);
                }

                LockUtil.lock(aid);
                try {
                    SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                    SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                    SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                    InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
                    HoldingRecordProc holdingRecordProc = new HoldingRecordProc(flow, aid, tc);
                    RefundRecordProc refundRecordProc = new RefundRecordProc(flow, aid, tc);
                    StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(flow, aid, tc);
                    tc.setAutoCommit(false);
                    boolean commit = false;
                    try {
                        // 清空出入库记录
                        rt = inOutStoreRecordProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空出入库汇总
                        rt = inOutStoreRecordProc.clearSummaryData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空预扣记录
                        rt = holdingRecordProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空退库存记录
                        rt = refundRecordProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        rt = storeOrderRecordProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 删除aid+unionPriId 下 sku库存销售
                        rt = storeSalesSkuProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }
                        // 删除aid+unionPriId 下 spu库存销售汇总
                        rt = spuBizSummaryProc.clearData(aid, unionPriId);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        int flag = ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE;
                        int batchCount = 200;
                        FaiList<FaiList<Long>> batchSkuIds = Utils.splitList(skuIds, batchCount);
                        for(FaiList<Long> itemSkuIds : batchSkuIds) {
                            rt = reportSummary(aid, null, flag, itemSkuIds, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                            if(rt != Errno.OK) {
                                return rt;
                            }
                        }
                        FaiList<FaiList<Integer>> batchPdIds = Utils.splitList(new FaiList<>(pdIdSet), batchCount);
                        for(FaiList<Integer> itemPdIds : batchPdIds) {
                            rt = reportSummary(aid, itemPdIds, flag, null, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                            if(rt != Errno.OK) {
                                return rt;
                            }
                        }

                        commit = true;
                        tc.commit();
                    }finally {
                        if(!commit) {
                            tc.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                        }
                        CacheCtrl.clearAllCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                tc.closeDao();
            }
            session.write(rt);
            Log.logStd("clearBizData ok;flow=%s;aid=%s;unionPriId=%s", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int clearAcct(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Utils.isEmptyList(unionPriIds)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriIds=%s;", flow, aid, unionPriIds);
                return rt;
            }

            // 事务
            TransactionCtrl tc = new TransactionCtrl();
            try {
                LockUtil.lock(aid);
                try {
                    StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                    SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                    SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                    SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                    InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
                    HoldingRecordProc holdingRecordProc = new HoldingRecordProc(flow, aid, tc);
                    RefundRecordProc refundRecordProc = new RefundRecordProc(flow, aid, tc);
                    StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(flow, aid, tc);
                    tc.setAutoCommit(false);
                    boolean commit = false;
                    try {
                        // 清空出入库记录
                        rt = inOutStoreRecordProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空出入库汇总
                        rt = inOutStoreRecordProc.clearSummaryData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空预扣记录
                        rt = holdingRecordProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空退库存记录
                        rt = refundRecordProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 清空直扣库存记录
                        rt = storeOrderRecordProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 删除aid+unionPriId 下 sku库存销售
                        rt = storeSalesSkuProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }
                        // 删除aid+unionPriId 下 spu库存销售汇总
                        rt = spuBizSummaryProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 删除aid+sourceUnionPriId 下 sku库存销售汇总
                        rt = skuSummaryProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        // 删除aid+sourceUnionPriId 下 spu库存销售汇总
                        rt = spuSummaryProc.clearData(aid, unionPriIds);
                        if(rt != Errno.OK) {
                            return rt;
                        }

                        commit = true;
                        tc.commit();
                    }finally {
                        if(!commit) {
                            tc.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                        }
                        CacheCtrl.clearAllCache(aid);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                tc.closeDao();
            }
            session.write(rt);
            Log.logStd("clearAcct ok;flow=%s;aid=%s;unionPriIds=%s;", flow, aid, unionPriIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 清除当前aid的所有缓存
     */
    public int clearAllCache(FaiSession session, int flow, int aid) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            boolean allOption = true;
            try {
                LockUtil.lock(aid);
                allOption = CacheCtrl.clearAllCache(aid);
            }finally {
                LockUtil.unlock(aid);
            }
            if(allOption){
                rt = Errno.OK;
            }
            session.write(rt);
            Log.logStd("flow=%s;aid=%s;allOption=%s", flow, aid, allOption);
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
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.SYS_TYPE, SpuBizSummaryEntity.Info.SYS_TYPE);
        spuBizSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.ReportInfo.SOURCE_UNION_PRI_ID, SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID);
        Object obj = reportInfo.getObject(StoreSalesSkuEntity.ReportInfo.BIT_OR_FLAG);
        if(obj instanceof BigInteger){
            spuBizSummaryInfo.setInt(SpuBizSummaryEntity.Info.FLAG, ((BigInteger)obj).intValue());
        }
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
        skuSummaryInfo.assign(reportInfo, StoreSalesSkuEntity.Info.SYS_TYPE, SkuSummaryEntity.Info.SYS_TYPE);
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
