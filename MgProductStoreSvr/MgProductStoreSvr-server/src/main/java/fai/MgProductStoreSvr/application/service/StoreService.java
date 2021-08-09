package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 处理既有库存销售sku又有出入库记录的请求
 */
public class StoreService {

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


    /**
     * 删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList, String xid) throws IOException{
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
                        rt = skuSummaryProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuBizSummaryProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuSummaryProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 软删除
                        rt = inOutStoreRecordProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = storeSalesSkuProc.batchDel(aid, pdIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 记录补偿
                        if (isSaga) {
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
    public int batchDelPdAllStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            LockUtil.lock(aid);
            try {
                TransactionCtrl tc = new TransactionCtrl();
                try {
                    StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                    InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
                    SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                    SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                    SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                    StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                    tc.setAutoCommit(false);
                    boolean commit = false;
                    try {
                        Ref<Param> sagaRef = new Ref<>();
                        // 获取补偿信息
                        rt = storeSagaProc.getInfoWithAdd(xid, branchId, sagaRef);
                        if (rt != Errno.OK) {
                            // 如果rt=NOT_FOUND，说明出现空补偿或悬挂现象，插入saga记录占位后return OK告知分布式事务组件回滚成功
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }
                        Param sagaInfo = sagaRef.value;
                        Integer status = sagaInfo.getInt(StoreSagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == StoreSagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // 获取 Saga 操作记录
                        Ref<FaiList<Param>> salesSkuSagaListRef = new Ref<>();
                        rt = storeSalesSkuProc.getSagaList(xid, branchId, salesSkuSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> salesSkuSagaList = salesSkuSagaListRef.value;

                        Ref<FaiList<Param>> ioStoreRecordSagaListRef = new Ref<>();
                        rt = inOutStoreRecordProc.getInOutStoreRecordSagaList(xid, branchId, ioStoreRecordSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> ioStoreRecordSagaList = ioStoreRecordSagaListRef.value;

                        Ref<FaiList<Param>> spuSummarySagaListRef = new Ref<>();
                        rt = spuSummaryProc.getSagaList(xid, branchId, spuSummarySagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> spuSummarySagaList = spuSummarySagaListRef.value;

                        Ref<FaiList<Param>> spuBizSummarySagaListRef = new Ref<>();
                        rt = spuBizSummaryProc.getSagaList(xid, branchId, spuBizSummarySagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> spuBizSummarySagaList = spuBizSummarySagaListRef.value;

                        Ref<FaiList<Param>> skuSummarySagaListRef = new Ref<>();
                        rt = skuSummaryProc.getSagaList(xid, branchId, skuSummarySagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> skuSummarySagaList = skuSummarySagaListRef.value;
                        // ----------------------------------- 补偿操作 start ----------------------------------
                        // 1、补偿 销售库存 SKU
                        if (!Util.isEmptyList(salesSkuSagaList)) {
                            rt = storeSalesSkuProc.batchDelRollback(aid, salesSkuSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 2、补偿 出入库记录
                        if (!Util.isEmptyList(ioStoreRecordSagaList)) {
                            rt = inOutStoreRecordProc.batchDelRollback(aid, ioStoreRecordSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 3、补偿 spu 汇总
                        if (!Util.isEmptyList(spuSummarySagaList)) {
                            rt = spuBizSummaryProc.batchDelRollback(aid, spuSummarySagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 4、补偿 商品规格销售 spu
                        if (!Util.isEmptyList(spuBizSummarySagaList)) {
                            rt = spuBizSummaryProc.batchDelRollback(aid, spuSummarySagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 5、补偿 sku 汇总
                        if (!Util.isEmptyList(skuSummarySagaList)) {
                            rt = skuSummaryProc.batchDelRollback(aid, salesSkuSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // ----------------------------------- 补偿操作 end ------------------------------------
                        commit = true;
                    } finally {
                        if (!commit) {
                            tc.rollback();
                        }
                    }
                } finally {
                    tc.closeDao();
                }
            } finally {
                LockUtil.unlock(aid);
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     *  导入数据
     */
    public int importStoreSales(FaiSession session, int flow, int aid, int sourceTid, int sourceUnionPriId, String xid, FaiList<Param> storeSaleSkuList, Param inStoreRecordInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || sourceTid<=0 || sourceUnionPriId <= 0|| Util.isEmptyList(storeSaleSkuList) || Str.isEmpty(inStoreRecordInfo)) {
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
                Param addInStoreRecordInfo = inStoreRecordInfo.clone();
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.IN);
                addInStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, count);
                addInStoreRecordInfo.setList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.PRICE, costPrice);
                addInStoreRecordInfo.setLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, costPrice * count);
                inStoreRecordList.add(addInStoreRecordInfo);
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
                        // 添加入库记录 及 汇总
                        rt = inOutStoreRecordProc.batchAdd(aid, inStoreRecordList, skuBizSkuStoreSalesInfoMap, isSaga);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // 更新总成本 修改表 mgStoreSaleSKU_0xxx 的 mwTotalCost、mwCost、fifoTotalCost字段 (这里不用补偿，因为修改的是刚添加的数据，补偿时删除数据即可)
                        rt = storeSalesSkuProc.batchUpdateTotalCost(aid, skuBizSkuStoreSalesInfoMap);
                        if(rt != Errno.OK){
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
                    rt = reportSummary(aid, new FaiList<>(pdIdSet), ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                            new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, isSaga);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    // 添加补偿记录 (ps:这里是指 xid + branchId 下，主要记录了 status)
                    if (!Str.isEmpty(xid)) {
                        StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                        rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                    }
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
    public int importStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Param sagaInfo;
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                Set<Integer> pdIdSet = new HashSet<>();
                Set<Long> skuIdSet = new HashSet<>();
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, tc);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);

                LockUtil.lock(aid);
                try {
                    tc.setAutoCommit(false);
                    try {
                        Ref<Param> infoRef = new Ref<>();
                        // 获取补偿信息
                        rt = storeSagaProc.getInfoWithAdd(xid, branchId, infoRef);
                        if (rt != Errno.OK) {
                            // 如果rt=NOT_FOUND，说明出现空补偿或悬挂现象，插入saga记录占位后return OK告知分布式事务组件回滚成功
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }

                        sagaInfo = infoRef.value;
                        Integer status = sagaInfo.getInt(StoreSagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == StoreSagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // 1、补偿出入库信息
                        Ref<FaiList<Param>> ioStoreRecordSagaListRef = new Ref<>();
                        // 获取 Saga 记录
                        rt = inOutStoreRecordProc.getInOutStoreRecordSagaList(xid, branchId, ioStoreRecordSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> ioStoreRecordSagaList = ioStoreRecordSagaListRef.value;
                        if (!Util.isEmptyList(ioStoreRecordSagaList)) {
                            // 在删除之前，需要获取到 skuIdSet 和 pdIdSet，用于上报的补偿
                            ioStoreRecordSagaList.forEach(ioStoreRecordSagaInfo -> {
                                pdIdSet.add(ioStoreRecordSagaInfo.getInt(InOutStoreRecordEntity.Info.PD_ID));
                                skuIdSet.add(ioStoreRecordSagaInfo.getLong(InOutStoreRecordEntity.Info.SKU_ID));
                            });
                            // 因为很清楚之前做的是添加操作，所以这里直接调用删除
                            // 同时很清楚，其实之前添加的都有一个共同的 ioStoreRecId ，这个 id 是在 aid 下自增的，所以 matcher 只需要 aid EQ + ioStoreRecId EQ
                            Integer ioStoreRecId = ioStoreRecordSagaList.get(0).getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                            // 补偿 出入库记录表 & 出入库汇总表 (ps:两者同理，都是通过 aid + ioStoreRecId)
                            rt = inOutStoreRecordProc.batchAddRollback(aid, ioStoreRecId);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            // 恢复 ioStoreRecId
                            inOutStoreRecordProc.restoreMaxId(aid);
                        }
                        // 2、补偿库存销售表
                        Ref<FaiList<Param>> storeSalesSagaListRef = new Ref<>();
                        rt = storeSalesSkuProc.getSagaList(xid, branchId, storeSalesSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> storeSalesSagaList = storeSalesSagaListRef.value;
                        if (!Util.isEmptyList(storeSalesSagaList)) {
                            rt = storeSalesSkuProc.batchAddRollback(aid, storeSalesSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 3、修改补偿信息
                        rt = storeSagaProc.setStatus(xid, branchId, StoreSagaValObj.Status.ROLLBACK_OK);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        commit = true;
                        tc.commit();
                    } finally {
                        if (!commit) {
                            tc.rollback();
                        }
                    }
                } finally {
                    LockUtil.unlock(aid);
                }
                // 上报信息
                try {
                    commit = false;
                    tc.setAutoCommit(false);
                    // 补偿那些修改的上报信息
                    // 查询 mgSkuSummarySaga_0x 表的补偿信息
                    Ref<FaiList<Param>> skuSummarySagaListRef = new Ref<>();
                    rt = skuSummaryProc.getSagaList(xid, branchId, skuSummarySagaListRef);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                        return rt;
                    }
                    FaiList<Param> skuSummarySagaList = skuSummarySagaListRef.value;
                    if (!Util.isEmptyList(skuSummarySagaList)) {
                        rt = skuSummaryProc.batchAddRollback(aid, skuSummarySagaList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 查询 mgSpuSummarySaga_0x 表的补偿信息
                    Ref<FaiList<Param>> spuSummarySagaListRef = new Ref<>();
                    rt = spuSummaryProc.getSagaList(xid, branchId, spuSummarySagaListRef);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                        return rt;
                    }
                    FaiList<Param> spuSummarySagaList = spuSummarySagaListRef.value;
                    if (!Util.isEmptyList(spuSummarySagaList)) {
                        rt = spuSummaryProc.batchAddRollback(aid, spuSummarySagaList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 查询 mgSpuBizSummarySaga_0x 表的补偿信息
                    Ref<FaiList<Param>> spuBizSummarySagaListRef = new Ref<>();
                    rt = spuBizSummaryProc.getSagaList(xid, branchId, spuBizSummarySagaListRef);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                        return rt;
                    }
                    FaiList<Param> spuBizSummarySagaList = spuBizSummarySagaListRef.value;
                    if (!Util.isEmptyList(spuBizSummarySagaList)) {
                        rt = spuBizSummaryProc.batchAddRollback(aid, spuBizSummarySagaList);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                    // 上报
                    rt = reportSummary(aid, new FaiList<>(pdIdSet), ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                            new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND){
                        return rt;
                    }
                    rt = Errno.OK;
                    commit = true;
                } finally {
                    if (!commit) {
                        tc.rollback();
                        return rt;
                    }
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    spuSummaryProc.setDirtyCacheEx(aid);
                    tc.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);
                    spuSummaryProc.deleteDirtyCache(aid);
                }
            } finally {
                tc.closeDao();
            }
        } finally {
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
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
                        FaiList<FaiList<Long>> batchSkuIds = Util.splitList(skuIds, batchCount);
                        for(FaiList<Long> itemSkuIds : batchSkuIds) {
                            rt = reportSummary(aid, null, flag, itemSkuIds, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                            if(rt != Errno.OK) {
                                return rt;
                            }
                        }
                        FaiList<FaiList<Integer>> batchPdIds = Util.splitList(new FaiList<>(pdIdSet), batchCount);
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
            if (aid <= 0 || Util.isEmptyList(unionPriIds)) {
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
