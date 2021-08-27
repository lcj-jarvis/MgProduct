package fai.MgProductStoreSvr.application.service;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.domain.comm.*;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.HoldingRecordDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.MgProductStoreSvr.interfaces.entity.SkuCountChangeEntity;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * 主要处理 库存销售sku相关 请求
 */
public class StoreSalesSkuService extends StoreService {
    public int reportSummary(FaiSession session, int flow, int aid, FaiList<Integer> pdIds, FaiList<Long> skuIds, boolean reportCount, boolean reportPrice) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || (skuIds.isEmpty() && pdIds.isEmpty())) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;aid=%s;pdIds=%s;skuIds=%s;reportCount=%s;reportPrice=%s;",aid, pdIds, skuIds, reportCount, reportPrice);
                return rt;
            }
            int flag = 0;
            if(reportCount) {
                flag |= ReportValObj.Flag.REPORT_COUNT;
            }
            if(reportPrice) {
                flag |= ReportValObj.Flag.REPORT_PRICE;
            }
            if(flag == 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err, price and count all unreport;aid=%s;pdIds=%s;skuIds=%s;reportCount=%s;reportPrice=%s;",aid, pdIds, skuIds, reportCount, reportPrice);
                return rt;
            }

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                LockUtil.lock(aid);

                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);

                try {
                    transactionCtrl.setAutoCommit(false);
                    // 同步方式汇总信息
                    rt = reportSummary(aid, pdIds, flag, skuIds, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    // 事务提交前先设置一个较短的过期时间
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    spuSummaryProc.setDirtyCacheEx(aid);
                    transactionCtrl.commit();
                    // 提交成功再删除缓存
                    spuBizSummaryProc.deleteDirtyCache(aid);
                    spuSummaryProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;aid=%s;pdIds=%s;skuIds=%s;reportCount=%s;reportPrice=%s;",aid, pdIds, skuIds, reportCount, reportPrice);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 刷新库存销售sku信息
     */
    public int refreshSkuStoreSales(FaiSession session, int flow, int aid, int tid, int unionPriId, int sysType, String xid, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) throws IOException {
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

                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);

                // 查出当前已有的sku
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriId, pdId, listRef, StoreSalesSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Param> storeSalesSkuInfoList = listRef.value;

                // skuId - info
                Map<Long, Param> skuIdPdScSkuInfoMap = Utils.getMap(pdScSkuInfoList, StoreSalesSkuEntity.Info.SKU_ID);
                // 筛选出需要删除的sku
                FaiList<Long> delSkuIdList = new FaiList<>();
                for (Param storeSalesSkuInfo : storeSalesSkuInfoList) {
                    Long skuId = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                    Param pdScSkuInfo = skuIdPdScSkuInfoMap.remove(skuId);
                    if(pdScSkuInfo == null){
                        delSkuIdList.add(skuId);
                    }
                }
                FaiList<Integer> pdIds = new FaiList<>(Collections.singletonList(pdId));
                boolean isSaga = !Str.isEmpty(xid);
                // 生成需要添加的sku
                FaiList<Param> addInfoList = new FaiList<>();
                for (Map.Entry<Long, Param> skuIdPdScSkuInfoEntry : skuIdPdScSkuInfoMap.entrySet()) {
                    addInfoList.add(new Param()
                            .setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId)
                            .setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, unionPriId)
                            .setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId)
                            .setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType)
                            .setLong(StoreSalesSkuEntity.Info.SKU_ID, skuIdPdScSkuInfoEntry.getKey())
                    );
                }
                try {
                    transactionCtrl.setAutoCommit(false);
                    boolean notModify = true;
                    if(!delSkuIdList.isEmpty()){
                        notModify = false;
                        rt = storeSalesSkuProc.batchDel(aid, pdId, delSkuIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = skuSummaryProc.batchDel(aid, pdId, delSkuIdList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuBizSummaryProc.batchDel(aid, pdIds, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = spuSummaryProc.batchDel(aid, pdIds, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    if(!addInfoList.isEmpty()){
                        notModify = false;
                        rt = storeSalesSkuProc.batchAdd(aid, pdId, addInfoList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    // 当本次刷新没有修改操作就直接返回
                    if(notModify){
                        FaiBuffer sendBuf = new FaiBuffer(true);
                        session.write(sendBuf);
                        Log.logDbg("not modify;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                        return rt = Errno.OK;
                    }
                    // 同步方式汇总信息
                    rt = reportSummary(aid, pdIds, ReportValObj.Flag.REPORT_COUNT| ReportValObj.Flag.REPORT_PRICE
                            , null, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, isSaga);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    if (isSaga) {
                        // 记录 Saga 状态
                        StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                        rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCtrl.rollback();
                        return rt;
                    }
                    // 事务提交前先设置一个较短的过期时间
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    spuSummaryProc.setDirtyCacheEx(aid);
                    transactionCtrl.commit();
                    // 提交成功再删除缓存
                    spuBizSummaryProc.deleteDirtyCache(aid);
                    spuSummaryProc.deleteDirtyCache(aid);
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
     * refreshSkuStoreSales 的补偿方法
     */
    public int refreshSkuStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            try {
                boolean commit = false;
                LockUtil.lock(aid);
                try {
                    tc.setAutoCommit(false);
                    StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                    SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                    SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                    SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                    StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                    try {
                        Ref<Param> sagaInfoRef = new Ref<>();
                        // 获取补偿记录
                        rt = storeSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }

                        Param sagaInfo = sagaInfoRef.value;
                        Integer status = sagaInfo.getInt(StoreSagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == StoreSagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // 查询 库存销售 SKU 的 Saga 操作记录表
                        Ref<FaiList<Param>> storeSalesAllSagaListRef = new Ref<>();
                        rt = storeSalesSkuProc.getSagaList(xid, branchId, storeSalesAllSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> storeSalesAllSagaList = storeSalesAllSagaListRef.value;
                        // 查询 库存销售 spu 的 Saga 操作记录表
                        Ref<FaiList<Param>> spuBizAllSagaListRef = new Ref<>();
                        rt = spuBizSummaryProc.getSagaList(xid, branchId, spuBizAllSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> spuBizAllSagaList = spuBizAllSagaListRef.value;
                        // 查询 spu 汇总 的 Saga 操作记录表
                        Ref<FaiList<Param>> spuSummaryAllSagaListRef = new Ref<>();
                        rt = spuSummaryProc.getSagaList(xid, branchId, spuSummaryAllSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> spuSummaryAllSagaList = spuSummaryAllSagaListRef.value;
                        // 查询 sku 汇总 的 Saga 操作记录表 (ps:因为上报的时候没有上报 sku，所以操作表里可能有的只能是 del 数据)
                        Ref<FaiList<Param>> skuSummaryDelSagaListRef = new Ref<>();
                        rt = skuSummaryProc.getSagaList(xid, branchId, skuSummaryDelSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> skuSumDelSagaList = skuSummaryDelSagaListRef.value;
                        // 存在没有修改的场景，只需要直接返回 OK 就行了
                        if (Util.isEmptyList(storeSalesAllSagaList) && Util.isEmptyList(spuBizAllSagaList)
                                && Util.isEmptyList(spuSummaryAllSagaList) && Util.isEmptyList(skuSumDelSagaList)) {
                            commit = true;
                            return rt = Errno.OK;
                        }
                        // 数据分组 根据 SagaOp 分组
                        FaiList<Param> spuBizAddSagaList = new FaiList<>();
                        FaiList<Param> spuBizDelSagaList = new FaiList<>();
                        for (Param spuBizInfo : spuBizAllSagaList) {
                            Integer sagaOp = spuBizInfo.getInt(StoreSagaEntity.Info.SAGA_OP);
                            if (sagaOp == StoreSagaValObj.SagaOp.ADD) {
                                spuBizAddSagaList.add(spuBizInfo);
                            } else {
                                spuBizDelSagaList.add(spuBizInfo);
                            }
                        }

                        FaiList<Param> spuSummaryAddSagaList = new FaiList<>();
                        FaiList<Param> spuSummaryDelSagaList = new FaiList<>();
                        for (Param spuSumInfo : spuSummaryAllSagaList) {
                            Integer sagaOp = spuSumInfo.getInt(StoreSagaEntity.Info.SAGA_OP);
                            if (sagaOp == StoreSagaValObj.SagaOp.ADD) {
                                spuSummaryAddSagaList.add(spuSumInfo);
                            } else {
                                spuSummaryDelSagaList.add(spuSumInfo);
                            }
                        }

                        FaiList<Param> storeSalesAddSagaList = new FaiList<>();
                        FaiList<Param> storeSalesDelSagaList = new FaiList<>();
                        for (Param info : storeSalesAllSagaList) {
                            Integer sagaOp = info.getInt(StoreSagaEntity.Info.SAGA_OP);
                            if (sagaOp == StoreSagaValObj.SagaOp.ADD) {
                                storeSalesAddSagaList.add(info);
                            } else {
                                storeSalesDelSagaList.add(info);
                            }
                        }

                        // --------------------------------------------- 补偿 start ------------------------------------------------
                        // 1、对添加的上报做补偿
                        if (!Util.isEmptyList(spuSummaryAddSagaList)) {
                            rt = spuSummaryProc.batchAddRollback(aid, spuSummaryAddSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        if (!Util.isEmptyList(spuBizAddSagaList)) {
                            rt = spuBizSummaryProc.batchAddRollback(aid, spuBizAddSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 2、对添加的库存 sku 销售做补偿
                        if (!Util.isEmptyList(storeSalesAddSagaList)) {
                            rt = storeSalesSkuProc.batchAddRollback(aid, storeSalesAddSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 后面上报需要使用
                        int pdId = 0;
                        // 3、对删除的信息做补偿
                        if (!Util.isEmptyList(spuSummaryDelSagaList)) {
                            rt = spuSummaryProc.batchDelRollback(aid, spuSummaryDelSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            if (pdId == 0) {
                                spuSummaryDelSagaList.get(0).getInt(SpuSummaryEntity.Info.PD_ID);
                            }
                        }
                        if (!Util.isEmptyList(spuBizDelSagaList)) {
                            rt = spuBizSummaryProc.batchDelRollback(aid, spuBizDelSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            if (pdId == 0) {
                                spuSummaryDelSagaList.get(0).getInt(SpuSummaryEntity.Info.PD_ID);
                            }
                        }
                        if (!Util.isEmptyList(skuSumDelSagaList)) {
                            rt = skuSummaryProc.batchDelRollback(aid, skuSumDelSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            if (pdId == 0) {
                                spuSummaryDelSagaList.get(0).getInt(SpuSummaryEntity.Info.PD_ID);
                            }
                        }
                        if (!Util.isEmptyList(storeSalesDelSagaList)) {
                            rt = storeSalesSkuProc.batchDelRollback(aid, storeSalesDelSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                            if (pdId == 0) {
                                spuSummaryDelSagaList.get(0).getInt(SpuSummaryEntity.Info.PD_ID);
                            }
                        }

                        // 3、上报修改
                        rt = reportSummary(aid, new FaiList<>(Collections.singletonList(pdId)), ReportValObj.Flag.REPORT_COUNT| ReportValObj.Flag.REPORT_PRICE,
                                null, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                        // 允许 rt = Errno.NOT_FOUND , 存在这种场景，因为前面的补偿将添加的汇总给删除了，可能会存在 NOT_FOUND 情况
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        // 4、修改补偿状态
                        rt = storeSagaProc.setStatus(xid, branchId, StoreSagaValObj.Status.ROLLBACK_OK);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // --------------------------------------------- 补偿 end --------------------------------------------------
                        commit = true;
                    } finally {
                        if (!commit) {
                            tc.rollback();
                        }
                    }
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    spuSummaryProc.setDirtyCacheEx(aid);
                    tc.commit();
                    spuBizSummaryProc.deleteDirtyCache(aid);
                    spuSummaryProc.deleteDirtyCache(aid);
                } finally {
                    LockUtil.unlock(aid);
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
            FaiList<Param> holdingRecordList = new FaiList<>();
            FaiList<Long> skuIdList = new FaiList<>();
            for (Param spuSalesStoreInfo : spuStoreSalesInfoList) {
                int unionPriId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, 0);
                int pdId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.PD_ID, 0);
                int rlPdId = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.RL_PD_ID, 0);
                int sysType = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.SYS_TYPE, 0);
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
                int flag = spuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.FLAG, 0);
                if(spuSalesStoreInfo.containsKey(StoreSalesSkuEntity.Info.PRICE)){
                    flag |= StoreSalesSkuValObj.FLag.SETED_PRICE;
                }
                Param skuSalesStoreData = new Param();
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.AID, aid);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType);
                skuSalesStoreData.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                skuSalesStoreData.setInt(StoreSalesSkuEntity.Info.FLAG, flag);
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
                FaiList<Param> holdingOrderList = spuSalesStoreInfo.getListNullIsEmpty(fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity.Info.HOLDING_ORDER_LIST);
                if(!holdingOrderList.isEmpty()){
                    for (Param holdingOrder : holdingOrderList) {
                        String orderId = holdingOrder.getString("orderId");
                        int count = holdingOrder.getInt("count");
                        holdingRecordList.add(
                          new Param().setInt(HoldingRecordEntity.Info.AID, aid)
                                  .setInt(HoldingRecordEntity.Info.UNION_PRI_ID, unionPriId)
                                  .setLong(HoldingRecordEntity.Info.SKU_ID, skuId)
                                  .setString(HoldingRecordEntity.Info.RL_ORDER_CODE, orderId)
                                  .setInt(HoldingRecordEntity.Info.COUNT, count)
                        );
                    }
                }
            }
            maxUpdateKeySet.removeAll(Arrays.asList(StoreSalesSkuEntity.Info.AID, StoreSalesSkuEntity.Info.UNION_PRI_ID, StoreSalesSkuEntity.Info.PD_ID, StoreSalesSkuEntity.Info.RL_PD_ID, StoreSalesSkuEntity.Info.SYS_TYPE, StoreSalesSkuEntity.Info.SKU_ID));

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                LockUtil.lock(aid);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SkuSummaryDaoCtrl skuSummaryDaoCtrl = SkuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);

                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, skuSummaryDaoCtrl, holdingRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(skuSummaryDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = holdingRecordProc.batchSynchronous(aid, holdingRecordList);
                    if(rt != Errno.OK){
                        return rt;
                    }
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
                    // 记录spu业务维度的汇总
                    Map<Integer/*unionPriId*/, Map<Integer/*pdId*/, Param/*spuBizSummary*/>> unionPriId_pdId_spuBizSummaryInfoMapMap = new HashMap<>();
                    // 记录spu维度的汇总
                    Map<Integer/*pdId*/, Param/*spuSummary*/> pdIdSpuSummaryInfoMap = new HashMap<>();
                    // 记录sku维度的汇总
                    Map<Long/*skuId*/, Param/*skuSummary*/> skuIdSkuSummaryInfoMap = new HashMap<>();
                    for (Param reportInfo : reportInfoList) {
                        int pdId = reportInfo.getInt(StoreSalesSkuEntity.Info.PD_ID);
                        int unionPriId = reportInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                        long skuId = reportInfo.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                        // 计算spu+业务维度的汇总
                        {
                            Param spuBizSummaryInfo = new Param();
                            assemblySpuBizSummaryInfo(spuBizSummaryInfo, reportInfo, ReportValObj.Flag.REPORT_PRICE| ReportValObj.Flag.REPORT_COUNT);
                            Map<Integer, Param> pdId_spuBizSummaryInfoMap = unionPriId_pdId_spuBizSummaryInfoMapMap.get(unionPriId);
                            if(pdId_spuBizSummaryInfoMap == null){
                                pdId_spuBizSummaryInfoMap = new HashMap<>();
                                unionPriId_pdId_spuBizSummaryInfoMapMap.put(unionPriId, pdId_spuBizSummaryInfoMap);
                            }
                            pdId_spuBizSummaryInfoMap.put(pdId, spuBizSummaryInfo);
                        }
                        Object bitOrFlagObj = reportInfo.getObject(StoreSalesSkuEntity.ReportInfo.BIT_OR_FLAG);
                        int bigOrFlag = ((BigInteger)bitOrFlagObj).intValue();
                        boolean setPrice = Misc.checkBit(bigOrFlag, StoreSalesSkuValObj.FLag.SETED_PRICE);

                        int sumCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_COUNT);
                        int sumRemainCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_REMAIN_COUNT);
                        int sumHoldingCount = reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.SUM_HOLDING_COUNT);
                        long maxPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MAX_PRICE);
                        long minPrice= reportInfo.getInt(StoreSalesSkuEntity.ReportInfo.MIN_PRICE);
                        if(!setPrice){
                            minPrice = Long.MAX_VALUE;
                            maxPrice = Long.MIN_VALUE;
                        }
                        // 计算 spu维度的汇总
                        {
                            Param spuSummaryInfo = pdIdSpuSummaryInfoMap.get(pdId);
                            if(spuSummaryInfo == null){
                                spuSummaryInfo = new Param();
                                spuSummaryInfo.setInt(SpuSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                pdIdSpuSummaryInfoMap.put(pdId, spuSummaryInfo);
                            }
                            int lastCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = spuSummaryInfo.getInt(SpuSummaryEntity.Info.HOLDING_COUNT, 0);
                            long lastMaxPrice = spuSummaryInfo.getLong(SpuSummaryEntity.Info.MAX_PRICE, Long.MIN_VALUE);
                            long lastMinPrice = spuSummaryInfo.getLong(SpuSummaryEntity.Info.MIN_PRICE, Long.MAX_VALUE);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.COUNT, lastCount+sumCount);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            spuSummaryInfo.setInt(SpuSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                            spuSummaryInfo.setLong(SpuSummaryEntity.Info.MAX_PRICE, Math.max(lastMaxPrice, maxPrice));
                            spuSummaryInfo.setLong(SpuSummaryEntity.Info.MIN_PRICE, Math.min(lastMinPrice, minPrice));
                        }
                        // 计算sku维度的汇总
                        {
                            Param skuSummaryInfo = skuIdSkuSummaryInfoMap.get(skuId);
                            if(skuSummaryInfo == null){
                                skuSummaryInfo = new Param();
                                skuSummaryInfo.setInt(SkuSummaryEntity.Info.PD_ID, pdId);
                                skuSummaryInfo.setInt(SkuSummaryEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                                skuIdSkuSummaryInfoMap.put(skuId, skuSummaryInfo);
                            }
                            int lastCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.COUNT, 0);
                            int lastRemainCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.REMAIN_COUNT, 0);
                            int lastHoldingCount = skuSummaryInfo.getInt(SkuSummaryEntity.Info.HOLDING_COUNT, 0);
                            long lastMaxPrice = skuSummaryInfo.getLong(SkuSummaryEntity.Info.MAX_PRICE, Long.MIN_VALUE);
                            long lastMinPrice = skuSummaryInfo.getLong(SkuSummaryEntity.Info.MIN_PRICE, Long.MAX_VALUE);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.COUNT, lastCount+sumCount);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.REMAIN_COUNT, lastRemainCount + sumRemainCount);
                            skuSummaryInfo.setInt(SkuSummaryEntity.Info.HOLDING_COUNT, lastHoldingCount + sumHoldingCount);
                            skuSummaryInfo.setLong(SkuSummaryEntity.Info.MAX_PRICE, Math.max(lastMaxPrice, maxPrice));
                            skuSummaryInfo.setLong(SkuSummaryEntity.Info.MIN_PRICE, Math.min(lastMinPrice, minPrice));
                        }
                    }

                    rt = spuBizSummaryProc.report4synSPU2SKU(aid, unionPriId_pdId_spuBizSummaryInfoMapMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = spuSummaryProc.report4synSPU2SKU(aid, pdIdSpuSummaryInfoMap);
                    if(rt != Errno.OK){
                        return rt;
                    }

                    rt = skuSummaryProc.report4synSPU2SKU(aid, skuIdSkuSummaryInfoMap);
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
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            long endTime = System.currentTimeMillis();
            Log.logStd("ok;flow=%s;aid=%s;consume=%s;",flow, aid, (endTime-startTime));
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    public int setSkuStoreSales(FaiSession session, int flow, int aid, int tid, int unionPriId, String xid, int pdId, int rlPdId, int sysType, FaiList<ParamUpdater> updaterList) throws IOException {
        return batchSetSkuStoreSales(session, flow, aid, tid, null, new FaiList<>(Collections.singletonList(unionPriId)), xid, pdId, rlPdId, sysType, updaterList);
    }
    public int batchSetSkuStoreSales(FaiSession session, int flow, int aid, int tid, Integer ownerUnionPriId, FaiList<Integer> unionPriIdList, int pdId, int rlPdId, int sysType, FaiList<ParamUpdater> updaterList) throws IOException {
        return batchSetSkuStoreSales(session, flow, aid, tid, ownerUnionPriId, unionPriIdList, null, pdId, rlPdId, sysType, updaterList);
    }
    /**
     * 批量修改库存销售sku信息
     */
    public int batchSetSkuStoreSales(FaiSession session, int flow, int aid, int tid, Integer ownerUnionPriId, FaiList<Integer> unionPriIdList, String xid, int pdId, int rlPdId, int sysType, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {

            if (aid <= 0 || Util.isEmptyList(unionPriIdList) || pdId <= 0 || rlPdId <= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriIdList=%s;pdId=%s;rlPdId=%s;updaterList=%s", flow, aid, unionPriIdList, pdId, rlPdId, updaterList);
                return rt;
            }
            FaiList<Long> skuIdList = new FaiList<>();
            for (ParamUpdater updater : updaterList) {
                Long skuId = updater.getData().getLong(StoreSalesSkuEntity.Info.SKU_ID);
                if(skuId == null){
                    Log.logErr("skuId err;flow=%d;aid=%d;unionPriIdList=%s;pdId=%s;rlPdId=%s;updater=%s", flow, aid, unionPriIdList, pdId, rlPdId, updater.toJson());
                    return Errno.ARGS_ERROR;
                }
                skuIdList.add(skuId);
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);

                        // 兼容门店通逻辑，门店通添加商品的时候，只会添加总部的库存销售信息(为了设置价格)，各个门店最开始是不会有销售信息的
                        // 所以这里设置价格的时候，先查出没有销售信息的unionPriId，并新增销售信息数据
                        // 这个逻辑不需要分布式事务，之前调用的接口 ownerUnionPriId 传的是 null 不会进入下面逻辑，也就不需要补偿
                        if(tid == FaiValObj.TermId.YK && ownerUnionPriId != null) {
                            Map<SkuBizKey, PdKey> needCheckSkuStoreKeyPdKeyMap = new HashMap<>();
                            for(Integer unionPriId : unionPriIdList) {
                                for(Long skuId : skuIdList) {
                                    SkuBizKey skuBizKey = new SkuBizKey(unionPriId, skuId);
                                    if (unionPriId != ownerUnionPriId) {
                                        needCheckSkuStoreKeyPdKeyMap.put(skuBizKey, new PdKey(unionPriId, pdId, rlPdId, sysType));
                                    }
                                }
                            }
                            if(!needCheckSkuStoreKeyPdKeyMap.isEmpty()) {
                                rt = storeSalesSkuProc.checkAndAdd(aid, ownerUnionPriId, needCheckSkuStoreKeyPdKeyMap);
                                if(rt != Errno.OK){
                                    return rt;
                                }
                            }
                        }
                        boolean isSaga = !Str.isEmpty(xid);
                        // 修改商品规格销售 SKU 表
                        rt = storeSalesSkuProc.batchSet(aid, unionPriIdList, pdId, updaterList, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        // 上报信息
                        rt = reportSummary(aid, new FaiList<>(Collections.singletonList(pdId)), ReportValObj.Flag.REPORT_PRICE|ReportValObj.Flag.REPORT_COUNT,
                                skuIdList, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc, isSaga);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if (isSaga) {
                            // 向 mgStoreSaga_0x 表插入记录
                            StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, transactionCtrl);
                            rt = storeSagaProc.add(aid, xid, RootContext.getBranchId());
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
            Log.logStd("ok;aid=%s;unionPriIdList=%s;pdId=%s;rlPdId=%s;",aid, unionPriIdList, pdId, rlPdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * setSkuStoreSales 的补偿方法
     */
    public int setSkuStoreSalesRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            try {
                tc.setAutoCommit(false);
                LockUtil.lock(aid);
                try {
                    StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
                    SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
                    SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                    SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
                    StoreSagaProc storeSagaProc = new StoreSagaProc(flow, aid, tc);
                    try {
                        Ref<Param> sagaInfoRef = new Ref<>();
                        // 获取补偿信息
                        rt = storeSagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                        if (rt != Errno.OK) {
                            if (rt == Errno.NOT_FOUND) {
                                // 已经插入一条空补偿
                                commit = true;
                                rt = Errno.OK;
                                Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                            }
                            return rt;
                        }
                        Param sagaInfo = sagaInfoRef.value;
                        Integer status = sagaInfo.getInt(StoreSagaEntity.Info.STATUS);
                        // 幂等性保证
                        if (status == StoreSagaValObj.Status.ROLLBACK_OK) {
                            commit = true;
                            Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                            return rt = Errno.OK;
                        }
                        // -------------------------------------------- 补偿操作 start -----------------------------------------
                        // 1、先将添加的汇总信息删除
                        Ref<FaiList<Param>> skuSummarySagaListRef = new Ref<>();
                        // 查询 mgSkuSummarySaga_0x 表的补偿信息
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
                        // 补偿那些修改的上报信息
                        Ref<FaiList<Param>> spuBizSummarySagaListRef = new Ref<>();
                        // 查询 mgSpuBizSummarySaga_0x 表的补偿信息
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
                        // 2、关于 销售库存 Sku 的补偿
                        Ref<FaiList<Param>> storeSalesSkuSagaListRef = new Ref<>();
                        // 获取 Saga 记录
                        rt = storeSalesSkuProc.getSagaList(xid, branchId, storeSalesSkuSagaListRef);
                        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                            return rt;
                        }
                        FaiList<Param> storeSalesSkuSagaList = storeSalesSkuSagaListRef.value;
                        int pdId = 0;
                        Set<Long> skuIdSet = new HashSet<>();
                        if (!Util.isEmptyList(storeSalesSkuSagaList)) {
                            // 获取 pdId 用于之后的上报
                            pdId = storeSalesSkuSagaList.get(0).getInt(StoreSalesSkuEntity.Info.PD_ID);
                            // 获取 skuList 用于之后的上报
                            storeSalesSkuSagaList.forEach(info -> skuIdSet.add(info.getLong(StoreSalesSkuEntity.Info.SKU_ID)));
                            // 补偿修改
                            rt = storeSalesSkuProc.batchSetRollback(aid, storeSalesSkuSagaList);
                            if (rt != Errno.OK) {
                                return rt;
                            }
                        }
                        // 3、上报
                        rt = reportSummary(aid, new FaiList<>(Collections.singletonList(pdId)), ReportValObj.Flag.REPORT_PRICE|ReportValObj.Flag.REPORT_COUNT,
                                new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
                        // 允许 rt = Errno.NOT_FOUND , 存在这种场景，因为前面的补偿将添加的汇总给删除了，可能会存在 NOT_FOUND 情况
                        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                            return rt;
                        }
                        // 3、修改补偿状态
                        rt = storeSagaProc.setStatus(xid, branchId, StoreSagaValObj.Status.ROLLBACK_OK);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        // -------------------------------------------- 补偿操作 end -------------------------------------------
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

    /**
     *  批量新增库存销售数据
     */
    public int batchAddStoreSales(FaiSession session, int flow, int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> storeSaleSkuList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || sourceTid<=0 || sourceUnionPriId <= 0|| Util.isEmptyList(storeSaleSkuList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;sourceUnionPriId=%s;storeSaleSkuList=%s;", flow, aid, sourceUnionPriId, storeSaleSkuList);
                return rt;
            }
            Set<Integer> pdIdSet = new HashSet<>();
            Set<Long> skuIdSet = new HashSet<>();
            Set<SkuBizKey> skuBizKeySet = new HashSet<>();
            Map<Integer, FaiList<Long>> unionPriId_SkuIds = new HashMap<>();
            for (Param storeSaleSku : storeSaleSkuList) {
                int count = storeSaleSku.getInt(StoreSalesSkuEntity.Info.COUNT, 0);
                storeSaleSku.setInt(StoreSalesSkuEntity.Info.COUNT, count);
                storeSaleSku.setInt(StoreSalesSkuEntity.Info.REMAIN_COUNT, count);
                long skuId = storeSaleSku.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int pdId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.PD_ID);
                int unionPriId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                skuBizKeySet.add(new SkuBizKey(unionPriId, skuId));
                FaiList<Long> skuIds = unionPriId_SkuIds.get(unionPriId);
                if(skuIds == null) {
                    skuIds = new FaiList<>();
                    unionPriId_SkuIds.put(unionPriId, skuIds);
                }
                skuIds.add(skuId);
                pdIdSet.add(pdId);
                skuIdSet.add(skuId);
            }
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);

                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);

                        // 已存在的数据做更新操作，这个逻辑是因为门店通最开始，添加商品时，各个门店没有库存销售数据，后来补充了，所以跑工具修复线上数据，同步库存销售数据到所有门店
                        // 但是因为门店通有个逻辑，新增门店时，商品是不会同步到新增的门店中的，所以也不会有库存销售信息，但是这个时候，跑工具已经把库存销售信息同步过来了
                        {
                            Map<SkuBizKey, Param> existMap = new HashMap<>();
                            for(Map.Entry<Integer, FaiList<Long>> uid_skuIdEntry : unionPriId_SkuIds.entrySet()) {
                                int unionPriId = uid_skuIdEntry.getKey();
                                FaiList<Long> skuIds = uid_skuIdEntry.getValue();
                                Ref<FaiList<Param>> listRef = new Ref();
                                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, skuIds, listRef, StoreSalesSkuEntity.getValidKeys());
                                if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                                    return rt;
                                }
                                for(Param info : listRef.value) {
                                    existMap.put(new SkuBizKey(unionPriId, info.getLong(StoreSalesSkuEntity.Info.SKU_ID)), info);
                                }
                            }
                            if(!existMap.isEmpty()) {
                                FaiList<ParamUpdater> updateList = new FaiList<>(existMap.size());
                                for(Iterator<Param> iterator = storeSaleSkuList.iterator();iterator.hasNext();){
                                    Param storeSaleSku = iterator.next();
                                    long skuId = storeSaleSku.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                                    int unionPriId = storeSaleSku.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                                    SkuBizKey skuBizKey = new SkuBizKey(unionPriId, skuId);
                                    if(existMap.containsKey(skuBizKey)) {
                                        updateList.add(new ParamUpdater(storeSaleSku));
                                        iterator.remove();
                                    }
                                }
                                // 批量更新
                                rt = storeSalesSkuProc.batchSet(aid, updateList, existMap);
                                if(rt != Errno.OK) {
                                    return rt;
                                }
                            }
                        }

                        if(!storeSaleSkuList.isEmpty()) {
                            rt = storeSalesSkuProc.batchAdd(aid, null, storeSaleSkuList);
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
                    LockUtil.unlock(aid);
                }
                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = reportSummary(aid, new FaiList<>(pdIdSet), ReportValObj.Flag.REPORT_COUNT|ReportValObj.Flag.REPORT_PRICE,
                            new FaiList<>(skuIdSet), storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
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
     * 批量扣减库存
     * @return
     */
    public int batchReducePdSkuStore(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode) || reduceMode <= 0 || expireTimeSeconds < 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt,"arg err;flow=%d;aid=%d;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;", flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
                return rt;
            }
            TreeMap<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            TreeMap<RecordKey, Integer> recordCountMap = new TreeMap<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(SkuCountChangeEntity.Info.SKU_ID);
                int itemId = info.getInt(SkuCountChangeEntity.Info.ITEM_ID, 0);
                int count = info.getInt(SkuCountChangeEntity.Info.COUNT);
                skuIdList.add(skuId);
                recordCountMap.put(new RecordKey(skuId, itemId), count);
                count += skuIdCountMap.getOrDefault(skuId, 0);
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
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdCountMap, recordCountMap, true, false);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdCountMap.isEmpty()){ // 重复扣减了
                            Log.logStd("find repeat reduce；aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdCountList=%s", aid, unionPriId, rlOrderCode, skuIdCountList);
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            return rt = Errno.OK;
                        }
                        rt = holdingRecordProc.batchAdd(aid, unionPriId, recordCountMap, rlOrderCode, expireTimeSeconds);
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
            stat.end(rt != Errno.OK && rt != MgProductErrno.Store.SHORTAGE, rt);
        }
        return rt;
    }

    /**
     * 检测是否存在预扣记录
     */
    private int checkHoldingRecordExists(HoldingRecordProc holdingRecordProc, int aid, int unionPriId, FaiList<Long> skuIdList, String rlOrderCode, Map<Long, Integer> skuIdCountMap, TreeMap<RecordKey, Integer> recordCountMap, boolean notJudgeDel, boolean isMakeup){
        int rt = Errno.ERROR;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = holdingRecordProc.getListFromDao(aid, unionPriId, skuIdList, rlOrderCode, listRef);
        if(rt != Errno.OK){
            if(isMakeup){
               return rt;
            }
            if(rt != Errno.NOT_FOUND){
                return rt;
            }
        }
        FaiList<Param> list = listRef.value;
        Map<Long, Set<RecordKey>> skuIdRecordKeysMap = new HashMap<>();
        boolean useOld = false;
        if(useOld){
            Map<Long, Integer> combineSkuIdCount = new HashMap<>();
            for (Param info : list) {
                long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
                int count = info.getInt(HoldingRecordEntity.Info.COUNT);
                Integer combineCount = combineSkuIdCount.getOrDefault(skuId, 0);
                combineCount+=count;
                combineSkuIdCount.put(skuId, combineCount);
            }
            FaiList<Param> combineList = new FaiList<>(combineSkuIdCount.size());
            for (Param info : list) {
                long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
                boolean alreadyDel = info.getBoolean(HoldingRecordEntity.Info.ALREADY_DEL);
                Integer count = combineSkuIdCount.remove(skuId);
                if(count != null){
                    combineList.add(
                            new Param()
                                    .setLong(HoldingRecordEntity.Info.SKU_ID, skuId)
                                    .setBoolean(HoldingRecordEntity.Info.ALREADY_DEL, alreadyDel)
                                    .setInt(HoldingRecordEntity.Info.COUNT, count)
                    );
                }
            }
            list = combineList;

            for (RecordKey recordKey : recordCountMap.keySet()) {
                Set<RecordKey> recordKeys = skuIdRecordKeysMap.getOrDefault(recordKey.skuId, new HashSet<>());
                recordKeys.add(recordKey);
                skuIdRecordKeysMap.put(recordKey.skuId, recordKeys);
            }
        }
        for (Param info : list) {
            boolean alreadyDel = info.getBoolean(HoldingRecordEntity.Info.ALREADY_DEL);
            if(notJudgeDel || alreadyDel){ // 不判断删除 或者 已经删除
                long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
                int itemId = 0;
                Integer recordCount = info.getInt(HoldingRecordEntity.Info.COUNT);
                Integer count = null;
                if(useOld){
                    count = skuIdCountMap.remove(skuId);
                    Set<RecordKey> recordKeys = skuIdRecordKeysMap.get(skuId);
                    for (RecordKey recordKey : recordKeys) {
                        recordCountMap.remove(recordKey);
                    }
                }else{
                    itemId = info.getInt(HoldingRecordEntity.Info.ITEM_ID);
                    count = recordCountMap.remove(new RecordKey(skuId, itemId));
                    if(count != null){
                        int totalCount = skuIdCountMap.get(skuId);
                        totalCount = totalCount - count;
                        if(totalCount == 0){
                            skuIdCountMap.remove(skuId);
                        }else {
                            skuIdCountMap.put(skuId, totalCount);
                        }
                    }
                }
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
            if(Str.isEmpty(outStoreRecordInfo.getString(InOutStoreRecordEntity.Info.RL_ORDER_CODE))){
                outStoreRecordInfo.setString(InOutStoreRecordEntity.Info.RL_ORDER_CODE, rlOrderCode);
            }
            TreeMap<Long, Integer> skuIdChangeCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            TreeMap<RecordKey, Integer> recordCountMap = new TreeMap<>();
            Set<SkuBizKey> skuBizKeySet = new HashSet<>();
            FaiList<Integer> pdIdList = new FaiList<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(SkuCountChangeEntity.Info.SKU_ID);
                int itemId = info.getInt(SkuCountChangeEntity.Info.ITEM_ID, 0);
                int count = info.getInt(SkuCountChangeEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuBizKeySet.add(new SkuBizKey(unionPriId, skuId));

                recordCountMap.put(new RecordKey(skuId, itemId), count);

                count += skuIdChangeCountMap.getOrDefault(skuId, 0);
                skuIdChangeCountMap.put(skuId, count);
            }

            MgProductSpecCli mgProductSpecCli = createMgProductSpecCli(flow);
            FaiList<Param> skuInfoList = new FaiList<>();
            rt = mgProductSpecCli.getPdSkuScInfoListBySkuIdList(aid, tid, skuIdList, skuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt, "MgProductSpecCli getPdSkuScInfoListBySkuIdList err;flow=%s;aid=%s;skuIdList=%s", flow, aid, skuIdList);
                return rt;
            }
            Map<Long, FaiList<Integer>> skuIdInPdScStrIdListMap = Utils.getMap(skuInfoList, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            Ref<Integer> ioStoreRecordIdRef = new Ref<>();
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, holdingRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, transactionCtrl);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdChangeCountMap, recordCountMap, false, false);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdChangeCountMap.isEmpty()){ // 重复扣减了
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            sendBuf.putInt(StoreSalesSkuDto.Key.IN_OUT_STORE_REC_ID, -1);
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

                        // 获取修改后的库存销售sku信息
                        Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap = new HashMap<>();
                        rt = storeSalesSkuProc.getInfoMap4OutRecordFromDao(aid, skuBizKeySet, changeCountAfterSkuStoreSalesInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        for (Param skuStoreSalesInfo : changeCountAfterSkuStoreSalesInfoMap.values()) {
                            pdIdList.add(skuStoreSalesInfo.getInt(StoreSalesSkuEntity.Info.PD_ID));
                        }

                        // 添加出库记录
                        outStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.OUT);
                        rt = inOutStoreRecordProc.batchAddOutStoreRecord(aid, unionPriId, skuIdChangeCountMap, changeCountAfterSkuStoreSalesInfoMap, outStoreRecordInfo, skuIdInPdScStrIdListMap, ioStoreRecordIdRef);
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
            sendBuf.putInt(StoreSalesSkuDto.Key.IN_OUT_STORE_REC_ID, ioStoreRecordIdRef.value);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdChangeCountMap=%s;", aid, unionPriId, rlOrderCode, skuIdChangeCountMap);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量补偿库存，不需要生成入库记录
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
            TreeMap<RecordKey, Integer> recordCountMap = new TreeMap<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(SkuCountChangeEntity.Info.SKU_ID);
                int itemId = info.getInt(SkuCountChangeEntity.Info.ITEM_ID, 0);
                int count = info.getInt(SkuCountChangeEntity.Info.COUNT);
                skuIdList.add(skuId);
                recordCountMap.put(new RecordKey(skuId, itemId), count);
                count += skuIdCountMap.getOrDefault(skuId, 0);
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
                        rt = checkHoldingRecordExists(holdingRecordProc, aid, unionPriId, skuIdList, rlOrderCode, skuIdCountMap, recordCountMap, false, true);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        if(skuIdCountMap.isEmpty()){ // 重复补偿了
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            Log.logStd("find repeat makeup;aid=%d;unionPriId=%s;rlOrderCode=%s;skuIdCountList=%s", aid, unionPriId, rlOrderCode, skuIdCountList);
                            return rt = Errno.OK;
                        }
                        // 逻辑删除
                        rt = holdingRecordProc.batchLogicDel(aid, unionPriId, skuIdList, rlOrderCode);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                    // 补偿库存
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
            stat.end(rt != Errno.OK && rt < MgProductErrno.MIN_VALUE, rt);
        }
        return rt;
    }

    /**
     * 管理态调用 <br/>
     * 刷新 rlOrderCode 的预扣记录。<br/>
     * 根据 allHoldingRecordList 和已有的预扣尽量进行对比，
     * 如果都有，则对比数量，数量不一致，就多退少补。
     * 如果 holdingRecordList中有 db中没有 就生成预扣记录，并进行预扣库存
     * 如果 holdingRecordList中没有 db中有 就删除db中的预扣记录，并进行补偿库存。
     * @param rlOrderCode 订单id/code 等
     * @param allHoldingRecordList 当前订单的所有预扣记录 [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
     */
    public int refreshHoldingRecordOfRlOrderCode(FaiSession session, int flow, int aid, int unionPriId, String rlOrderCode, FaiList<Param> allHoldingRecordList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || Str.isEmpty(rlOrderCode) || allHoldingRecordList == null){
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "arg err;flow=%d;aid=%d;unionPriId=%s;rlOrderCode=%s;allHoldingRecordList=%s;", flow, aid, unionPriId, rlOrderCode, allHoldingRecordList);
                return rt;
            }
            Map<RecordKey, Integer> recordCountMap = new HashMap<>();
            for (Param info : allHoldingRecordList) {
                long skuId = info.getLong(SkuCountChangeEntity.Info.SKU_ID);
                int itemId = info.getInt(SkuCountChangeEntity.Info.ITEM_ID, 0);
                int count = info.getInt(SkuCountChangeEntity.Info.COUNT);
                recordCountMap.put(new RecordKey(skuId, itemId), count);
            }
            FaiList<Long> updateSkuIdList = new FaiList<>();
            int reduceMode = StoreSalesSkuValObj.ReduceMode.HOLDING;
            boolean holdingMode = reduceMode == StoreSalesSkuValObj.ReduceMode.HOLDING;
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, transactionCtrl);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(flow, aid, transactionCtrl);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, transactionCtrl);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, transactionCtrl);
                SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, transactionCtrl);

                LockUtil.lock(aid);
                try {
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = holdingRecordProc.getListFromDao(aid, unionPriId, null, rlOrderCode, listRef);
                    if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                        return rt;
                    }
                    Set<RecordKey> delRecord = new HashSet<>();
                    Map<RecordKey, Integer> updateRecordMap = new HashMap<>();
                    TreeMap<RecordKey, Integer> addRecordMap = new TreeMap<>();
                    Map<Long, Integer> skuCountMap = new HashMap();
                    rt = splitRecordCountToAddOrDelOrSet(recordCountMap, listRef.value, delRecord, updateRecordMap, addRecordMap, skuCountMap);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    TreeMap<Long, Integer> reduceMap = new TreeMap<>();
                    TreeMap<Long, Integer> makeupMap = new TreeMap<>();
                    skuCountMap.forEach((skuId, count)->{
                        updateSkuIdList.add(skuId);
                        if(count > 0){
                            reduceMap.put(skuId, count);
                        }else if(count < 0){
                            makeupMap.put(skuId, -count);
                        }
                    });
                    try {
                        transactionCtrl.setAutoCommit(false);
                        if(!addRecordMap.isEmpty()){
                            rt = holdingRecordProc.batchAdd(aid, unionPriId, addRecordMap, rlOrderCode, 0);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        if(!delRecord.isEmpty()){
                            rt = holdingRecordProc.batchDel(aid, unionPriId, delRecord, rlOrderCode);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        if(!updateRecordMap.isEmpty()){
                            rt = holdingRecordProc.batchSet(aid, unionPriId, rlOrderCode, updateRecordMap);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }

                        if(!reduceMap.isEmpty()){
                            rt = storeSalesSkuProc.batchReduceStore(aid, unionPriId, reduceMap, holdingMode, false);
                            if(rt != Errno.OK){
                                return rt;
                            }
                        }
                        if(!makeupMap.isEmpty()){
                            rt = storeSalesSkuProc.batchMakeUpStore(aid, unionPriId, makeupMap, holdingMode);
                            if(rt != Errno.OK){
                                return rt;
                            }
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
                    LockUtil.unlock(aid);
                }
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, updateSkuIdList, listRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                FaiList<Integer> pdIdList = Utils.getValList(listRef.value, StoreSalesSkuEntity.Info.PD_ID);
                try {
                    transactionCtrl.setAutoCommit(false);
                    rt = reportSummary(aid, pdIdList, ReportValObj.Flag.REPORT_COUNT,
                            updateSkuIdList, storeSalesSkuProc, spuBizSummaryProc, spuSummaryProc, skuSummaryProc);
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
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;rlOrderCode=%s;recordCountMap=%s;", flow, aid, unionPriId, rlOrderCode, recordCountMap);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 遍历 oldRecordList 解析出那些记录是要添加、那些是要修改、那些是要删除
     * @param recordCountMap 新数据
     * @param oldRecordList 旧数据
     * @param delRecord 删除的记录
     * @param updateRecordMap 更新的记录
     * @param addRecordMap 添加的记录
     * @param skuCountMap 变化的数量
     */
    private int splitRecordCountToAddOrDelOrSet(Map<RecordKey, Integer> recordCountMap, FaiList<Param> oldRecordList, Set<RecordKey> delRecord, Map<RecordKey, Integer> updateRecordMap, Map<RecordKey, Integer> addRecordMap, Map<Long, Integer> skuCountMap) {
        int rt = Errno.NOT_FOUND;
        for (Param info : oldRecordList) {
            long skuId = info.getLong(HoldingRecordEntity.Info.SKU_ID);
            int itemId = info.getInt(HoldingRecordEntity.Info.ITEM_ID);
            int count = info.getInt(HoldingRecordEntity.Info.COUNT);
            boolean alreadyDel = info.getBoolean(HoldingRecordEntity.Info.ALREADY_DEL);
            RecordKey recordKey = new RecordKey(skuId, itemId);
            Integer newCount = recordCountMap.remove(recordKey);
            int changeCount = -count;
            if(newCount == null){
                if(alreadyDel){
                    continue;
                }
                delRecord.add(recordKey);
            }else{
                if(alreadyDel){
                    Log.logErr(rt,"alreadyDel;info=%s;", info);
                    return rt;
                }
                changeCount += newCount;
            }
            if(changeCount == 0){
                continue;
            }
            int skuCount = skuCountMap.getOrDefault(skuId, 0);
            skuCount += changeCount;
            skuCountMap.put(skuId, skuCount);
        }
        recordCountMap.forEach((recordKey, count)->{
            long skuId = recordKey.skuId;
            int itemId = recordKey.itemId;
            addRecordMap.put(new RecordKey(skuId, itemId), count);
            int skuCount = skuCountMap.getOrDefault(skuId, 0);
            skuCount += count;
            skuCountMap.put(skuId, skuCount);
        });
        return rt = Errno.OK;
    }

    /**
     * 批量退库存，需要生成入库记录
     */
    public int batchRefundStore(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlRefundId, Param inStoreRecordInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlRefundId) || inStoreRecordInfo == null || inStoreRecordInfo.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdCountList=%s;rlRefundId=%s;inStoreRecordInfo=%s;", flow, aid, unionPriId, skuIdCountList, rlRefundId, inStoreRecordInfo);
                return rt;
            }
            String rlOrderCode = inStoreRecordInfo.getString(InOutStoreRecordEntity.Info.RL_ORDER_CODE);
            int ioStoreRecId = inStoreRecordInfo.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, 0);
            if(Str.isEmpty(inStoreRecordInfo.getString(InOutStoreRecordEntity.Info.RL_REFUND_ID))){
                inStoreRecordInfo.setString(InOutStoreRecordEntity.Info.RL_REFUND_ID, rlRefundId);
            }
            /* 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
             *  Pair.first <===> remainCount
             *  Pair.second <===> count
             */
            TreeMap<SkuBizKey, Pair<Integer, Integer>> skuBizChangeCountMap = new TreeMap<>();
            TreeMap<Long, Integer> skuIdCountMap = new TreeMap<>(); // 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
            Set<SkuBizKey> skuBizKeySet = new HashSet<>();
            FaiList<Integer> pdIdList = new FaiList<>();
            FaiList<Long> skuIdList = new FaiList<>(skuIdCountList.size());
            for (Param info : skuIdCountList) {
                long skuId = info.getLong(StoreSalesSkuEntity.Info.SKU_ID);
                int count = info.getInt(StoreSalesSkuEntity.Info.COUNT);
                skuIdList.add(skuId);
                skuIdCountMap.put(skuId, count);
                SkuBizKey skuBizKey = new SkuBizKey(unionPriId, skuId);
                skuBizKeySet.add(skuBizKey);
                Pair<Integer, Integer> pair = new Pair<>(count, 0);
                skuBizChangeCountMap.put(skuBizKey, pair);
            }

            MgProductSpecCli mgProductSpecCli = createMgProductSpecCli(flow);
            FaiList<Param> skuInfoList = new FaiList<>();
            rt = mgProductSpecCli.getPdSkuScInfoListBySkuIdList(aid, tid, skuIdList, skuInfoList);
            if(rt != Errno.OK){
                Log.logErr(rt, "MgProductSpecCli getPdSkuScInfoListBySkuIdList err;flow=%s;aid=%s;skuIdList=%s", flow, aid, skuIdList);
                return rt;
            }
            Map<Long, FaiList<Integer>> skuIdInPdScStrIdListMap = Utils.getMap(skuInfoList, ProductSpecSkuEntity.Info.SKU_ID, ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            Ref<Integer> ioStoreRecordIdRef = new Ref<>();
            // 事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                RefundRecordDaoCtrl refundRecordDaoCtrl = RefundRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(storeSalesSkuDaoCtrl, refundRecordDaoCtrl)){
                    return rt = Errno.ERROR;
                }

                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(flow, aid, transactionCtrl);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                RefundRecordProc refundRecordProc = new RefundRecordProc(refundRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    { // 检查是否已经退库存
                        Ref<FaiList<Param>> listRef = new Ref<>();
                        rt = refundRecordProc.getListFromDao(aid, unionPriId, skuIdList, rlRefundId, listRef);
                        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                            return rt;
                        }
                        if(!listRef.value.isEmpty()){
                            Log.logStd("repeat refund;flow=%s;aid=%s;unionPirId=%s;skuIdList=%s;refundId=%s;", flow, aid, unionPriId, skuIdCountList, rlRefundId);
                            rt = Errno.OK;
                            FaiBuffer sendBuf = new FaiBuffer(true);
                            session.write(sendBuf);
                            return rt;
                        }
                    }
                    Map<Long, Pair<Long, Long>> skuIdPriceMap = new HashMap<>();
                    {
                        // 查询入库成本
                        Ref<FaiList<Param>> listRef = new Ref<>();
                        rt = inOutStoreRecordProc.getListFromDao(aid, unionPriId, skuIdList, ioStoreRecId, rlOrderCode, listRef, InOutStoreRecordEntity.Info.SKU_ID, InOutStoreRecordEntity.Info.PRICE, InOutStoreRecordEntity.Info.MW_PRICE);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        for (Param info : listRef.value) {
                            Long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID);
                            Long price = info.getLong(InOutStoreRecordEntity.Info.PRICE);
                            Long mwPrice = info.getLong(InOutStoreRecordEntity.Info.MW_PRICE);
                            skuIdPriceMap.put(skuId, new Pair<>(price, mwPrice));
                        }
                    }
                    try {
                        transactionCtrl.setAutoCommit(false);

                        // 添加退库存记录
                        rt = refundRecordProc.batchAdd(aid, unionPriId, skuIdCountMap, rlRefundId);
                        if(rt != Errno.OK){
                            return rt;
                        }

                        // 修改库存
                        rt = storeSalesSkuProc.batchChangeStore(aid, skuBizChangeCountMap);

                        // 获取修改库存后的库存销售sku信息
                        Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap = new HashMap<>();
                        rt = storeSalesSkuProc.getInfoMap4OutRecordFromDao(aid, skuBizKeySet, changeCountAfterSkuStoreSalesInfoMap);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                        for (Param skuStoreSalesInfo : changeCountAfterSkuStoreSalesInfoMap.values()) {
                            pdIdList.add(skuStoreSalesInfo.getInt(StoreSalesSkuEntity.Info.PD_ID));
                        }

                        // 添加入库记录
                        inStoreRecordInfo.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.IN);
                        rt = inOutStoreRecordProc.batchAddInStoreRecord(aid, unionPriId, skuIdCountMap, changeCountAfterSkuStoreSalesInfoMap, inStoreRecordInfo, skuIdInPdScStrIdListMap, skuIdPriceMap, ioStoreRecordIdRef);
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
                        storeSalesSkuProc.deleteDirtyCache(aid);
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
            sendBuf.putInt(StoreSalesSkuDto.Key.IN_OUT_STORE_REC_ID, ioStoreRecordIdRef.value);
            session.write(sendBuf);
            Log.logStd("aid=%d;unionPriId=%s;rlRefundId=%s;skuIdCountMap=%s;", aid, unionPriId, rlRefundId, skuIdCountMap);
            return rt;
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
    }

    /**
     * 根据uid和pdId获取库存销售sku信息
     * @param useSourceFieldList 是否使用源商品的字段数据进行覆盖关联的字段
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
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriId, pdId, listRef);;
                if(rt != Errno.OK){
                    return rt;
                }
                list = listRef.value;
                // 判断是否需要使用 源商品数据进行覆盖关联的数据
                if(useSourceFieldList != null && !useSourceFieldList.isEmpty()){
                    Set<String> useSourceFieldSet = new HashSet<>(useSourceFieldList);
                    useSourceFieldSet.retainAll(Arrays.asList(StoreSalesSkuEntity.getValidKeys()));
                    if(list.size() > 0 && useSourceFieldSet.size() > 0){
                        useSourceFieldSet.add(StoreSalesSkuEntity.Info.SKU_ID);
                        // 获取第一个关联的数据，进而获取源业务id
                        Param first = list.get(0);
                        int tmpUnionPriId = first.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                        // 获取到源业务id
                        int sourceUnionPriId = first.getInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID);
                        // 当当前查询的不是源业务的数据，就查询出源业务的sku数据进行覆盖
                        if(tmpUnionPriId != sourceUnionPriId){
                            FaiList<Long> skuIdList = Utils.getValList(list, StoreSalesSkuEntity.Info.SKU_ID);
                            listRef = new Ref<>();
                            // 获取源业务的库存销售sku信息
                            rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, sourceUnionPriId, skuIdList, listRef, useSourceFieldSet.toArray(new String[]{}));
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
                            // 进行覆盖
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
     * 根据 skuIdList 获取相应的库存销售信息
     */
    public int getSkuStoreSalesBySkuIdList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Long> skuIdList, FaiList<String> useSourceFieldList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || skuIdList == null || skuIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;", flow, aid, unionPriId, skuIdList);
                return rt;
            }
            FaiList<Param> list;

            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, unionPriId, skuIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
                list = listRef.value;
                // 判断是否需要使用 源商品数据进行覆盖关联的数据
                if(useSourceFieldList != null && !useSourceFieldList.isEmpty()){
                    Set<String> useSourceFieldSet = new HashSet<>(useSourceFieldList);
                    useSourceFieldSet.retainAll(Arrays.asList(StoreSalesSkuEntity.getValidKeys()));
                    if(list.size() > 0 && useSourceFieldSet.size() > 0){
                        useSourceFieldSet.add(StoreSalesSkuEntity.Info.SKU_ID);
                        // 获取第一个关联的数据，进而获取源业务id
                        Param first = list.get(0);
                        int tmpUnionPriId = first.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                        // 获取到源业务id
                        int sourceUnionPriId = first.getInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID);
                        // 当当前查询的不是源业务的数据，就查询出源业务的sku数据进行覆盖
                        if(tmpUnionPriId != sourceUnionPriId){
                            listRef = new Ref<>();
                            // 获取源业务的库存销售sku信息
                            rt = storeSalesSkuProc.getListFromDaoBySkuIdList(aid, sourceUnionPriId, skuIdList, listRef, useSourceFieldSet.toArray(new String[]{}));
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
                            // 进行覆盖
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
            Log.logDbg("ok;aid=%d;unionPriId=%s;skuIdList=%s;", aid, unionPriId, skuIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据 skuId 和 uidList 获取相应的库存销售信息
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
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriIdList, new FaiList<>(Arrays.asList(skuId)), listRef);
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
                Log.logErr("arg err;flow=%d;aid=%d;pdId=%s;", flow, aid, pdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getListFromDaoByPdIdListAndUidList(aid, new FaiList<>(Arrays.asList(pdId)), null, listRef);
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
    /**
     * 批量获取数据
     * @param uidList 联合主键id
     * @param pdIdList 商品id
     */
    public int batchGetSkuStoreSalesByUidAndPdId(FaiSession session, int flow, int aid, FaiList<Integer> uidList, FaiList<Integer> pdIdList)throws IOException{
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(uidList) || Util.isEmptyList(pdIdList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;uidList=%s;pdIdList=%s;", flow, aid, uidList, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getListFromDaoByPdIdListAndUidList(aid, pdIdList, uidList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, listRef.value);
            Log.logDbg("ok;aid=%d;uidList=%s;pdIdList=%s;", aid, uidList, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    /**
     * 批量获取数据
     * @param uidList 联合主键id
     * @param skuIdList skuId
     */
    public int batchGetSkuStoreSalesByUidAndSkuId(FaiSession session, int flow, int aid, FaiList<Integer> uidList, FaiList<Long> skuIdList)throws IOException{
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Util.isEmptyList(uidList) || Util.isEmptyList(skuIdList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;uidList=%s;skuIdList=%s;", flow, aid, uidList, skuIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
            try {
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                rt = storeSalesSkuProc.getListFromDao(aid, uidList, skuIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                storeSalesSkuDaoCtrl.closeDao();
            }
            sendPdScSkuSalesStore(session, listRef.value);
            Log.logDbg("ok;aid=%d;uidList=%s;skuIdList=%s;", aid, uidList, skuIdList);
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
     * 获取预扣情况
     */
    public int getHoldingRecordList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Long> skuIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || skuIdList == null || skuIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuIdList=%s;", flow, aid, unionPriId, skuIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstance(flow, aid);
            try {
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                rt = holdingRecordProc.getNotDelListFromDao(aid, unionPriId, skuIdList, null, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                holdingRecordDaoCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            listRef.value.toBuffer(sendBuf, HoldingRecordDto.Key.INFO_LIST, HoldingRecordDto.getInfoDto());
            session.write(sendBuf);
            Log.logDbg("ok;aid=%d;unionPriId=%s;skuIdList=%s;", aid, unionPriId, skuIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * MQ 异步上报
     */
    private void asynchronousReport(int flow, int aid, int unionPriId, FaiList<Long> skuIdList, FaiList<Integer> pdIdList) {
        pdIdList = new FaiList<>(new HashSet<>(pdIdList)); //去重
        if (pdIdList.size() > 0) {
            for (Integer pdId : pdIdList) {
                addSpuBizSummaryMq(flow, aid, unionPriId, pdId);
            }
        }
        skuIdList = new FaiList<>(new HashSet<>(skuIdList)); // 去重
        if (skuIdList.size() > 0) {
            for (Long skuId : skuIdList) {
                addSkuSummaryReportMq(flow, aid, skuId);
            }
        }
    }
    private int addSpuBizSummaryMq(int flow, int aid, int unionPriId, int pdId){
        Param sendInfo = new Param();
        sendInfo.setInt(SpuBizSummaryEntity.Info.AID, aid);
        sendInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
        sendInfo.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
        FaiMqMessage message = new FaiMqMessage();
        // 指定topic
        message.setTopic(MqConfig.SpuBizReport.TOPIC);
        // 指定tag
        message.setTag(MqConfig.SpuBizReport.TAG);
        // 添加流水号
        message.setFlow(flow);
        // aid-unionPriId-pdId 做幂等
        message.setKey(aid+"-"+unionPriId+"-"+pdId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.SpuBizReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            Log.logDbg("send=%s", send);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e, MqConfig.SpuBizReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s;sendInfo=%s;", message.getFlow(), message.getMsgId(), sendInfo);
        }
        return Errno.ERROR;
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
        // aid-skuId 做幂等
        message.setKey(aid+"-"+skuId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.SpuBizReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            Log.logDbg("send=%s", send);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e, MqConfig.SpuBizReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s;sendInfo=%s;", message.getFlow(), message.getMsgId(), sendInfo);
        }
        return Errno.ERROR;
    }
}
