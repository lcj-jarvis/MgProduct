package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.Misc2;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.dto.BizSalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.SalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;

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

            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                LockUtil.lock(aid);
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesSummaryDaoCtrl)){
                    Log.logErr("registered BizSalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(salesSummaryDaoCtrl)){
                    Log.logErr("registered SalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);

                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = storeSalesSkuProc.getListFromDao(aid, unionPriId, pdId, listRef, StoreSalesSkuEntity.Info.SKU_ID);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                FaiList<Param> storeSalesSkuInfoList = listRef.value;
                Map<Long, Param> skuIdPdScSkuInfoMap = Misc2.getMap(pdScSkuInfoList, StoreSalesSkuEntity.Info.SKU_ID);// TODO 使用接口包的key
                pdScSkuInfoList = null; // help gc
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
                            .setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId)
                            .setLong(StoreSalesSkuEntity.Info.SKU_ID, skuIdPdScSkuInfoEntry.getKey())
                    );
                }
                skuIdPdScSkuInfoMap = null; // help gc
                try {
                    storeSalesSkuDaoCtrl.setAutoCommit(false);
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
                        transactionCrtl.rollback();
                        return rt;
                    }
                    transactionCrtl.commit();
                    transactionCrtl.closeDao();
                }
            }finally {
                LockUtil.unlock(aid);
                transactionCrtl.closeDao();
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
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesSummaryDaoCtrl)){
                    Log.logErr("registered BizSalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(salesSummaryDaoCtrl)){
                    Log.logErr("registered SalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);

                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCrtl.setAutoCommit(false);
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
                            transactionCrtl.rollback();
                            return rt;
                        }
                        transactionCrtl.commit();
                        transactionCrtl.closeDao();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCrtl.closeDao();
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
     * 扣减库存
     * @return
     */
    public int reducePdSkuStore(FaiSession session, int flow, int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode, int expireTimeSeconds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuId<= 0 || rlOrderId <= 0 || count <= 0 || reduceMode <= 0 || expireTimeSeconds < 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;reduceMode=%s;expireTimeSeconds=%s;", flow, aid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
                return rt;
            }

            boolean holdingMode = reduceMode == StoreSalesSkuValObj.ReduceMode.HOLDING;
            // 事务
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                SotreOrderRecordDaoCtrl sotreOrderRecordDaoCtrl = SotreOrderRecordDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(sotreOrderRecordDaoCtrl)){
                    Log.logErr("registered SotreOrderRecordDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(holdingRecordDaoCtrl)){
                    Log.logErr("registered HoldingRecordDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesReportDaoCtrl)){
                    Log.logErr("registered BizSalesReportDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                StoreOrderRecordProc storeOrderRecordProc = new StoreOrderRecordProc(sotreOrderRecordDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);

                Ref<Param> infoRef = new Ref<>();
                rt = storeSalesSkuProc.getInfoFromDao(aid, unionPriId, skuId, infoRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                int pdId = infoRef.value.getInt(StoreSalesSkuEntity.Info.PD_ID);
                try {
                    transactionCrtl.setAutoCommit(false);
                    if(holdingMode){ // 生成预扣记录
                        rt = holdingRecordProc.add(aid, unionPriId, skuId, rlOrderId, count, expireTimeSeconds);
                    }else { // 生成库存订单关联记录
                        rt = storeOrderRecordProc.add(aid, unionPriId, skuId, rlOrderId, count);
                    }
                    if(rt != Errno.OK){
                        return rt;
                    }
                    rt = storeSalesSkuProc.reduceStore(aid, unionPriId, skuId, count, holdingMode, false);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCrtl.rollback();
                        return rt;
                    }
                    transactionCrtl.commit();
                }
                // 提交库存上报任务，提交失败暂不处理，不参与事务
                if(bizSalesReportProc.addReportCountTask(aid, unionPriId, pdId) != Errno.OK){
                    transactionCrtl.rollback();
                }else{
                    transactionCrtl.commit();
                }
            }finally {
                transactionCrtl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logDbg("aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;reduceMode=%s;expireTimeSeconds=%s;", aid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 扣减预扣库存
     * @return
     */
    public int reducePdSkuHoldingStore(FaiSession session, int flow, int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count){
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId<= 0 || skuId<= 0 || rlOrderId <= 0 || count <= 0 ) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;", flow, aid, unionPriId, skuId, rlOrderId, count);
                return rt;
            }
            // 事务
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(holdingRecordDaoCtrl)){
                    Log.logErr("registered HoldingRecordDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                try {
                    transactionCrtl.setAutoCommit(false);
                    // 删掉预扣记录
                    rt = holdingRecordProc.logicDel(aid, unionPriId, skuId, rlOrderId);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    // 扣减预扣库存
                    rt = storeSalesSkuProc.reduceStore(aid, unionPriId, skuId, count, true, true);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCrtl.rollback();
                        return rt;
                    }
                    transactionCrtl.commit();
                    transactionCrtl.closeDao();
                }
            }finally {
                transactionCrtl.closeDao();
            }
            Log.logDbg("aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;", aid, unionPriId, skuId, rlOrderId, count);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 补偿库存
     */
    public int makeUpStore(FaiSession session, int flow, int aid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || skuId <= 0 || rlOrderId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s;", flow, aid, unionPriId, skuId, rlOrderId);
                return rt;
            }
            boolean holdingMode = reduceMode == StoreSalesSkuValObj.ReduceMode.HOLDING;

            // 事务
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }

                HoldingRecordDaoCtrl holdingRecordDaoCtrl = HoldingRecordDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(holdingRecordDaoCtrl)){
                    Log.logErr("registered HoldingRecordDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }
                BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesReportDaoCtrl)){
                    Log.logErr("registered BizSalesReportDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;skuId=%s;rlOrderId=%s", flow, aid, unionPriId, skuId, rlOrderId);
                    return rt=Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                HoldingRecordProc holdingRecordProc = new HoldingRecordProc(holdingRecordDaoCtrl, flow);
                BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);

                Ref<Param> infoRef = new Ref<>();

                rt = storeSalesSkuProc.getInfoFromDao(aid, unionPriId, skuId, infoRef, StoreSalesSkuEntity.Info.PD_ID);
                if(rt != Errno.OK){
                    return rt;
                }
                int pdId = infoRef.value.getInt(StoreSalesSkuEntity.Info.PD_ID);

                try {
                    transactionCrtl.setAutoCommit(false);
                    rt = storeSalesSkuProc.makeUpStore(aid, unionPriId, skuId, count, holdingMode);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    if(holdingMode){
                        rt = holdingRecordProc.logicDel(aid, unionPriId, skuId, rlOrderId);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                }finally {
                    if(rt != Errno.OK){
                        transactionCrtl.rollback();
                        return rt;
                    }
                    transactionCrtl.commit();
                }
                // 提交库存上报任务，提交失败暂不处理，不参与事务
                if(bizSalesReportProc.addReportCountTask(aid, unionPriId, pdId) != Errno.OK){
                    transactionCrtl.rollback();
                }else{
                    transactionCrtl.commit();
                }

            }finally {
                transactionCrtl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;skuId=%s;rlOrderId=%s;", flow, aid, unionPriId, skuId, rlOrderId);
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
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0|| infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;infoList=%s;unionPriId=%s;", flow, aid, unionPriId, infoList);
                return rt;
            }
            Map<SkuStoreKey, Integer> skuStoreCountMap = new HashMap<>(infoList.size()*4/3+1);
            Set<Integer> pdIdSet = new HashSet<>();
            for (Param info : infoList) {
                info.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                /*int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
                if (unionPriId <= 0) {
                    Log.logErr("unionPriId err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }*/
                long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
                if (skuId <= 0) {
                    Log.logErr("skuId err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }
                int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
                if (pdId <= 0) {
                    Log.logErr("pdId err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }
                int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
                if (rlPdId <= 0) {
                    Log.logErr("rlPdId err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }
                int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
                if (optType <= 0) {
                    Log.logErr("optType err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }
                int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, 0);
                if (changeCount <= 0) {
                    Log.logErr("changeCount err aid=%s", aid);
                    return Errno.ARGS_ERROR;
                }
                pdIdSet.add(pdId);
                SkuStoreKey skuStoreKey = new SkuStoreKey(unionPriId, skuId);
                Integer count = skuStoreCountMap.get(skuStoreKey);
                if(count == null){
                    count = 0;
                }
                switch (optType){
                    case InOutStoreRecordValObj.OptType.IN:
                    {
                        count += changeCount;
                        break;
                    }
                    case InOutStoreRecordValObj.OptType.OUT:
                    {
                        count -= changeCount;
                        break;
                    }
                    default:
                        Log.logStd("arg err;info=%s", info);
                        return Errno.ARGS_ERROR;
                }
                skuStoreCountMap.put(skuStoreKey, count);
            }

            // 事务
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;", flow, aid);
                    return rt=Errno.ERROR;
                }
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(inOutStoreRecordDaoCtrl)){
                    Log.logErr("registered InOutStoreRecordDaoCtrl err;flow=%d;aid=%d;", flow, aid);
                    return rt=Errno.ERROR;
                }
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesSummaryDaoCtrl)){
                    Log.logErr("registered BizSalesSummaryDaoCtrl err;flow=%d;aid=%d;", flow, aid);
                    return rt=Errno.ERROR;
                }
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(salesSummaryDaoCtrl)){
                    Log.logErr("registered SalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }

                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCrtl.setAutoCommit(false);
                        rt = inOutStoreRecordProc.batchAdd(aid, infoList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = storeSalesSkuProc.batchChangeStore(aid, skuStoreCountMap);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        for (Integer pdId : pdIdSet) {
                            Ref<FaiList<Param>> reportListRef = new Ref<>();
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
                    }catch (Exception e){
                        Log.logDbg(e, "whalelog");
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCrtl.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCrtl.commit();
                        transactionCrtl.closeDao();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCrtl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logDbg("ok!;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 删除出入库存记录，不会修改到库存
     */
    public int delInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Integer> idList) throws IOException{
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0|| idList == null || idList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;infoList=%s;idList=%s;", flow, aid, unionPriId, idList);
                return rt;
            }

            InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstance(flow, aid);
            try {
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    inOutStoreRecordDaoCtrl.setAutoCommit(false);
                    rt = inOutStoreRecordProc.del(aid, idList);
                    if(rt != Errno.OK){
                        inOutStoreRecordDaoCtrl.rollback();
                        return rt;
                    }
                    inOutStoreRecordDaoCtrl.commit();
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                inOutStoreRecordDaoCtrl.closeDao();
            }
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
            TransactionCrtl transactionCrtl = new TransactionCrtl();
            try {
                BizSalesReportDaoCtrl bizSalesReportDaoCtrl = BizSalesReportDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesReportDaoCtrl)){
                    Log.logErr("registered BizSalesReportDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
                    return rt=Errno.ERROR;
                }
                StoreSalesSkuDaoCtrl storeSalesSkuDaoCtrl = StoreSalesSkuDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(storeSalesSkuDaoCtrl)){
                    Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
                    return rt=Errno.ERROR;
                }
                BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(bizSalesSummaryDaoCtrl)){
                    Log.logErr("registered BizSalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
                    return rt=Errno.ERROR;
                }
                SalesSummaryDaoCtrl salesSummaryDaoCtrl = SalesSummaryDaoCtrl.getInstance(flow, aid);
                if(!transactionCrtl.registered(salesSummaryDaoCtrl)){
                    Log.logErr("registered SalesSummaryDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }

                BizSalesReportProc bizSalesReportProc = new BizSalesReportProc(bizSalesReportDaoCtrl, flow);
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                SalesSummaryProc salesSummaryProc = new SalesSummaryProc(salesSummaryDaoCtrl, flow);

                Ref<Param> infoRef = new Ref<>();
                rt = bizSalesReportProc.getInfo(aid, unionPriId, pdId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                try {
                    LockUtil.lock(aid);
                    Param bizSalesReportInfo = infoRef.value;
                    int flag = bizSalesReportInfo.getInt(BizSalesReportEntity.Info.FLAG, 0);
                    infoRef.value = null;
                    rt = storeSalesSkuProc.getReportInfo(aid, pdId, unionPriId, infoRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    Param reportInfo = infoRef.value;
                    Param bizSalesSummaryInfo = new Param();
                    bizSalesSummaryInfo.setInt(BizSalesSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                    bizSalesSummaryInfo.setInt(BizSalesSummaryEntity.Info.PD_ID, pdId);
                    assemblyBizSalesSummaryInfo(bizSalesSummaryInfo, reportInfo, flag);
                    try { // 上报数据 并 删除上报任务
                        transactionCrtl.setAutoCommit(false);
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
                            transactionCrtl.rollback();
                            return rt;
                        }
                        transactionCrtl.commit();
                        transactionCrtl.closeDao();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCrtl.closeDao();
            }
            Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", flow, aid, unionPriId, pdId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }


    /**
     * 获取商品业务销售信息
     */
    public int getBizSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int unionPriId, int pdId, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || pdId <= 0 || rlPdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", flow, aid, unionPriId, pdId, rlPdId);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            BizSalesSummaryDaoCtrl bizSalesSummaryDaoCtrl = BizSalesSummaryDaoCtrl.getInstance(flow, aid);
            try {
                BizSalesSummaryProc bizSalesSummaryProc = new BizSalesSummaryProc(bizSalesSummaryDaoCtrl, flow);
                rt = bizSalesSummaryProc.getInfoListFromDao(aid, pdId, null, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                bizSalesSummaryDaoCtrl.closeDao();
            }
            sendBizSalesSummary(session, listRef.value);
            Log.logDbg("aid=%d;unionPriId=%s;pdId=%s;rlPdId=%s;", aid, unionPriId, pdId, rlPdId);
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

}
