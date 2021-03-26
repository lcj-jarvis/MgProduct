package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.InOutStoreRecordArgCheck;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.PdKey;
import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordValObj;
import fai.MgProductStoreSvr.domain.entity.ReportValObj;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.*;
import fai.MgProductStoreSvr.interfaces.dto.InOutStoreRecordDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;

public class RecordService extends StoreService {
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
                if(optType == InOutStoreRecordValObj.OptType.IN){
                    data.assign(info, InOutStoreRecordEntity.Info.CHANGE_COUNT, InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                }
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
                if(info.containsKey(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST)){
                    data.setString(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, info.getList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST).toJson());
                }
                batchAddDataList.add(data);
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                InOutStoreRecordDaoCtrl inOutStoreRecordDaoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(inOutStoreRecordDaoCtrl)){
                    rt = Errno.ERROR;
                    Log.logErr(rt,"checkRegistered err;flow=%s;aid=%s;", flow, aid);
                    return rt;
                }
                InOutStoreRecordProc inOutStoreRecordProc = new InOutStoreRecordProc(inOutStoreRecordDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = inOutStoreRecordProc.synBatchAdd(aid, sourceTid, unionPriIdSet, skuIdSet, batchAddDataList);
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
            Log.logStd(rt,"ok;flow=%s;aid=%s;", flow, aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 添加出入库记录
     */
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int ownerUnionPriId, FaiList<Param> infoList) throws IOException     {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || ownerUnionPriId <= 0|| infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;infoList=%s;ownerUnionPriId=%s;", flow, aid, ownerUnionPriId, infoList);
                return rt;
            }
            /* 使用 有序map, 避免事务中 批量修改时如果无序 相互锁住 导致死锁
             *  Pair.first <===> remainCount
             *  Pair.second <===> count
             */
            TreeMap<SkuStoreKey, Pair<Integer, Integer>> skuStoreChangeCountMap = new TreeMap<>();
            Set<Integer> pdIdSet = new HashSet<>();
            Map<SkuStoreKey, PdKey> needCheckSkuStoreKeyPdKeyMap = new HashMap<>();
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
                if (!InOutStoreRecordArgCheck.isValidOptType(optType)) {
                    Log.logErr("optType err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, -1);
                if (!InOutStoreRecordArgCheck.isValidChangeCount(changeCount)) {
                    Log.logErr("changeCount err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                if(!InOutStoreRecordArgCheck.isValidRemark(info)){
                    Log.logErr("remark err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                if(!InOutStoreRecordArgCheck.isValidRlOrderCode(info)){
                    Log.logErr("rlOrderCode err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }
                if(!InOutStoreRecordArgCheck.isValidRlRefundId(info)){
                    Log.logErr("rlRefundId err;aid=%s;info=%s;", aid, info);
                    return Errno.ARGS_ERROR;
                }

                int flag = info.getInt(InOutStoreRecordEntity.Info.FLAG, 0);

                pdIdSet.add(pdId);
                skuIdSet.add(skuId);
                SkuStoreKey skuStoreKey = new SkuStoreKey(unionPriId, skuId);
                if(unionPriId != ownerUnionPriId){
                    needCheckSkuStoreKeyPdKeyMap.put(skuStoreKey, new PdKey(unionPriId, pdId, rlPdId));
                }
                info.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId);
                Pair<Integer, Integer> pair = skuStoreChangeCountMap.get(skuStoreKey);
                if(pair == null){
                    pair = new Pair<>(0, 0);
                }
                Integer count = pair.first;
                try {
                    count = InOutStoreRecordValObj.OptType.computeCount(optType, count, changeCount);
                }catch (RuntimeException e){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt, e, "arg err;flow=%d;aid=%d;info=%s;", flow, aid, info);
                    return rt;
                }
                pair.first = count;
                pair.second = count;
                if(Misc.checkBit(flag, InOutStoreRecordValObj.FLag.NOT_CHANGE_COUNT)){
                    pair.second = 0;
                }
                skuStoreChangeCountMap.put(skuStoreKey, pair);
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
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            inOutStoreRecordProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        transactionCtrl.commit();
                        storeSalesSkuProc.deleteDirtyCache(aid);
                    }
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = reportSummary(aid, new FaiList<>(pdIdSet), ReportValObj.Flag.REPORT_COUNT,
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
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;;aid=%d;ownerUnionPriId=%s;", aid, ownerUnionPriId);
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

            if(searchArg.limit > InOutStoreRecordValObj.SearchArg.Limit.MAX){
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
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
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

}
