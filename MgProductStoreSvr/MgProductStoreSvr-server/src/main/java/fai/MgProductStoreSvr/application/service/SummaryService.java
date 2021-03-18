package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.Utils;
import fai.MgProductStoreSvr.domain.entity.ReportValObj;
import fai.MgProductStoreSvr.domain.entity.SkuSummaryEntity;
import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.domain.repository.*;
import fai.MgProductStoreSvr.domain.serviceProc.SkuSummaryProc;
import fai.MgProductStoreSvr.domain.serviceProc.SpuBizSummaryProc;
import fai.MgProductStoreSvr.domain.serviceProc.SpuSummaryProc;
import fai.MgProductStoreSvr.domain.serviceProc.StoreSalesSkuProc;
import fai.MgProductStoreSvr.interfaces.dto.SkuSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.SpuSummaryDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SummaryService extends StoreService {

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
                SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                SpuSummaryDaoCtrl spuSummaryDaoCtrl = SpuSummaryDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
                if(!transactionCtrl.checkRegistered(spuBizSummaryDaoCtrl, spuSummaryDaoCtrl, storeSalesSkuDaoCtrl)){
                    return rt = Errno.ERROR;
                }
                StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(storeSalesSkuDaoCtrl, flow);
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                SpuSummaryProc spuSummaryProc = new SpuSummaryProc(spuSummaryDaoCtrl, flow);

                int flag = ReportValObj.Flag.REPORT_COUNT;


                Ref<Param> infoRef = new Ref<>();
                rt = storeSalesSkuProc.getReportInfo(aid, pdId, unionPriId, infoRef);
                if(rt != Errno.OK){
                    return rt;
                }
                Param reportInfo = infoRef.value;

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
                        rt = spuSummaryReport(spuBizSummaryProc, spuSummaryProc, aid, pdId, flag);
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
                rt = spuBizSummaryProc.getInfoListByPdIdList(aid, unionPriId, pdIdList, listRef);
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
                            rt = spuBizSummaryProc.getInfoListByPdIdList(aid, sourceUnionPriId, sourcePdIdList, listRef);
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

    /**
     * 根据pdIdList 获取所关联的 spu 业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoListByPdIdList(FaiSession session, int flow, int aid, int tid, FaiList<Integer> pdIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || pdIdList == null || pdIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;pdIdList=%s;", flow, aid, pdIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                rt = spuBizSummaryProc.getInfoListByUnionPriIdListFromDao(aid, null, pdIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            sendSpuBizSummary(session, listRef.value);
            Log.logDbg("ok;aid=%d;pdIdList=%s;", aid, pdIdList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }


    /**
     * 获取 spu 业务库存销售汇总信息 数据状态
     */
    public int getSpuBizSummaryDataStatus(FaiSession session, int flow, int aid, int unionPriId)throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<Integer> totalRef = new Ref<>();
            Long visitorDataLastUpdateTime;
            Long manageDataLastUpdateTime;
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                visitorDataLastUpdateTime = spuBizSummaryProc.getLastUpdateTime(DataType.Visitor, aid, unionPriId);
                manageDataLastUpdateTime = spuBizSummaryProc.getLastUpdateTime(DataType.Manage, aid, unionPriId);
                rt = spuBizSummaryProc.getTotal(aid, unionPriId, totalRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            Param dataStatus = new Param()
                    .setInt(DataStatus.Info.TOTAL_SIZE, totalRef.value)
                    .setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, visitorDataLastUpdateTime)
                    .setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataLastUpdateTime)
                    ;
            FaiBuffer sendBody = new FaiBuffer();
            dataStatus.toBuffer(sendBody, SpuBizSummaryDto.Key.INFO, DataStatus.Dto.getDataStatusDto());
            session.write(sendBody);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 获取全部数据的部分字段
     */
    public int getSpuBizSummaryAllData(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                rt = spuBizSummaryProc.getAllDataFromDao(aid, unionPriId, listRef, SpuBizSummaryEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSpuBizSummary(session, listRef.value);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    /**
     * 搜索全部数据的部分字段
     */
    public int searchSpuBizSummaryFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {

            Ref<FaiList<Param>> listRef = new Ref<>();
            SpuBizSummaryDaoCtrl spuBizSummaryDaoCtrl = SpuBizSummaryDaoCtrl.getInstance(flow, aid);
            try {
                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(spuBizSummaryDaoCtrl, flow);
                rt = spuBizSummaryProc.searchAllDataFromDao(aid, unionPriId, searchArg, listRef, SpuBizSummaryEntity.getManageVisitorKeys());
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }finally {
                spuBizSummaryDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            sendSpuBizSummary(session, listRef.value, searchArg);
            Log.logDbg("ok;aid=%d;unionPriId=%s;", aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
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
                rt = spuSummaryProc.getList(aid, pdIdList, listRef);
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

    private void sendSpuBizSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        sendSpuBizSummary(session, infoList, null);
    }
    private void sendSpuBizSummary(FaiSession session, FaiList<Param> infoList, SearchArg searchArg)throws IOException  {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpuBizSummaryDto.Key.INFO_LIST, SpuBizSummaryDto.getInfoDto());
        if(searchArg != null && searchArg.totalSize != null){
            sendBuf.putInt(SpuBizSummaryDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
    }

    private void sendSpuSummary(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpuSummaryDto.Key.INFO_LIST, SpuSummaryDto.getInfoDto());
        session.write(sendBuf);
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
}
