package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.SkuStoreKey;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordValObj;
import fai.MgProductStoreSvr.domain.repository.InOutStoreRecordDaoCtrl;
import fai.comm.util.*;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

public class InOutStoreRecordProc {
    public InOutStoreRecordProc(InOutStoreRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    /**
     * 添加一条出库记录
     */
    public int addOutStoreRecord(int aid, int unionPriId, long skuId, Param info) {
        if(aid <= 0 || unionPriId <= 0 || skuId <= 0 || info == null || info.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;skuId=%s;info=%s;", m_flow, aid, unionPriId, skuId, info);
            return Errno.ARGS_ERROR;
        }
        info.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
        info.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
        return batchAdd(aid, new FaiList<>(Arrays.asList(info)));
    }

    public int batchAdd(int aid, FaiList<Param> infoList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Calendar now = Calendar.getInstance();
        String yyMMdd = Parser.parseString(now, "yyMMdd");
        for (Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
            int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
            int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, 0);
            if(unionPriId == 0 || skuId == 0 || optType == 0 || changeCount == 0){
                Log.logStd("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }
            // 查出 最近的一条 出/入 库记录 因为最近的一条记录总是记录这条记录产生后的剩余库存
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher();
            searchArg.matcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            searchArg.matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
            searchArg.cmpor = new ParamComparator(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, true);
            Ref<Param> infoRef = new Ref<>();
            rt = m_daoCtrl.selectFirst(searchArg, infoRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logStd("selectFirst error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt;
            }
            Param recentlyRecord = infoRef.value;
            int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, recentlyRecord.getInt(InOutStoreRecordEntity.Info.PD_ID, 0));
            int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, recentlyRecord.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0));
            if(pdId == 0 || rlPdId == 0){
                Log.logStd("arg 2 error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }

            int oldRemainCount = recentlyRecord.getInt(InOutStoreRecordEntity.Info.REMAIN_COUNT, 0);
            int remainCount = 0;
            try {
                remainCount = InOutStoreRecordValObj.OptType.computeCount(optType, oldRemainCount, changeCount);
            }catch (RuntimeException e){
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, e, "arg err;flow=%d;aid=%d;info=%s;oldRemainCount=%s;", m_flow, aid, info, oldRemainCount);
                return rt;
            }
            Integer ioStoreRecId = m_daoCtrl.buildId();
            if(ioStoreRecId == null){
                Log.logErr("buildId err aid=%s", aid);
                return Errno.ERROR;
            }

            Param data = new Param();
            data.setInt(InOutStoreRecordEntity.Info.AID, aid);
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
            data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
            data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);

            data.assign(info, InOutStoreRecordEntity.Info.C_TYPE);
            data.assign(info, InOutStoreRecordEntity.Info.S_TYPE);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, changeCount);
            data.setInt(InOutStoreRecordEntity.Info.REMAIN_COUNT, remainCount);
            if(optType == InOutStoreRecordValObj.OptType.IN){ // 入库操作记录可用库存
                data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, changeCount);
            }
            data.assign(info, InOutStoreRecordEntity.Info.PRICE);
            data.setString(InOutStoreRecordEntity.Info.NUMBER, yyMMdd+String.format("%04d", ioStoreRecId));
            data.assign(info, InOutStoreRecordEntity.Info.OPT_SID);
            data.assign(info, InOutStoreRecordEntity.Info.HEAD_SID);
            data.assign(info, InOutStoreRecordEntity.Info.OPT_TIME);
            data.assign(info, InOutStoreRecordEntity.Info.FLAG);
            data.assign(info, InOutStoreRecordEntity.Info.REMARK);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_INT_PROP1);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_PROP1);
            data.setCalendar(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, now);
            data.setCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, now);

            if(optType == InOutStoreRecordValObj.OptType.OUT){ // 如果是出库操作 需要消耗入库记录的库存

                rt = reduceStoreInRecord(aid, unionPriId, skuId, changeCount);
                if(rt != Errno.OK){
                    return rt;
                }
            }

            rt = m_daoCtrl.insert(data);
            if(rt != Errno.OK){
                Log.logErr("dao insert err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;data=%s", m_flow, aid,unionPriId, skuId, data);
                return rt;
            }
        }
        Log.logStd("ok!flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    /**
     * 先进先出方式扣减入库记录的有效库存
     */
    private int reduceStoreInRecord(int aid, int unionPriId, long skuId, int changeCount){
        int rt = Errno.ERROR;
        // 批量查询的大小
        int batchSize = changeCount > 10 ? 10 : changeCount;
        SearchArg searchArg = new SearchArg();
        ParamMatcher commMatcher = new ParamMatcher();
        commMatcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        commMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        commMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        commMatcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0); // 可用库存必须大于0

        searchArg.matcher = commMatcher;
        searchArg.limit = batchSize;
        while (changeCount > 0){
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK){
                Log.logErr("dao select err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;changeCount=%s", m_flow, aid,unionPriId, skuId, changeCount);
                return rt;
            }
            FaiList<Param> list = listRef.value;
            FaiList<Param> dataList = new FaiList<>();
            for (Param info : list) {
                int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                Param data = new Param();
                dataList.add(data);
                data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 0);
                { // for batch matcher
                    data.setInt(InOutStoreRecordEntity.Info.AID, aid);
                    data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                    data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                    data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
                }
                if(availableCount >= changeCount){
                    data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, availableCount-changeCount);
                    changeCount = 0;
                    break;
                }else{
                    changeCount -= availableCount;
                }
            }
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData().setString(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, "?");

            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
            batchMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
            batchMatcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, "?");
            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, dataList);
            if(rt != Errno.OK){
                Log.logErr("dao batchUpdate err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;dataList=%s", m_flow, aid,unionPriId, skuId, dataList);
                return rt;
            }
        }
        Log.logStd("ok!flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.PD_ID, ParamMatcher.EQ, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }




    public int clearIdBuilderCache(int aid){
        int rt = m_daoCtrl.clearIdBuilderCache(aid);
        return rt;
    }

    private int m_flow;
    private InOutStoreRecordDaoCtrl m_daoCtrl;



}
