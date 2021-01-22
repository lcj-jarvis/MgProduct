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
     * 批量添加出库记录
     */
    public int batchAddOutStoreRecord(int aid, int unionPriId, Map<Long, Integer> skuIdChangeCountMap, Map<SkuStoreKey, Integer> changeCountAfterSkuStoreCountMap, Param info) {
        if(aid <= 0 || unionPriId <= 0 || skuIdChangeCountMap == null || info == null || info.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdChangeCountMap=%s;info=%s;", m_flow, aid, unionPriId, skuIdChangeCountMap, info);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(skuIdChangeCountMap.size());
        skuIdChangeCountMap.forEach((skuId, count)->{
            Param data = info.clone();
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, count);
            dataList.add(data);
        });

        return batchAdd(aid, dataList, changeCountAfterSkuStoreCountMap);
    }

    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuStoreKey, Integer> changeCountAfterSkuStoreCountMap) {
        if(aid <= 0 || infoList == null || infoList.isEmpty() || changeCountAfterSkuStoreCountMap == null){
            Log.logStd("arg error;flow=%d;aid=%s;infoList=%s;changeCountAfterSkuStoreCountMap=%s;", m_flow, aid, infoList, changeCountAfterSkuStoreCountMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        Calendar now = Calendar.getInstance();
        String yyMMdd = Parser.parseString(now, "yyMMdd");

        Integer ioStoreRecId = m_daoCtrl.buildId();
        if(ioStoreRecId == null){
            Log.logErr("buildId err aid=%s", aid);
            return Errno.ERROR;
        }
        String number = yyMMdd+String.format("%04d", ioStoreRecId);

        for (Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
            int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
            int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, 0);
            if(unionPriId == 0 || skuId == 0 || optType == 0 || changeCount == 0){
                Log.logStd("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }
            int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
            int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
            Integer remainCount = info.getInt(InOutStoreRecordEntity.Info.REMAIN_COUNT);
            if(remainCount == null){ // 导入数据时不处理
                remainCount = changeCountAfterSkuStoreCountMap.get(new SkuStoreKey(unionPriId, skuId));
            }
            if(pdId == 0 || rlPdId == 0){
                Log.logStd("arg 3 error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
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
            data.setString(InOutStoreRecordEntity.Info.NUMBER, number);
            data.assign(info, InOutStoreRecordEntity.Info.OPT_SID);
            data.assign(info, InOutStoreRecordEntity.Info.HEAD_SID);
            data.assign(info, InOutStoreRecordEntity.Info.OPT_TIME);
            data.assign(info, InOutStoreRecordEntity.Info.FLAG);
            data.assign(info, InOutStoreRecordEntity.Info.REMARK);
            data.assign(info, InOutStoreRecordEntity.Info.RL_ORDER_CODE);
            data.assign(info, InOutStoreRecordEntity.Info.RL_REFUND_ID);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_INT_PROP1);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_PROP1);
            data.assign(info, InOutStoreRecordEntity.Info.SYS_UPDATE_TIME);
            data.assign(info, InOutStoreRecordEntity.Info.SYS_CREATE_TIME);
            if(!data.containsKey(InOutStoreRecordEntity.Info.SYS_CREATE_TIME)){
                data.setCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, now);
            }
            if(!data.containsKey(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME)){
                data.setCalendar(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, now);
            }

            if(optType == InOutStoreRecordValObj.OptType.OUT){ // 如果是出库操作 需要消耗入库记录的库存
                rt = reduceStoreInRecord(aid, unionPriId, skuId, remainCount);
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
    private int reduceStoreInRecord(int aid, int unionPriId, long skuId, int remainCount){
        int rt = Errno.ERROR;
        // 批量查询的大小
        int batchSize = 10;
        SearchArg searchArg = new SearchArg();
        ParamMatcher commMatcher = new ParamMatcher();
        commMatcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        commMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        commMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        commMatcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0); // 可用库存必须大于0

        searchArg.matcher = commMatcher;
        searchArg.cmpor = new ParamComparator(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, true); // 跟进创建时间降序排序
        searchArg.limit = batchSize;
        int setIoStoreRecId = 0;
        int setAvailableCount = 0;
        FaiList<Integer> notFillZeroIdList = new FaiList<>();
        int start = 0;
        while (remainCount > 0){
            searchArg.start = start;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef, InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
            if(rt != Errno.OK){
                Log.logErr("dao select err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;remainCount=%s", m_flow, aid,unionPriId, skuId, remainCount);
                return rt;
            }
            FaiList<Param> list = listRef.value;
            for (Param info : list) {
                int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                notFillZeroIdList.add(ioStoreRecId);
                if(availableCount >= remainCount){
                    setAvailableCount = remainCount;
                    setIoStoreRecId = ioStoreRecId;
                    remainCount = 0;
                    break;
                }else{
                    remainCount -= availableCount;
                }
            }
            start += batchSize;
        }
        ParamUpdater fillZeroUpdater = new ParamUpdater();
        fillZeroUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 0);
        ParamMatcher fillZeroMatcher = commMatcher.clone();
        fillZeroMatcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.NOT_IN, notFillZeroIdList);
        rt = m_daoCtrl.update(fillZeroUpdater, fillZeroMatcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;fillZeroMatcher.json=%s;", m_flow, aid, fillZeroMatcher.toJson());
            return rt;
        }
        ParamUpdater setUpdater = new ParamUpdater();
        setUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, setAvailableCount);
        ParamMatcher setMatcher = commMatcher.clone();
        setMatcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, setIoStoreRecId);
        rt = m_daoCtrl.update(fillZeroUpdater, fillZeroMatcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;setAvailableCount=%s;setIoStoreRecId=%s;", m_flow, aid, setAvailableCount, setIoStoreRecId);
            return rt;
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
