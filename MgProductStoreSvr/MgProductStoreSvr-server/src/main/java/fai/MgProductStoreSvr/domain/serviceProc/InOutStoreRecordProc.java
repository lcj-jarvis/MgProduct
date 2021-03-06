package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.entity.*;
import fai.MgProductStoreSvr.domain.repository.dao.InOutStoreRecordDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.dao.saga.InOutStoreRecordSagaDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.dao.InOutStoreSumDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.dao.saga.InOutStoreSumSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class InOutStoreRecordProc {

    public InOutStoreRecordProc(int flow, int aid, TransactionCtrl tc) {
        m_daoCtrl = InOutStoreRecordDaoCtrl.getInstance(flow, aid);
        m_sumDaoCtrl = InOutStoreSumDaoCtrl.getInstance(flow, aid);
        m_sagaDaoCtrl = InOutStoreRecordSagaDaoCtrl.getInstanceWithRegistered(flow, aid, tc);
        m_sagaSumDaoCtrl = InOutStoreSumSagaDaoCtrl.getInstanceWithRegistered(flow, aid, tc);
        if(m_daoCtrl == null || m_sumDaoCtrl == null || m_sagaDaoCtrl == null || m_sagaSumDaoCtrl == null){
            throw new RuntimeException(String.format("daoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        if(!init(tc)) {
            throw new RuntimeException(String.format("InOutStoreRecordProc init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public int batchResetCostPrice(int aid, int sysType, int rlPdId, FaiList<Param> infoList, Calendar optTime, Map<SkuBizKey, Param> changeCountAfterSkuStoreInfoMap) {
        int rt;
        if(aid <= 0 || infoList == null || infoList.isEmpty() || optTime == null){
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg error;flow=%d;aid=%s;infoList=%s;optTime=%s;", m_flow, aid, infoList, optTime);
            return rt;
        }
        // ????????????????????????
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.DAO_ERROR;
            Log.logErr(rt, "dao have to set autoCommit false;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>();
        FaiList<Integer> unionPriIds = new FaiList<>();
        for(Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
            long costPrice = info.getLong(InOutStoreRecordEntity.Info.PRICE, 0L);
            if(unionPriId == 0 || skuId == 0 || costPrice < 0){
                rt = Errno.ARGS_ERROR;
                Log.logStd(rt, "arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt;
            }
            unionPriIds.add(unionPriId);
            // ????????????
            Param inData = new Param();
            inData.setLong(InOutStoreRecordEntity.Info.PRICE, costPrice);
            inData.setLong(InOutStoreRecordEntity.Info.MW_PRICE, 0L);
            inData.setCalendar(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, now);
            inData.setInt(InOutStoreRecordEntity.Info.FLAG, InOutStoreRecordValObj.FLag.RESET_PRICE);

            inData.setInt(InOutStoreRecordEntity.Info.AID, aid);
            inData.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            inData.setCalendar(InOutStoreRecordEntity.Info.OPT_TIME, optTime);
            inData.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
            inData.setInt(InOutStoreRecordEntity.Info.SYS_TYPE, sysType);
            inData.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            // ??????????????????match????????? ????????????update???????????????price?????????????????????????????????????????????
            // ?????????price??????0?????????????????????????????????????????????????????????????????????????????????????????????
            inData.setLong(InOutStoreRecordEntity.Info.PRICE+"match", 0L);
            inData.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.IN);
            dataList.add(inData);

            // ????????????
            // clone???set?????????Param???key????????????????????????
            Param outData = inData.clone();
            outData.setLong(InOutStoreRecordEntity.Info.MW_PRICE, costPrice);
            outData.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, InOutStoreRecordValObj.OptType.OUT);
            dataList.add(outData);

            Ref<Integer> countRef = new Ref<>();
            rt = getAvailableCount(aid, unionPriId, sysType, skuId, rlPdId, optTime, countRef);
            if(rt != Errno.OK) {
                return rt;
            }
            // ???????????????????????????
            int availabCount = countRef.value;
            long totalCost = availabCount*costPrice;
            long inMwTotalCost = availabCount*costPrice;
            Param storeSalesSkuInfo = changeCountAfterSkuStoreInfoMap.get(new SkuBizKey(unionPriId, skuId));
            long remainCount = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.REMAIN_COUNT, 0L);
            long holdingCount = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.HOLDING_COUNT, 0L);
            long fifoTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 0L);
            long mwTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 0L);
            storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost + totalCost);
            storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost + inMwTotalCost);

            BigDecimal total = new BigDecimal(mwTotalCost + inMwTotalCost);
            long relRemainCount = remainCount+holdingCount;
            if(relRemainCount != 0) {
                storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_COST, total.divide(new BigDecimal(relRemainCount), BigDecimal.ROUND_HALF_UP).longValue());
            }
        }
        ParamMatcher doBatchMatcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.OPT_TIME, ParamMatcher.LT, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.RL_PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.SYS_TYPE, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.PRICE, ParamMatcher.EQ, "?");
        doBatchMatcher.and(InOutStoreRecordEntity.Info.OPT_TYPE, ParamMatcher.EQ, "?");

        Param item = new Param();
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        item.setString(InOutStoreRecordEntity.Info.PRICE, "?");
        item.setString(InOutStoreRecordEntity.Info.MW_PRICE, "?");
        item.setString(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, "?");
        doBatchUpdater.add(InOutStoreRecordEntity.Info.FLAG, ParamUpdater.LOR, "?");

        rt = m_daoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            Log.logErr("doBatchUpdate err;flow=%s;aid=%s;dataList=%s", m_flow, aid, dataList);
            return rt;
        }

        // ???????????????????????????????????????
        rt = updateTotalCost(unionPriIds, aid);
        if(rt != Errno.OK) {
            return rt;
        }

        Log.logStd("ok;flow=%s;aid=%s;rlPdId=%s;optTime=%s;infoList=%s;", m_flow, aid, rlPdId, Parser.parseString(optTime), infoList);
        return rt;
    }

    private int updateTotalCost(FaiList<Integer> unionPriIds, int aid) {
        int rt;
        String unionPriIdsStr = unionPriIds.toString().replaceFirst("\\[", "(").replaceFirst("\\]", ")");
        // update tablenameHold set totalPrice=price*changeCount, mwTotalPrice=mwPrice*changeCount where aid=9859944 and optType=2 and flag&2=2
        String outSql = "update " + m_daoCtrl.TABLE_HOLD + " set " + InOutStoreRecordEntity.Info.TOTAL_PRICE + "=" + InOutStoreRecordEntity.Info.PRICE + ParamUpdater.MUL + InOutStoreRecordEntity.Info.CHANGE_COUNT
                + ", " + InOutStoreRecordEntity.Info.MW_TOTAL_PRICE + "=" + InOutStoreRecordEntity.Info.MW_PRICE + ParamUpdater.MUL + InOutStoreRecordEntity.Info.CHANGE_COUNT
                + " where " + InOutStoreRecordEntity.Info.AID + "=" + aid
                + " and " + InOutStoreRecordEntity.Info.OPT_TYPE + "=" + InOutStoreRecordValObj.OptType.OUT
                + " and " + InOutStoreRecordEntity.Info.FLAG + "&" + InOutStoreRecordValObj.FLag.RESET_PRICE + "=" + InOutStoreRecordValObj.FLag.RESET_PRICE
                + " and " + InOutStoreRecordEntity.Info.UNION_PRI_ID + " in " + unionPriIdsStr;

        rt = m_daoCtrl.executeUpdate(outSql);
        if(rt != Errno.OK) {
            Log.logErr("update out totalPrice err;flow=%s;aid=%s;sql=%s", m_flow, aid, outSql);
            return rt;
        }

        // update tablenameHold set totalPrice=price*changeCount where aid=9859944 and optType=1 and flag&2=2
        String inSql = "update " + m_daoCtrl.TABLE_HOLD + " set " + InOutStoreRecordEntity.Info.TOTAL_PRICE + " = " + InOutStoreRecordEntity.Info.PRICE + ParamUpdater.MUL + InOutStoreRecordEntity.Info.CHANGE_COUNT
                + " where " + InOutStoreRecordEntity.Info.AID + "=" + aid
                + " and " + InOutStoreRecordEntity.Info.OPT_TYPE + "=" + InOutStoreRecordValObj.OptType.IN
                + " and " + InOutStoreRecordEntity.Info.FLAG + "&" + InOutStoreRecordValObj.FLag.RESET_PRICE + "=" + InOutStoreRecordValObj.FLag.RESET_PRICE
                + " and " + InOutStoreRecordEntity.Info.UNION_PRI_ID + " in " + unionPriIdsStr;
        rt = m_daoCtrl.executeUpdate(inSql);
        if(rt != Errno.OK) {
            Log.logErr("update in totalPrice err;flow=%s;aid=%s;sql=%s", m_flow, aid, inSql);
            return rt;
        }
        return rt;
    }

    private int getAvailableCount(int aid, int unionPriId, int sysType, long skuId, int rlPdId, Calendar optTime, Ref<Integer> countRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.OPT_TIME, ParamMatcher.LT, optTime);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.PRICE, ParamMatcher.EQ, 0);

        Ref<Param> resultRef = new Ref<>();
        String sumField = "sum(" + InOutStoreRecordEntity.Info.AVAILABLE_COUNT + ") as sum";
        int rt = m_daoCtrl.selectFirst(searchArg, resultRef, new String[]{sumField});
        if(rt != Errno.OK) {
            Log.logErr(rt, "select sum availableCount error;flow=%d;match=%s;", m_flow, searchArg.matcher.toJson());
            return rt;
        }
        Param result = resultRef.value;
        if(Str.isEmpty(result)) {
            countRef.value = 0;
            return rt;
        }
        countRef.value = result.getInt("sum");
        return rt;
    }

    //??????????????????????????????????????????????????????
    @Deprecated
    public int synBatchAdd(int aid, int sourceTid, Set<Integer> unionPriIdSet, Set<Long> skuIdSet, FaiList<Param> dataList) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ ,aid);
        searchArg.matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.IN, new FaiList<>(unionPriIdSet));
        searchArg.matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.IN, new FaiList<>(skuIdSet));
        String[] fields = {InOutStoreRecordEntity.Info.UNION_PRI_ID, InOutStoreRecordEntity.Info.SKU_ID, InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID};
        if(FaiValObj.TermId.YK == sourceTid){
            fields = new String[]{InOutStoreRecordEntity.Info.UNION_PRI_ID, InOutStoreRecordEntity.Info.SKU_ID, InOutStoreRecordEntity.Info.KEEP_INT_PROP1};
        }
        Ref<FaiList<Param>> dbPrimaryKeyListRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, dbPrimaryKeyListRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"dao insert err;flow=%s;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Set<TmpKey> primaryKeySet = new HashSet<>(dbPrimaryKeyListRef.value.size()*4/3+1);
        for (Param info : dbPrimaryKeyListRef.value) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID);
            int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, 0);
            if(FaiValObj.TermId.YK == sourceTid){
                ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.KEEP_INT_PROP1);
            }
            primaryKeySet.add(new TmpKey(unionPriId, skuId, ioStoreRecId));
        }
        Log.logDbg("whalelog  flow=%s;aid=%s;primaryKeySet.size=%s;", m_flow, aid, primaryKeySet.size());
        FaiList<Param> addDataList = new FaiList<>();
        for (Param data : dataList) {
            int unionPriId = data.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID);
            long skuId = data.getLong(InOutStoreRecordEntity.Info.SKU_ID);
            int ioStoreRecId = data.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
            if(primaryKeySet.contains(new TmpKey(unionPriId, skuId, ioStoreRecId))){
                continue;
            }
            if(FaiValObj.TermId.YK == sourceTid){
                Integer buildIoStoreRecId = m_daoCtrl.buildId();
                if(buildIoStoreRecId == null){
                    rt = Errno.ERROR;
                    Log.logErr(rt,"buildId err;m_flow=%s;aid=%s;", m_flow, aid);
                    return rt;
                }
                data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, buildIoStoreRecId);
                data.setInt(InOutStoreRecordEntity.Info.KEEP_INT_PROP1, ioStoreRecId);
            }
            addDataList.add(data);
        }
        if(!addDataList.isEmpty()){
            rt = m_daoCtrl.batchInsert(addDataList, null, true);
            if(rt != Errno.OK){
                Log.logErr(rt,"dao insert err;flow=%s;aid=%s;dataList=%s;", m_flow, aid, dataList);
            }
            if(FaiValObj.TermId.YK != sourceTid){
                m_daoCtrl.restoreMaxId();
            }
        }
        Log.logStd(rt,"ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    /**
     * ????????????????????????
     */
    public int batchAddOutStoreRecord(int aid, int unionPriId, Map<Long, Integer> skuIdChangeCountMap, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap, Param info, Map<Long, FaiList<Integer>> skuIdInPdScStrIdListMap, Ref<Integer> idRef) {
        if(aid <= 0 || unionPriId <= 0 || skuIdChangeCountMap == null || info == null || info.isEmpty() || skuIdInPdScStrIdListMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdChangeCountMap=%s;info=%s;skuIdInPdScStrIdListMap=%s;", m_flow, aid, unionPriId, skuIdChangeCountMap, info, skuIdInPdScStrIdListMap);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(skuIdChangeCountMap.size());
        for (Map.Entry<Long, Integer> entry : skuIdChangeCountMap.entrySet()) {
            long skuId = entry.getKey();
            int count = entry.getValue();
            Param data = info.clone();
            FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdListMap.get(skuId);
            if(inPdScStrIdList == null){
                int rt = Errno.NOT_FOUND;
                Log.logErr(rt, "not found;flow=%s;aid=%s;unionPriId=%s;skuId=%s;", m_flow, aid, unionPriId, skuId);
                return rt;
            }
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, count);
            data.setList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
            dataList.add(data);
        }

        return batchAdd(aid, dataList, changeCountAfterSkuStoreSalesInfoMap, idRef);
    }
    /**
     * ????????????????????????
     */
    public int batchAddInStoreRecord(int aid, int unionPriId, Map<Long, Integer> skuIdChangeCountMap, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap, Param info, Map<Long, FaiList<Integer>> skuIdInPdScStrIdListMap, Map<Long, Pair<Long, Long>> skuIdPriceMap, Ref<Integer> idRef) {
        if(aid <= 0 || unionPriId <= 0 || skuIdChangeCountMap == null || info == null || info.isEmpty() || skuIdInPdScStrIdListMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriId=%s;skuIdChangeCountMap=%s;info=%s;skuIdInPdScStrIdListMap=%s;", m_flow, aid, unionPriId, skuIdChangeCountMap, info, skuIdInPdScStrIdListMap);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(skuIdChangeCountMap.size());
        for (Map.Entry<Long, Integer> entry : skuIdChangeCountMap.entrySet()) {
            long skuId = entry.getKey();
            int count = entry.getValue();
            Pair<Long, Long> pricePair = skuIdPriceMap.get(skuId);
            Param data = info.clone();
            FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdListMap.get(skuId);
            if(inPdScStrIdList == null){
                inPdScStrIdList = new FaiList<>();
            }
            long price = 0L;
            long mwPrice = 0L;
            if(pricePair != null){
                price = pricePair.first;
                mwPrice = pricePair.second;
            }
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setLong(InOutStoreRecordEntity.Info.PRICE, price);
            data.setLong(InOutStoreRecordEntity.Info.MW_PRICE, mwPrice);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, count);
            data.setList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
            dataList.add(data);
        }
        Log.logDbg("whalelog skuIdPriceMap=%s;dataList=%s;", skuIdPriceMap, dataList);

        return batchAdd(aid, dataList, changeCountAfterSkuStoreSalesInfoMap, idRef);
    }


    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap) {
        return  batchAdd(aid, infoList, changeCountAfterSkuStoreSalesInfoMap, null, false);
    }
    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap, boolean isSaga) {
        return batchAdd(aid, infoList, changeCountAfterSkuStoreSalesInfoMap, null, isSaga);
    }
    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap,  Ref<Integer> idRef) {
        return batchAdd(aid, infoList, changeCountAfterSkuStoreSalesInfoMap, idRef, false);
    }
    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap, Ref<Integer> idRef, boolean isSaga) {
        if(aid <= 0 || infoList == null || infoList.isEmpty() || changeCountAfterSkuStoreSalesInfoMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;infoList=%s;changeCountAfterSkuStoreSalesInfoMap=%s;", m_flow, aid, infoList, changeCountAfterSkuStoreSalesInfoMap);
            return Errno.ARGS_ERROR;
        }
        // ????????????????????????
        if(m_daoCtrl.isAutoCommit() || m_sumDaoCtrl.isAutoCommit()) {
            Log.logErr("dao and sumDao have to set autoCommit false;flow=%d;aid=%d;", m_flow, aid);
            return Errno.DAO_ERROR;
        }

        int rt;
        Calendar now = Calendar.getInstance();
        String yyMMdd = Parser.parseString(now, "yyMMdd");

        Integer ioStoreRecId = m_daoCtrl.buildId();
        if(ioStoreRecId == null){
            Log.logErr("buildId err aid=%s", aid);
            return Errno.ERROR;
        }
        if(idRef != null){
            idRef.value = ioStoreRecId;
        }
        String number = InOutStoreRecordValObj.Number.genNumber(yyMMdd, ioStoreRecId);
        FaiList<Param> dataList = new FaiList<>();

        HashMap<Integer, Param> summaryMap = new HashMap<>();

        for (Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
            int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
            int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, -1);
            int stauts = info.getInt(InOutStoreRecordEntity.Info.STATUS, 0);
            if(unionPriId == 0 || skuId == 0 || optType == 0 || changeCount < 0){
                Log.logStd("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }
            int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
            int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
            int sysType = info.getInt(InOutStoreRecordEntity.Info.SYS_TYPE, 0);
            int sourceUnionPriId = info.getInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, 0);
            Integer remainCount = info.getInt(InOutStoreRecordEntity.Info.REMAIN_COUNT); // ????????????????????????????????????????????????????????????

            if(remainCount == null){
                Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                // ???????????? + ????????????
                remainCount = storeSalesSkuInfo.getInt(StoreSalesSkuEntity.Info.REMAIN_COUNT) + storeSalesSkuInfo.getInt(StoreSalesSkuEntity.Info.HOLDING_COUNT);
                pdId = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                rlPdId = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
                sysType = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.SYS_TYPE, sysType);
                if(sourceUnionPriId == 0){
                    sourceUnionPriId = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
                }
            }
            if(remainCount < 0){
                Log.logStd("remainCount error;flow=%d;aid=%s;remainCount=%s;info=%s;", m_flow, aid, remainCount, info);
                return rt = Errno.ARGS_ERROR;
            }
            if(pdId == 0 || rlPdId == 0){
                Log.logStd("arg 3 error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }
            long price = info.getLong(InOutStoreRecordEntity.Info.PRICE, 0L);
            long inMwPrice = info.getLong(InOutStoreRecordEntity.Info.MW_PRICE, price);
            if(inMwPrice == 0L){
                inMwPrice = price;
            }

            Param data = new Param();
            data.setInt(InOutStoreRecordEntity.Info.AID, aid);
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setInt(InOutStoreRecordEntity.Info.SYS_TYPE, sysType);
            data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
            data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
            data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);
            data.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);
            // ?????????0
            data.setLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, 0L);
            data.setLong(InOutStoreRecordEntity.Info.MW_TOTAL_PRICE, 0L);
            data.setInt(InOutStoreRecordEntity.Info.STATUS, stauts);

            data.assign(info, InOutStoreRecordEntity.Info.C_TYPE);
            data.assign(info, InOutStoreRecordEntity.Info.S_TYPE);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, changeCount);
            data.setInt(InOutStoreRecordEntity.Info.REMAIN_COUNT, remainCount);

            if(optType == InOutStoreRecordValObj.OptType.IN){ // ??????????????????????????????
                // ???????????????????????????????????????
                data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, changeCount);
                // ?????????????????????????????????????????????
                if(price > 0 && changeCount > 0){
                    long totalCost = changeCount * price;
                    data.setLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, totalCost);
                    long inMwTotalCost = changeCount * inMwPrice;
                    Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                    long fifoTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 0L);
                    long mwTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 0L);
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost + totalCost);
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost + inMwTotalCost);

                    // ???????????????????????????????????????
                    long mwCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_COST, 0L);
                    if(mwCost == 0) {
                        mwCost = new BigDecimal(mwTotalCost + inMwTotalCost).divide(new BigDecimal(remainCount), BigDecimal.ROUND_HALF_UP).longValue();
                    }else {
                        // (remainCount*mwCost+inMwTotalCost)/(remainCount)
                        mwCost = new BigDecimal((remainCount-changeCount) * mwCost + inMwTotalCost).divide(new BigDecimal(remainCount), BigDecimal.ROUND_HALF_UP).longValue();
                    }
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_COST, mwCost);
                }
            }
            // ??????????????????
            FaiList<Integer> inPdScStrIdList = info.getList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST);
            if(inPdScStrIdList != null){
                data.setString(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
            }

            Long mvPrice = info.getLong(InOutStoreRecordEntity.Info.MW_PRICE);
            if(mvPrice == null) {
                mvPrice = 0L;
            }
            data.setLong(InOutStoreRecordEntity.Info.PRICE, price);
            data.setLong(InOutStoreRecordEntity.Info.MW_PRICE, mvPrice);
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
            if(!data.containsKey(InOutStoreRecordEntity.Info.OPT_TIME)){
                data.setCalendar(InOutStoreRecordEntity.Info.OPT_TIME, now);
            }
            if(!data.containsKey(InOutStoreRecordEntity.Info.SYS_CREATE_TIME)){
                data.setCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, now);
            }
            if(!data.containsKey(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME)){
                data.setCalendar(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, now);
            }

            if(optType == InOutStoreRecordValObj.OptType.OUT && changeCount > 0){ // ????????????????????? ?????????????????????????????????
                Ref<Long> fifoOutTotalCostRef = new Ref<>(0L);
                rt = reduceStoreInRecord(aid, unionPriId, skuId, changeCount, remainCount, fifoOutTotalCostRef);
                if(rt != Errno.OK){
                    return rt;
                }
                Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                // ???????????????????????????????????????
                {
                    // ???????????????
                    BigDecimal outTotal = new BigDecimal(fifoOutTotalCostRef.value);
                    BigDecimal result = outTotal.divide(new BigDecimal(changeCount), BigDecimal.ROUND_HALF_UP);
                    long fifoPrice = result.longValue();
                    data.setLong(InOutStoreRecordEntity.Info.PRICE, fifoPrice);
                    data.setLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, fifoOutTotalCostRef.value);
                    // ?????????????????????
                    Long fifoTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 0L);
                    fifoTotalCost -= fifoOutTotalCostRef.value;
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost);
                }
                // ???????????????????????????????????????
                {
                    // ???????????????
                    long mwTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 0L);

                    // ?????????????????????????????????
                    long mwCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_COST, 0L);
                    if(mwCost == 0) {
                        BigDecimal total = new BigDecimal(mwTotalCost);
                        BigDecimal result = total.divide(new BigDecimal(remainCount + changeCount), BigDecimal.ROUND_HALF_UP);// ????????????
                        mwCost = result.longValue();
                        storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_COST, mwCost);
                    }

                    data.setLong(InOutStoreRecordEntity.Info.MW_PRICE, mwCost);
                    data.setLong(InOutStoreRecordEntity.Info.MW_TOTAL_PRICE, mwCost*changeCount);
                    // ????????????????????????
                    mwTotalCost -= mwCost*changeCount;
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost);
                }
            }
            dataList.add(data);

            // ???unionPriId????????????
            Param sumInfo = summaryMap.get(unionPriId);
            if(sumInfo == null) {
                sumInfo = new Param();
                summaryMap.put(unionPriId, sumInfo);
                sumInfo.setInt(InOutStoreSumEntity.Info.AID, aid);
                sumInfo.setInt(InOutStoreSumEntity.Info.UNION_PRI_ID, unionPriId);
                sumInfo.setString(InOutStoreSumEntity.Info.NUMBER, number);
                sumInfo.setInt(InOutStoreSumEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
                sumInfo.assign(data, InOutStoreSumEntity.Info.OPT_TYPE);
                sumInfo.assign(data, InOutStoreSumEntity.Info.S_TYPE);
                sumInfo.assign(data, InOutStoreSumEntity.Info.C_TYPE);
                sumInfo.assign(data, InOutStoreSumEntity.Info.REMARK);
                sumInfo.assign(data, InOutStoreSumEntity.Info.OPT_SID);
                sumInfo.assign(data, InOutStoreSumEntity.Info.OPT_TIME);
                sumInfo.setCalendar(InOutStoreSumEntity.Info.SYS_CREATE_TIME, now);
                sumInfo.setCalendar(InOutStoreSumEntity.Info.SYS_UPDATE_TIME, now);
            }
            // ????????????????????? * ????????????
            long totalFifoPrice = data.getLong(InOutStoreRecordEntity.Info.TOTAL_PRICE, 0L);
            long sumFifoPrice = sumInfo.getLong(InOutStoreSumEntity.Info.PRICE, 0L);
            sumInfo.setLong(InOutStoreSumEntity.Info.PRICE, totalFifoPrice + sumFifoPrice);

            long totalMwPrice = data.getLong(InOutStoreRecordEntity.Info.MW_TOTAL_PRICE, 0L);
            long sumMwPrice = sumInfo.getLong(InOutStoreSumEntity.Info.MW_PRICE, 0L);
            sumInfo.setLong(InOutStoreSumEntity.Info.MW_PRICE, totalMwPrice + sumMwPrice);
        }

        rt = m_daoCtrl.batchInsert(dataList, null, false); // ??????????????????null
        if(rt != Errno.OK){
            Log.logErr("dao insert err;flow=%s;aid=%s;dataList=%s", m_flow, aid, dataList);
            return rt;
        }

        // ?????????????????????????????? Saga ??????
        if (isSaga) {
            rt = addInsOp4Saga(aid, dataList, RECORD_TYPE);
            if (rt != Errno.OK) {
                return rt;
            }
        }

        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        if(summaryMap.isEmpty()) {
            return rt;
        }
        FaiList<Param> summaryList = new FaiList<>();
        Set<Integer> keySet = summaryMap.keySet();
        for(Integer key : keySet) {
            summaryList.add(summaryMap.get(key));
        }
        // ??????????????????
        rt = addSummary(aid, summaryList, isSaga);
        return rt;
    }

    /**
     * fifo ??????????????????????????????????????? ?????????fifo????????????????????????
     */
    private int reduceStoreInRecord(int aid, int unionPriId, long skuId, int totalChangeCount, int remainCount, Ref<Long> fifoOutTotalCostRef){
        int rt = Errno.ERROR;
        // ?????????????????????
        int batchSize = 10;

        ParamMatcher commMatcher = new ParamMatcher();
        commMatcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        commMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        commMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);

        boolean needInit = true;
        {   // ???????????? ???/??? ????????? ??????????????????????????????????????????
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = commMatcher.clone();
            Ref<Param> infoRef = new Ref<>();
            rt = m_daoCtrl.selectFirst(searchArg, infoRef);
            if(rt != Errno.OK){
                Log.logErr(rt, "selectFirst err;flow=%s;aid=%s;matcher.json=%s;", m_flow, aid, searchArg.matcher.toJson());
                return rt;
            }
            Param firstRecord = infoRef.value;
            int optType = firstRecord.getInt(InOutStoreRecordEntity.Info.OPT_TYPE);
            if(optType != InOutStoreRecordValObj.OptType.IN){ // ???????????????????????????????????????????????????????????????
                Log.logStd("record maybe err;flow=%s;aid=%s;firstRecord=%s;", m_flow, aid, firstRecord);
                // ?????????????????????????????????
                searchArg.matcher.and(InOutStoreRecordEntity.Info.OPT_TYPE, ParamMatcher.EQ, InOutStoreRecordValObj.OptType.IN);
                rt = m_daoCtrl.selectFirst(searchArg, infoRef);
                if(rt != Errno.OK){
                    Log.logErr(rt, "selectFirst 2 err;flow=%s;aid=%s;matcher.json=%s;", m_flow, aid, searchArg.matcher.toJson());
                    return rt;
                }
                firstRecord = infoRef.value;
            }
            int availableCount = firstRecord.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
            int changeCount = firstRecord.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT);
            if(availableCount != changeCount){ // ??????????????????????????????????????????????????????????????????????????????
                needInit = false;
            }
        }
        /* eg:
         * ??????????????????
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           5                      5               5
         *    11        1           5                      5              10
         *    12        2           0                      8               2
         *    13        1           6                      6               8
         * ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           0                      5               5
         *    11        1           2                      5              10
         *    12        2           0                      8               2
         *    13        1           6                      6               8
         * ?????????????????????
         *  id ???13 ??? availableCount ??? 6  8(?????????????????????remainCount)-6 = 2
         *  id ???11 ??? availableCount ??? 5  5>2 ?????? availableCount???????????????2
         *  id < 11 availableCount -> 0
         * =============================================================================
         * ??????????????????????????????????????????
         * ?????????????????????????????????changeCount = 5
         * ????????????????????????????????????????????????
         *  id ???11 ??? availableCount ??? 2->0 (2)
         *  1d ???13 ??? availableCount ??? 6->3 (3)
         *  2+3 = 5
         * ????????????
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           0                      5               5
         *    11        1           0                      5              10
         *    12        2           0                      8               2
         *    13        1           3                      6               8
         *    14        2           0                      5               3
         * ?????????????????????????????????????????????
         *      ?????? availableCount>0 and optType=1 order by id asc ???????????????????????????
         */
        if(needInit){ // ???????????????????????????????????????????????????????????????  ????????????????????????????????????????????????????????????
            batchSize = 10;

            SearchArg searchArg = new SearchArg();
            ParamMatcher initCommMatcher = commMatcher.clone();
            initCommMatcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0);// ????????????????????????0
            searchArg.matcher = initCommMatcher;

            ParamComparator cmpor = new ParamComparator();
            cmpor.addKey(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, true);//??????id ??????

            searchArg.cmpor = cmpor;
            searchArg.limit = batchSize;

            // ???????????????????????????????????????id
            int lastAvailableIoStoreRecId = 0;
            // ??????????????????????????????????????????????????????
            int lastAvailableCount = 0;
            FaiList<Integer> notFillZeroIdList = new FaiList<>();
            int start = 0;
            OUT:
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
                    Log.logDbg("whalelog   info=%s remainCount=%s;", info, remainCount);
                    int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                    int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                    notFillZeroIdList.add(ioStoreRecId);
                    if(availableCount >= remainCount){
                        lastAvailableCount = remainCount;
                        lastAvailableIoStoreRecId = ioStoreRecId;
                        break OUT;
                    }else{
                        remainCount -= availableCount;
                    }
                }
                if(list.size() != batchSize){
                    Log.logErr("data err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;remainCount=%s", m_flow, aid,unionPriId, skuId, remainCount);
                    return Errno.ERROR;
                }
                start += batchSize;
            }

            ParamMatcher fillZeroMatcher = initCommMatcher.clone();
            if(!notFillZeroIdList.isEmpty()){
                fillZeroMatcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.NOT_IN, notFillZeroIdList);
            }

            ParamMatcher lastAvailableMatcher = initCommMatcher.clone();
            lastAvailableMatcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, lastAvailableIoStoreRecId);

            // ??????fifo?????????????????????
            {
                long totalCost = 0;
                {
                    SearchArg fillZeroSearchArg = new SearchArg();
                    fillZeroSearchArg.matcher = fillZeroMatcher;
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = m_daoCtrl.select(fillZeroSearchArg, listRef, InOutStoreRecordEntity.Info.AVAILABLE_COUNT, InOutStoreRecordEntity.Info.PRICE);
                    if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                        Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;", m_flow, aid,unionPriId, skuId);
                        return rt;
                    }
                    for (Param info : listRef.value) {
                        int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                        long price = info.getLong(InOutStoreRecordEntity.Info.PRICE);
                        totalCost += availableCount*price;
                    }
                }
                Log.logStd("whalelog  0 totalCost=%s", totalCost);

                if(lastAvailableIoStoreRecId > 0){
                    SearchArg lastAvailableSearchArg = new SearchArg();
                    lastAvailableSearchArg.matcher = lastAvailableMatcher;
                    Ref<Param> infoRef = new Ref<>();
                    rt = m_daoCtrl.selectFirst(lastAvailableSearchArg, infoRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "selectFirst err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;lastAvailableIoStoreRecId=%s;", m_flow, aid,unionPriId, skuId, lastAvailableIoStoreRecId);
                        return rt;
                    }
                    Param info = infoRef.value;
                    int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                    long price = info.getLong(InOutStoreRecordEntity.Info.PRICE);
                    totalCost += (availableCount-lastAvailableCount)*price;
                }
                Log.logStd("whalelog  1 totalCost=%s", totalCost);

                fifoOutTotalCostRef.value = totalCost;
            }
            // ?????????????????????????????????
            {
                {
                    // ??????????????????????????????????????????0
                    ParamUpdater fillZeroUpdater = new ParamUpdater();
                    fillZeroUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 0);
                    rt = m_daoCtrl.update(fillZeroUpdater, fillZeroMatcher);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;fillZeroMatcher.json=%s;", m_flow, aid, fillZeroMatcher.toJson());
                        return rt;
                    }
                }
                if(lastAvailableIoStoreRecId > 0){
                    // ?????????????????????????????????
                    ParamUpdater lastAvailableUpdater = new ParamUpdater();
                    lastAvailableUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, lastAvailableCount);
                    rt = m_daoCtrl.update(lastAvailableUpdater, lastAvailableMatcher);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;lastAvailableCount=%s;lastAvailableIoStoreRecId=%s;", m_flow, aid, lastAvailableCount, lastAvailableIoStoreRecId);
                        return rt;
                    }
                }
            }
        }else{ // ??????????????????????????????????????????????????????
            batchSize = totalChangeCount > batchSize ? batchSize:totalChangeCount;
            SearchArg searchArg = new SearchArg();

            ParamMatcher matcher = commMatcher.clone();
            matcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0); // ??????????????????0
            searchArg.matcher = matcher;

            ParamComparator cmpor = new ParamComparator();
            cmpor.addKey(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);// ??????id ??????
            searchArg.cmpor = cmpor;

            searchArg.limit = batchSize;
            int start = 0;
            FaiList<Param> dataList = new FaiList<>();
            long totalCost = 0;
            OUT:
            while (totalChangeCount > 0){
                searchArg.start = start;
                Ref<FaiList<Param>> listRef = new Ref<>();

                rt = m_daoCtrl.select(searchArg, listRef, InOutStoreRecordEntity.Info.AVAILABLE_COUNT, InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, InOutStoreRecordEntity.Info.PRICE);
                if(rt != Errno.OK){
                    Log.logErr("dao select err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;totalChangeCount=%s", m_flow, aid,unionPriId, skuId, totalChangeCount);
                    return rt;
                }
                FaiList<Param> list = listRef.value;
                for (Param info : list) {
                    Log.logStd("whalelog totalChangeCount=%s; info=%s", totalChangeCount, info);
                    int availableCount = info.getInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT);
                    int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                    long price = info.getLong(InOutStoreRecordEntity.Info.PRICE);
                    Param data = new Param();
                    dataList.add(data);
                    data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 0);
                    { // for batch matcher
                        data.setInt(InOutStoreRecordEntity.Info.AID, aid);
                        data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
                        data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
                        data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
                    }
                    if(availableCount >= totalChangeCount){
                        data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, availableCount-totalChangeCount);
                        totalCost += price * totalChangeCount;
                        break OUT;
                    }else{
                        totalChangeCount -= availableCount;
                    }
                    totalCost += price * availableCount;
                }
                if(list.size() != batchSize){
                    Log.logErr("data err;flow=%s;aid=%s;unionPriId=%s;skuId=%s;totalChangeCount=%s", m_flow, aid,unionPriId, skuId, totalChangeCount);
                    return Errno.ERROR;
                }
                start += batchSize;
            }
            Log.logStd("whalelog  dataList=%s", dataList);
            fifoOutTotalCostRef.value = totalCost;
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
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    /**
     * ???????????? ?????????
     * @param aid aid
     * @param pdIdList pdIdList
     * @param isSaga ???????????????????????????
     * @return {@link Errno}
     */
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        int rt;
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);

        if (isSaga) {
            // ??????????????? ???????????? Saga ????????????
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            // ?????? ???????????? + pdId
            rt = m_daoCtrl.select(searchArg, listRef, InOutStoreRecordEntity.getMaxUpdateAndPriKeys());
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "select ioStoreRecList error;flow=%d;aid=%d;pdIdList=%s", m_flow, aid, pdIdList);
                return rt;
            }
            // ?????????????????????
            preAddUpdateSaga(aid, listRef.value);
        }
        ParamUpdater updater = new ParamUpdater(new Param().setInt(InOutStoreRecordEntity.Info.STATUS, InOutStoreRecordValObj.Status.DEL));
        rt = m_daoCtrl.update(updater, matcher);

        if(rt != Errno.OK){
            Log.logErr(rt, "soft delete err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int clearData(int aid, int unionPriId) {
        FaiList<Integer> unionPriIds = new FaiList<>();
        unionPriIds.add(unionPriId);
        return clearData(aid, unionPriIds);
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        // ?????????idBuilder
        m_daoCtrl.restoreMaxId();
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int searchFromDao(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr("dao.select error;flow=%d;aid=%s;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
            return rt;
        }
        initDbInfoList(listRef.value);
        Log.logStd(rt, "ok;flow=%d;aid=%s;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }

    /**
     *  ?????????db????????????
     */
    private void initDbInfoList(FaiList<Param> dbInfoList){
        if(dbInfoList == null || dbInfoList.isEmpty()){
            return;
        }
        for (Param info : dbInfoList) {
            info.setList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, FaiList.parseIntList(info.getString(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST), new FaiList<>()));
        }
    }

    public int getListFromDao(int aid, int unionPriId, FaiList<Long> skuIdList, int ioStoreRecId, String rlOrderCode, Ref<FaiList<Param>> listRef, String ... fields) {
        ParamMatcher matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        if(ioStoreRecId > 0){
            matcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, ioStoreRecId);
        }else if(!Str.isEmpty(rlOrderCode)){
            matcher.and(InOutStoreRecordEntity.Info.RL_ORDER_CODE, ParamMatcher.EQ, rlOrderCode);
        }else{
            listRef.value = new FaiList<>();
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr("dao.select error;flow=%d;aid=%s;matcher=%s;", m_flow, aid, matcher.toJson());
            return rt;
        }
        Log.logStd(rt, "ok;flow=%d;aid=%s;unionPriId=%s;matcher=%s;", m_flow, aid, unionPriId, searchArg.matcher.toJson());
        return rt;
    }

    public void restoreData(int aid, FaiList<Integer> pdIds, boolean isSaga) {
        int rt;
        if (Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;pdIds is empty;flow=%d;aid=%d;", m_flow, aid);
        }
        if (isSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(InOutStoreRecordEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "dao.get restore data error;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
            }
            preAddUpdateSaga(aid, listRef.value);
        }

        ParamUpdater updater = new ParamUpdater(new Param().setInt(InOutStoreRecordEntity.Info.STATUS, InOutStoreRecordValObj.Status.DEFAULT));
        ParamMatcher matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        rt = m_daoCtrl.update(updater, matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "dao.restore data error;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }
    }

    public int clearIdBuilderCache(int aid){
        int rt = m_daoCtrl.clearIdBuilderCache(aid);
        return rt;
    }

    public void restoreMaxId(int aid) {
        m_daoCtrl.restoreMaxId();
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    private static class TmpKey{
        private int unionPriId;
        private long skuId;
        private int ioStoreRecId;

        public TmpKey(int unionPriId, long skuId, int ioStoreRecId) {
            this.unionPriId = unionPriId;
            this.skuId = skuId;
            this.ioStoreRecId = ioStoreRecId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TmpKey tmpKey = (TmpKey) o;
            return unionPriId == tmpKey.unionPriId &&
                    skuId == tmpKey.skuId &&
                    ioStoreRecId == tmpKey.ioStoreRecId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(unionPriId, skuId, ioStoreRecId);
        }
    }

    /*** ???????????? start ***/
    public int addSummary(int aid, FaiList<Param> list, boolean isSaga) {
        int rt;
        if(Utils.isEmptyList(list)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "add inOutStore summary empty;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_sumDaoCtrl.batchInsert(list, null, !isSaga);
        if(rt != Errno.OK) {
            Log.logErr(rt, "add inOutStore summary error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        // ?????????????????????????????? Saga ??????
        if (isSaga) {
            rt = addInsOp4Saga(aid, list, SUM_TYPE);
            if(rt != Errno.OK) {
                Log.logErr(rt, "insert inOutStore summary saga error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        Log.logStd(rt, "add ok;flow=%d;aid=%s;", m_flow, aid);
        return rt;
    }

    public int getSummaryListFromDB(int aid, SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        int rt = m_sumDaoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr("dao.select error;flow=%d;aid=%s;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
            return rt;
        }
        Log.logStd(rt, "ok;flow=%d;aid=%s;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        return rt;
    }

    public int clearSummaryData(int aid, int unionPriId) {
        FaiList<Integer> unionPriIds = new FaiList<>();
        unionPriIds.add(unionPriId);
        return clearSummaryData(aid, unionPriIds);
    }

    public int clearSummaryData(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "clearSummaryData unionPriIds is empty;aid=%d;unionPriIds=%s;", aid, unionPriIds);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(InOutStoreSumEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreSumEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);

        rt = m_sumDaoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }
    /*** ???????????? end ***/

    private boolean init(TransactionCtrl tc) {
        return tc.register(m_daoCtrl, m_sumDaoCtrl);
    }

    /**
     * ??????Saga????????????
     * @param xid ????????????id
     * @param branchId ????????????id
     * @param recListRef ??????????????????
     * @param sumListRef ??????????????????
     * @return {@link Errno}
     */
    public int getSagaList(String xid, Long branchId, Ref<FaiList<Param>> recListRef, Ref<FaiList<Param>> sumListRef) {
        int rt = Errno.ERROR;
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        searchArg.matcher = matcher;
        if (recListRef != null) {
            rt = m_sagaDaoCtrl.select(searchArg, recListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
                return rt;
            }
        }
        if (sumListRef != null) {
            rt = m_sagaSumDaoCtrl.select(searchArg, recListRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "select sagaList error;flow=%d", m_flow);
                return rt;
            }
        }
        return rt;
    }

    // ?????????????????????
    private void preAddUpdateSaga(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            Log.logStd("preAddUpdateSaga list is empty;flow=%d;aid=%d", m_flow, aid);
            return;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        String[] keys = InOutStoreRecordEntity.getMaxUpdateAndPriKeys();
        // ????????????
        for (Param info : list) {
            int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID);
            long skuId = info.getInt(InOutStoreRecordEntity.Info.SKU_ID);
            PrimaryKey primaryKey = new PrimaryKey(aid, ioStoreRecId, unionPriId, skuId);
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            Param sagaOpInfo = new Param();
            for (String key : keys) {
                sagaOpInfo.assign(info, key);
            }
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, sagaOpInfo);
        }
    }

    // ??????????????????
    private int addInsOp4Saga(int aid, FaiList<Param> dataList, int type) {
        if (Utils.isEmptyList(dataList)) {
            Log.logStd("addInsOp4Saga list is empty");
            return Errno.OK;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        FaiList<Param> sagaList = new FaiList<>();
        // mgInOutStoreRecord_0xxx ???????????? aid + unionPriId + skuId + ioStoreRecId
        // mgInOutStoreSum_0xxx ???????????? aid + unionPriId + ioStoreRecId
        if (type == RECORD_TYPE) {
            dataList.forEach(data -> {
                Param saga = new Param();
                saga.assign(data, InOutStoreRecordEntity.Info.AID);
                saga.assign(data, InOutStoreRecordEntity.Info.UNION_PRI_ID);
                saga.assign(data, InOutStoreRecordEntity.Info.SKU_ID);
                saga.assign(data, InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
                saga.setString(SagaEntity.Common.XID, xid);
                saga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                saga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                saga.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaList.add(saga);
            });
        } else {
            dataList.forEach(data -> {
                Param saga = new Param();
                saga.assign(data, InOutStoreSumEntity.Info.AID);
                saga.assign(data, InOutStoreSumEntity.Info.UNION_PRI_ID);
                saga.assign(data, InOutStoreSumEntity.Info.IN_OUT_STORE_REC_ID);
                saga.setString(SagaEntity.Common.XID, xid);
                saga.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                saga.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                saga.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaList.add(saga);
            });
        }
        // ???????????????????????????????????? daoCtrl
        int rt;
        if (type == RECORD_TYPE) {
            rt = m_sagaDaoCtrl.batchInsert(sagaList, null, false);
        } else {
            rt = m_sagaSumDaoCtrl.batchInsert(sagaList, null, false);
        }
        if(rt != Errno.OK){
            Log.logErr("addInsOp4Saga err;flow=%s;aid=%s;sagaList=%s;type=%d", m_flow, aid, sagaList, type);
            return rt;
        }
        Log.logStd("addInsOp4Saga ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    // ?????? Saga ?????? ????????????????????????????????????????????????????????????????????? Saga ??????????????????
    public int addUpdateSaga2Db(int aid) {
        int rt;
        if (sagaMap.isEmpty()) {
            return Errno.OK;
        }
        rt = m_sagaDaoCtrl.batchInsert(new FaiList<>(sagaMap.values()), null, false);
        if (rt != Errno.OK) {
            Log.logErr("insert sagaMap error;flow=%d;aid=%d;sagaList=%s", m_flow, aid, sagaMap.values().toString());
            return rt;
        }
        Log.logStd("addUpdateSaga2Db ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    /**
     * InOutStoreRecordProc ??????
     * @param aid aid
     * @param xid ????????????id
     * @param branchId ????????????id
     */
    public void rollback4Saga(int aid, String xid, Long branchId) {
        Ref<FaiList<Param>> recListRef = new Ref<>();
        Ref<FaiList<Param>> sumListRef = new Ref<>();
        // ?????? Record ??? Saga ????????????
        int rt = getSagaList(xid, branchId, recListRef, sumListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get sagaOpList err;flow=%d;aid=%;xid=%s;branchId=%s", m_flow, aid, xid, branchId);
        }

        if (recListRef.value != null && !recListRef.value.isEmpty()) {
            // ???????????????
            Map<Integer, List<Param>> groupBySagaOp = recListRef.value.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

            // ???????????? ???????????????????????????
            // rollback4Del(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));

            // ?????? Record ????????????
            rollbackRec4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

            // ????????????
            rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE));
        }

        if (sumListRef.value != null && !sumListRef.value.isEmpty()) {
            // ?????? Sum ???????????? ????????? Saga ???????????????????????????????????????????????????????????????????????????????????????
            rollbackSum4Add(aid, sumListRef.value);
        }
    }

    // ????????????
    private void rollbackSum4Add(int aid, FaiList<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        // ?????? unionPriId ??????????????????????????? db ?????????
        Map<Integer, List<Param>> groupByUidList = list.stream().collect(Collectors.groupingBy(x -> x.getInt(InOutStoreSumEntity.Info.UNION_PRI_ID)));
        for (Map.Entry<Integer, List<Param>> entry : groupByUidList.entrySet()) {
            int unionPriId = entry.getKey();
            List<Param> paramList = entry.getValue();
            FaiList<Integer> ioStoreIdList = Utils.getValList(new FaiList<>(paramList), InOutStoreSumEntity.Info.IN_OUT_STORE_REC_ID);
            ParamMatcher matcher = new ParamMatcher(InOutStoreSumEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(InOutStoreSumEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(InOutStoreSumEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.IN, ioStoreIdList);
            int rt = m_sumDaoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                throw new MgException(rt, "delete err;flow=%d;aid=%d;unionPriId=%d;ioStoreIdList=%s", m_flow, aid, unionPriId, ioStoreIdList);
            }
        }
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    // ????????????
    private void rollback4Update(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        String[] updateKeys = InOutStoreRecordEntity.getMaxUpdateAndPriKeys();
        FaiList<String> keys = new FaiList<>(Arrays.asList(updateKeys));
        // ????????????
        keys.remove(InOutStoreRecordEntity.Info.AID);
        keys.remove(InOutStoreRecordEntity.Info.UNION_PRI_ID);
        keys.remove(InOutStoreRecordEntity.Info.SKU_ID);
        keys.remove(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
        FaiList<Param> dataList = new FaiList<>(list.size());
        for (Param info : list) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID);
            int ioStoreRecId = info.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID);
            Param data = new Param();
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(InOutStoreRecordEntity.Info.AID, aid);
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            dataList.add(data);
        }
        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, "?");
        matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch update err;flow=%d;aid=%d;dataList=%d", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    // ????????????
    /*private void rollback4Del(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        // ?????? Saga ??????
        FaiList<Param> infoList = Util.removeSpecificColumn(new FaiList<>(list), SagaEntity.Common.XID, SagaEntity.Common.BRANCH_ID, SagaEntity.Common.SAGA_OP, SagaEntity.Common.SAGA_TIME);
        int rt = m_daoCtrl.batchInsert(infoList, null, false);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch insert err;flow=%d;aid=%d;infoList=%s", m_flow, aid, infoList);
        }
        Log.logStd("rollback del ok;flow=%d;aid=%d", m_flow, aid);
    }*/

    // ???????????? ???????????? ??????????????????????????????
    private void rollbackRec4Add(int aid, List<Param> list) {
        if (Utils.isEmptyList(list)) {
            return;
        }
        // ?????? unionPriId ??????????????????????????? db ?????????
        Map<Integer, List<Param>> groupByUidList = list.stream().collect(Collectors.groupingBy(x -> x.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID)));
        for (Map.Entry<Integer, List<Param>> unionEntry : groupByUidList.entrySet()) {
            Integer unionPriId = unionEntry.getKey();
            List<Param> listByUid = unionEntry.getValue();
            // ????????? ioStoreRecId ???????????????
            Map<Integer, List<Param>> groupByIoStoreIdList = listByUid.stream().collect(Collectors.groupingBy(x -> x.getInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID)));
            for (Map.Entry<Integer, List<Param>> ioStoreIdEntry : groupByIoStoreIdList.entrySet()) {
                Integer ioStoreRecId = ioStoreIdEntry.getKey();
                List<Param> listByIoStoreId = ioStoreIdEntry.getValue();
                FaiList<Long> skuIdList = Utils.getValList(new FaiList<>(listByIoStoreId), InOutStoreRecordEntity.Info.SKU_ID);
                ParamMatcher matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
                matcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.EQ, ioStoreRecId);
                matcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
                int rt = m_daoCtrl.delete(matcher);
                if (rt != Errno.OK) {
                    throw new MgException("delete record err;flow=%d;aid=%d;unionPriId=%d;ioStoreRecId=%d;skuIdList=%s", m_flow, aid, unionPriId, ioStoreRecId, skuIdList);
                }
            }
        }
        restoreMaxId(aid);
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    private HashMap<PrimaryKey, Param> sagaMap;

    private static class PrimaryKey {
        int aid;
        int ioStoreRecId;
        int unionPriId;
        long skuId;

        public PrimaryKey(int aid, int ioStoreRecId, int unionPriId, long skuId) {
            this.aid = aid;
            this.ioStoreRecId = ioStoreRecId;
            this.unionPriId = unionPriId;
            this.skuId = skuId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    ioStoreRecId == that.ioStoreRecId &&
                    unionPriId == that.unionPriId &&
                    skuId == that.skuId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, ioStoreRecId, unionPriId, skuId);
        }
    }


    private int m_flow;
    private InOutStoreRecordDaoCtrl m_daoCtrl;
    private InOutStoreSumDaoCtrl m_sumDaoCtrl;
    private InOutStoreRecordSagaDaoCtrl m_sagaDaoCtrl;
    private InOutStoreSumSagaDaoCtrl m_sagaSumDaoCtrl;

    // ??????????????? record ??? ?????? sum ???
    private static final int RECORD_TYPE = 1;
    private static final int SUM_TYPE = 2;
}
