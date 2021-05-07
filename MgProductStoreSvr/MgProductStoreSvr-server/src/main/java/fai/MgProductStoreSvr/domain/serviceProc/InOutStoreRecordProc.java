package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordValObj;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.domain.repository.InOutStoreRecordDaoCtrl;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.math.BigDecimal;
import java.util.*;

public class InOutStoreRecordProc {

    public InOutStoreRecordProc(InOutStoreRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public InOutStoreRecordProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = InOutStoreRecordDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_daoCtrl == null){
            throw new RuntimeException(String.format("InOutStoreRecordDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
    }

    //同步批量增加
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
     * 批量添加出库记录
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
     * 批量添加入库记录
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
        return  batchAdd(aid, infoList, changeCountAfterSkuStoreSalesInfoMap, null);
    }

    public int batchAdd(int aid, FaiList<Param> infoList, Map<SkuBizKey, Param> changeCountAfterSkuStoreSalesInfoMap, Ref<Integer> idRef) {
        if(aid <= 0 || infoList == null || infoList.isEmpty() || changeCountAfterSkuStoreSalesInfoMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;infoList=%s;changeCountAfterSkuStoreSalesInfoMap=%s;", m_flow, aid, infoList, changeCountAfterSkuStoreSalesInfoMap);
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
        if(idRef != null){
            idRef.value = ioStoreRecId;
        }
        String number = InOutStoreRecordValObj.Number.genNumber(yyMMdd, ioStoreRecId);
        FaiList<Param> dataList = new FaiList<>();
        for (Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, 0);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID, 0L);
            int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE, 0);
            int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, -1);
            if(unionPriId == 0 || skuId == 0 || optType == 0 || changeCount < 0){
                Log.logStd("arg error;flow=%d;aid=%s;info=%s;", m_flow, aid, info);
                return rt = Errno.ARGS_ERROR;
            }
            int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID, 0);
            int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, 0);
            int sourceUnionPriId = info.getInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, 0);
            Integer remainCount = info.getInt(InOutStoreRecordEntity.Info.REMAIN_COUNT); // 初次初始化库存的时候，直接设置了剩余库存

            if(remainCount == null){
                Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                remainCount = storeSalesSkuInfo.getInt(StoreSalesSkuEntity.Info.REMAIN_COUNT)+storeSalesSkuInfo.getInt(StoreSalesSkuEntity.Info.HOLDING_COUNT);
                pdId = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
                rlPdId = storeSalesSkuInfo.getInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
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
            data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
            data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
            data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);
            data.setInt(InOutStoreRecordEntity.Info.SOURCE_UNION_PRI_ID, sourceUnionPriId);

            data.assign(info, InOutStoreRecordEntity.Info.C_TYPE);
            data.assign(info, InOutStoreRecordEntity.Info.S_TYPE);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, changeCount);
            data.setInt(InOutStoreRecordEntity.Info.REMAIN_COUNT, remainCount);
            if(optType == InOutStoreRecordValObj.OptType.IN){ // 入库操作记录可用库存
                // 设置可以库存，用于计算成本
                data.setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, changeCount);
                // 当有成本价时需要计算到总成本中
                if(price > 0 && changeCount > 0){
                    long totalCost = changeCount*price;
                    long inMwTotalCost = changeCount*inMwPrice;
                    Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                    long fifoTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 0L);
                    long mwTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 0L);
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost + totalCost);
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost + inMwTotalCost);
                }
            }
            // 转化为字符串
            FaiList<Integer> inPdScStrIdList = info.getList(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST);
            if(inPdScStrIdList != null){
                data.setString(InOutStoreRecordEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
            }

            data.setLong(InOutStoreRecordEntity.Info.PRICE, price);
            data.assign(info, InOutStoreRecordEntity.Info.MW_PRICE);
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

            if(optType == InOutStoreRecordValObj.OptType.OUT && changeCount > 0){ // 如果是出库操作 需要消耗入库记录的库存
                Ref<Long> fifoOutTotalCostRef = new Ref<>(0L);
                rt = reduceStoreInRecord(aid, unionPriId, skuId, changeCount, remainCount, fifoOutTotalCostRef);
                if(rt != Errno.OK){
                    return rt;
                }
                Param storeSalesSkuInfo = changeCountAfterSkuStoreSalesInfoMap.get(new SkuBizKey(unionPriId, skuId));
                // 计算先进先出方式相关的价格
                {
                    // 计算平均价
                    BigDecimal outTotal = new BigDecimal(fifoOutTotalCostRef.value);
                    BigDecimal result = outTotal.divide(new BigDecimal(changeCount), BigDecimal.ROUND_HALF_UP);
                    long fifoPrice = result.longValue();
                    data.setLong(InOutStoreRecordEntity.Info.PRICE, fifoPrice);
                    // 计算剩余总成本
                    Long fifoTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, 0L);
                    fifoTotalCost -= fifoOutTotalCostRef.value;
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.FIFO_TOTAL_COST, fifoTotalCost);
                }
                // 计算移动加权方式相关的价格
                {
                    // 计算平均价
                    long mwTotalCost = storeSalesSkuInfo.getLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, 0L);
                    BigDecimal total = new BigDecimal(mwTotalCost);
                    BigDecimal result = total.divide(new BigDecimal(remainCount + changeCount), BigDecimal.ROUND_HALF_UP);// 四舍五入
                    long mwPrice = result.longValue();
                    data.setLong(InOutStoreRecordEntity.Info.MW_PRICE, mwPrice);
                    // 计算剩余的总成本
                    mwTotalCost -= mwPrice*changeCount;
                    storeSalesSkuInfo.setLong(StoreSalesSkuEntity.Info.MW_TOTAL_COST, mwTotalCost);
                }
            }
            dataList.add(data);
        }
        rt = m_daoCtrl.batchInsert(dataList, null, false); // 暂时不设置为null
        if(rt != Errno.OK){
            Log.logErr("dao insert err;flow=%s;aid=%s;dataList=%s", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    /**
     * fifo 方式扣减入库记录的有效库存 并计算fifo方式的出库总成本
     */
    private int reduceStoreInRecord(int aid, int unionPriId, long skuId, int totalChangeCount, int remainCount, Ref<Long> fifoOutTotalCostRef){
        int rt = Errno.ERROR;
        // 批量查询的大小
        int batchSize = 10;

        ParamMatcher commMatcher = new ParamMatcher();
        commMatcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        commMatcher.and(InOutStoreRecordEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        commMatcher.and(InOutStoreRecordEntity.Info.SKU_ID, ParamMatcher.EQ, skuId);

        boolean needInit = true;
        {   // 查第一条 出/入 库记录 正常逻辑第一条记录必然是入库
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
            if(optType != InOutStoreRecordValObj.OptType.IN){ // 如果第一条记录不是入库操作说明数据有点问题
                Log.logStd("record maybe err;flow=%s;aid=%s;firstRecord=%s;", m_flow, aid, firstRecord);
                // 再查一次第一条入库记录
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
            if(availableCount != changeCount){ // 当入库记录的数量不等于可用数量时，说明已经初始化过了
                needInit = false;
            }
        }
        /* eg:
         * 还未初始化过
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           5                      5               5
         *    11        1           5                      5              10
         *    12        2           0                      8               2
         *    13        1           6                      6               8
         * 像上面这种可用库存累加起来和剩余库存不匹配，这种就需要走初始逻辑。即从后往前计算可用库存
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           0                      5               5
         *    11        1           2                      5              10
         *    12        2           0                      8               2
         *    13        1           6                      6               8
         * 从后往前计算：
         *  id 为13 的 availableCount 为 6  8(最后一条记录的remainCount)-6 = 2
         *  id 为11 的 availableCount 为 5  5>2 所以 availableCount需要设置为2
         *  id < 11 availableCount -> 0
         * =============================================================================
         * 初始化过后，就从前往后计算。
         * 假设这时有个出库操作，changeCount = 5
         * 那么，从前往后计算可用库存就是：
         *  id 为11 的 availableCount 由 2->0 (2)
         *  1d 为13 的 availableCount 由 6->3 (3)
         *  2+3 = 5
         * 最后就是
         *    id    optType     availableCount      changeCount      remainCount
         *    10        1           0                      5               5
         *    11        1           0                      5              10
         *    12        2           0                      8               2
         *    13        1           3                      6               8
         *    14        2           0                      5               3
         * 问题：从前往后怎么找到第一个？
         *      通过 availableCount>0 and optType=1 order by id asc 这样就可以找到了。
         */
        if(needInit){ // 第一次需要初始化，从最新的入库记录开始计算  为了兼容同步过来的历史入库记录的可用库存
            batchSize = 10;

            SearchArg searchArg = new SearchArg();
            ParamMatcher initCommMatcher = commMatcher.clone();
            initCommMatcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0);// 可用库存必须大于0
            searchArg.matcher = initCommMatcher;

            ParamComparator cmpor = new ParamComparator();
            cmpor.addKey(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, true);//根据id 降序

            searchArg.cmpor = cmpor;
            searchArg.limit = batchSize;

            // 最后一条有效库存的入库记录id
            int lastAvailableIoStoreRecId = 0;
            // 最后一条有效库存的入库记录的有效库存
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

            // 计算fifo出库的总成本价
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
            // 修改入库记录的可用库存
            {
                {
                    // 其他入库记录的可用库存设置为0
                    ParamUpdater fillZeroUpdater = new ParamUpdater();
                    fillZeroUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, 0);
                    rt = m_daoCtrl.update(fillZeroUpdater, fillZeroMatcher);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;fillZeroMatcher.json=%s;", m_flow, aid, fillZeroMatcher.toJson());
                        return rt;
                    }
                }
                if(lastAvailableIoStoreRecId > 0){
                    // 修改入库记录的可用库存
                    ParamUpdater lastAvailableUpdater = new ParamUpdater();
                    lastAvailableUpdater.getData().setInt(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, lastAvailableCount);
                    rt = m_daoCtrl.update(lastAvailableUpdater, lastAvailableMatcher);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "m_daoCtrl.update err;flow=%s;aid=%s;lastAvailableCount=%s;lastAvailableIoStoreRecId=%s;", m_flow, aid, lastAvailableCount, lastAvailableIoStoreRecId);
                        return rt;
                    }
                }
            }
        }else{ // 从最后一条有效库存的出库记录开始计算
            batchSize = totalChangeCount > batchSize ? batchSize:totalChangeCount;
            SearchArg searchArg = new SearchArg();

            ParamMatcher matcher = commMatcher.clone();
            matcher.and(InOutStoreRecordEntity.Info.AVAILABLE_COUNT, ParamMatcher.GT, 0); // 有效库存大于0
            searchArg.matcher = matcher;

            ParamComparator cmpor = new ParamComparator();
            cmpor.addKey(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID);// 根据id 升序
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

    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
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
     *  初始化db中的数据
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

    public int clearIdBuilderCache(int aid){
        int rt = m_daoCtrl.clearIdBuilderCache(aid);
        return rt;
    }

    private int m_flow;
    private InOutStoreRecordDaoCtrl m_daoCtrl;
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
}
