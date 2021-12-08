package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.domain.entity.ProductSpecValObj;
import fai.MgProductSpecSvr.domain.repository.ProductSpecCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductSpecProc {
    public ProductSpecProc(ProductSpecDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
    }
    public ProductSpecProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecDaoCtrl.getInstance(flow, aid);
        m_sagaDaoCtrl = ProductSpecSagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        if (m_sagaDaoCtrl == null) {
            throw new RuntimeException(String.format("ProductSpecDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
    }

    public int batchAdd(int aid, Map<Integer, List<Param>> pdIdSpecListMap, Ref<Boolean> needRefreshSkuRef, boolean isSaga) {
        int rt = Errno.ARGS_ERROR;
        if(aid <= 0){
            Log.logErr(rt,"arg error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        FaiList<Param> dataList = new FaiList<>();
        Calendar now = Calendar.getInstance();
        Integer pdScId = m_daoCtrl.getId();
        if(pdScId == null){
            Log.logErr("pdScId error;flow=%d;aid=%s;tpScId=%s;", m_flow, aid, pdScId);
            return Errno.ERROR;
        }
        for (Map.Entry<Integer, List<Param>> pdIdSpecListEntry : pdIdSpecListMap.entrySet()) {
            Integer pdId = pdIdSpecListEntry.getKey();
            List<Param> infoList = pdIdSpecListEntry.getValue();
            if(pdId == null || pdId <= 0 || Util.isEmptyList(infoList)){
                Log.logErr(rt, "arg error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
                return rt;
            }
            for (Param info : infoList) {
                Param data = new Param();
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                data.assign( info, ProductSpecEntity.Info.SC_STR_ID);
                pdScId++;
                data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
                data.assign( info, ProductSpecEntity.Info.SOURCE_TID);
                data.assign( info, ProductSpecEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign( info, ProductSpecEntity.Info.SORT);
                data.assign( info, ProductSpecEntity.Info.FLAG);
                FaiList<Param> inPdScValList = info.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                for (Param inPdScValInfo : inPdScValList) {
                    // 判断是否勾选，c:true
                    if(inPdScValInfo.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                        int flag = data.getInt(ProductSpecEntity.Info.FLAG, 0) | ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
                        if(needRefreshSkuRef != null){
                            needRefreshSkuRef.value = true;
                        }
                        data.setInt(ProductSpecEntity.Info.FLAG, flag);
                        break;
                    }
                }
                String inScValListStr = inPdScValList.toJson();
                data.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, inScValListStr);
                data.setCalendar(ProductSpecEntity.Info.SYS_CREATE_TIME, now);
                data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);
                dataList.add(data);
            }
        }
        cacheManage.addNeedDelCachedPdIdSet(aid, pdIdSpecListMap.keySet());
        if(m_daoCtrl.updateId(pdScId) == null){
            rt = Errno.ERROR;
            Log.logErr(rt, "dao.updateId error;flow=%d;aid=%s;pdScId=%s;", m_flow, aid, pdScId);
            return rt;
        }
        rt = m_daoCtrl.batchInsert(dataList, null, false);
        if(rt != Errno.OK) {
            Log.logErr(rt, "dao.batchInsert error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }

        // 添加 Saga 操作记录
        if (isSaga) {
            rt = addInsOp4Saga(aid, dataList);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList) {
        return batchAdd(aid, pdId, infoList, null);
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, Ref<Boolean> needRefreshSkuRef) {
        return batchAdd(aid, Collections.singletonMap(pdId, infoList), needRefreshSkuRef);
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, Ref<Boolean> needRefreshSkuRef, boolean isSaga) {
        return batchAdd(aid, Collections.singletonMap(pdId, infoList), needRefreshSkuRef, isSaga);
    }
    public int batchAdd(int aid, Map<Integer, List<Param>> pdIdSpecListMap, Ref<Boolean> needRefreshSkuRef) {
        return batchAdd(aid, pdIdSpecListMap, needRefreshSkuRef, false);
    }

    /**
     * 批量同步，有就更新，没有就添加
     * @param aid
     * @param pdId_pdScIdMap
     * @return
     */
    public int batchSynchronousSPU2SKU(int aid, Map<Integer, Param> pdId_InfoMap, Map<Integer, Integer> pdId_pdScIdMap) {
        if(aid <= 0 || pdId_InfoMap == null || pdId_pdScIdMap == null){
            Log.logErr("arg error;flow=%d;aid=%s;pdId_InfoMap=%s;pdId_pdScIdMap=%s;", m_flow, aid, pdId_InfoMap, pdId_pdScIdMap);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Integer> pdIdList = new FaiList<>(pdId_InfoMap.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef, ProductSpecEntity.Info.PD_ID, ProductSpecEntity.Info.PD_SC_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "dao select error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Calendar now = Calendar.getInstance();
        FaiList<Param> oldInfoList = listRef.value;
        FaiList<Param> batchUpdateDataList = new FaiList<>();
        for (Param oldInfo : oldInfoList) {
            int pdId = oldInfo.getInt(ProductSpecEntity.Info.PD_ID);
            int pdScId = oldInfo.getInt(ProductSpecEntity.Info.PD_SC_ID);
            Integer absent = pdId_pdScIdMap.putIfAbsent(pdId, pdScId);
            if(absent != null){
                rt = Errno.ERROR;
                Log.logErr(rt,"data err;aid=%s;pdId=%s;pdScId=%s;absent=%s;", aid, pdId, pdScId, absent);
                return rt;
            }
            // 标记 修改 的数据需要删除缓存
            cacheManage.addNeedDelCachedPdId(aid, pdId);

            Param info = pdId_InfoMap.remove(pdId);
            Param data = new Param();
            { // updater
                data.assign(info, ProductSpecEntity.Info.SC_STR_ID);
                data.assign(info, ProductSpecEntity.Info.SORT);
                data.assign(info, ProductSpecEntity.Info.FLAG);
                data.assign(info, ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);
            }
            { // matcher
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
            }
            batchUpdateDataList.add(data);
        }
        if(!batchUpdateDataList.isEmpty()){
            ParamUpdater batchUpdater = new ParamUpdater();
            batchUpdater.getData()
                    .setString(ProductSpecEntity.Info.SC_STR_ID, "?")
                    .setString(ProductSpecEntity.Info.SORT, "?")
                    .setString(ProductSpecEntity.Info.FLAG, "?")
                    .setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, "?")
                    .setString(ProductSpecEntity.Info.SYS_UPDATE_TIME, "?")
            ;
            ParamMatcher batchMatcher = new ParamMatcher();
            batchMatcher.and(ProductSpecEntity.Info.AID, ParamMatcher.EQ, "?");
            batchMatcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, "?");
            batchMatcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.batchUpdate(batchUpdater, batchMatcher, batchUpdateDataList);
            if(rt != Errno.OK){
                Log.logErr(rt, "dao batchUpdate error;flow=%d;aid=%s;batchUpdateDataList=%s;", m_flow, aid, batchUpdateDataList);
                return rt;
            }
        }

        if(!pdId_InfoMap.isEmpty()){
            FaiList<Param> batchAddInfoList = new FaiList<>();
            for (Map.Entry<Integer, Param> pdIdInfoEntry : pdId_InfoMap.entrySet()) {
                int pdId = pdIdInfoEntry.getKey();
                Param info = pdIdInfoEntry.getValue();
                Param data = new Param();
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                Integer pdScId = m_daoCtrl.buildId();
                if(pdScId == null){
                    Log.logErr("batchAdd arg error;flow=%d;aid=%s;tpScId=%s;info=%s;", m_flow, aid, pdScId, info);
                    return Errno.ERROR;
                }
                Integer absent = pdId_pdScIdMap.putIfAbsent(pdId, pdScId);
                if(absent != null){
                    Log.logErr("data err;aid=%s;pdId=%s;pdScId=%s;absent=%s;", aid, pdId, pdScId, absent);
                    return Errno.ERROR;
                }
                data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
                data.assign(info, ProductSpecEntity.Info.SC_STR_ID);
                data.assign(info, ProductSpecEntity.Info.SOURCE_TID);
                data.assign(info, ProductSpecEntity.Info.SOURCE_UNION_PRI_ID);
                data.assign(info, ProductSpecEntity.Info.SORT);
                data.assign(info, ProductSpecEntity.Info.FLAG);
                data.assign(info, ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);
                data.setCalendar(ProductSpecEntity.Info.SYS_CREATE_TIME, now);
                batchAddInfoList.add(data);
            }

            rt = m_daoCtrl.batchInsert(batchAddInfoList, null);
            if(rt != Errno.OK){
                Log.logErr(rt, "dao batchInsert error;flow=%d;aid=%s;batchAddInfoList=%s;", m_flow, aid, batchAddInfoList);
                return rt;
            }
        }

        Log.logStd("ok!flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean softDel, boolean isSaga) {
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty()){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        if (isSaga) {
            // 根据 是否软删除 记录不同的 Saga 操作
            if(softDel) {
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher.clone();
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = m_daoCtrl.select(searchArg, listRef);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    Log.logErr(rt, "dao.select error;flow=%d;aid=%s;matcher=%s;", m_flow, aid, matcher.toJson());
                    return rt;
                }
                // 预记录修改操作数据
                preAddUpdateSaga(aid, listRef.value);
            }else {
                // 记录删除操作数据
                rt = addDelOp4Saga(aid, matcher);
                if (rt != Errno.OK) {
                    return rt;
                }
            }
        }
        if(softDel) {
            ParamUpdater updater = new ParamUpdater();
            updater.getData().setInt(ProductSpecEntity.Info.STATUS, ProductSpecValObj.Status.DEL);
            rt = m_daoCtrl.update(updater, matcher);
        }else {
            rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
                return rt;
            }
        }

        Log.logStd("batchDel ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    public int batchDel(int aid, int pdId, FaiList<Integer> pdScIdList, Ref<Boolean> needRefreshSkuRef) {
        return batchDel(aid, pdId, pdScIdList, needRefreshSkuRef, false);
    }

    public int batchDel(int aid, int pdId, FaiList<Integer> pdScIdList, Ref<Boolean> needRefreshSkuRef, boolean isSaga) {
        if(aid <= 0 || pdId <=0 || (pdScIdList != null && pdScIdList.isEmpty())){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdId=%s;tpScDtIdList=%s;", m_flow, aid, pdId, pdScIdList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(pdScIdList != null){
            matcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.IN, pdScIdList);
        }
        if(needRefreshSkuRef != null){
            ParamMatcher queryMatcher = matcher.clone();
            queryMatcher.and(ProductSpecEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED, ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED);
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = queryMatcher;
            Ref<Integer> countRef = new Ref<>();
            rt = m_daoCtrl.selectCount(searchArg, countRef);
            if(rt != Errno.OK){
                return rt;
            }
            if(countRef.value > 0){
                needRefreshSkuRef.value = true;
            }
        }
        cacheManage.addNeedDelCachedPdId(aid, pdId);
        // 记录 Saga 操作
        if (isSaga) {
            rt = addDelOp4Saga(aid, matcher);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdId=%s;idList=%s;", m_flow, aid, pdId, pdScIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;idList=%s;", m_flow, aid, pdId, pdScIdList);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds) {
        if(aid <= 0 || Util.isEmptyList(unionPriIds)){
            Log.logErr("clearData arg error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clearData error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        // 处理下idBuilder
        m_daoCtrl.restoreMaxId();
        Log.logStd("clearData ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList, Ref<Boolean> needRefreshSkuRef, boolean isSaga) {
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdId=%s;tpScDtIdList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        FaiList<Integer> pdScIdList = new FaiList<>(updaterList.size());
        Set<Integer> scStrIdSet = new HashSet<>();
        // 排除非法的key，留下可以修改的 key，并将 updaterList 中的 pdScId 加入 pdScIdList 中，scStrId 加入 scStrIdSet 中
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(updaterList, ProductSpecEntity.getValidKeys(), data->{
            pdScIdList.add(data.getInt(ProductSpecEntity.Info.PD_SC_ID));
            Integer scStrId = data.getInt(ProductSpecEntity.Info.SC_STR_ID);
            if(scStrId != null){
                scStrIdSet.add(scStrId);
            }
        });
        maxUpdaterKeys.remove(ProductSpecEntity.Info.PD_SC_ID);

        // 获取已经存在的 规格字符串id 集合
        Set<Integer> alreadyExistedScStrIdSet = new HashSet<>();
        if(!scStrIdSet.isEmpty()){
            Ref<FaiList<Param>> listRef = new Ref<>();
            // 根据 aid + pdId + scStrId 查找 list，并将其中的 inPdScValList 从 String 转换成 FaiList
            rt = getListFromDao(aid, pdId, new FaiList<>(scStrIdSet), listRef, ProductSpecEntity.Info.SC_STR_ID);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            alreadyExistedScStrIdSet = OptMisc.getValSet(listRef.value, ProductSpecEntity.Info.SC_STR_ID);
        }

        Ref<FaiList<Param>> listRef = new Ref<>();
        // 获取旧数据 list
        rt = getListFromDaoByPdScIdList(aid, pdId, pdScIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        if(pdScIdList.size() != listRef.value.size()){
            Log.logStd("batchDel arg err;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return rt = Errno.NOT_FOUND;
        }

        // 记录 Saga 操作
        if (isSaga) {
            preAddUpdateSaga(aid, listRef.value);
        }

        // 用于转化将要改的数据的主键，因为存在：旧数据（商品规格1，商品规格2）新数据（商品规格2，商品规格1），即同一个商品下有两个商品规格的名称对调，而规格值集合不变
        FaiList<Integer> needConvertScStrIdList = new FaiList<>();
        Map<Integer, Param> oldDataMap = new HashMap<>(listRef.value.size()*4/3+1);
        for (Param info : listRef.value) {
            Integer pdScId = info.getInt(ProductSpecEntity.Info.PD_SC_ID);
            oldDataMap.put(pdScId, info);
            Integer oldScStrId = info.getInt(ProductSpecEntity.Info.SC_STR_ID);
            if(alreadyExistedScStrIdSet.remove(oldScStrId)){
                needConvertScStrIdList.add(oldScStrId);
            }
        }
        listRef.value = null; // help gc
        if(!alreadyExistedScStrIdSet.isEmpty()){ // 要改的商品规格名称已经存在
            rt = Errno.ALREADY_EXISTED;
            Log.logErr(rt, "already existed;flow=%s;aid=%s;pdId=%s;alreadyExistedScStrIdSet=%s", m_flow, aid, pdId, alreadyExistedScStrIdSet);
            return rt;
        }

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(ProductSpecEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        for (ParamUpdater updater : updaterList) {
            Integer pdScId = updater.getData().getInt(ProductSpecEntity.Info.PD_SC_ID);
            Param oldData = oldDataMap.remove(pdScId); // help gc
            int oldFlag = oldData.getInt(ProductSpecEntity.Info.FLAG, 0);
            Param updatedData = updater.update(oldData, true);
            // 如果不允许 inPdScValList 为空，则进入下面逻辑
            if(!Misc.checkBit(oldFlag, ProductSpecValObj.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY)){
                if(!needRefreshSkuRef.value){
                    FaiList<Integer> oldCheckIdList = getCheckIdList(oldData);
                    FaiList<Integer> updatedCheckIdList = getCheckIdList(updatedData);
                    // 当原先一个规格值都未勾选，更新后存在有勾选的规格值，则需要刷新sku
                    if(oldCheckIdList.size() == 0 && updatedCheckIdList.size() > 0){
                        needRefreshSkuRef.value = true;
                    }
                    // 当原先存在有勾选的规格值，更新后一个规格值都未勾选，则需要刷新sku
                    if(oldCheckIdList.size() > 0 && updatedCheckIdList.size() == 0){
                        needRefreshSkuRef.value = true;
                    }
                }
            }
            FaiList<Param> updatedInPdScValList = updatedData.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            int flag = updatedData.getInt(ProductSpecEntity.Info.FLAG, 0);
            if(updatedInPdScValList.size() == 0){
                flag &= ~ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
            }else{
                flag |= ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
            }
            updatedData.setInt(ProductSpecEntity.Info.FLAG, flag);
            updatedData.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, updatedInPdScValList.toJson());

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
            }
            dataList.add(data);
        }

        if(!needConvertScStrIdList.isEmpty()){
            ParamMatcher convertMatcher = new ParamMatcher();
            convertMatcher.and(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
            convertMatcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            convertMatcher.and(ProductSpecEntity.Info.SC_STR_ID, ParamMatcher.IN, needConvertScStrIdList);
            ParamUpdater convertUpdater = new ParamUpdater();
            convertUpdater.add(ProductSpecEntity.Info.SC_STR_ID, ParamUpdater.DEC, Integer.MAX_VALUE);
            rt = m_daoCtrl.update(convertUpdater, convertMatcher);
            if(rt != Errno.OK) {
                Log.logErr(rt, "update error;flow=%d;aid=%s;convertUpdater.sql=%s;convertMatcher.sql=%s;data=%s;", m_flow, aid, convertUpdater.getSql(), convertMatcher.getSql());
                return rt;
            }
        }

        cacheManage.addNeedDelCachedPdId(aid, pdId);
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    private FaiList<Integer> getCheckIdList(Param info) {
        FaiList<Param> inPdScValList = info.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
        FaiList<Integer> checkIdList = new FaiList<>();
        inPdScValList.forEach(item->{
            if(item.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                checkIdList.add(item.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
            }
        });
        return checkIdList;
    }
    public int getListFromDao(int aid, int pdId, FaiList<Integer> scStrIdList, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductSpecEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;pdId=%s;scStrIdList=%s;", m_flow, aid, pdId, scStrIdList);
            return rt;
        }
        initDBInfoList(listRef.value);
        Log.logStd(rt,"ok;flow=%s;aid=%s;pdId=%s;scStrIdList=%s;",m_flow, aid, pdId, scStrIdList);
        return rt;
    }

    public int getListFromDaoByPdScIdList(int aid, int pdId, FaiList<Integer> pdScIdList, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(pdScIdList != null){
            matcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.IN, pdScIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;pdId=%s;pdScIdList=%s;", m_flow, aid, pdId, pdScIdList);
            return rt;
        }
        initDBInfoList(listRef.value);
        Log.logStd(rt,"ok;flow=%s;aid=%s;pdId=%s;pdScIdList=%s;",m_flow, aid, pdId, pdScIdList);
        return rt;
    }

    public int getListFromDao(int aid, FaiList<Integer> pdIds, Ref<FaiList<Param>> listRef, String ... fields) {
        if(pdIds == null || pdIds.isEmpty()) {
            Log.logErr("select pdIds is empty;flow=%s;aid=%s;pdIds=%s;",m_flow, aid, pdIds);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;pdIds=%s;", m_flow, aid, pdIds);
            return rt;
        }
        // 将json形式的规格值inPdScValList 转成 FaiList，并保存
        initDBInfoList(listRef.value);
        Log.logStd(rt,"ok;flow=%s;aid=%s;pdIds=%s;",m_flow, aid, pdIds);
        return rt;
    }

    /**
     * 获取 Saga 操作记录
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param sagaOpListRef 接收返回的集合
     * @return {@link Errno}
     */
    public int getSagaOpList(String xid, Long branchId, Ref<FaiList<Param>> sagaOpListRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        int rt = m_sagaDaoCtrl.select(searchArg, sagaOpListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "productSpecProc dao.getSagaOpList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    // 记录删除操作 Saga
    private int addDelOp4Saga(int aid, ParamMatcher matcher) {
        if (matcher == null || matcher.isEmpty()) {
            Log.logStd("addDelOp4Saga arg matcher is empty");
            return Errno.OK;
        }
        int rt;
        Ref<FaiList<Param>> listRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        rt = m_daoCtrl.select(searchArg, listRef);
        if (rt != Errno.OK) {
            if (rt == Errno.NOT_FOUND) {
                return Errno.OK;
            }
            Log.logErr("select productSpec error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
            return rt;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        listRef.value.forEach(info -> {
            info.setString(SagaEntity.Common.XID, xid);
            info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
            info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
        });
        rt = m_sagaDaoCtrl.batchInsert(listRef.value, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d;list=%s", m_flow, aid, listRef.value);
            return rt;
        }
        return rt;
    }

    // 记录添加操作 Saga
    public int addInsOp4Saga(int aid, FaiList<Param> list) {
        if (Util.isEmptyList(list)) {
            Log.logStd("addInsOp4Saga list is empty;flow=%d;aid=%d", m_flow, aid);
            return Errno.OK;
        }
        // 添加 Saga 操作记录
        FaiList<Param> sagaOpList = new FaiList<>();
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        list.forEach(data -> {
            Param sagaOpInfo = new Param();
            // 记录主键 + Saga 字段
            sagaOpInfo.assign(data, ProductSpecEntity.Info.AID);
            sagaOpInfo.assign(data, ProductSpecEntity.Info.PD_ID);
            sagaOpInfo.assign(data, ProductSpecEntity.Info.SC_STR_ID);
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaOpList.add(sagaOpInfo);
        });
        int rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "productSpecProc sagaOpList batch insert error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    // 预记录修改操作数据
    private void preAddUpdateSaga(int aid, FaiList<Param> list) {
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(list)) {
            Log.logStd("preAddUpdateSaga list is empty;flow=%d;aid=%d", m_flow, aid);
            return;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        for (Param info : list) {
            int curAid = info.getInt(ProductSpecEntity.Info.AID);
            int curPdId = info.getInt(ProductSpecEntity.Info.PD_ID);
            int curScStrId = info.getInt(ProductSpecEntity.Info.SC_STR_ID);
            PrimaryKey primaryKey = new PrimaryKey(curAid, curPdId, curScStrId);
            // 确保 sagaMap 中没有这条数据，saga 需要记录最原始的那条数据
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            Param sagaOpInfo = new Param();
            // 记录所有能修改的字段 + 主键
            String[] validKeys = ProductSpecEntity.getPriAndUpdateKey();
            for (String key : validKeys) {
                if (key.equals(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST)) {
                    sagaOpInfo.setString(key, info.getList(key).toJson());
                } else {
                    sagaOpInfo.assign(info, key);
                }
            }
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, sagaOpInfo);
        }
    }

    // 添加 Saga 记录
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
        return rt;
    }

    /**
     * spec 回滚
     * @param aid aid
     * @param xid 全局事务id
     * @param branchId 分支事务id
     */
    public void rollback4Saga(int aid, String xid, Long branchId) {
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getSagaOpList(xid, branchId, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get sagaOpList err;flow=%d;aid=%;xid=%s;branchId=%s", m_flow, aid, xid, branchId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            Log.logStd("specProc sagaOpList is empty");
            return;
        }

        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = listRef.value.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚删除
        rollback4Del(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));

        // 回滚新增
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚修改
        rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE));
    }

    // 回滚修改
    private void rollback4Update(int aid, List<Param> list) {
        if (Util.isEmptyList(list)) {
            return;
        }
        String[] sagaKeys = ProductSpecEntity.getPriAndUpdateKey();
        FaiList<String> keys = new FaiList<>(Arrays.asList(sagaKeys));
        // 去除主键 (这里比较特殊，因为主键之一的 scStrId 是修改字段 (这里不去除)，在修改时我们是通过 aid + pdId + pdScId 去匹配修改)
        keys.remove(ProductSpecEntity.Info.AID);
        keys.remove(ProductSpecEntity.Info.PD_ID);
        keys.remove(ProductSpecEntity.Info.PD_SC_ID);

        FaiList<Param> dataList = new FaiList<>();
        for (Param info : list) {
            Param data = new Param();
            int pdId = info.getInt(ProductSpecEntity.Info.PD_ID);
            int pdScId = info.getInt(ProductSpecEntity.Info.PD_SC_ID);
            // for update
            for (String key : keys) {
                data.assign(info, key);
            }
            // for matcher
            data.setInt(ProductSpecEntity.Info.AID, aid);
            data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
            data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);

            dataList.add(data);
        }

        ParamUpdater updater = new ParamUpdater();
        for (String key : keys) {
            updater.getData().setString(key, "?");
        }

        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        matcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch update err;flow=%d;aid=%d;dataList=%d", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚添加
    private void rollback4Add(int aid, List<Param> list) {
        if (Util.isEmptyList(list)) {
            return;
        }
        int rt;
        // 根据 pdId 分组，减少在循环里操作db的次数
        Map<Integer, List<Param>> groupByPdIdMap = list.stream().collect(Collectors.groupingBy(spec -> spec.getInt(ProductSpecEntity.Info.PD_ID)));
        for (Map.Entry<Integer, List<Param>> entry : groupByPdIdMap.entrySet()) {
            Integer pdId = entry.getKey();
            List<Param> sagaList = entry.getValue();
            FaiList<Integer> scStrIdList = new FaiList<>();
            for (Param info : sagaList) {
                scStrIdList.addNotNull(info.getInt(ProductSpecEntity.Info.SC_STR_ID));
            }
            ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductSpecEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
            rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                throw new MgException(rt, "delete err;flow=%d;aid=%d;pdId=%d;scStrIdList=%s", m_flow, aid, pdId, scStrIdList);
            }
        }
        restoreMaxId(aid, false);
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    // 回滚删除
    private void rollback4Del(int aid, List<Param> list) {
        if (Util.isEmptyList(list)) {
            return;
        }
        // 去除 Saga 字段
        for (Param info : list) {
            info.remove(SagaEntity.Common.XID);
            info.remove(SagaEntity.Common.BRANCH_ID);
            info.remove(SagaEntity.Common.SAGA_OP);
            info.remove(SagaEntity.Common.SAGA_TIME);
        }

        int rt = m_daoCtrl.batchInsert(new FaiList<>(list), null, false);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batch insert err;flow=%d;aid=%d;list=%s", m_flow, aid, list);
        }
        Log.logStd("rollback del ok;flow=%;aid=%d", m_flow, aid);
    }

    private void initDBInfoList(FaiList<Param> infoList){
        if(infoList == null || infoList.isEmpty()){
            return;
        }
        infoList.forEach(info->{
            String inPdScValListStr = info.getString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            info.setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, FaiList.parseParamList(inPdScValListStr, new FaiList<>()));
        });
    }

    public int getList(int aid, int pdId, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || pdId <=0 || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;listRef=%s;", m_flow, aid, pdId, listRef);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> pdScList = ProductSpecCacheCtrl.getPdScList(aid, pdId);
        if(pdScList != null){
            listRef.value = pdScList;
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        try {
            LockUtil.readLock(aid);
            // double check
            pdScList = ProductSpecCacheCtrl.getPdScList(aid, pdId);
            if(pdScList != null){
                listRef.value = pdScList;
                return Errno.OK;
            }

            rt = getListFromDaoByPdScIdList(aid, pdId, null, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt,"getListFormDao err;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
                return rt;
            }
            pdScList = listRef.value;
            ProductSpecCacheCtrl.initPdScList(aid, pdId, pdScList);
        }finally {
            LockUtil.unReadLock(aid);
        }
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    // 门店迁移服务接口，后续不要用
    public void migrateYkService(int aid, FaiList<Param> specList) {
        int rt;
        if (aid <= 0) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;flow=%d;aid=%d", m_flow ,aid);
        }
        if (specList == null || specList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;flow=%d;aid=%d", m_flow ,aid);
        }
        Integer pdScId = m_daoCtrl.getId();
        if(pdScId == null){
            rt = Errno.ERROR;
            throw new MgException(rt, "pdScId error;flow=%d;aid=%s;tpScId=%s;", m_flow, aid, pdScId);
        }
        FaiList<Integer> pdIdList = new FaiList<>();
        FaiList<Param> dataList = new FaiList<>(specList.size());
        for (Param info : specList) {
            Param data = new Param();
            int pdId = info.getInt(ProductSpecEntity.Info.PD_ID);
            data.setInt(ProductSpecEntity.Info.AID, aid);
            pdScId++;
            data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
            data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
            data.assign(info, ProductSpecEntity.Info.SC_STR_ID);
            data.assign(info, ProductSpecEntity.Info.SOURCE_TID);
            data.assign(info, ProductSpecEntity.Info.SOURCE_UNION_PRI_ID);
            data.assign(info, ProductSpecEntity.Info.FLAG);
            data.assign(info, ProductSpecEntity.Info.STATUS);
            data.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, new FaiList<>().toJson());
            data.assign(info, ProductSpecEntity.Info.SYS_CREATE_TIME);
            data.assign(info, ProductSpecEntity.Info.SYS_UPDATE_TIME);
            pdIdList.add(pdId);
            dataList.add(data);
        }
        cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        if (m_daoCtrl.updateId(pdScId) == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao.updateId error;flow=%d;aid=%s;pdScId=%s;", m_flow, aid, pdScId);
        }
        Log.logDbg("joke:add specList=%s", specList);
        rt = m_daoCtrl.batchInsert(dataList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "dao.batchInsert error;flow=%d;aid=%s;dataList=%s;", m_flow, aid, dataList);
        }
        Log.logStd("ok;flow=%d;aid=%d;", m_flow, aid);
    }

    public void restoreMaxId(int aid, boolean needLock) {
        m_daoCtrl.restoreMaxId(needLock);
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }


    private int m_flow;
    private ProductSpecDaoCtrl m_daoCtrl;
    private ProductSpecSagaDaoCtrl m_sagaDaoCtrl;

    private CacheManage cacheManage = new CacheManage();

    private static class CacheManage{

        public CacheManage() {
            init();
        }

        private Set<Integer> needDelCachedPdIdSet;
        private void addNeedDelCachedPdIdSet(int aid, Set<Integer> pdIdSet){
            if(pdIdSet == null || pdIdSet.isEmpty()){
                return;
            }
            ProductSpecCacheCtrl.setCacheDirty(aid, pdIdSet);
            needDelCachedPdIdSet.addAll(pdIdSet);
        }
        private void addNeedDelCachedPdIdList(int aid, FaiList<Integer> pdIdList){
            if(pdIdList == null || pdIdList.isEmpty()){
                return;
            }
            HashSet<Integer> pdIdSet = new HashSet<>(pdIdList);
            addNeedDelCachedPdIdSet(aid, pdIdSet);
        }
        private void addNeedDelCachedPdId(int aid, int pdId){
            ProductSpecCacheCtrl.setCacheDirty(aid, pdId);
            needDelCachedPdIdSet.add(pdId);
        }

        private boolean deleteDirtyCache(int aid){
            try {
                boolean boo = ProductSpecCacheCtrl.delCache(aid, needDelCachedPdIdSet);
                return boo;
            }finally {
                init();
            }
        }

        private void init() {
            needDelCachedPdIdSet = new HashSet<>();
        }
    }

    private Map<PrimaryKey, Param> sagaMap;

    private static class PrimaryKey {
        int aid;
        int pdId;
        int scStrId;

        public PrimaryKey(int aid, int pdId, int scStrId) {
            this.aid = aid;
            this.pdId = pdId;
            this.scStrId = scStrId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    pdId == that.pdId &&
                    scStrId == that.scStrId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, pdId, scStrId);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", pdId=" + pdId +
                    ", scStrId=" + scStrId +
                    '}';
        }
    }
}
