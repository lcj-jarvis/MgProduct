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
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

public class ProductSpecProc {
    public ProductSpecProc(ProductSpecDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
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
        if (isSaga) {
            // 添加 Saga 操作记录
            FaiList<Param> sagaOpList = new FaiList<>();
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            dataList.forEach(data -> {
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
            if (sagaOpList.isEmpty()) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;sagaOpList is empty;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
            rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
            if (rt != Errno.OK) {
                Log.logErr(rt, "productSpecProc sagaOpList batch insert error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
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
     * batchAdd 的补偿方法
     *
     * @param aid aid
     * @param sagaOpList Saga 操作记录
     * @return {@link Errno}
     */
    public int batchAddRollback(int aid, FaiList<Param> sagaOpList) {
        int rt = Errno.ERROR;
        if (Util.isEmptyList(sagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args err;sagaOpList is empty;");
            return rt;
        }
        // 根据 pdId 分组，减少在循环里操作db的次数
        Map<Integer, List<Param>> listMap = sagaOpList.stream().collect(Collectors.groupingBy(spec -> spec.getInt(ProductSpecEntity.Info.PD_ID)));
        for (Map.Entry<Integer, List<Param>> entry : listMap.entrySet()) {
            Integer pdId = entry.getKey();
            List<Param> list = entry.getValue();
            FaiList<Integer> scStrIdList = new FaiList<>();
            for (Param info : list) {
                scStrIdList.addNotNull(info.getInt(ProductSpecEntity.Info.SC_STR_ID));
            }
            ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductSpecEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
            rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                Log.logErr(rt, "batchAddRollback dao.delete error;flow=%d;aid=%d;pdId=%d;scStrIdList=%s", m_flow, aid, pdId, scStrIdList);
                return rt;
            }
        }
        Log.logStd("batchAddRollback ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
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
    public int batchDel(int aid, FaiList<Integer> pdIdList, boolean isSaga) {
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty()){
            Log.logErr("batchDel arg error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return Errno.ARGS_ERROR;
        }
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        cacheManage.addNeedDelCachedPdIdList(aid, pdIdList);
        if (isSaga) {
            // 需要查询一遍旧数据，录入操作表中
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "productSpecProc select oldList err;flow=%d;aid=%d", m_flow, aid);
                return rt;
            }
            // 添加 Saga 操作记录
            if (!Util.isEmptyList(listRef.value)) {
                String xid = RootContext.getXID();
                Long branchId = RootContext.getBranchId();
                Calendar now = Calendar.getInstance();
                listRef.value.forEach(info -> {
                    info.setString(SagaEntity.Common.XID, xid);
                    info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                    info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                });
                rt = m_sagaDaoCtrl.batchInsert(listRef.value);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d", m_flow, aid);
                }
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }

        Log.logStd("batchDel ok;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    /**
     * batchDel 的补偿方法
     */
    public int batchDelRollback(int aid, FaiList<Param> sagaOpList) {
        int rt;
        if (Util.isEmptyList(sagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;sagaOpList is empty;");
            return rt;
        }
        // 去除 Saga 字段
        Util.removeSpecificColumn(sagaOpList, SagaEntity.Common.XID, SagaEntity.Common.BRANCH_ID, SagaEntity.Common.SAGA_OP, SagaEntity.Common.SAGA_TIME );
        rt = m_daoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchDel rollback error;flow=%d;aid=%s;sagaOpList=%s;", m_flow, aid, sagaOpList);
            return rt;
        }
        Log.logStd("batchDel rollback ok;flow=%d;aid=%d;", m_flow, aid);
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
            Ref<FaiList<Param>> listRef = new Ref<>();
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            rt = m_daoCtrl.select(searchArg, listRef);
            if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr("select productSpec error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
                return rt;
            }
            if (!fai.middleground.svrutil.misc.Utils.isEmptyList(listRef.value)) {
                String xid = RootContext.getXID();
                Long branchId = RootContext.getBranchId();
                Calendar now = Calendar.getInstance();
                listRef.value.forEach(info -> {
                    info.setString(SagaEntity.Common.XID, xid);
                    info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                    info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                    info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                });
                rt = m_sagaDaoCtrl.batchInsert(listRef.value);
                if (rt != Errno.OK) {
                    Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d;", m_flow, aid);
                    return rt;
                }
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

        // 记录 Saga 操作记录
        if (isSaga) {
            FaiList<Param> sagaOpList = new FaiList<>(listRef.value.size());
            String xid = RootContext.getXID();
            Long branchId = RootContext.getBranchId();
            Calendar now = Calendar.getInstance();
            listRef.value.forEach(info -> {
                Param sagaOpInfo = new Param();
                maxUpdaterKeys.add(ProductSpecEntity.Info.PD_SC_ID);
                maxUpdaterKeys.forEach(key -> {
                    if (ProductSpecEntity.Info.IN_PD_SC_VAL_LIST.equals(key)) {
                        sagaOpInfo.setString(key, info.getList(key).toJson());
                    } else {
                        sagaOpInfo.assign(info, key);
                    }
                });
                maxUpdaterKeys.remove(ProductSpecEntity.Info.PD_SC_ID);
                sagaOpInfo.setInt(ProductSpecEntity.Info.AID, aid);
                sagaOpInfo.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
                sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaOpList.add(sagaOpInfo);
            });
            if (!fai.middleground.svrutil.misc.Utils.isEmptyList(sagaOpList)) {
                rt = m_sagaDaoCtrl.batchInsert(sagaOpList);
                if (rt != Errno.OK) {
                    Log.logErr("insert sagaOpList error;flow=%d;aid=%d", m_flow, aid);
                    return rt;
                }
            }
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

    public int batchSetRollback(int aid, FaiList<Param> modifySagaOpList) {
        int rt;
        if (fai.middleground.svrutil.misc.Utils.isEmptyList(modifySagaOpList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args err;modifySagaOpList is empty;");
            return rt;
        }
        FaiList<ParamUpdater> updaterList = new FaiList<>();
        modifySagaOpList.clone().forEach(data -> updaterList.add(new ParamUpdater(data)));
        Set<String> maxUpdaterKeys = Utils.retainValidUpdaterList(updaterList, ProductSpecEntity.getValidKeys(), null);
        maxUpdaterKeys.remove(ProductSpecEntity.Info.PD_SC_ID);
        maxUpdaterKeys.add(ProductSpecEntity.Info.SYS_UPDATE_TIME);

        // prepare updater
        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key -> doBatchUpdater.getData().setString(key, "?"));

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.EQ, "?");

        FaiList<Param> dataList = new FaiList<>(modifySagaOpList.size());
        modifySagaOpList.forEach(oldData -> {
            Param data = new Param();
            { // for prepare updater
                maxUpdaterKeys.forEach(key -> data.assign(oldData, key));
            }
            { // for prepare matcher
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.assign(oldData, ProductSpecEntity.Info.PD_ID);
                data.assign(oldData, ProductSpecEntity.Info.PD_SC_ID);
            }
            dataList.add(data);
        });

        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "batchSetRollback err;flow=%d;aid=%d", m_flow, aid);
            return rt;
        }
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
}
