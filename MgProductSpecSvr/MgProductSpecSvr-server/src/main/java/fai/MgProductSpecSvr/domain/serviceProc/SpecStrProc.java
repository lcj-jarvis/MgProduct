package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.entity.SpecStrEntity;
import fai.MgProductSpecSvr.domain.repository.SpecStrCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;

public class SpecStrProc {
    public SpecStrProc(SpecStrDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public SpecStrProc(int flow, int aid, TransactionCtrl tc) {
        m_daoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        m_sagaDaoCtrl = SpecStrSagaDaoCtrl.getInstanceWithRegistered(flow, aid, tc);
        if (!tc.register(m_daoCtrl) || m_sagaDaoCtrl == null) {
            throw new RuntimeException(String.format("SpecStrDaoCtrl or SpecStrSagaDaoCtrl init error;flow=%d;aid=%d", flow, aid));
        }
        m_flow = flow;
    }

    public int getListWithBatchAdd(int aid, FaiList<String> nameList, Param nameIdMap) {
        return getListWithBatchAdd(aid, nameList, nameIdMap, false);
    }
    /**
     * 需要外部加锁
     * 根据规格字符串获取对应的id, 不存在的话就生成。
     * @param nameIdMap 规格字符串和对应id的映射
     * @param isSaga 是否是分布式事务状态下
     */
    public int getListWithBatchAdd(int aid, FaiList<String> nameList, Param nameIdMap, boolean isSaga) {
        if(aid <= 0 || nameList == null || nameList.isEmpty() || nameIdMap == null){
            Log.logErr("arg err;aid=%d;nameList=%s;nameIdMap=%s;", aid, nameList, nameIdMap);
            return Errno.ARGS_ERROR;
        }

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getListByNames(aid, nameList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        HashSet<String> nameSet = new HashSet<>(nameList);
        FaiList<Param> infoList = listRef.value;

        for (Param info : infoList) {
            String name = info.getString(SpecStrEntity.Info.NAME);
            if(nameSet.remove(name)){
                nameIdMap.setInt(name, info.getInt(SpecStrEntity.Info.SC_STR_ID));
            }
        }

        FaiList<Param> needBatchAddList = new FaiList<>(nameSet.size());
        for (String name : nameSet) {
            needBatchAddList.add(new Param().setString(SpecStrEntity.Info.NAME, name));
        }
        if(!needBatchAddList.isEmpty()){
            rt = batchAdd(aid, needBatchAddList, nameIdMap);
            // 添加 Saga 操作记录
            if (isSaga) {
                rt = addInsOp4Saga(aid, new FaiList<>(nameSet));
                if (rt != Errno.OK) {
                    return rt;
                }
            }
        }
        return rt;
    }

    public int batchAdd(int aid, FaiList<Param> infoList, Param rtInfo) {
        return batchAdd(aid, infoList, null, rtInfo);
    }

    private int batchAdd(int aid, FaiList<Param> infoList, FaiList<Integer> rtIdList, Param nameIdMap) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logErr("batchAdd error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(SpecStrEntity.Info.AID, aid);
            String name = info.getString(SpecStrEntity.Info.NAME);
            data.setString(SpecStrEntity.Info.NAME, name);
            Integer scStrId = m_daoCtrl.buildId();
            if(scStrId == null){
                Log.logStd("batchAdd arg error;flow=%d;aid=%s;tpScId=%s;info=%s;", m_flow, aid, scStrId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(scStrId);
            }
            if(nameIdMap != null){
                nameIdMap.setInt(name, scStrId);
            }
            data.setInt(SpecStrEntity.Info.SC_STR_ID, scStrId);
            data.setCalendar(SpecStrEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null, false);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        if(!SpecStrCacheCtrl.setCacheList(aid, dataList)){
            if(!SpecStrCacheCtrl.delAllCache(aid)){
                Log.logStd("SpecStrCacheCtrl delAllCache err;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        Log.logStd("batchAdd ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    private int getListByNames(int aid, FaiList<String> nameList, Ref<FaiList<Param>> listRef) {
        HashSet<String> nameSet = new HashSet<>(nameList);
        FaiList<Param> resultList = new FaiList<>();

        getListByNamesFromCache(aid, nameSet, resultList);
        if(nameSet.isEmpty()){
            listRef.value = resultList;
            return Errno.OK;
        }
        int rt;
        FaiList<Param> dbInfoList;
        try {
            LockUtil.readLock(aid);
            // double check
            getListByNamesFromCache(aid, nameSet, resultList);
            if(nameSet.isEmpty()){
                listRef.value = resultList;
                return Errno.OK;
            }

            ParamMatcher matcher = new ParamMatcher(SpecStrEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpecStrEntity.Info.NAME, ParamMatcher.IN, new FaiList<>(nameSet));
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            rt =  m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getList error;flow=%d;aid=%s;nameSet=%s;", m_flow, aid, nameSet);
                return rt;
            }
            dbInfoList = listRef.value;
            FaiList<Param> newCacheList = dbInfoList;
            SpecStrCacheCtrl.setCacheList(aid, newCacheList);
        }finally {
            LockUtil.unReadLock(aid);
        }

        resultList.addAll(dbInfoList);
        listRef.value = resultList;
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;nameList=%s;", m_flow, aid, nameList);
        return rt = Errno.OK;
    }

    private void getListByNamesFromCache(int aid, HashSet<String> nameSet, FaiList<Param> resultList) {
        FaiList<Param> cacheList = SpecStrCacheCtrl.getCacheListByNames(aid, new FaiList<>(nameSet));
        if(cacheList != null){
            for (Param cacheInfo : cacheList) {
                nameSet.remove(cacheInfo.getString(SpecStrEntity.Info.NAME));
                resultList.add(cacheInfo);
            }
        }
    }

    public int getNameIdMapByNames(int aid, FaiList<String> nameList, Ref<Param> nameIdMapRef){
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getListByNames(aid, nameList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Param nameIdMap  = new Param(true); // mapMode
        listRef.value.stream().forEach(info -> {
            nameIdMap.setInt(info.getString(SpecStrEntity.Info.NAME), info.getInt(SpecStrEntity.Info.SC_STR_ID));
        });
        nameIdMapRef.value = nameIdMap;
        return rt;
    }

    public int getList(int aid, FaiList<Integer> scStrIdList, Ref<FaiList<Param>> listRef) {
        //去重
        HashSet<Integer> scStrIdSet = new HashSet<>(scStrIdList);
        scStrIdList = new FaiList<>(scStrIdSet);
        FaiList<Param> resultList = new FaiList<>(scStrIdSet.size());

        getListFromCache(aid, scStrIdList, scStrIdSet, resultList);
        if(scStrIdSet.isEmpty()){
            listRef.value = resultList;
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        FaiList<Param> dbInfoList = null;
        try {
            LockUtil.readLock(aid);
            // double check
            getListFromCache(aid, scStrIdList, scStrIdSet, resultList);
            if(scStrIdSet.isEmpty()){
                listRef.value = resultList;
                return Errno.OK;
            }

            ParamMatcher matcher = new ParamMatcher(SpecStrEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpecStrEntity.Info.SC_STR_ID, ParamMatcher.IN, new FaiList<>(scStrIdSet));
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            rt =  m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getList error;flow=%d;aid=%s;scStrIdSet=%s;", m_flow, aid, scStrIdSet);
                return rt;
            }
            dbInfoList = listRef.value;
            FaiList<Param> newCacheList = new FaiList<>(dbInfoList);
            for (Param info : dbInfoList) {
                scStrIdSet.remove(info.getInt(SpecStrEntity.Info.SC_STR_ID));
            }
            for (Integer scStrId : scStrIdSet) { // 缓存上空数据
                newCacheList.add(new Param().setInt(SpecStrEntity.Info.SC_STR_ID, scStrId));
            }
            SpecStrCacheCtrl.setCacheList(aid, newCacheList);
        }finally {
            LockUtil.unReadLock(aid);
        }

        resultList.addAll(dbInfoList);
        listRef.value = resultList;

        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;scStrIdList=%s;", m_flow, aid, scStrIdList);
        return rt = Errno.OK;
    }

    /**
     * 获取 Saga 操作记录
     * @param xid 全局事务di
     * @param branchId 分支事务id
     * @param specStrSagaOpListRef 接收参数
     * @return {@link Errno}
     */
    public int getSagaOpList(String xid, Long branchId, Ref<FaiList<Param>> specStrSagaOpListRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        int rt = m_sagaDaoCtrl.select(searchArg, specStrSagaOpListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "SpecStrProc dao.getSagaOpList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    // 记录添加操作
    public int addInsOp4Saga(int aid, FaiList<String> nameList) {
        int rt;
        if (Util.isEmptyList(nameList)) {
            Log.logStd("addInsOp4Saga nameList is empty");
            return Errno.OK;
        }
        FaiList<Param> sagaOpList = new FaiList<>();
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        nameList.forEach(name -> {
            Param sagaOpInfo = new Param();
            sagaOpInfo.setInt(SpecStrEntity.Info.AID, aid);
            sagaOpInfo.setString(SpecStrEntity.Info.NAME, name);
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaOpList.add(sagaOpInfo);
        });
        rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "dao.insert sagaOpList error;flow=%d;aid=%d;sagaOpList=%s",m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    /**
     * specStr 回滚
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
        if (listRef.value.isEmpty()) {
            Log.logStd("specStrProc sagaOpList is empty");
            return;
        }

        // 这里只有添加类型的 Saga 记录
        // 回滚添加
        rollback4Add(aid, listRef.value);
    }

    // 回滚添加
    private void rollback4Add(int aid, FaiList<Param> list) {
        if (Util.isEmptyList(list)) {
            return;
        }
        FaiList<String> nameList = Utils.getValList(list, SpecStrEntity.Info.NAME);
        ParamMatcher matcher = new ParamMatcher(SpecStrEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecStrEntity.Info.NAME, ParamMatcher.IN, nameList);

        int rt = m_daoCtrl.delete(matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "delete err;flow=%d;aid=%d;nameList=%s", m_flow, aid, nameList);
        }
        restoreMaxId(aid, false);
        Log.logStd("rollback add ok;flow=%d;aid=%d", m_flow, aid);
    }

    private void getListFromCache(int aid, FaiList<Integer> scStrIdList, HashSet<Integer> scStrIdSet, FaiList<Param> resultList) {
        FaiList<Param> cacheList = SpecStrCacheCtrl.getCacheList(aid, scStrIdList);
        if(cacheList != null){
            for (Param cacheInfo : cacheList) {
                scStrIdSet.remove(cacheInfo.getInt(SpecStrEntity.Info.SC_STR_ID));
                if(cacheInfo.getInt(SpecStrEntity.Info.AID) == null){ // 去掉缓存的空数据
                    continue;
                }
                resultList.add(cacheInfo);
            }
        }
    }

    public void restoreMaxId(int aid, boolean needLock) {
        m_daoCtrl.restoreMaxId(needLock);
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    private int m_flow;
    private SpecStrDaoCtrl m_daoCtrl;
    private SpecStrSagaDaoCtrl m_sagaDaoCtrl;
}
