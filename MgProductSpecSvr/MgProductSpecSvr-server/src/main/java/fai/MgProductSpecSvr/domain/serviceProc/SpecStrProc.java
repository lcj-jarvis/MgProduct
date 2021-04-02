package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.entity.SpecStrEntity;
import fai.MgProductSpecSvr.domain.repository.SpecStrCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.HashSet;

public class SpecStrProc {
    public SpecStrProc(SpecStrDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    /**
     * 外部加锁
     * @param aid
     * @param nameList
     * @param nameIdMap
     * @return
     */
    public int getListWithBatchAdd(int aid, FaiList<String> nameList, Param nameIdMap) {
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
        int rt = Errno.ERROR;
        FaiList<Param> dbInfoList = null;
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


    private int m_flow;
    private SpecStrDaoCtrl m_daoCtrl;
}
