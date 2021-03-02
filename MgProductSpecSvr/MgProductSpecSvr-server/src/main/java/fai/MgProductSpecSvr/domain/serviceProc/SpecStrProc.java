package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.entity.SpecStrEntity;
import fai.MgProductSpecSvr.domain.repository.SpecStrCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;

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
            Log.logStd("batchAdd error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
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
        FaiList<Param> list = SpecStrCacheCtrl.getCacheListByNames(aid, nameList);
        HashSet<String> nameSet = new HashSet<>(nameList);
        if(list != null){
            Iterator<Param> iterator = list.iterator();
            while (iterator.hasNext()){
                Param info = iterator.next();
                nameSet.remove(info.getString(SpecStrEntity.Info.NAME));
            }
            if(nameSet.isEmpty()){
                listRef.value = list;
                return Errno.OK;
            }
            nameList = new FaiList<>(nameSet);
        }else{
            list = new FaiList<>();
        }

        ParamMatcher matcher = new ParamMatcher(SpecStrEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecStrEntity.Info.NAME, ParamMatcher.IN, nameList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;nameList=%s;", m_flow, aid, nameList);
            return rt;
        }
        FaiList<Param> newCacheList = new FaiList<>(listRef.value);
        list.addAll(listRef.value);
        listRef.value = list;

        SpecStrCacheCtrl.setCacheList(aid, newCacheList);
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;nameList=%s;", m_flow, aid, nameList);
        return rt = Errno.OK;
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

        FaiList<Param> list = SpecStrCacheCtrl.getCacheList(aid, scStrIdList);
        if(list != null){
            Iterator<Param> iterator = list.iterator();
            while (iterator.hasNext()){
                Param info = iterator.next();
                scStrIdSet.remove(info.getInt(SpecStrEntity.Info.SC_STR_ID));
                if(info.getInt(SpecStrEntity.Info.AID) == null){ // 去掉缓存的空数据
                    iterator.remove();
                }
            }

            if(scStrIdSet.isEmpty()){
                listRef.value = list;
                return Errno.OK;
            }
            scStrIdList = new FaiList<>(scStrIdSet);
        }else{
            list = new FaiList<>();
        }
        ParamMatcher matcher = new ParamMatcher(SpecStrEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpecStrEntity.Info.SC_STR_ID, ParamMatcher.IN, scStrIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt =  m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getList error;flow=%d;aid=%s;scStrIdList=%s;", m_flow, aid, scStrIdList);
            return rt;
        }
        FaiList<Param> newCacheList = new FaiList<>(listRef.value);
        list.addAll(listRef.value);
        listRef.value = list;
        for (Param info : listRef.value) {
            scStrIdSet.remove(info.getInt(SpecStrEntity.Info.SC_STR_ID));
        }
        for (Integer scStrId : scStrIdSet) { // 缓存上空数据
            newCacheList.add(new Param().setInt(SpecStrEntity.Info.SC_STR_ID, scStrId));
        }
        SpecStrCacheCtrl.setCacheList(aid, newCacheList);
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;scStrIdList=%s;", m_flow, aid, scStrIdList);
        return rt = Errno.OK;
    }


    private int m_flow;
    private SpecStrDaoCtrl m_daoCtrl;
}
