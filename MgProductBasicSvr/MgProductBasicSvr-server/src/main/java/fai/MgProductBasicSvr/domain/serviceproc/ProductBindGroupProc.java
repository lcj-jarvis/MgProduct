package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindGroupDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProductBindGroupProc {
    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductBindGroupDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public FaiList<Param> getPdBindGroupList(int aid, int unionPriId, FaiList<Integer> rlPdIdList) {
        if(rlPdIdList == null || rlPdIdList.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "get rlPdIdList is empty;aid=%d;rlPdIdList=%s;", aid, rlPdIdList);
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>(rlPdIdList);
        return getList(aid, unionPriId, rlPdIds);
    }

    public void addPdBindGroupList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Integer> rlGroupIdList) {
        int rt;
        if(rlGroupIdList == null || rlGroupIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(int rlGroupId : rlGroupIdList) {
            Param info = new Param();

            info.setInt(ProductBindGroupEntity.Info.AID, aid);
            info.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
            info.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);
            info.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
            info.setCalendar(ProductBindGroupEntity.Info.CREATE_TIME, now);
            addList.add(info);
        }
        rt = m_dao.batchInsert(addList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind group error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public int delPdBindGroup(int aid, int unionPriId, ParamMatcher matcher) {
        int rt;
        if(matcher == null) {
            matcher = new ParamMatcher();
        }
        Ref<Integer> refRowCount = new Ref<>();
        matcher.and(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher.toJson());
        }
        Log.logStd("del bind group ok;aid=%d;uid=%d;matcher=%s;", aid, unionPriId, matcher.toJson());
        return refRowCount.value;
    }

    public int delPdBindGroupList(int aid, FaiList<Integer> pdIds) {
        int rt;
        if(Util.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        Log.logStd("delPdBindGroupList ok;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        return refRowCount.value;
    }

    public int delPdBindGroupList(int aid, int unionPriId, int rlPdId, FaiList<Integer> rlGroupIds) {
        int rt;
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPdId=%d;rlGroupIds=%s;", m_flow, aid, rlPdId, rlGroupIds);
        }
        Log.logStd("delPdBindGroupList ok;flow=%d;aid=%d;rlPdId=%d;rlGroupIds=%s;", m_flow, aid, rlPdId, rlGroupIds);
        return refRowCount.value;
    }

    public int delPdBindGroupList(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPdIds=%s;", m_flow, aid, rlPdIds);
        }
        Log.logStd("delPdBindGroupList ok;flow=%d;aid=%d;rlPdIds=%s;", m_flow, aid, rlPdIds);
        return refRowCount.value;
    }

    public int delPdBindGroupListByRlGroupIds(int aid, int unionPriId, FaiList<Integer> rlGroupIds) {
        int rt;
        if(Util.isEmptyList(rlGroupIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;uid=%d;rlGroupIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        }
        Log.logStd("delPdBindGroupList ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        return refRowCount.value;
    }

    // 清空指定aid+unionPriId的数据
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "clearAcct error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
        }
        Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
    }

    public FaiList<Integer> getRlPdIdsByGroupId(int aid, int unionPriId, FaiList<Integer> rlGroupIds) {
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "args error;rlGroupIds is null;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select error;flow=%d;aid=%d;unionPriId=%d;rlGroupIds=%s;", m_flow, aid, unionPriId, rlGroupIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param info : listRef.value) {
            rlPdIds.add(info.getInt(ProductBindGroupEntity.Info.RL_PD_ID));
        }
        return rlPdIds;
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductBindGroupCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductBindGroupCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getCountFromDB(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductBindGroupCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public FaiList<Param> searchFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        return listRef.value;
    }

    private int getCountFromDB(int aid, int unionPriId) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if(countRef.value == null) {
            countRef.value = 0;
        }
        return countRef.value;
    }

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> rlPdIds) {
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
        }
        // 缓存中获取
        FaiList<Param> list = ProductBindGroupCache.getCacheList(aid, unionPriId, new FaiList<>(rlPdIds));
        if(list == null) {
            list = new FaiList<Param>();
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(rlPdIds);
        for(Param info : list) {
            Integer rlPdId = info.getInt(ProductBindGroupEntity.Info.RL_PD_ID);
            noCacheIds.remove(rlPdId);
        }

        if(noCacheIds.isEmpty()) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheIds);
        Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, noCacheIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
            Map<Integer, List<Param>> groupByRlPdId = tmpRef.value.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductBindGroupEntity.Info.RL_PD_ID)));
            for(Integer rlPdId : groupByRlPdId.keySet()) {
                // 添加到缓存
                ProductBindGroupCache.addCacheList(aid, unionPriId, rlPdId, new FaiList<>(groupByRlPdId.get(rlPdId) ));
            }
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;aid=%d;unionPriId=%d;", aid, unionPriId);
        }

        return list;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        // 事务注册失败则设置dao为null，防止在TransactionCtrl无法控制的情况下使用dao
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductBindGroupDaoCtrl m_dao;
}
