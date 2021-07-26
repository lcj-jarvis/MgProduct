package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindTagCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindTagDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author LuChaoJi
 * @date 2021-07-14 14:28
 */
public class ProductBindTagProc {

    private int m_flow;
    private ProductBindTagDaoCtrl m_dao;

    public ProductBindTagProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductBindTagDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        // 事务注册失败则设置dao为null，防止在TransactionCtrl无法控制的情况下使用dao
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductBindTagDaoCtrl err;");
        }
    }

    public FaiList<Param> getPdBindTagList(int aid, int unionPriId, FaiList<Integer> rlPdIdList) {
        //去重
        FaiList<Integer> rlPdIds = new FaiList<>(new HashSet<>(rlPdIdList));
        //获取缓存的所有数据
        FaiList<Param> cacheList = ProductBindTagCache.getCacheList(aid, unionPriId, rlPdIds);
        if (cacheList == null) {
            cacheList = new FaiList<>();
        }
        cacheList.remove(null);
        if (cacheList.size() == rlPdIds.size()) {
            return cacheList;
        }

        LockUtil.PdBindTagLock.readLock(aid);
        try {
            //双检，防止缓存穿透
            cacheList = ProductBindTagCache.getCacheList(aid, unionPriId, rlPdIds);
            if (cacheList == null) {
                cacheList = new FaiList<>();
            }
            cacheList.remove(null);
            if (cacheList.size() == rlPdIds.size()) {
                return cacheList;
            }

            //获取未缓存的rlPdId
            FaiList<Integer> noCacheRlPdIds = new FaiList<>();
            //缓存的rlPdId
            FaiList<Integer> cacheRlPdIds = new FaiList<>();
            cacheList.forEach(param -> {
                Integer rlPdId = param.getInt(ProductBindTagEntity.Info.RL_PD_ID);
                cacheRlPdIds.add(rlPdId);
            });
            rlPdIds.forEach(rlPdId -> {
                if (!cacheRlPdIds.contains(rlPdId)) {
                    noCacheRlPdIds.add(rlPdId);
                }
            });

            //查询未缓存的
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheRlPdIds);
            FaiList<Param> noCacheList = getListByConditions(aid, unionPriId, searchArg, null);

            Map<Integer, List<Param>> groupByRlPdIds = noCacheList.stream()
                    .collect(Collectors.groupingBy(x -> x.getInt(ProductBindGroupEntity.Info.RL_PD_ID)));

            if (!noCacheList.isEmpty()) {
                //添加缓存
                for (Integer rlPdId:groupByRlPdIds.keySet()) {
                    ProductBindTagCache.addCacheList(aid, unionPriId, rlPdId, new FaiList<>(groupByRlPdIds.get(rlPdId)));
                }
                //组装完整的结果
                cacheList.addAll(noCacheList);
            }
        } finally {
            LockUtil.PdBindTagLock.readUnLock(aid);
        }
        return cacheList;
    }

    public FaiList<Param> getListByConditions(Integer aid, Integer unionPriId, SearchArg searchArg, FaiList<String> onlyNeedFields) {
        //无searchArg
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        //有searchArg，无查询条件
        if (searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //避免查询过来的条件已经包含这两个查询条件,就先删除，防止重复添加查询条件
        searchArg.matcher.remove(ProductBindTagEntity.Info.AID);
        searchArg.matcher.remove(ProductBindTagEntity.Info.UNION_PRI_ID);
        //有searchArg，有查询条件，加多两个查询条件
        searchArg.matcher.and(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        //查询
        Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, tmpRef, onlyNeedFields);
        FaiList<Param> result = new FaiList<>();
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;matcher=%s", m_flow, aid, unionPriId, searchArg.matcher);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            result.addAll(tmpRef.value);
        }
        if (result.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;matcher=%s", m_flow, aid, unionPriId, searchArg.matcher);
        }
        return result;
    }

    public FaiList<Integer> getRlPdIdsByTagIds(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        //查询条件
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds);
        FaiList<Integer> rlPdIds = new FaiList<>();
        FaiList<Param> result = getListByConditions(aid, unionPriId, searchArg, null);
        result.forEach(param -> {
            Integer rlPdId = param.getInt(ProductBindTagEntity.Info.RL_PD_ID);
            rlPdIds.add(rlPdId);
        });
        return rlPdIds;
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductBindTagCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductBindTagCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        FaiList<Param> result = getListByConditions(aid, unionPriId, null, null);
        int count = result.size();
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductBindTagCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public int delPdBindTagList(int aid, int unionPriId, int rlPdId, FaiList<Integer> delRlTagIds) {
        int rt;
        if(Util.isEmptyList(delRlTagIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        matcher.and(ProductBindTagEntity.Info.RL_TAG_ID, ParamMatcher.IN, delRlTagIds);
        return delPdBindTag(aid, unionPriId, matcher);
    }

    public void addPdBindTagList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Integer> addRlTagIds) {
        int rt;
        if(Util.isEmptyList(addRlTagIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(int rlTagId : addRlTagIds) {
            Param info = new Param();
            info.setInt(ProductBindTagEntity.Info.AID, aid);
            info.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindTagEntity.Info.RL_TAG_ID, rlTagId);
            info.setInt(ProductBindTagEntity.Info.UNION_PRI_ID, unionPriId);
            info.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
            info.setCalendar(ProductBindTagEntity.Info.CREATE_TIME, now);
            addList.add(info);
        }
        rt = m_dao.batchInsert(addList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind tag error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public int delPdBindTagList(int aid, int unionPriId, FaiList<Integer> delRlPdIds) {
        int rt;
        if(Util.isEmptyList(delRlPdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.RL_PD_ID, ParamMatcher.IN, delRlPdIds);
        return delPdBindTag(aid, unionPriId, matcher);
    }

    public int delPdBindTag(Integer aid, Integer unionPriId, ParamMatcher matcher) {
        int rt;
        if (matcher == null) {
            matcher = new ParamMatcher();
        }
        matcher.remove(ProductBindTagEntity.Info.AID);
        matcher.remove(ProductBindTagEntity.Info.UNION_PRI_ID);
        if (aid != null) {
            matcher.and(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        }
        if (unionPriId != null) {
            matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        }

        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher);
        }
        Log.logStd("delPdBindTagList ok;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher);
        return refRowCount.value;
    }

    public int delPdBindTagList(int aid, FaiList<Integer> pdIds) {
        int rt;
        if(Util.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        return delPdBindTag(aid, null, matcher);
    }

    /**
     * 清空指定aid+unionPriId的数据
     */
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(Util.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        delPdBindTag(aid, null, matcher);
    }
}
