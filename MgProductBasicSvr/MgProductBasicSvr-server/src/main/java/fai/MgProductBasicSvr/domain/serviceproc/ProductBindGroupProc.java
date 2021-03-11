package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindGroupDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
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
        rt = m_dao.batchInsert(addList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind group error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public void delPdBindGroupList(int aid, int unionPriId, int rlPdId, FaiList<Integer> rlGroupIds) {
        int rt;
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.IN, rlGroupIds);
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPdId=%d;rlGroupIds=%s;", m_flow, aid, rlPdId, rlGroupIds);
        }
        Log.logStd("delPdBindGroupList ok;flow=%d;aid=%d;rlPdId=%d;rlGroupIds=%s;", m_flow, aid, rlPdId, rlGroupIds);
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

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> rlPdIds) {
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "arrgs error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
        }
        // 缓存中获取
        List<String> rlPdIdStrs = rlPdIds.stream().map(String::valueOf).collect(Collectors.toList());
        FaiList<Param> list = ProductBindGroupCache.getCacheList(aid, unionPriId, rlPdIdStrs);
        if(list != null && !list.isEmpty()) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;aid=%d;unionPriId=%d;", aid, unionPriId);
            return listRef.value;
        }
        // 添加到缓存
        ProductBindGroupCache.addCacheList(aid, unionPriId, list);
        return listRef.value;
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
