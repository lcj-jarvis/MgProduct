package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.BasicSagaEntity;
import fai.MgProductBasicSvr.domain.entity.BasicSagaValObj;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindGroupDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductBindGroupSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProductBindGroupProc {
    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    public ProductBindGroupProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_dao = ProductBindGroupDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(m_xid)) {
            this.m_sagaDao = ProductBindGroupSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
        }
        init(tc);
    }

    public FaiList<Param> getPdBindGroupList(int aid, int unionPriId, FaiList<Integer> rlPdIdList) {
        if(rlPdIdList == null || rlPdIdList.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "get rlPdIdList is empty;aid=%d;rlPdIdList=%s;", aid, rlPdIdList);
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>(rlPdIdList);
        return getList(aid, unionPriId, rlPdIds);
    }

    public void updateBindGroupList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Integer> rlGroupIdList) {
        if(rlGroupIdList == null) {
            return;
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>();
        rlPdIds.add(rlPdId);
        FaiList<Param> list = getList(aid, unionPriId, rlPdIds);
        FaiList<Integer> delGroupIds = new FaiList<>();
        FaiList<Integer> oldGroupIds = Utils.getValList(list, ProductBindGroupEntity.Info.RL_GROUP_ID);
        for(Integer rlGroupId : oldGroupIds) {
            if(rlGroupIdList.contains(rlGroupId)) {
                // 移除已存在的rlGroupId，rlGroupIdList中剩下的就是要新增的
                rlGroupIdList.remove(rlGroupId);
            }else {
                // 传过来的rlGroupIdList中不存在的就是要删除的
                delGroupIds.add(rlGroupId);
            }
        }

        // 新增
        if(!rlGroupIdList.isEmpty()) {
            addPdBindGroupList(aid, unionPriId, rlPdId, pdId, rlGroupIdList);
        }

        // 删除
        if(!delGroupIds.isEmpty()) {
            delPdBindGroupList(aid, unionPriId, rlPdId, delGroupIds);
        }
    }

    public void addList4SagaRollback(FaiList<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        for(Param info : list) {
            info.remove(BasicSagaEntity.Common.XID);
            info.remove(BasicSagaEntity.Common.BRANCH_ID);
            info.remove(BasicSagaEntity.Common.SAGA_OP);
        }
        int rt = m_dao.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind group error;flow=%d;list=%s;", m_flow, list);
        }
        Log.logStd("rollback bind groups ok;list=%s;", list);
    }

    public void addPdBindGroupList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Integer> rlGroupIdList) {
        int rt;
        if(rlGroupIdList == null || rlGroupIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<>();
        FaiList<Param> sagaList = new FaiList<>();
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

            // 开启了分布式事务，记录添加数据的主键
            if(addSaga) {
                Param sagaInfo = new Param();
                sagaInfo.setInt(ProductBindGroupEntity.Info.AID, aid);
                sagaInfo.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
                sagaInfo.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
                sagaInfo.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);

                sagaInfo.setString(BasicSagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(BasicSagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                sagaInfo.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.ADD);
                sagaList.add(sagaInfo);
            }
        }
        rt = m_dao.batchInsert(addList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind group error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 使用分布式事务时，记录新增的数据
        addSagaList(aid, sagaList);
        Log.logStd("add bind groups ok;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlGroupIds=%s;", aid, unionPriId, rlPdId, pdId, rlGroupIdList);
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
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindGroupEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            long branchId = RootContext.getBranchId();
            for(Param info : list) {
                info.setString(BasicSagaEntity.Common.XID, m_xid);
                info.setLong(BasicSagaEntity.Common.BRANCH_ID, branchId);
                info.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.DEL);
            }
            // 插入
            addSagaList(aid, list);
        }

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

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = searchFromDb(aid, searchArg, null);
            for(Param info : list) {
                info.setString(BasicSagaEntity.Common.XID, m_xid);
                info.setLong(BasicSagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                info.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.DEL);
            }
            // 插入
            addSagaList(aid, list);
        }

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

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
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
        FaiList<Param> tmpList = searchFromDb(aid, searchArg, null);
        if(!Utils.isEmptyList(tmpList)) {
            list.addAll(tmpList);
            Map<Integer, List<Param>> groupByRlPdId = tmpList.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductBindGroupEntity.Info.RL_PD_ID)));
            for(Integer rlPdId : groupByRlPdId.keySet()) {
                // 添加到缓存
                ProductBindGroupCache.addCacheList(aid, unionPriId, rlPdId, new FaiList<>(groupByRlPdId.get(rlPdId) ));
            }
        }

        return list;
    }

    /**
     * saga补偿
     * 绑定关系表只会有新增和删除操作
     */
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("bind group need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(BasicSagaEntity.Common.SAGA_OP)));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(BasicSagaValObj.SagaOp.ADD));

        // 回滚删除操作
        rollback4Delete(aid, groupBySagaOp.get(BasicSagaValObj.SagaOp.DEL));
    }

    /**
     * 新增的数据补偿：根据主键删除
     */
    private void rollback4Add(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        int rt;
        for(Param info : list) {
            int unionPriId = info.getInt(ProductBindGroupEntity.Info.UNION_PRI_ID);
            int rlPdId = info.getInt(ProductBindGroupEntity.Info.RL_PD_ID);
            int rlGroupId = info.getInt(ProductBindGroupEntity.Info.RL_GROUP_ID);

            ParamMatcher matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindGroupEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            matcher.and(ProductBindGroupEntity.Info.RL_GROUP_ID, ParamMatcher.EQ, rlGroupId);

            rt = m_dao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del bind group error;flow=%d;aid=%d;xid=%s;matcher=%s;", m_flow, aid, m_xid, matcher.toJson());
            }
        }

        Log.logStd("rollback add ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    /**
     * 删除的数据补偿：插入已删除的数据
     */
    private void rollback4Delete(int aid, List<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }

        for(Param relInfo : list) {
            relInfo.remove(BasicSagaEntity.Common.XID);
            relInfo.remove(BasicSagaEntity.Common.BRANCH_ID);
            relInfo.remove(BasicSagaEntity.Common.SAGA_OP);
        }
        int rt = m_dao.batchInsert(new FaiList<>(list), null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "add list error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(BasicSagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(BasicSagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        int rt = m_sagaDao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get saga list error;aid=%d;xid=%s;branchId=%d;", aid, xid, branchId);
        }
        return tmpRef.value;
    }

    private void addSagaList(int aid, FaiList<Param> sagaList) {
        // 是否开启了分布式事务
        if(!addSaga || Utils.isEmptyList(sagaList)) {
            return;
        }
        // 如果开启了分布式事务，那么本地事务必须关闭auto commit
        // 因为这时候肯定要操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }

        int rt = m_sagaDao.batchInsert(sagaList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga batch insert product bind group error;flow=%d;aid=%d;xid=%s;", m_flow, aid, m_xid);
        }
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        // 事务注册失败则设置dao为null，防止在TransactionCtrl无法控制的情况下使用dao
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }

        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductBindGroupSagaDaoCtrl err;");
        }
    }

    private int m_flow;
    private String m_xid;
    private boolean addSaga;
    private ProductBindGroupSagaDaoCtrl m_sagaDao;
    private ProductBindGroupDaoCtrl m_dao;
}
