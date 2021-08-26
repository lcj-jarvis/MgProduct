package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindTagCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindTagDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductBindTagSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author LuChaoJi
 * @date 2021-07-14 14:28
 */
public class ProductBindTagProc {

    private int m_flow;
    private String m_xid;
    private boolean addSaga;
    private ProductBindTagSagaDaoCtrl m_sagaDao;
    private ProductBindTagDaoCtrl m_dao;

    public ProductBindTagProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    public ProductBindTagProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_dao = ProductBindTagDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(m_xid)) {
            this.m_sagaDao = ProductBindTagSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
        }
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

        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductBindTagSagaDaoCtrl err;");
        }
    }

    public FaiList<Param> getPdBindTagList(int aid, int unionPriId, FaiList<Integer> pdIdList) {
        //去重
        HashSet<Integer> pdIds = new HashSet<>(pdIdList);
        //获取缓存的所有数据
        FaiList<Param> cacheList = ProductBindTagCache.getCacheList(aid, unionPriId, pdIds);
        if (cacheList == null) {
            cacheList = new FaiList<>();
        }
        cacheList.remove(null);
        if (cacheList.size() == pdIds.size()) {
            return cacheList;
        }

        LockUtil.PdBindTagLock.readLock(aid);
        try {
            //双检，防止缓存穿透
            cacheList = ProductBindTagCache.getCacheList(aid, unionPriId, pdIds);
            if (cacheList == null) {
                cacheList = new FaiList<>();
            }
            cacheList.remove(null);
            if (cacheList.size() == pdIds.size()) {
                return cacheList;
            }

            //获取未缓存的pdId
            FaiList<Integer> noCacheIds = new FaiList<>();
            noCacheIds.addAll(pdIds);
            cacheList.forEach(param -> {
                Integer pdId = param.getInt(ProductBindTagEntity.Info.PD_ID);
                noCacheIds.remove(pdId);
            });

            //查询未缓存的
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            searchArg.matcher.and(ProductBindTagEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
            FaiList<Param> noCacheList = getListByConditions(aid, searchArg, null);

            Map<Integer, List<Param>> groupByPdIds = noCacheList.stream()
                    .collect(Collectors.groupingBy(x -> x.getInt(ProductBindTagEntity.Info.PD_ID)));

            if (!noCacheList.isEmpty()) {
                //添加缓存
                for (Integer pdId : groupByPdIds.keySet()) {
                    ProductBindTagCache.addCacheList(aid, unionPriId, pdId, new FaiList<>(groupByPdIds.get(pdId)));
                }
                //组装完整的结果
                cacheList.addAll(noCacheList);
            }
        } finally {
            LockUtil.PdBindTagLock.readUnLock(aid);
        }
        return cacheList;
    }

    public FaiList<Param> getListByConditions(Integer aid, SearchArg searchArg, FaiList<String> onlyNeedFields) {
        //无searchArg
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        //有searchArg，无查询条件
        if (searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //有searchArg，有查询条件，加多两个查询条件
        if(searchArg.matcher.getValue(ProductBindTagEntity.Info.AID, ParamMatcher.EQ) == null) {
            throw new MgException("condition without aid;flow=%d;aid=%d;matcher=%s", m_flow, aid, searchArg.matcher.toJson());
        }

        //查询
        Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
        int rt = m_dao.select(searchArg, tmpRef, onlyNeedFields);
        FaiList<Param> result = new FaiList<>();
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;matcher=%s", m_flow, aid, searchArg.matcher.toJson());
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            result.addAll(tmpRef.value);
        }
        if (result.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s", m_flow, aid, searchArg.matcher.toJson());
        }
        return result;
    }

    public FaiList<Integer> getRlPdIdsByTagIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlTagIds) {
        //查询条件
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindTagEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        searchArg.matcher.and(ProductBindTagEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds);
        FaiList<Integer> rlPdIds = new FaiList<>();
        FaiList<Param> result = getListByConditions(aid, searchArg, null);
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
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        FaiList<Param> result = getListByConditions(aid, searchArg, null);
        int count = result.size();
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductBindTagCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public int delPdBindTagList(int aid, int unionPriId, int pdId, FaiList<Integer> delRlTagIds) {
        int rt;
        if(Utils.isEmptyList(delRlTagIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(ProductBindTagEntity.Info.RL_TAG_ID, ParamMatcher.IN, delRlTagIds);

        return delPdBindTag(aid, unionPriId, matcher);
    }

    public void addPdBindTagList(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Integer> addRlTagIds) {
        int rt;
        if(Utils.isEmptyList(addRlTagIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        for(int rlTagId : addRlTagIds) {
            Param info = new Param();
            info.setInt(ProductBindTagEntity.Info.SYS_TYPE, sysType);
            info.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindTagEntity.Info.RL_TAG_ID, rlTagId);
            info.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
            addList.add(info);
        }
        batchBindTagList(aid, unionPriId, addList);
    }

    public void batchBindTagList(int aid, int unionPriId, FaiList<Param> infoList) {
        int rt;
        if(Utils.isEmptyList(infoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        FaiList<Param> sagaList = new FaiList<>();
        for(Param info : infoList) {
            int rlPdId = info.getInt(ProductBindTagEntity.Info.RL_PD_ID, 0);
            int rlTagId = info.getInt(ProductBindTagEntity.Info.RL_TAG_ID, 0);
            int pdId = info.getInt(ProductBindTagEntity.Info.PD_ID, 0);
            if(rlPdId <= 0 || rlTagId <= 0 || pdId <= 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;info=%s;", info);
            }
            int sysType = info.getInt(ProductBindGroupEntity.Info.SYS_TYPE, 0);
            Param addData = new Param();
            addData.setInt(ProductBindTagEntity.Info.AID, aid);
            addData.setInt(ProductBindTagEntity.Info.SYS_TYPE, sysType);
            addData.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
            addData.setInt(ProductBindTagEntity.Info.RL_TAG_ID, rlTagId);
            addData.setInt(ProductBindTagEntity.Info.UNION_PRI_ID, unionPriId);
            addData.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
            addData.setCalendar(ProductBindTagEntity.Info.CREATE_TIME, now);
            addList.add(addData);

            if(addSaga) {
                Param sagaInfo = new Param();
                sagaInfo.setInt(ProductBindTagEntity.Info.AID, aid);
                sagaInfo.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
                sagaInfo.setInt(ProductBindTagEntity.Info.RL_TAG_ID, rlTagId);
                sagaInfo.setInt(ProductBindTagEntity.Info.UNION_PRI_ID, unionPriId);

                sagaInfo.setString(SagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(SagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                sagaInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                sagaInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                sagaList.add(sagaInfo);
            }
        }
        rt = m_dao.batchInsert(addList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind tag error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 使用分布式事务时，记录下新增的数据主键
        addSagaList(aid, sagaList);
        Log.logStd("batch bind tags ok;aid=%d;uid=%d;addRlTags=%s;", aid, unionPriId, addList);
    }

    public void updateBindTagList(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Integer> rlTagIds) {
        if(rlTagIds == null) {
            return;
        }
        FaiList<Integer> pdIds = new FaiList<>();
        pdIds.add(pdId);
        FaiList<Param> list = getPdBindTagList(aid, unionPriId, pdIds);
        FaiList<Integer> oldTagIds = Utils.getValList(list, ProductBindTagEntity.Info.RL_TAG_ID);

        FaiList<Integer> delTagIds = new FaiList<>();
        for(Integer rlTagId : oldTagIds) {
            if(rlTagIds.contains(rlTagId)) {
                // 移除已存在的rlTagId，rlTagIds中剩下的就是要新增的
                rlTagIds.remove(rlTagId);
            }else {
                // 传过来的rlTagIds中不存在的就是要删除的
                delTagIds.add(rlTagId);
            }
        }

        // 新增
        if(!rlTagIds.isEmpty()) {
            addPdBindTagList(aid, unionPriId, sysType, rlPdId, pdId, rlTagIds);
        }

        // 删除
        if(!delTagIds.isEmpty()) {
            delPdBindTagList(aid, unionPriId, pdId, delTagIds);
        }
    }

    public int delPdBindTagList(int aid, int unionPriId, FaiList<Integer> delPdIds) {
        int rt;
        if(Utils.isEmptyList(delPdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.PD_ID, ParamMatcher.IN, delPdIds);
        return delPdBindTag(aid, unionPriId, matcher);
    }

    public int delPdBindTag(Integer aid, Integer unionPriId, ParamMatcher matcher) {
        int rt;
        if (matcher == null) {
            matcher = new ParamMatcher();
        }
        if (aid != null) {
            matcher.and(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        }
        if (unionPriId != null) {
            matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        }

        // 开启了分布式事务，记录删除的数据
        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            FaiList<Param> list = getListByConditions(aid, searchArg, null);
            Calendar now = Calendar.getInstance();
            for(Param info : list) {
                info.setString(SagaEntity.Common.XID, m_xid);
                info.setLong(SagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
                info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            }
            // 插入
            addSagaList(aid, list);
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
        if(Utils.isEmptyList(pdIds)) {
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
        if(Utils.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        delPdBindTag(aid, null, matcher);
    }

    /**
     * saga补偿
     * 绑定关系表只会有新增和删除操作
     */
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("bind tag need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
            return;
        }
        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = list.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚新增操作
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚删除操作
        rollback4Delete(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));
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
            int unionPriId = info.getInt(ProductBindTagEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(ProductBindTagEntity.Info.PD_ID);
            int rlTagId = info.getInt(ProductBindTagEntity.Info.RL_TAG_ID);

            ParamMatcher matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindTagEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            matcher.and(ProductBindTagEntity.Info.RL_TAG_ID, ParamMatcher.EQ, rlTagId);

            rt = m_dao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del bind tag error;flow=%d;aid=%d;xid=%s;matcher=%s;", m_flow, aid, m_xid, matcher.toJson());
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
            relInfo.remove(SagaEntity.Common.XID);
            relInfo.remove(SagaEntity.Common.BRANCH_ID);
            relInfo.remove(SagaEntity.Common.SAGA_OP);
            relInfo.remove(SagaEntity.Common.SAGA_TIME);
        }
        int rt = m_dao.batchInsert(new FaiList<>(list), null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "add list error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(SagaEntity.Common.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Common.BRANCH_ID, ParamMatcher.EQ, branchId);
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
        // 因为这时候肯定是操作多张表的数据
        if(m_sagaDao.isAutoCommit() || m_dao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }

        int rt = m_sagaDao.batchInsert(sagaList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga batch insert product bind tag error;flow=%d;aid=%d;xid=%s;", m_flow, aid, m_xid);
        }
    }
}
