package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.BasicSagaEntity;
import fai.MgProductBasicSvr.domain.entity.BasicSagaValObj;
import fai.MgProductBasicSvr.domain.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindPropDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.saga.ProductBindPropSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProductBindPropProc {

    public ProductBindPropProc(int flow, int aid, TransactionCtrl tc) {
        this(flow, aid, tc, null, false);
    }

    public ProductBindPropProc(int flow, int aid, TransactionCtrl tc, String xid, boolean addSaga) {
        this.m_flow = flow;
        this.m_bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
        this.m_xid = xid;
        if(!Str.isEmpty(xid)) {
            this.m_sagaDao = ProductBindPropSagaDaoCtrl.getInstance(flow, aid);
            this.addSaga = addSaga;
        }
        init(tc);
    }

    public FaiList<Param> getPdBindPropList(int aid, int unionPriId, int sysType, int rlPdId) {
        return getList(aid, unionPriId, sysType, rlPdId);
    }

    public void addPdBindPropList(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        for(Param tmpinfo : infoList) {
            Param info = new Param();
            int rlPropId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PROP_ID, 0);
            int propValId = tmpinfo.getInt(ProductBindPropEntity.Info.PROP_VAL_ID, 0);
            if(rlPropId == 0 || propValId == 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, unionPriId, rlPdId, rlPropId, propValId);
            }

            info.setInt(ProductBindPropEntity.Info.SYS_TYPE, sysType);
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropId);
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValId);
            info.setInt(ProductBindPropEntity.Info.PD_ID, pdId);
            addList.add(info);
        }
        batchBindPropList(aid, unionPriId, addList);
    }

    public void batchBindPropList(int aid, int unionPriId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        FaiList<Param> sagaList = new FaiList<>();
        for(Param tmpinfo : infoList) {
            Param info = new Param();
            int rlPropId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PROP_ID, 0);
            int propValId = tmpinfo.getInt(ProductBindPropEntity.Info.PROP_VAL_ID, 0);
            int pdId = tmpinfo.getInt(ProductBindPropEntity.Info.PD_ID, 0);
            int rlPdId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PD_ID, 0);
            if(rlPropId <= 0 || propValId <= 0 || pdId <= 0 || rlPdId <= 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;unionPriId=%d;info=%s;", m_flow, aid, unionPriId, info);
            }
            int sysType = tmpinfo.getInt(ProductBindPropEntity.Info.SYS_TYPE, 0);

            info.setInt(ProductBindPropEntity.Info.AID, aid);
            info.setInt(ProductBindPropEntity.Info.SYS_TYPE, sysType);
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropId);
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValId);
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, unionPriId);
            info.setInt(ProductBindPropEntity.Info.PD_ID, pdId);
            info.setCalendar(ProductBindPropEntity.Info.CREATE_TIME, now);
            addList.add(info);

            if(addSaga) {
                Param sagaInfo = new Param();
                sagaInfo.setInt(ProductBindPropEntity.Info.AID, aid);
                sagaInfo.setInt(ProductBindPropEntity.Info.SYS_TYPE, sysType);
                sagaInfo.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
                sagaInfo.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropId);
                sagaInfo.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValId);
                sagaInfo.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, unionPriId);

                sagaInfo.setString(BasicSagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(BasicSagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                sagaInfo.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.ADD);
                sagaList.add(sagaInfo);
            }
        }
        rt = m_bindPropDao.batchInsert(addList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind prop error;flow=%d;aid=%d;", m_flow, aid);
        }
        // 使用分布式事务时，记录下新增数据的主键
        addSagaList(aid, sagaList);
    }

    public void updatePdBindProp(int aid, int unionPriId, int sysType, int rlPdId, int pdId, FaiList<Param> bindPropList){
        if(bindPropList == null) {
            return;
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        searchArg.matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        FaiList<Param> oldList = searchFromDb(aid, searchArg, null);

        FaiList<Param> delList = new FaiList<>();
        for(Param info : oldList) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
            Param tmpInfo = Misc.getFirst(bindPropList, matcher);
            if(Str.isEmpty(tmpInfo)) {
                // 不存在, 说明是要删除的
                delList.add(info);
            }else {
                // 已存在, bindPropList移除，最后bindPropList剩余的是要新增的
                bindPropList.remove(tmpInfo);
            }
        }

        // 新增
        if(!bindPropList.isEmpty()) {
            addPdBindPropList(aid, unionPriId, sysType, rlPdId, pdId, bindPropList);
        }

        // 删除
        if(!delList.isEmpty()) {
            delPdBindPropList(aid, unionPriId, sysType, rlPdId, delList, true);
        }
    }

    /**
     * @param delList 要删除的数据
     * @param isComplete 传过来的delList是不是完整的数据
     * 如果是完整的数据，那分布式事务记录数据的时候，就不需要再查一次了
     * @return 删除数量
     */
    public int delPdBindPropList(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Param> delList, boolean isComplete) {
        int rt;
        if(delList == null || delList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        FaiList<Param> oldList = new FaiList<>();
        // 分布式事务下，传过来的delList如果是完整的数据，则直接记录到saga表
        // 如果不确定是完整的数据，则查出完整数据，记录到saga
        if(addSaga && !isComplete) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            searchArg.matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
            searchArg.matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            oldList = searchFromDb(aid, searchArg, null);
        }
        int delCount = 0;

        FaiList<Param> sagaList = new FaiList<>();

        for(Param info : delList) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            // 开启了分布式事务，记录删除的数据
            if(addSaga) {
                Param sagaInfo;
                if(isComplete) {
                   sagaInfo = info.clone();
                }else {
                    ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
                    matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
                    sagaInfo = Misc.getFirst(oldList, matcher);
                }
                // 要删除的数据不存在
                if(Str.isEmpty(sagaInfo)) {
                    continue;
                }
                sagaInfo.setString(BasicSagaEntity.Common.XID, m_xid);
                sagaInfo.setLong(BasicSagaEntity.Common.BRANCH_ID, RootContext.getBranchId());
                sagaInfo.setInt(BasicSagaEntity.Common.SAGA_OP, BasicSagaValObj.SagaOp.DEL);
                sagaList.add(sagaInfo);
            }
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
            Ref<Integer> refRowCount = new Ref<>();
            rt = m_bindPropDao.delete(matcher, refRowCount);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, rlPdId, rlPropId, propValId);
            }
            delCount += refRowCount.value;
        }

        // 插入
        addSagaList(aid, sagaList);

        Log.logStd("delPdBindPropList ok;flow=%d;aid=%d;rlPdId=%d;delCount=%d;", m_flow, aid, rlPdId, delCount);
        return delCount;
    }

    public int delPdBindProp(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null) {
            throw new MgException("matcher is null;aid=%d;", aid);
        }
        Ref<Integer> refRowCount = new Ref<>();
        matcher.and(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        rt = m_bindPropDao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, matcher.toJson());
        }

        Log.logStd("del bind prop ok;aid=%d;matcher=%s;", aid, matcher.toJson());
        return refRowCount.value;
    }

    public int delPdBindProp(int aid, FaiList<Integer> pdIds) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "del error, pdIds is empty;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        Ref<Integer> refRowCount = new Ref<>();
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        if(addSaga) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindPropEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
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

        rt = m_bindPropDao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;sql=%s;", m_flow, aid, matcher.toJson());
        }

        return refRowCount.value;
    }

    // 清空指定aid+unionPriId的数据
    public void clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt;
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "clearAcct error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        rt = m_bindPropDao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
        }
        Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPridId=%s;", m_flow, aid, unionPriIds);
    }

    /**
     * 根据参数id+参数值id的列表，筛选出商品业务id
     * 目前是直接查db
     */
    public FaiList<Integer> getRlPdByPropVal(int aid, int unionPriId, int sysType, FaiList<Param> proIdsAndValIds) {
        int rt;
        FaiList<Integer> rlPropIds = new FaiList<Integer>();
        FaiList<Integer> propValIds = new FaiList<Integer>();
        for(Param info : proIdsAndValIds) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            if(!rlPropIds.contains(rlPropId)) {
                rlPropIds.add(rlPropId);
            }
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            if(!propValIds.contains(propValId)) {
                propValIds.add(propValId);
            }
        }
        if(rlPropIds.isEmpty() || propValIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;rlPropIds or propValIds is empty;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        // 先将可能符合条件的数据查出来，再做筛选, 避免循环查db
        ParamMatcher sqlMatcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        sqlMatcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        sqlMatcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        sqlMatcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlPropIds);
        sqlMatcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, propValIds);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = sqlMatcher;
        rt = m_bindPropDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "select error bind prop;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        FaiList<Integer> rlPdIds = new FaiList<>();
        FaiList<Param> list = listRef.value;
        if (list == null || list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rlPdIds;
        }

        for(Param info : proIdsAndValIds) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            searchRlPdByPropVal(aid, unionPriId, sysType, rlPropId, propValId, rlPdIds, list);
            if(rlPdIds.isEmpty()) {
                break;
            }
        }

        return rlPdIds;
    }

    private void searchRlPdByPropVal(int aid, int unionPriId, int sysType, int rlPropId, int propValId, FaiList<Integer> rlPdIds, FaiList<Param> searchList) {
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
        if(!rlPdIds.isEmpty()) {
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> tmpList = searcher.getParamList(searchList);
        rlPdIds.clear();
        for(Param info : tmpList) {
            int rlPdId = info.getInt(ProductBindPropEntity.Info.RL_PD_ID);
            if(!rlPdIds.contains(rlPdId)) {
                rlPdIds.add(rlPdId);
            }
        }
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductBindPropCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductBindPropCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getCountFromDB(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductBindPropCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bindPropDao.select(searchArg, listRef, selectFields);
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
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_bindPropDao.selectCount(searchArg, countRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if(countRef.value == null) {
            countRef.value = 0;
        }
        return countRef.value;
    }

    private FaiList<Param> getList(int aid, int unionPriId, int sysType, int rlPdId) {
        // 缓存中获取
        FaiList<Param> list = ProductBindPropCache.getCacheList(aid, unionPriId, sysType, rlPdId);
        if(list != null && !list.isEmpty()) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        searchArg.matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bindPropDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
        }
        list = listRef.value;

        if (list == null){
            list = new FaiList<Param>();
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", m_flow, aid, unionPriId, rlPdId);
            return list;
        }
        // 添加到缓存
        ProductBindPropCache.setCacheList(aid, unionPriId, sysType, rlPdId, list);
        return list;
    }

    /**
     * saga补偿
     * 绑定关系表只会有新增和删除操作
     */
    public void rollback4Saga(int aid, long branchId) {
        FaiList<Param> list = getSagaList(aid, m_xid, branchId);
        if(list.isEmpty()) {
            Log.logStd("bind pdProp need rollback is empty;aid=%d;xid=%s;branchId=%s;", aid, m_xid, branchId);
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
            int unionPriId = info.getInt(ProductBindPropEntity.Info.UNION_PRI_ID);
            int sysType = info.getInt(ProductBindPropEntity.Info.SYS_TYPE);
            int rlPdId = info.getInt(ProductBindPropEntity.Info.RL_PD_ID);
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);
            matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
            matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);

            rt = m_bindPropDao.delete(matcher);
            if(rt != Errno.OK) {
                throw new MgException(rt, "del bind prop error;flow=%d;aid=%d;xid=%s;matcher=%s;", m_flow, aid, m_xid, matcher.toJson());
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
        int rt = m_bindPropDao.batchInsert(new FaiList<>(list), null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "add list error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
        }

        Log.logStd("rollback del ok;aid=%d;xid=%s;list=%s;", aid, m_xid, list);
    }

    private FaiList<Param> getSagaList(int aid, String xid, long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
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
        if(m_sagaDao.isAutoCommit() || m_bindPropDao.isAutoCommit()) {
            throw new MgException("dao need close auto commit;");
        }

        int rt = m_sagaDao.batchInsert(sagaList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "saga batch insert product bind prop error;flow=%d;aid=%d;xid=%s;", m_flow, aid, m_xid);
        }
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_bindPropDao)) {
            throw new MgException("registered ProductBindPropDaoCtrl err;");
        }
        if(m_sagaDao != null && !tc.register(m_sagaDao)) {
            throw new MgException("registered ProductBindPropSagaDaoCtrl err;");
        }
    }

    private int m_flow;
    private String m_xid;
    private boolean addSaga;
    private ProductBindPropSagaDaoCtrl m_sagaDao;
    private ProductBindPropDaoCtrl m_bindPropDao;
}
