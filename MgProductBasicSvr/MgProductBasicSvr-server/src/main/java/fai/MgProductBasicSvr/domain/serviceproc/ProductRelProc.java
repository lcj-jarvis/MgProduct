package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelValObj;
import fai.MgProductBasicSvr.domain.repository.cache.ProductRelCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductRelProc {

    public ProductRelProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductRelDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public int addProductRel(int aid, int unionPriId, Param relData) {
        int rt;
        if(Str.isEmpty(relData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        int count = getPdRelCount(aid, unionPriId);
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductRelValObj.Limit.COUNT_MAX);
        }
        int rlPdId = createAndSetRlPdId(aid, unionPriId, relData);
        rt = m_dao.insert(relData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product rel error;flow=%d;aid=%d;uid=%d;relData=%s;", m_flow, aid, unionPriId, relData);
        }
        return rlPdId;
    }

    public FaiList<Integer> batchAddProductRel(int aid, int unionPriId, Param pdInfo, FaiList<Param> relDataList) {
        int rt;
        if(relDataList == null || relDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;relDataList=%s;", m_flow, aid, relDataList);
        }

        int count = getPdRelCount(aid, unionPriId);
        if(count >= ProductRelValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;uid=%d;count=%d;limit=%d;", m_flow, aid, unionPriId, count, ProductRelValObj.Limit.COUNT_MAX);
        }

        int maxId = m_dao.getId(aid, unionPriId);
        FaiList<Integer> rlPdIds = new FaiList<Integer>();
        for(Param relData : relDataList) {
            if(!Str.isEmpty(pdInfo)) {
                relData.assign(pdInfo, ProductRelEntity.Info.PD_ID);
                relData.assign(pdInfo, ProductRelEntity.Info.PD_TYPE);
            }
            Integer curPdId = relData.getInt(ProductRelEntity.Info.PD_ID);
            if(curPdId == null) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error, pdId is null;flow=%d;aid=%d;uid=%d;", m_flow, aid);
            }

            int rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, ++maxId);
            if(rlPdId > maxId) {
                maxId = rlPdId;
            }
            rlPdIds.add(rlPdId);
        }

        rt = m_dao.batchInsert(relDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product rel error;flow=%d;aid=%d;", m_flow, aid);
        }
        m_dao.updateId(aid, unionPriId, maxId, false);
        return rlPdIds;
    }

    public int getPdRelCount(int aid, int unionPriId) {
        Param dataStatus = getDataStatus(aid, unionPriId);
        Integer count = dataStatus.getInt(DataStatus.Info.TOTAL_SIZE);
        if(count == null) {
            throw new MgException(Errno.ERROR, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return count;
    }

    public int delProductRel(int aid, int unionPriId, FaiList<Integer> rlPdIds, boolean softDel) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdIds=%s", m_flow, aid, unionPriId, rlPdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            return updatePdRel(aid, matcher, updater);
        }
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;unionPridId=%d;rlPdIds=%d;", m_flow, aid, unionPriId, rlPdIds);
        }
        return refRowCount.value;
    }

    /**
     * 根据pdId, 删除所有关联数据
     */
    public FaiList<Param> delProductRelByPdId(int aid, FaiList<Integer> pdIds, boolean softDel) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get unionPriId error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            return null;
        }
        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            updatePdRel(aid, matcher, updater);
            return listRef.value;
        }
        rt = m_dao.delete(matcher);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product rel error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }

        return listRef.value;
    }

    public void setSingle(int aid, int unionPriId, Integer rlPdId, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.EQ, rlPdId);

        updatePdRel(aid, matcher, updater);
    }

    public void setPdRels(int aid, int unionPriId, FaiList<Integer> rlPdIds, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }
        ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);

        updatePdRel(aid, matcher, updater);
    }

    public static ParamUpdater assignUpdate(int flow, int aid, ParamUpdater recvUpdater) {
        int rt;
        if (recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "updater=null;aid=%d;", flow, aid);
        }
        Param recvInfo = recvUpdater.getData();

        Param data = new Param();
        data.assign(recvInfo, ProductRelEntity.Info.PD_TYPE);
        data.assign(recvInfo, ProductRelEntity.Info.LAST_SID);
        data.assign(recvInfo, ProductRelEntity.Info.LAST_UPDATE_TIME);
        data.assign(recvInfo, ProductRelEntity.Info.STATUS);
        data.assign(recvInfo, ProductRelEntity.Info.UP_SALE_TIME);
        if(data.isEmpty()) {
            Log.logDbg("no pd rel field changed;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        Calendar now = Calendar.getInstance();
        data.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        data.setCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        ParamUpdater updater = new ParamUpdater(data);
        updater.add(recvUpdater.getOpList(ProductRelEntity.Info.FLAG));
        return updater;
    }

    private int updatePdRel(int aid, ParamMatcher matcher, ParamUpdater updater) {
        int rt;
        if(updater == null || updater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, updater is null;flow=%d;aid=%d;", m_flow, aid);
        }
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;", m_flow, aid);
        }
        matcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.update(updater, matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRel error;flow=%d;aid=%d;", m_flow, aid);
        }
        return refRowCount.value;
    }

    public Param getProductRel(int aid, int unionPriId, int rlPdId) {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;uid=%d;rlPdId=%s", m_flow, aid, unionPriId, rlPdId);
        }
        Param info = ProductRelCacheCtrl.InfoCache.getCacheInfo(aid, unionPriId, rlPdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> rlPdIds = new HashSet<>();
        rlPdIds.add(rlPdId);
        FaiList<Param> relList = getList(aid, unionPriId, rlPdIds);
        if(relList == null || relList.isEmpty()) {
            return new Param();
        }

        return relList.get(0);
    }

    public FaiList<Param> getProductRelList(int aid, int unionPriId, FaiList<Integer> rlPdIdList) {
        int rt;
        if(rlPdIdList == null || rlPdIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get rlPdIdList is empty;aid=%d;rlPdIdList=%s;", aid, rlPdIdList);
        }
        HashSet<Integer> rlPdIds = new HashSet<Integer>(rlPdIdList);

        return getList(aid, unionPriId, rlPdIds);
    }

    /** pdId+unionPriId 和 rlPdId的映射 **/
    public FaiList<Param> getRlPdIdList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get error, pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.RlIdRelCache.getCacheList(aid, unionPriId, pdIds);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == pdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<Integer>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        //只查aid+pdId+unionPriId+rlPdId
        rt = m_dao.select(searchArg, tmpRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get product rel info error;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
            // 添加到缓存
            ProductRelCacheCtrl.RlIdRelCache.addCacheList(aid, unionPriId, tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        return list;
    }

    public FaiList<Param> getBoundUniPriIds(int aid, FaiList<Integer> pdIds) {
        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        //只查aid+pdId+unionPriId+rlPdId
        int rt = m_dao.select(searchArg, listRef, ProductRelEntity.Info.AID, ProductRelEntity.Info.UNION_PRI_ID, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getRlPdInfoByPdIds fail;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);

        }

        return listRef.value;
    }

    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductRelCacheCtrl.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductRelCacheCtrl.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getPdRelCountFromDB(aid, unionPriId);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductRelCacheCtrl.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    /**
     * 在 aid + unionPriId 下搜索
     */
    public FaiList<Param> searchFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);

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

    private int getPdRelCountFromDB(int aid, int unionPriId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }

        return countRef.value;
    }

    private int createAndSetRlPdId(int aid, int unionPriId, Param relData) {
        int rt;
        Integer rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID, 0);
        if(rlPdId <= 0) {
            rlPdId = m_dao.buildId(aid, unionPriId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId build error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }else {
            rlPdId = m_dao.updateId(aid, unionPriId, rlPdId, false);
            if (rlPdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "rlPdId update error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
        }
        relData.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
        return rlPdId;
    }

    private FaiList<Param> getList(int aid, int unionPriId, HashSet<Integer> rlPdIds) {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIds is empty;aid=%d;rlPdIds=%s;", aid, rlPdIds);
        }
        List<String> rlPdIdStrs = rlPdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductRelCacheCtrl.InfoCache.getCacheList(aid, unionPriId, rlPdIdStrs);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == rlPdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(rlPdIds);
        for(Param info : list) {
            Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
            noCacheIds.remove(rlPdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        searchArg.matcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, noCacheIds);
        //searchArg.matcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.NE, ProductRelValObj.Status.DEL);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return list;
        }

        // 添加到缓存
        ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, tmpRef.value);

        return list;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductGroupRelDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductRelDaoCtrl m_dao;
}
