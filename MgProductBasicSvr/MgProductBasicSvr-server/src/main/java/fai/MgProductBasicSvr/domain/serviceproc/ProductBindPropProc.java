package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.entity.ProductBindPropEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.repository.dao.ProductBindPropDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

public class ProductBindPropProc {

    public ProductBindPropProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public FaiList<Param> getPdBindPropList(int aid, int unionPriId, int rlPdId) {
        return getList(aid, unionPriId, rlPdId);
    }

    public void addPdBindPropList(int aid, int unionPriId, int rlPdId, int pdId, FaiList<Param> infoList) {
        int rt;
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;");
        }
        FaiList<Param> addList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(Param tmpinfo : infoList) {
            Param info = new Param();
            int rlPropId = tmpinfo.getInt(ProductBindPropEntity.Info.RL_PROP_ID, 0);
            int propValId = tmpinfo.getInt(ProductBindPropEntity.Info.PROP_VAL_ID, 0);
            if(rlPropId == 0 || propValId == 0) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;rlPropId=%d;propValId=%d;", m_flow, aid, unionPriId, rlPdId, rlPropId, propValId);
            }


            info.setInt(ProductBindPropEntity.Info.AID, aid);
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, rlPropId);
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, propValId);
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, unionPriId);
            info.setInt(ProductBindPropEntity.Info.PD_ID, pdId);
            info.setCalendar(ProductBindPropEntity.Info.CREATE_TIME, now);
            addList.add(info);
        }
        rt = m_bindPropDao.batchInsert(addList, null, true);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product bind prop error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public int delPdBindPropList(int aid, int unionPriId, int rlPdId, FaiList<Param> delList) {
        int rt;
        if(delList == null || delList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error;flow=%d;aid=%d;", m_flow, aid);
        }
        int delCount = 0;
        for(Param info : delList) {
            int rlPropId = info.getInt(ProductBindPropEntity.Info.RL_PROP_ID);
            int propValId = info.getInt(ProductBindPropEntity.Info.PROP_VAL_ID);
            ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
        Log.logStd("delPdBindPropList ok;flow=%d;aid=%d;rlPdId=%d;delCount=%d;", m_flow, aid, rlPdId, delCount);
        return delCount;
    }

    public int delPdBindProp(int aid, int unionPriId, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, matcher is empty;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        Ref<Integer> refRowCount = new Ref<>();
        matcher.and(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        rt = m_bindPropDao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPropId=%d;delPropValIds=%d;", m_flow, aid);
        }

        return refRowCount.value;
    }

    public int delPdBindPropByValId(int aid, int unionPriId, int rlPropId, FaiList<Integer> delPropValIds) {
        int rt;
        if(Util.isEmptyList(delPropValIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, delPropValIds is empty;flow=%d;aid=%d;", m_flow, aid);
        }
        Ref<Integer> refRowCount = new Ref<>();
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, delPropValIds);
        rt = m_bindPropDao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPropId=%d;delPropValIds=%d;", m_flow, aid, rlPropId, delPropValIds);
        }

        return refRowCount.value;
    }

    public int delPdBindPropByPropId(int aid, int unionPriId, FaiList<Integer> rlPropIds) {
        int rt;
        if(Util.isEmptyList(rlPropIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, rlPropIds is empty;flow=%d;aid=%d;", m_flow, aid);
        }
        Ref<Integer> refRowCount = new Ref<>();
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlPropIds);
        rt = m_bindPropDao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del info error;flow=%d;aid=%d;rlPropIds=%d;uid=%d;", m_flow, aid, rlPropIds, unionPriId);
        }

        return refRowCount.value;
    }

    /**
     * 根据参数id+参数值id的列表，筛选出商品业务id
     * 目前是直接查db
     */
    public FaiList<Integer> getRlPdByPropVal(int aid, int unionPriId, FaiList<Param> proIdsAndValIds) {
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
            searchRlPdByPropVal(aid, unionPriId, rlPropId, propValId, rlPdIds, list);
            if(rlPdIds.isEmpty()) {
                break;
            }
        }

        return rlPdIds;
    }

    private void searchRlPdByPropVal(int aid, int unionPriId, int rlPropId, int propValId, FaiList<Integer> rlPdIds, FaiList<Param> searchList) {
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, propValId);
        if(!rlPdIds.isEmpty()) {
            matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        }
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
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

    public FaiList<Param> searchFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bindPropDao.select(searchArg, listRef, selectFields);
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

    private FaiList<Param> getList(int aid, int unionPriId, int rlPdId) {
        // 缓存中获取
        FaiList<Param> list = ProductBindPropCache.getCacheList(aid, unionPriId, rlPdId);
        if(list != null && !list.isEmpty()) {
            return list;
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
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
        ProductBindPropCache.setCacheList(aid, unionPriId, rlPdId, list);
        return list;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_bindPropDao)) {
            throw new MgException("registered ProductBindPropDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductBindPropDaoCtrl m_bindPropDao;
}
