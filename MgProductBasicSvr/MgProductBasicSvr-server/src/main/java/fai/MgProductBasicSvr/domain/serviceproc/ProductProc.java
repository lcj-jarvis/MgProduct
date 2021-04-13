package fai.MgProductBasicSvr.domain.serviceproc;

import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.MgProductBasicSvr.domain.repository.cache.ProductCacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductProc {
    public ProductProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    public int addProduct(int aid, Param pdData) {
        int rt;
        if(Str.isEmpty(pdData)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdData);
        }
        int count = getPdCount(aid);
        if(count >= ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        int pdId = creatAndSetId(aid, pdData);
        rt = m_dao.insert(pdData);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        return pdId;
    }

    public FaiList<Integer> batchAddProduct(int aid, FaiList<Param> pdDataList) {
        int rt;
        if(pdDataList == null || pdDataList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, pdDataList is empty;flow=%d;aid=%d;pdData=%s", m_flow, aid, pdDataList);
        }
        int count = getPdCount(aid);
        if(count > ProductValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductValObj.Limit.COUNT_MAX);
        }
        FaiList<Integer> pdIdList = new FaiList<>();
        for(Param pdData : pdDataList) {
            int pdId = creatAndSetId(aid, pdData);
            pdIdList.add(pdId);
        }

        rt = m_dao.batchInsert(pdDataList, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product error;flow=%d;aid=%d;", m_flow, aid);
        }
        return pdIdList;
    }

    public void setSingle(int aid, int pdId, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        updateProduct(aid, matcher, updater);
    }

    public void setProducts(int aid, FaiList<Integer> pdIds, ParamUpdater recvUpdater) {
        ParamUpdater updater = assignUpdate(m_flow, aid, recvUpdater);
        if (updater == null || updater.isEmpty()) {
            return;
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        updateProduct(aid, matcher, updater);
    }

    public static ParamUpdater assignUpdate(int flow, int aid, ParamUpdater recvUpdater) {
        int rt;
        if (recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "updater=null;aid=%d;", flow, aid);
        }
        Param recvInfo = recvUpdater.getData();
        Param data = new Param();
        String name = recvInfo.getString(ProductEntity.Info.NAME);
        if (name != null && !MgProductCheck.checkProductName(name)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, name not valid;flow=%d;aid=%d;name=%s", flow, aid, name);
        }
        data.assign(recvInfo, ProductEntity.Info.NAME);
        data.assign(recvInfo, ProductEntity.Info.PD_TYPE);
        data.assign(recvInfo, ProductEntity.Info.IMG_LIST);
        data.assign(recvInfo, ProductEntity.Info.VIDEO_LIST);
        data.assign(recvInfo, ProductEntity.Info.KEEP_PROP1);
        data.assign(recvInfo, ProductEntity.Info.KEEP_PROP2);
        data.assign(recvInfo, ProductEntity.Info.KEEP_PROP3);
        data.assign(recvInfo, ProductEntity.Info.KEEP_INT_PROP1);
        data.assign(recvInfo, ProductEntity.Info.KEEP_INT_PROP2);
        if(data.isEmpty()) {
            Log.logDbg("no pd basic field changed;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        // status这个字段因为软删除统一字段，所以和商品业务表一致，目前就是软删除的时候可以改。如果之后其他场景要修改这个字段的话，为避免和业务表修改弄混，需要另外提供接口
        //data.assign(recvInfo, ProductEntity.Info.STATUS);
        data.setCalendar(ProductEntity.Info.UPDATE_TIME, Calendar.getInstance());

        ParamUpdater updater = new ParamUpdater(data);
        updater.add(recvUpdater.getOpList(ProductEntity.Info.FLAG));
        updater.add(recvUpdater.getOpList(ProductEntity.Info.FLAG1));

        return updater;
    }

    private int creatAndSetId(int aid, Param info) {
        int rt;
        Integer pdId = info.getInt(ProductEntity.Info.PD_ID, 0);
        if(pdId <= 0) {
            pdId = m_dao.buildId(aid, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            pdId = m_dao.updateId(aid, pdId, false);
            if (pdId == null) {
                rt = Errno.ERROR;
                throw new MgException(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        info.setInt(ProductEntity.Info.PD_ID, pdId);
        return pdId;
    }

    private int updateProduct(int aid, ParamMatcher matcher, ParamUpdater updater) {
        int rt;
        if(updater == null || updater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, updater is null;flow=%d;aid=%d;", m_flow, aid);
        }
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;", m_flow, aid);
        }
        matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.update(updater, matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updateProduct error;flow=%d;aid=%d;", m_flow, aid);
        }
        return refRowCount.value;
    }

    public int deleteProductList(int aid, int tid, FaiList<Integer> pdIds, boolean softDel) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;pdIds=%s", m_flow, aid, pdIds);
        }

        ParamMatcher matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
        matcher.and(ProductEntity.Info.SOURCE_TID, ParamMatcher.EQ, tid);
        if(softDel) {
            Param updateInfo = new Param();
            updateInfo.setInt(ProductEntity.Info.STATUS, ProductValObj.Status.DEL);
            ParamUpdater updater = new ParamUpdater(updateInfo);
            return updateProduct(aid, matcher, updater);
        }
        Ref<Integer> refRowCount = new Ref<>();
        rt = m_dao.delete(matcher, refRowCount);
        if(rt != Errno.OK) {
            throw new MgException(rt, "del product list error;flow=%d;aid=%d;pdIds=%d;", m_flow, aid, pdIds);
        }

        return refRowCount.value;
    }

    public int getPdCount(int aid) {
        // 从缓存中获取
        Param dataStatus = getDataStatus(aid);
        Integer count = dataStatus.getInt(DataStatus.Info.TOTAL_SIZE);
        if(count == null) {
            throw new MgException(Errno.ERROR, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
        }
        return count;
    }

    public Param getProductInfo(int aid, int pdId) {
        Param info = ProductCacheCtrl.InfoCache.getCacheInfo(aid, pdId);
        if (!Str.isEmpty(info)) {
            return info;
        }
        HashSet<Integer> pdIds = new HashSet<>();
        pdIds.add(pdId);
        FaiList<Param> list = getList(aid, pdIds);
        if(Util.isEmptyList(list)) {
            return new Param();
        }
        info = list.get(0);
        return info;
    }

    public FaiList<Param> getProductList(int aid, FaiList<Integer> pdIdList) {
        int rt;
        if(pdIdList == null || pdIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "pdIdList is empty;aid=%d;pdIdList=%s;", aid, pdIdList);
        }
        HashSet<Integer> pdIds = new HashSet<Integer>(pdIdList);

        return getList(aid, pdIds);
    }

    public Param getDataStatus(int aid) {
        Param statusInfo = ProductCacheCtrl.DataStatusCache.get(aid);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductCacheCtrl.DataStatusCache.expire(aid, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int count = getPdCountFromDb(aid);
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, count);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductCacheCtrl.DataStatusCache.add(aid, statusInfo);
        return statusInfo;
    }

    public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> selectFields) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.STATUS, ParamMatcher.NE, ProductValObj.Status.DEL);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef, selectFields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        if(listRef.value == null) {
            listRef.value = new FaiList<Param>();
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        return listRef.value;
    }

    private FaiList<Param> getList(int aid, HashSet<Integer> pdIds) {
        int rt;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "get pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }
        List<String> pdIdStrs = pdIds.stream().map(String::valueOf).collect(Collectors.toList());
        // 从缓存获取数据
        FaiList<Param> list = ProductCacheCtrl.InfoCache.getCacheList(aid, pdIdStrs);
        if(list == null) {
            list = new FaiList<Param>();
        }
        list.remove(null);
        // 查到的数据量和pdIds的数据量一致，则说明都有缓存
        if(list.size() == pdIds.size()) {
            return list;
        }

        // 拿到未缓存的pdId list
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }

        // db中获取
        Ref<FaiList<Param>> tmpRef = new Ref<>();
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductEntity.Info.PD_ID, ParamMatcher.IN, noCacheIds);
        searchArg.matcher.and(ProductEntity.Info.STATUS, ParamMatcher.NE, ProductValObj.Status.DEL);
        rt = m_dao.select(searchArg, tmpRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get list error;flow=%d;aid=%d;pdIds=%s;", m_flow, aid, pdIds);
        }
        if(tmpRef.value != null && !tmpRef.value.isEmpty()) {
            list.addAll(tmpRef.value);
        }

        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }

        // 添加到缓存
        ProductCacheCtrl.InfoCache.addCacheList(aid, tmpRef.value);

        return list;
    }

    private int getPdCountFromDb(int aid) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<Integer> countRef = new Ref<>();
        int rt = m_dao.selectCount(searchArg, countRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "get pd rel count error;flow=%d;aid=%d;", m_flow, aid);
        }
        return countRef.value;
    }

    private void init(TransactionCtrl tc) {
        if(tc == null) {
            return;
        }
        if(!tc.register(m_dao)) {
            throw new MgException("registered ProductDaoCtrl err;");
        }
    }

    private int m_flow;
    private ProductDaoCtrl m_dao;
}
