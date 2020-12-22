package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.Misc2;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuDaoCtrl;
import fai.comm.util.*;

import java.util.*;

public class ProductSpecSkuProc {
    public ProductSpecSkuProc(ProductSpecSkuDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, FaiList<Long> rtIdList) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(ProductSpecSkuEntity.Info.AID, aid);
            data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
            Long skuId = m_daoCtrl.buildId();
            if(skuId == null){
                Log.logErr("batchAdd arg error;flow=%d;aid=%s;skuId=%s;info=%s;", m_flow, aid, skuId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(skuId);
            }
            data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            data.assign(info, ProductSpecSkuEntity.Info.SORT);
            data.assign(info, ProductSpecSkuEntity.Info.SOURCE_TID); // TODO
            data.assign(info, ProductSpecSkuEntity.Info.SKU_NUM); // TODO
            FaiList<Integer> inPdScStrIdList = info.getListNullIsEmpty(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            Collections.sort(inPdScStrIdList); // 排序
            data.setString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList.toJson());
            data.assign(info, ProductSpecSkuEntity.Info.FLAG);
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("batchAdd ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int batchDel(int aid, int pdId, FaiList<Long> delSkuIdList) {
        if(aid <= 0 || pdId <= 0){
            return Errno.ARGS_ERROR;
        }
        ParamMatcher delMatcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(delSkuIdList != null){
            if(delSkuIdList.isEmpty()){
                return Errno.OK;
            }
            delMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, delSkuIdList);
        }
        Log.logDbg("delMatcher.sql=%s;delMatcher.json=%s;", delMatcher.getSql(), delMatcher.toJson());
        int rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;delSkuIdList=%s;", m_flow, aid, pdId, delSkuIdList);
        return rt;
    }

    public int refreshSku(int aid, int pdId, FaiList<FaiList<Integer>> skuList, FaiList<Long> rtIdList) {
        if(aid <= 0 || pdId <= 0 || skuList == null){
            return Errno.ARGS_ERROR;
        }
        int rt = batchDel(aid, pdId, null);
        if(rt != Errno.OK) {
            return rt;
        }
        FaiList<Param> infoList = new FaiList<>(skuList.size());
        skuList.forEach(inPdScStrIdList->{
            infoList.add(new Param().setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList));
        });
        rt = batchAdd(aid, pdId, infoList, rtIdList);
        if(rt != Errno.OK){
            return rt;
        }
        Log.logStd("refreshSku ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList){
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logStd("batchDel error;flow=%d;aid=%s;pdId=%updaterList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Long> skuIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Misc2.validUpdaterList(updaterList, ProductSpecSkuEntity.getValidKeys(), data->{
            skuIdList.add(data.getLong(ProductSpecSkuEntity.Info.SKU_ID));
        });

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getList(aid, pdId, skuIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Map<Integer, Param> oldDataMap = Misc2.getMap(listRef.value, ProductSpecSkuEntity.Info.SKU_ID);
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        updaterList.forEach(updater -> {
            long skuId = updater.getData().getLong(ProductSpecSkuEntity.Info.SKU_ID);
            Param oldData = oldDataMap.remove(skuId); // help gc
            Param updatedData = updater.update(oldData, true);

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(ProductSpecSkuEntity.Info.SYS_UPDATE_TIME, now);
            { // matcher
                data.setInt(ProductSpecSkuEntity.Info.AID, aid);
                data.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                data.setLong(ProductSpecSkuEntity.Info.SKU_ID, skuId);
            }

            dataList.add(data);
        });
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int getList(int aid, int pdId, Ref<FaiList<Param>> pdScSkuInfoListRef) {
        return getList(aid, pdId, null, pdScSkuInfoListRef);
    }

    public int getList(int aid, int pdId, FaiList<Long> skuIdList, Ref<FaiList<Param>> pdScSkuInfoListRef) {
        SearchArg searchArg = new SearchArg();
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(skuIdList != null){
            matcher.and(ProductSpecSkuEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        }
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, pdScSkuInfoListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "getList error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        initDBInfoList(pdScSkuInfoListRef.value);
        Log.logDbg("getList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt = Errno.OK;
    }
    private void initDBInfoList(FaiList<Param> infoList){
        if(infoList == null || infoList.isEmpty()){
            return;
        }
        infoList.forEach(info->{
            String inPdScStrIdListStr = info.getString(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST);
            info.setList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST, FaiList.parseIntList(inPdScStrIdListStr, new FaiList<>()));
        });
    }

    private int m_flow;
    private ProductSpecSkuDaoCtrl m_daoCtrl;



}
