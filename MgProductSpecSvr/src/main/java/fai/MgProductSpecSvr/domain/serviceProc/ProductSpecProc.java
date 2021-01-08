package fai.MgProductSpecSvr.domain.serviceProc;


import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Misc2;
import fai.MgProductSpecSvr.domain.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.domain.entity.ProductSpecValObj;
import fai.MgProductSpecSvr.domain.repository.ProductSpecCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecDaoCtrl;
import fai.comm.util.*;

import java.util.*;

public class ProductSpecProc {
    public ProductSpecProc(ProductSpecDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, FaiList<Integer> rtIdList) {
        return batchAdd(aid, pdId, infoList, rtIdList, null);
    }
    public int batchAdd(int aid, int pdId, FaiList<Param> infoList, FaiList<Integer> rtIdList, Ref<Boolean> needRefreshSkuRef) {
        if(aid <= 0 || pdId <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;pdId=%s;infoList=%s;", m_flow, aid, pdId, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        for (Param info : infoList) {
            Param data = new Param();
            data.setInt(ProductSpecEntity.Info.AID, aid);
            data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
            data.assign( info, ProductSpecEntity.Info.SC_STR_ID);
            Integer pdScId = m_daoCtrl.buildId();
            if(pdScId == null){
                Log.logErr("batchAdd arg error;flow=%d;aid=%s;tpScId=%s;info=%s;", m_flow, aid, pdScId, info);
                return Errno.ERROR;
            }
            if(rtIdList != null){
                rtIdList.add(pdScId);
            }
            data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
            data.assign( info, ProductSpecEntity.Info.SORT);
            data.assign( info, ProductSpecEntity.Info.FLAG);
            FaiList<Param> inPdScValList = info.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            for (Param inPdScValInfo : inPdScValList) {
                if(inPdScValInfo.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                    int flag = data.getInt(ProductSpecEntity.Info.FLAG, 0) | ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
                    if(needRefreshSkuRef != null){
                        needRefreshSkuRef.value = true;
                    }
                    data.setInt(ProductSpecEntity.Info.FLAG, flag);
                    break;
                }
            }
            String inScValListStr = inPdScValList.toJson();
            data.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, inScValListStr);
            data.setCalendar(ProductSpecEntity.Info.SYS_CREATE_TIME, now);
            data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAdd error;flow=%d;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        initDBInfoList(dataList);
        Log.logStd("batchAdd ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    public int batchDel(int aid, int pdId, FaiList<Integer> pdScIdList, Ref<Boolean> needRefreshSkuRef) {
        if(aid <= 0 || pdId <=0 || (pdScIdList != null && pdScIdList.isEmpty())){
            Log.logStd("batchDel error;flow=%d;aid=%s;pdId=%s;tpScDtIdList=%s;", m_flow, aid, pdId, pdScIdList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(pdScIdList != null){
            matcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.IN, pdScIdList);
        }
        if(needRefreshSkuRef != null){
            ParamMatcher queryMatcher = matcher.clone();
            queryMatcher.and(ProductSpecEntity.Info.FLAG, ParamMatcher.LAND, ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED, ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED);
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = queryMatcher;
            Ref<Integer> countRef = new Ref<>();
            rt = m_daoCtrl.selectCount(searchArg, countRef);
            if(rt != Errno.OK){
                return rt;
            }
            if(countRef.value > 0){
                needRefreshSkuRef.value = true;
            }
        }
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDel error;flow=%d;aid=%s;pdId=%s;idList=%s;", m_flow, aid, pdId, pdScIdList);
            return rt;
        }
        Log.logStd("batchDel ok;flow=%d;aid=%d;pdId=%s;idList=%s;", m_flow, aid, pdId, pdScIdList);
        return rt;
    }

    public int batchSet(int aid, int pdId, FaiList<ParamUpdater> updaterList, Ref<Boolean> needRefreshSkuRef) {
        if(aid <= 0 || pdId <=0 || updaterList == null || updaterList.isEmpty()){
            Log.logStd("batchDel error;flow=%d;aid=%s;pdId=%s;tpScDtIdList=%s;", m_flow, aid, pdId, updaterList);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;
        FaiList<Integer> pdScIdList = new FaiList<>(updaterList.size());
        Set<String> maxUpdaterKeys = Misc2.validUpdaterList(updaterList, ProductSpecEntity.getValidKeys(), data->{
            pdScIdList.add(data.getInt(ProductSpecEntity.Info.PD_SC_ID));
        });
        maxUpdaterKeys.remove(ProductSpecEntity.Info.PD_SC_ID);

        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = getListFormDao(aid, pdId, pdScIdList, listRef);
        if(rt != Errno.OK){
            return rt;
        }
        Map<Integer, Param> oldDataMap = Misc2.getMap(listRef.value, ProductSpecEntity.Info.PD_SC_ID);
        listRef.value = null; // help gc

        ParamUpdater doBatchUpdater = new ParamUpdater();
        maxUpdaterKeys.forEach(key->{
            doBatchUpdater.getData().setString(key, "?");
        });
        doBatchUpdater.getData().setString(ProductSpecEntity.Info.SYS_UPDATE_TIME, "?");

        ParamMatcher doBatchMatcher = new ParamMatcher();
        doBatchMatcher.and(ProductSpecEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.EQ, "?");

        Calendar now = Calendar.getInstance();
        FaiList<Param> dataList = new FaiList<>(updaterList.size());
        for (ParamUpdater updater : updaterList) {
            Integer pdScId = updater.getData().getInt(ProductSpecEntity.Info.PD_SC_ID);
            Param oldData = oldDataMap.remove(pdScId); // help gc
            Param updatedData = updater.update(oldData, true);
            if(!needRefreshSkuRef.value){
                FaiList<Integer> oldCheckIdList = getCheckIdList(oldData);
                FaiList<Integer> updatedCheckIdList = getCheckIdList(updatedData);
                // 当原先一个规格值都未勾选，更新后存在有勾选的规格值，则需要刷新sku
                if(oldCheckIdList.size() == 0 && updatedCheckIdList.size() > 0){
                    needRefreshSkuRef.value = true;
                }
                // 当原先存在有勾选的规格值，更新后一个规格值都未勾选，则需要刷新sku
                if(oldCheckIdList.size() > 0 && updatedCheckIdList.size() == 0){
                    needRefreshSkuRef.value = true;
                }
            }
            FaiList<Param> updatedInPdScValList = updatedData.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            int flag = updatedData.getInt(ProductSpecEntity.Info.FLAG, 0);
            if(updatedInPdScValList.size() == 0){
                flag &= ~ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
            }else{
                flag |= ProductSpecValObj.FLag.IN_PD_SC_VAL_LIST_CHECKED;
            }
            updatedData.setInt(ProductSpecEntity.Info.FLAG, flag);
            updatedData.setString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, updatedInPdScValList.toJson());

            Param data = new Param();
            maxUpdaterKeys.forEach(key->{
                data.assign(updatedData, key);
            });
            data.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, now);

            { // matcher
                data.setInt(ProductSpecEntity.Info.AID, aid);
                data.setInt(ProductSpecEntity.Info.PD_ID, pdId);
                data.setInt(ProductSpecEntity.Info.PD_SC_ID, pdScId);
            }
            dataList.add(data);
        }
        rt = m_daoCtrl.batchUpdate(doBatchUpdater, doBatchMatcher, dataList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchSet error;flow=%d;aid=%s;", m_flow, aid);
            return rt;
        }
        Log.logStd("batchSet ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }

    private FaiList<Integer> getCheckIdList(Param info) {
        FaiList<Param> inPdScValList = info.getList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
        FaiList<Integer> checkIdList = new FaiList<>();
        inPdScValList.forEach(item->{
            if(item.getBoolean(ProductSpecValObj.InPdScValList.Item.CHECK, false)){
                checkIdList.add(item.getInt(ProductSpecValObj.InPdScValList.Item.SC_STR_ID));
            }
        });
        return checkIdList;
    }
    public int getListFormDao(int aid, int pdId, FaiList<Integer> pdScIdList, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        if(pdScIdList != null){
            matcher.and(ProductSpecEntity.Info.PD_SC_ID, ParamMatcher.IN, pdScIdList);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "dao.select error;flow=%d;aid=%s;pdId=%s;pdScIdList=%s;", m_flow, aid, pdId, pdScIdList);
            return rt;
        }
        initDBInfoList(listRef.value);
        return rt;
    }
    private void initDBInfoList(FaiList<Param> infoList){
        if(infoList == null || infoList.isEmpty()){
            return;
        }
        infoList.forEach(info->{
            String inPdScValListStr = info.getString(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
            info.setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, FaiList.parseParamList(inPdScValListStr, new FaiList<>()));
        });
    }

    public int getList(int aid, int pdId, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || pdId <=0 || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;listRef=%s;", m_flow, aid, pdId, listRef);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> pdScList = ProductSpecCacheCtrl.getPdScList(aid, pdId);
        if(pdScList != null){
            listRef.value = pdScList;
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        try {
            LockUtil.lock(aid);
            rt = getListFormDao(aid, pdId, null, listRef);
            if(rt != Errno.OK){
                return rt;
            }
            pdScList = listRef.value;
            ProductSpecCacheCtrl.initPdScList(aid, pdId, pdScList);
        }finally {
            LockUtil.unlock(aid);
        }
        Log.logDbg(rt,"getList ok;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
        return rt = Errno.OK;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public boolean clearCache(int aid, int pdId) {
        return ProductSpecCacheCtrl.delAllCache(aid, pdId);
    }

    private int m_flow;
    private ProductSpecDaoCtrl m_daoCtrl;
}
