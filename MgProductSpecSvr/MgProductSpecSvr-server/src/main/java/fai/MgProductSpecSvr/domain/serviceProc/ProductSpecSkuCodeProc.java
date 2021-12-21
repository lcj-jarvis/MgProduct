package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuCodeEntity;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCodeCacheCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCodeDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.ProductSpecSkuCodeSagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class ProductSpecSkuCodeProc {
    public ProductSpecSkuCodeProc(ProductSpecSkuCodeDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
        sagaMap = new HashMap<>();
    }
    public ProductSpecSkuCodeProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        m_daoCtrl = ProductSpecSkuCodeDaoCtrl.getInstance(flow, aid);
        m_sagaDaoCtrl = ProductSpecSkuCodeSagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(!transactionCtrl.register(m_daoCtrl)){
            new RuntimeException("register dao err;flow="+flow+";aid="+aid);
        }
        if (m_sagaDaoCtrl == null) {
            throw new RuntimeException(String.format("ProductSpecSkuCodeSagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        m_flow = flow;
        sagaMap = new HashMap<>();
    }
    public int batchAdd(int aid, int unionPriId, FaiList<Param> skuCodeInfoList, boolean isSaga) {
        int rt;
        for (Param skuCodeInfo : skuCodeInfoList) {
            skuCodeInfo.setInt(ProductSpecSkuCodeEntity.Info.AID, aid);
            skuCodeInfo.setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId);
            cacheManage.setSkuIdDirty(aid, skuCodeInfo.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID));
        }
        cacheManage.setDataStatusDirty(aid, unionPriId);
        rt = m_daoCtrl.batchInsert(skuCodeInfoList, null, !isSaga);

        if (isSaga) {
            // 添加 Saga 操作记录
            rt = addInsOp4Saga(aid, skuCodeInfoList);
            if (rt != Errno.OK) {
                return rt;
            }
        }
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;skuCodeInfoList=%s;", m_flow, aid, unionPriId, skuCodeInfoList);
            return rt;
        }
        Log.logStd("insert ok!;flow=%s;aid=%s;", m_flow, aid);
        return rt;
    }

    // 更新 specSkuCode 表数据 （其中包括 修改、新增、删除）
    public int refresh(int aid, int unionPriId, int pdId, Map<String, Long> newSkuCodeSkuIdMap, FaiList<Long> needDelSkuCodeSkuIdList, FaiList<Param> skuCodeSortList, HashSet<Long> changeSkuCodeSkuIdSet, boolean isSaga) {
        if(newSkuCodeSkuIdMap.isEmpty() && needDelSkuCodeSkuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.OK;
        cacheManage.setSkuIdListDirty(aid, changeSkuCodeSkuIdSet);
        if(!needDelSkuCodeSkuIdList.isEmpty()){ // 删除skuCode
            cacheManage.setDataStatusDirty(aid, unionPriId);

            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, needDelSkuCodeSkuIdList);

            // 添加 Saga 操作记录
            if (isSaga) {
                rt = addDelOp4Saga(aid, matcher);
                if (rt != Errno.OK) {
                    return rt;
                }
            }

            rt = m_daoCtrl.delete(matcher);
            if(rt != Errno.OK){
                Log.logErr(rt, "delete err;flow=%s;unionPriId=%s;needDelSkuCodeSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuCodeSkuIdList);
                return rt;
            }
        }
        if(!newSkuCodeSkuIdMap.isEmpty()){
            HashMap<Long, String> newSkuIdSkuCodeMap = new HashMap<>(newSkuCodeSkuIdMap.size()*4/3+1);
            for (Map.Entry<String, Long> skuCodeSkuIdEntry : newSkuCodeSkuIdMap.entrySet()) {
                String skuCode = skuCodeSkuIdEntry.getKey();
                Long skuId = skuCodeSkuIdEntry.getValue();
                newSkuIdSkuCodeMap.put(skuId, skuCode);
            }
            // 查询 本次修改到所关联的所有旧数据
            StringBuilder skuCodeSetPlaceholder = new StringBuilder();
            StringBuilder skuIdSetPlaceholder = new StringBuilder();

            /*
             * 下面生成的 sql
             *  select * from mgProductSpecSkuCode_0xxx
             *  where aid = ? and unionPriId = ? and skuCode in (?,?,?,?.....)
             *  union all
             *  select * from mgProductSpecSkuCode_0xxx
             *  where aid = ? and unionPriId = ? and skuId in (?,?,?,?.....)
             */
            genPlaceholder4refresh(newSkuCodeSkuIdMap, newSkuIdSkuCodeMap, skuCodeSetPlaceholder, skuIdSetPlaceholder);

            String prepareSql = "select * from " + m_daoCtrl.getTableName()
                    + " where "+ ProductSpecSkuCodeEntity.Info.AID+" = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID+ " = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.SKU_CODE + " in " + skuCodeSetPlaceholder.toString()
                    + " union all "
                    + " select * from " + m_daoCtrl.getTableName()
                    + " where "+ ProductSpecSkuCodeEntity.Info.AID+" = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID+ " = ? "
                    + " and " + ProductSpecSkuCodeEntity.Info.SKU_ID + " in " + skuIdSetPlaceholder.toString()
                    + ";" ;

            Dao dao = m_daoCtrl.getDao();
            PreparedStatement preparedStatement = dao.prepareStatement(prepareSql);

            // 填充 sql 语句中的 ?
            rt = setPrepareStatement4refresh(preparedStatement, aid, unionPriId, newSkuCodeSkuIdMap.keySet(), newSkuIdSkuCodeMap.keySet());
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Param> list = dao.executeQuery(preparedStatement, null);

            // 已经存在的映射关系
            Map<String/*oldSkuCode*/, Long /*skuId*/> oldSkuCodeSkuIdMap = new HashMap<>(list.size());
            for (Param info : list) {
                String skuCode = info.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE);
                long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
                oldSkuCodeSkuIdMap.put(skuCode, skuId);
            }
            FaiList<Param> addDataList = new FaiList<>();
            Iterator<Map.Entry<String, Long>> itr = newSkuCodeSkuIdMap.entrySet().iterator();
            while (itr.hasNext()){
                Map.Entry<String, Long> newSkuCodeSkuIdEntry = itr.next();
                String skuCode = newSkuCodeSkuIdEntry.getKey();
                long newSkuId = newSkuCodeSkuIdEntry.getValue();
                Long oldSkuId = oldSkuCodeSkuIdMap.remove(skuCode);
                if(oldSkuId != null){
                    if(newSkuId == oldSkuId){
                        itr.remove();
                    }else{
                        // 判断是否是替换，从要更新的里面能拿出来，如果有 就说明是要替换
                        String newSkuCode = newSkuIdSkuCodeMap.get(oldSkuId);
                        if(newSkuCode == null){
                            rt = Errno.ALREADY_EXISTED;
                            Log.logErr(rt, "skuCode already;flow=%s;aid=%s;unionPriId=%s;skuCode=%s;oldSkuId=%s;newSkuId=%s;", m_flow, aid, unionPriId, skuCode, oldSkuId, newSkuId);
                            return rt;
                        }
                    }
                }else{
                    itr.remove();
                    addDataList.add(
                            new Param()
                            .setInt(ProductSpecSkuCodeEntity.Info.AID, aid)
                            .setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId)
                            .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                            .setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, newSkuId)
                            .setInt(ProductSpecSkuCodeEntity.Info.PD_ID, pdId)
                    );
                }
            }
            FaiList<Param> updateDataList = new FaiList<>(newSkuCodeSkuIdMap.size());
            // 记录下需要更新的 SkuCode
            FaiList<String> needUpdateSkuCodeList = new FaiList<>();
            for (Map.Entry<String, Long> newSkuCodeSkuIdEntry : newSkuCodeSkuIdMap.entrySet()) {
                String skuCode = newSkuCodeSkuIdEntry.getKey();
                long skuId = newSkuCodeSkuIdEntry.getValue();
                updateDataList.add(
                        new Param()
                                .setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, skuId)
                                .setInt(ProductSpecSkuCodeEntity.Info.AID, aid)
                                .setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId)
                                .setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode)
                );
                needUpdateSkuCodeList.add(skuCode);
            }
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;updateDataList=%s;", aid, unionPriId, updateDataList);
            FaiList<String> needDelList = new FaiList<>(oldSkuCodeSkuIdMap.keySet());
            Log.logDbg("whalelog  aid=%s;unionPriId=%s;needDelList=%s;", aid, unionPriId, needDelList);
            if(!needDelList.isEmpty()){
                cacheManage.setDataStatusDirty(aid, unionPriId);
                ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, needDelList);

                if (isSaga) {
                    // 添加 Saga 操作记录
                    rt = addDelOp4Saga(aid, matcher);
                    if (rt != Errno.OK) {
                        return rt;
                    }
                }

                rt = m_daoCtrl.delete(matcher);
                if(rt != Errno.OK){
                    Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;needDelList=%s;", needDelList);
                    return rt;
                }
                Log.logStd("delete ok!;flow=%s;aid=%s;unionPriId=%s;needDelList=%s;", m_flow, aid, unionPriId, needDelList);
            }
            if(!updateDataList.isEmpty()){
                cacheManage.setManageDirty(aid, unionPriId);
                // 预记录修改操作
                if (isSaga) {
                    // 查询旧数据
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                    searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, needUpdateSkuCodeList);
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = m_daoCtrl.select(searchArg, listRef);
                    if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "dao.select oldList err;flow=%d;aid=%d;", m_flow, aid);
                        return rt;
                    }

                    preAddUpdateSaga(aid, listRef.value);
                }

                ParamUpdater updater = new ParamUpdater();
                updater.getData().setString(ProductSpecSkuCodeEntity.Info.SKU_ID, "?");

                ParamMatcher matcher = new ParamMatcher();
                matcher.and(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
                matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, "?");
                rt = m_daoCtrl.batchUpdate(updater, matcher, updateDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt, "batchUpdate err;flow=%s;aid=%s;unionPriId=%s;updateDataList=%s;", updateDataList);
                    return rt;
                }
                Log.logStd("update ok!;flow=%s;aid=%s;unionPriId=%s;updateDataList=%s;", m_flow, aid, unionPriId, updateDataList);
            }
            if(!addDataList.isEmpty()){
                cacheManage.setDataStatusDirty(aid, unionPriId);

                if (isSaga) {
                    FaiList<Param> sagaOpList = new FaiList<>();
                    String xid = RootContext.getXID();
                    Long branchId = RootContext.getBranchId();
                    Calendar now = Calendar.getInstance();
                    addDataList.forEach(addInfo -> {
                        Param sagaOpInfo = new Param();
                        sagaOpInfo.assign(addInfo, ProductSpecSkuCodeEntity.Info.AID);
                        sagaOpInfo.assign(addInfo, ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
                        sagaOpInfo.assign(addInfo, ProductSpecSkuCodeEntity.Info.SKU_CODE);
                        sagaOpInfo.setString(SagaEntity.Common.XID, xid);
                        sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
                        sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
                        sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
                        sagaOpList.add(sagaOpInfo);
                    });
                    if (!Util.isEmptyList(sagaOpList)) {
                        // 添加 Saga 操作记录
                        rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
                        if (rt != Errno.OK) {
                            Log.logErr(rt, "dao.insert sagaOpList err;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
                            return rt;
                        }
                    }
                }

                rt = m_daoCtrl.batchInsert(addDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;addDataList=%s;", addDataList);
                    return rt;
                }
                Log.logStd("insert ok!;flow=%s;aid=%s;unionPriId=%s;addDataList=%s;", m_flow, aid, unionPriId, addDataList);
            }
        }
        if(!skuCodeSortList.isEmpty()){ // 设置排序
            cacheManage.setManageDirty(aid, unionPriId);

            if (isSaga) {
                FaiList<String> needUpdateSortSkuCodeList = new FaiList<>();
                skuCodeSortList.forEach(skuCodeSortInfo -> needUpdateSortSkuCodeList.add(skuCodeSortInfo.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE)));
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, needUpdateSortSkuCodeList);
                Ref<FaiList<Param>> listRef = new Ref<>();
                rt = m_daoCtrl.select(searchArg, listRef);
                if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    Log.logErr(rt, "dao.select oldList err;flow=%d;aid=%d;", m_flow, aid);
                    return rt;
                }
                preAddUpdateSaga(aid, listRef.value);
            }

            ParamUpdater updater = new ParamUpdater();
            updater.getData().setString(ProductSpecSkuCodeEntity.Info.SORT, "?");

            ParamMatcher matcher = new ParamMatcher();
            matcher.and(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, "?");

            rt = m_daoCtrl.doBatchUpdate(updater, matcher, skuCodeSortList, false);
            if(rt != Errno.OK){
                Log.logErr(rt, "doBatchUpdate err;flow=%s;aid=%s;unionPriId=%s;skuCodeSortList=%s;", m_flow, aid, unionPriId, skuCodeSortList);
                return rt;
            }
            Log.logStd("set sort ok!;flow=%s;aid=%s;unionPriId=%s;skuCodeSortList=%s;", m_flow, aid, unionPriId, skuCodeSortList);
        }
        Log.logStd("ok!;flow=%s;aid=%s;unionPriId=%s;needDelSkuCodeSkuIdList=%s;", m_flow, aid, unionPriId, needDelSkuCodeSkuIdList);
        return rt;
    }

    /**
     * 填充占位符
     */
    private int setPrepareStatement4refresh(PreparedStatement preparedStatement, int aid, int unionPriId, Set<String> skuCodeSet, Set<Long> skuIdSet){
        int rt = Errno.OK;
        try {
            int start = 1;
            preparedStatement.setInt(start++, aid);
            preparedStatement.setInt(start++, unionPriId);
            for (String skuCode : skuCodeSet) {
                preparedStatement.setString(start++, skuCode);
            }
            preparedStatement.setInt(start++, aid);
            preparedStatement.setInt(start++, unionPriId);
            for (Long skuId : skuIdSet) {
                preparedStatement.setLong(start++, skuId);
            }
        } catch (SQLException exp) {
            rt = Errno.DAO_SQL_ERROR;
            Log.logErr(rt, exp, "code=" + exp.getErrorCode());
            return rt;
        } catch (Exception exp) {
            rt = Errno.DAO_SQL_ERROR;
            Log.logErr(rt, exp);
            return rt;
        }
        return rt;
    }

    /**
     * 生成占位符
     */
    private void genPlaceholder4refresh(Map<String, Long> newSkuCodeSkuIdMap, HashMap<Long, String> newSkuIdSkuCodeMap, StringBuilder skuCodeSetPlaceholder, StringBuilder skuIdSetPlaceholder) {
        skuCodeSetPlaceholder.append("(");
        int len = newSkuCodeSkuIdMap.keySet().size();
        for (int i = 0; i < len; i++) {
            if(i != 0){
                skuCodeSetPlaceholder.append(", ");
            }
            skuCodeSetPlaceholder.append("?");
        }
        skuCodeSetPlaceholder.append(")");

        skuIdSetPlaceholder.append("(");
        len = newSkuIdSkuCodeMap.keySet().size();
        for (int i = 0; i < len; i++) {
            if(i != 0){
                skuIdSetPlaceholder.append(", ");
            }
            skuIdSetPlaceholder.append("?");
        }
        skuIdSetPlaceholder.append(")");
    }

    public int batchDel(int aid, Integer unionPriId, FaiList<Long> skuIdList) {
        return batchDel(aid, unionPriId, skuIdList, false);
    }

    /**
     * 删除 mgProductSpecSkuCode_0xxx 中的数据
     */
    public int batchDel(int aid, Integer unionPriId, FaiList<Long> skuIdList, boolean isSaga){
        if(skuIdList.isEmpty()){
            return Errno.OK;
        }
        int rt;
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        cacheManage.setSkuIdListDirty(aid, skuIdList);
        if(unionPriId != null){
            // 如果指定了 unionPriId ，则设置指定的 unionPriId 为脏数据
            cacheManage.setDataStatusDirty(aid, unionPriId);
        }else{
            // 没有指定 unionPriId， 则通过 aid + skuId 查询 unionPriIds （已经去重）,遍历设置为脏数据
            Dao.SelectArg selectArg = new Dao.SelectArg();
            selectArg.field = " distinct " + ProductSpecSkuCodeEntity.Info.UNION_PRI_ID ;
            selectArg.searchArg.matcher = matcher;
            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = m_daoCtrl.select(selectArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                Log.logErr(rt, "select err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
                return rt;
            }
            for (Param info : listRef.value) {
                Integer _unionPriId = info.getInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
                cacheManage.setDataStatusDirty(aid, _unionPriId);
            }
        }

        if (isSaga) {
            // 记录 Saga 操作
            rt = addDelOp4Saga(aid, matcher);
            if (rt != Errno.OK) {
                return rt;
            }
        }

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }

    public int clearData(int aid, FaiList<Integer> unionPriIds){
        if(aid <= 0 || Util.isEmptyList(unionPriIds)){
            Log.logErr("arg error;flow=%d;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt, "delete err;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriIds=%s;", m_flow, aid, unionPriIds);
        return rt;
    }

    public int getListFromDao(int aid, FaiList<Long> skuIdList, Ref<FaiList<Param>> listRef){
        int rt = Errno.ERROR;

        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_ID, ParamMatcher.IN, skuIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return rt;
        }
        Log.logStd(rt,"ok;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
        return rt;
    }

    public int getSkuCodeListFromDao(int aid, int unionPriId, FaiList<String> skuCodeList, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, skuCodeList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_CODE);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuCodeList=%s;", m_flow, aid, unionPriId, skuCodeList);
            return rt;
        }
        Log.logDbg(rt,"ok;flow=%s;aid=%s;unionPriId=%s;skuCodeList=%s;", m_flow, aid, unionPriId, skuCodeList);
        return rt = Errno.OK;
    }

    public int searchBySkuCode(int aid, int unionPriId, String skuCode, boolean isFuzzySearch, Ref<FaiList<Param>> listRef){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        if(isFuzzySearch){
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.LK, skuCode);
        }else{
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, skuCode);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_ID, ProductSpecSkuCodeEntity.Info.SKU_CODE);
        if(rt != Errno.OK){
            Log.logErr(rt, "select err;flow=%s;aid=%s;unionPriId=%s;skuCode=%s;", m_flow, aid, unionPriId, skuCode);
            return rt;
        }
        return rt;
    }

    public int getDataStatus(int aid, int unionPriId, Ref<Param> dataStatusRef){
        Param info = ProductSpecSkuCodeCacheCtrl.DataStatusCache.get(aid, unionPriId);
        boolean needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
        boolean needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);
        int rt = Errno.ERROR;
        if(needInitTotalSize || needInitManage){
            try {
                LockUtil.readLock(aid);
                info = ProductSpecSkuCodeCacheCtrl.DataStatusCache.get(aid, unionPriId);
                needInitTotalSize = info.getInt(DataStatus.Info.TOTAL_SIZE, -1) < 0;
                needInitManage = info.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) == null;
                info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

                if(needInitTotalSize){
                    SearchArg searchArg = new SearchArg();
                    searchArg.matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
                    // 门店的条形码是在总店的，分店是没有的，所以搜索数据状态的时候不要设置unionPriId，但是缓存数据状态的时候要加上unionPriId区分是在不同分店下的数据状态
                    // searchArg.matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    Ref<Integer> countRef = new Ref<>();
                    rt = m_daoCtrl.selectCount(searchArg, countRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "selectCount err;flow=%s;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
                        return rt;
                    }
                    info.setInt(DataStatus.Info.TOTAL_SIZE, countRef.value);
                }
                if(needInitManage){
                    info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
                }
                ProductSpecSkuCodeCacheCtrl.DataStatusCache.set(aid, unionPriId, info);
                rt = Errno.OK;
            }finally {
                LockUtil.unReadLock(aid);
            }
        }else{
            rt = Errno.OK;
        }
        dataStatusRef.value = info;
        return rt;
    }

    public int getAllDataFromDao(int aid, int unionPriId, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        // 门店的条形码是在总店的，分店是没有的，所以搜索的条形码的时候不要设置unionPriId
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }

    public int searchAllDataFromDao(int aid, int unionPriId, SearchArg searchArg, Ref<FaiList<Param>> listRef, String ... fields){
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        // 门店的条形码是在总店的，分店是没有的，所以搜索的条形码的时候不要设置unionPriId
        matcher.and(searchArg.matcher);
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef, fields);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt;
    }
    public int getSkuIdListFromDao(int aid, int unionPriId, FaiList<Integer> pdIdList, Ref<FaiList<Long>> skuIdListRef) {
        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductSpecSkuCodeEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef, ProductSpecSkuCodeEntity.Info.SKU_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt, "select err;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        FaiList<Long> skuIdList = new FaiList<>(listRef.value.size());
        for (Param skuIdInfo : listRef.value) {
            skuIdList.add(skuIdInfo.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID));
        }
        skuIdListRef.value = skuIdList;
        Log.logStd("ok;flow=%d;aid=%s;unionPriId=%s;", m_flow, aid, unionPriId);
        return rt = Errno.OK;
    }

    public int getList(int aid, FaiList<Long> skuIdList, Ref<Map<Long, FaiList<String>>> skuIdSkuCodeListMapRef){
        if(skuIdList == null || skuIdList.isEmpty()){
            Log.logErr("arg err;flow=%s;aid=%s;skuIdList=%s;", m_flow, aid, skuIdList);
            return Errno.ARGS_ERROR;
        }
        HashMap<Long, FaiList<String>> resultMap = new HashMap<>();
        skuIdSkuCodeListMapRef.value = resultMap;
        HashSet<Long> skuIdSet = new HashSet<>(skuIdList);
        ParamComparator comparator = new ParamComparator(ProductSpecSkuCodeEntity.Info.SORT);

        getListFromCache(aid, resultMap, skuIdSet, comparator);
        if(skuIdSet.isEmpty()){
            return Errno.OK;
        }
        int rt = Errno.ERROR;
        Map<Long, FaiList<Param>> map = null;
        try {
            LockUtil.readLock(aid);
            // double check
            getListFromCache(aid, resultMap, skuIdSet, comparator);
            if(skuIdSet.isEmpty()){
                return Errno.OK;
            }

            Ref<FaiList<Param>> listRef = new Ref<>();
            rt = getListFromDao(aid, new FaiList<>(skuIdSet), listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            map = new HashMap<>(listRef.value.size()*4/3+1);
            for (Param info : listRef.value) {
                Long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
                skuIdSet.remove(skuId);
                FaiList<Param> list = map.getOrDefault(skuId, new FaiList<>());
                list.add(info);
                if(list.size() == 1){
                    map.put(skuId, list);
                }
            }
            for (Long skuId : skuIdSet) {
                map.put(skuId, new FaiList<>());
            }
            ProductSpecSkuCodeCacheCtrl.setInfoCache(aid, map);
        }finally {
            LockUtil.unReadLock(aid);
        }
        map.forEach((skuId, skuCodeInfoList)->{
            comparator.sort(skuCodeInfoList);
            resultMap.put(skuId, Utils.getValList(skuCodeInfoList, ProductSpecSkuCodeEntity.Info.SKU_CODE));
        });
        return rt;
    }

    /**
     * 获取 Saga 操作记录
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param sagaOpListRef 接收返回的集合
     * @return {@link Errno}
     */
    public int getSagaOpList(String xid, Long branchId, Ref<FaiList<Param>> sagaOpListRef) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        int rt = m_sagaDaoCtrl.select(searchArg, sagaOpListRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "productSpecSkuCodeProc dao.getSagaOpList error;flow=%d", m_flow);
            return rt;
        }
        return rt;
    }

    private void preAddUpdateSaga(int aid, FaiList<Param> list) {
        if (Util.isEmptyList(list)) {
            Log.logStd("preAddUpdateSaga list is empty;flow=%d;aid=%d;", m_flow, aid);
            return;
        }

        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        for (Param info : list) {
            int unionPriId = info.getInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
            String skuCode = info.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE);
            PrimaryKey primaryKey = new PrimaryKey(aid, unionPriId, skuCode);
            // 一条数据只记录最原始的操作
            if (sagaMap.containsKey(primaryKey)) {
                continue;
            }
            Param sagaInfo = new Param();
            String[] sagaKeys = ProductSpecSkuCodeEntity.getSagaKeys();
            for (String key : sagaKeys) {
                sagaInfo.assign(info, key);
            }
            // 记录 Saga 字段
            sagaInfo.setString(SagaEntity.Common.XID, xid);
            sagaInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.UPDATE);
            sagaInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaMap.put(primaryKey, sagaInfo);
        }
    }

    // 记录添加操作
    private int addInsOp4Saga(int aid, FaiList<Param> list) {
        if (Util.isEmptyList(list)) {
            Log.logStd("addInsOp4Saga list is empty");
            return Errno.OK;
        }
        // 添加 Saga 操作记录
        FaiList<Param> sagaOpList = new FaiList<>();
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        list.forEach(data -> {
            Param sagaOpInfo = new Param();
            // 记录主键 + Saga 字段
            sagaOpInfo.assign(data, ProductSpecSkuCodeEntity.Info.AID);
            sagaOpInfo.assign(data, ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
            sagaOpInfo.assign(data, ProductSpecSkuCodeEntity.Info.SKU_CODE);
            sagaOpInfo.setString(SagaEntity.Common.XID, xid);
            sagaOpInfo.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            sagaOpInfo.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.ADD);
            sagaOpInfo.setCalendar(SagaEntity.Common.SAGA_TIME, now);
            sagaOpList.add(sagaOpInfo);
        });
        int rt = m_sagaDaoCtrl.batchInsert(sagaOpList, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "productSpecSkuCodeProc sagaOpList batch insert error;flow=%d;aid=%d;sagaOpList=%s", m_flow, aid, sagaOpList);
            return rt;
        }
        return rt;
    }

    // 记录删除操作
    private int addDelOp4Saga(int aid, ParamMatcher matcher) {
        if (matcher == null || matcher.isEmpty()) {
            Log.logStd("addDelOp4Saga matcher is empty");
            return Errno.OK;
        }
        int rt;

        // 查询旧数据
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<FaiList<Param>> listRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, listRef);
        if (rt != Errno.OK) {
            if (rt == Errno.NOT_FOUND) {
                return Errno.OK;
            }
            Log.logErr(rt, "select err;flow=%s;aid=%s;matcher=%s;", m_flow, aid, matcher);
            return rt;
        }
        String xid = RootContext.getXID();
        Long branchId = RootContext.getBranchId();
        Calendar now = Calendar.getInstance();
        listRef.value.forEach(info -> {
            info.setString(SagaEntity.Common.XID, xid);
            info.setLong(SagaEntity.Common.BRANCH_ID, branchId);
            info.setInt(SagaEntity.Common.SAGA_OP, SagaValObj.SagaOp.DEL);
            info.setCalendar(SagaEntity.Common.SAGA_TIME, now);
        });
        rt = m_sagaDaoCtrl.batchInsert(listRef.value, null, false);
        if (rt != Errno.OK) {
            Log.logErr(rt, "insert sagaOpList error;flow=%d;aid=%d;list=%s", m_flow, aid, listRef.value);
        }
        return rt;
    }

    /**
     * Saga 回滚
     * @param aid aid
     * @param xid 全局事务id
     * @param branchId 分支事务id
     */
    public void rollback4Saga(int aid, String xid, Long branchId) {
        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = getSagaOpList(xid, branchId, listRef);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get sagaOpList err;flow=%d;aid=%;xid=%s;branchId=%s", m_flow, aid, xid, branchId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            Log.logStd("skuCodeProc sagaOpList is empty");
            return;
        }

        // 按操作分类
        Map<Integer, List<Param>> groupBySagaOp = listRef.value.stream().collect(Collectors.groupingBy(x -> x.getInt(SagaEntity.Common.SAGA_OP)));

        // 回滚删除
        rollback4Del(aid, groupBySagaOp.get(SagaValObj.SagaOp.DEL));

        // 回滚新增
        rollback4Add(aid, groupBySagaOp.get(SagaValObj.SagaOp.ADD));

        // 回滚修改
        rollback4Update(aid, groupBySagaOp.get(SagaValObj.SagaOp.UPDATE));
    }

    /**
     * 回滚修改
     */
    private void rollback4Update(int aid, List<Param> sagaOpList) {
        if (Util.isEmptyList(sagaOpList)) {
            return;
        }
        FaiList<Param> dataList = new FaiList<>();
        for (Param info : sagaOpList) {
            Param data = new Param();
            int unionPriId = info.getInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID);
            String skuCode = info.getString(ProductSpecSkuCodeEntity.Info.SKU_CODE);
            long skuId = info.getLong(ProductSpecSkuCodeEntity.Info.SKU_ID);
            int sort = info.getInt(ProductSpecSkuCodeEntity.Info.SORT);
            // for update
            data.setLong(ProductSpecSkuCodeEntity.Info.SKU_ID, skuId);
            data.setInt(ProductSpecSkuCodeEntity.Info.SORT, sort);
            // for matcher
            data.setInt(ProductSpecSkuCodeEntity.Info.AID, aid);
            data.setInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, unionPriId);
            data.setString(ProductSpecSkuCodeEntity.Info.SKU_CODE, skuCode);
            dataList.add(data);
        }
        ParamUpdater updater = new ParamUpdater();
        updater.getData().setString(ProductSpecSkuCodeEntity.Info.SKU_ID, "?").setString(ProductSpecSkuCodeEntity.Info.SORT, "?");

        ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, "?");
        matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.EQ, "?");

        int rt = m_daoCtrl.batchUpdate(updater, matcher, dataList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batchUpdate err;flow=%d;aid=%d;dataList=%s", m_flow, aid, dataList);
        }
        Log.logStd("rollback update ok;flow=%d;aid=%d", m_flow, aid);
    }

    /**
     * 回滚新增
     */
    private void rollback4Add(int aid, List<Param> sagaOpList) {
        if (Util.isEmptyList(sagaOpList)) {
            return;
        }
        Map<Integer, List<Param>> groupByUnionPriId = sagaOpList.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID)));
        for (Integer unionPriId : groupByUnionPriId.keySet()) {
            List<Param> batchList = groupByUnionPriId.get(unionPriId);
            FaiList<String> skuCodeList = Utils.getValList(new FaiList<>(batchList), ProductSpecSkuCodeEntity.Info.SKU_CODE);
            ParamMatcher matcher = new ParamMatcher(ProductSpecSkuCodeEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(ProductSpecSkuCodeEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(ProductSpecSkuCodeEntity.Info.SKU_CODE, ParamMatcher.IN, skuCodeList);
            int rt = m_daoCtrl.delete(matcher);
            if (rt != Errno.OK) {
                throw new MgException(rt, "del specSkuCode err;flow=%d;aid=%d;skuCodeList=%s", m_flow, aid, skuCodeList);
            }
        }
        Log.logStd("rollback add ok;flow=%d;aid=%d;", m_flow, aid);
    }

    /**
     * 回滚删除操作
     */
    private void rollback4Del(int aid, List<Param> sagaOpList) {
        if (Util.isEmptyList(sagaOpList)) {
            return;
        }
        // 去除 Saga 字段
        for (Param info : sagaOpList) {
            info.remove(SagaEntity.Common.XID);
            info.remove(SagaEntity.Common.BRANCH_ID);
            info.remove(SagaEntity.Common.SAGA_OP);
            info.remove(SagaEntity.Common.SAGA_TIME);
        }
        int rt = m_daoCtrl.batchInsert(new FaiList<>(sagaOpList), null, false);
        if(rt != Errno.OK){
            throw new MgException(rt, "sagaDel rollback err;flow=%s;aid=%s;sagaOpList=%s;", m_flow, aid, sagaOpList);
        }
        Log.logStd("rollback del ok;flow=%s;aid=%s;", m_flow, aid);
    }

    // 将 sagaMap 中的修改数据插入 db
    public int addUpdateSaga2Db(int aid) {
        int rt;
        if (sagaMap.isEmpty()) {
            return Errno.OK;
        }
        rt = m_sagaDaoCtrl.batchInsert(new FaiList<>(sagaMap.values()), null, false);
        if (rt != Errno.OK) {
            Log.logErr("insert sagaMap error;flow=%d;aid=%d;sagaList=%s", m_flow, aid, sagaMap.values().toString());
            return rt;
        }
        return rt;
    }

    private ParamComparator getListFromCache(int aid, HashMap<Long, FaiList<String>> resultMap, HashSet<Long> skuIdSet, ParamComparator comparator) {
        Map<Long, FaiList<Param>> cacheMap = ProductSpecSkuCodeCacheCtrl.getInfoCache(aid, skuIdSet);
        cacheMap.forEach((skuId, skuCodeInfoList)->{
            comparator.sort(skuCodeInfoList);
            resultMap.put(skuId, Utils.getValList(skuCodeInfoList, ProductSpecSkuCodeEntity.Info.SKU_CODE));
            skuIdSet.remove(skuId);
        });
        return comparator;
    }

    public boolean deleteDirtyCache(int aid) {
        return cacheManage.deleteDirtyCache(aid);
    }

    private int m_flow;
    private ProductSpecSkuCodeDaoCtrl m_daoCtrl;
    private ProductSpecSkuCodeSagaDaoCtrl m_sagaDaoCtrl;

    private CacheManage cacheManage = new CacheManage();

    private static class CacheManage{

        public CacheManage() {
            init();
        }

        Map<Integer, Integer> unionPriIdDataFlagMap;
        Set<Long> skuIdSet;

        public boolean deleteDirtyCache(int aid){
            try {
                unionPriIdDataFlagMap.forEach((unionPriId, dataFlag)->{
                    ProductSpecSkuCodeCacheCtrl.DataStatusCache.clearDirty(aid, unionPriId, dataFlag);
                });
                ProductSpecSkuCodeCacheCtrl.delInfoCache(aid, skuIdSet);
            }finally {
                init();
            }
            return false;
        }

        public void setDataStatusDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            dataFlag |= ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.TOTAL_SIZE;
            dataFlag |= ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME;
            unionPriIdDataFlagMap.put(unionPriId, dataFlag);
        }

        public void setManageDirty(int aid, int unionPriId){
            int dataFlag = unionPriIdDataFlagMap.getOrDefault(unionPriId, 0);
            unionPriIdDataFlagMap.put(unionPriId, dataFlag| ProductSpecSkuCodeCacheCtrl.DataStatusCache.DataFlag.MANAGE_LAST_UPDATE_TIME);
        }

        private void init() {
            unionPriIdDataFlagMap = new HashMap<>();
            skuIdSet = new HashSet<>();
        }

        public void setSkuIdListDirty(int aid, Collection<Long> skuIdList) {
            skuIdSet.addAll(skuIdList);
        }
        public void setSkuIdDirty(int aid, Long skuId) {
            skuIdSet.add(skuId);
        }
    }

    private Map<PrimaryKey, Param> sagaMap;

    private static class PrimaryKey {
        int aid;
        int unionPriId;
        String skuCode;

        public PrimaryKey(int aid, int unionPriId, String skuCode) {
            this.aid = aid;
            this.unionPriId = unionPriId;
            this.skuCode = skuCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return aid == that.aid &&
                    unionPriId == that.unionPriId &&
                    Objects.equals(skuCode, that.skuCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aid, unionPriId, skuCode);
        }

        @Override
        public String toString() {
            return "PrimaryKey{" +
                    "aid=" + aid +
                    ", unionPriId=" + unionPriId +
                    ", skuCode='" + skuCode + '\'' +
                    '}';
        }
    }
}
