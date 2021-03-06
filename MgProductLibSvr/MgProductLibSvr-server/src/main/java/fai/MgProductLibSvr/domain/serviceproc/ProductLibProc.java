package fai.MgProductLibSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.common.ProductLibCheck;
import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibCache;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibBakDaoCtrl;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:23
 */
public class ProductLibProc {

    private int m_flow;
    private ProductLibDaoCtrl m_daoCtrl;
    private ProductLibBakDaoCtrl m_bakDao;

    public ProductLibProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductLibDaoCtrl.getInstance(flow, aid);
        this.m_bakDao = ProductLibBakDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if (transactionCrtl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductLibDao err;");
        }
        if(!transactionCrtl.register(m_daoCtrl)) {
            throw new MgException("registered ProductLibDao err;");

        }
    }

    private int creatAndSetId(int aid, Param libInfo) {
        Integer libId = libInfo.getInt(ProductLibEntity.Info.LIB_ID, 0);
        if(libId <= 0) {
            libId = m_daoCtrl.buildId(aid, false);
            if (libId == null) {
                throw new MgException(Errno.ERROR, "libId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            libId = m_daoCtrl.updateId(aid, libId, false);
            if (libId == null) {
                throw new MgException(Errno.ERROR, "libId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        libInfo.setInt(ProductLibEntity.Info.LIB_ID, libId);

        return libId;
    }

    /**
     * ???????????????????????????????????????????????????aid??????????????????.
     * @param searchArg ????????????
     */
    public FaiList<Param> getListFromCacheOrDb(int aid, SearchArg searchArg) {
        FaiList<Param> list;
        // ?????????????????????
        list = ProductLibCache.getCacheList(aid);
        if (!Util.isEmptyList(list)) {
            return list;
        }
        //??????????????????
        LockUtil.LibLock.readLock(aid);
        try {
            // check again
            list = ProductLibCache.getCacheList(aid);
            if (!Util.isEmptyList(list)) {
                return list;
            }
            //??????????????????db
            list = getListFromDb(aid, searchArg);
            //???????????????
            ProductLibCache.addCacheList(aid, list);
        } finally {
            LockUtil.LibLock.readUnLock(aid);
        }
        return list;
    }

    public FaiList<Param> getListFromDb(int aid, SearchArg searchArg) {
        FaiList<Param> list;
        //???searchArg
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        //???searchArg??????????????????
        if (searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //????????????????????????????????????????????????????????????,????????????
        searchArg.matcher.remove(ProductLibEntity.Info.AID);
        //???searchArg?????????????????????????????????????????????
        searchArg.matcher.and(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);

        //???????????????????????????????????????????????????aid????????????????????????????????????aid??????tableName?????????????????????????????????
        m_daoCtrl.setTableName(aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);

        //???????????????????????????tableName
        m_daoCtrl.restoreTableName();

        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        list = listRef.value;
        if (list == null) {
            list = new FaiList<Param>();
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }
        return list;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    public void delLibList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "matcher is null;aid=%d;", aid);
        }
        matcher.and(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delLibList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delLibList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
    }

    /**
     * ???????????????????????????????????????
     */
    public void setLibList(int aid, FaiList<ParamUpdater> libUpdaterList) {
        int rt;
        //????????????
        for(ParamUpdater updater : libUpdaterList){
            Param updateInfo = updater.getData();
            String libName = updateInfo.getString(ProductLibEntity.Info.LIB_NAME);
            if(libName != null && !ProductLibCheck.isNameValid(libName)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, libName);
            }
        }

        //????????????????????????????????????
        FaiList<Param> oldList = getListFromCacheOrDb(aid,null);
        //?????????????????????
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : libUpdaterList){
            Param updateInfo = updater.getData();
            int libId = updateInfo.getInt(ProductLibEntity.Info.LIB_ID, 0);
            //???????????????????????????
            Param oldInfo = Misc.getFirstNullIsEmpty(oldList, ProductLibEntity.Info.LIB_ID, libId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            //????????????????????????oldInfo???
            oldInfo = updater.update(oldInfo, true);

            //???????????????????????????sql?????????????????????
            Param data = new Param();
            data.assign(oldInfo, ProductLibEntity.Info.LIB_NAME);
            data.assign(oldInfo, ProductLibEntity.Info.LIB_TYPE);
            data.assign(oldInfo, ProductLibEntity.Info.FLAG);
            data.setCalendar(ProductLibEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductLibEntity.Info.AID);
            data.assign(oldInfo, ProductLibEntity.Info.LIB_ID);
            dataList.add(data);
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList empty;flow=%d;aid=%d;", m_flow, aid);
            return;
        }

        //?????????????????????
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductLibEntity.Info.LIB_ID, ParamMatcher.EQ, "?");
        //?????????????????????????????????????????????????????????
        Param item = new Param();
        item.setString(ProductLibEntity.Info.LIB_NAME, "?");
        item.setString(ProductLibEntity.Info.LIB_TYPE, "?");
        item.setString(ProductLibEntity.Info.FLAG, "?");
        item.setString(ProductLibEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        //?????????data???????????????????????????????????????sql???????????????????????????
        rt = m_daoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product Lib error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);

        }
    }

    /**
     * ???????????????????????????
     * @param libInfoList ????????????????????????
     * @param libIdsRef ?????????id
     */
    public void addLibBatch(int aid, FaiList<Param> libInfoList, FaiList<Integer> libIdsRef) {
        int rt;
        if(Util.isEmptyList(libInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;libInfo=%s", m_flow, aid, libInfoList);
        }

        FaiList<Param> list = getListFromCacheOrDb(aid,null);
        int count = list.size();
        //??????????????????????????????
        boolean isOverLimit = (count >= ProductLibValObj.Limit.COUNT_MAX) ||
                              (count + libInfoList.size() >  ProductLibValObj.Limit.COUNT_MAX);
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;currentCount=%d;limit=%d;wantAddSize=%d;", m_flow, aid,
                    count, ProductLibValObj.Limit.COUNT_MAX, libInfoList.size());
        }

        int libId = 0;
        //????????????????????????
        for (Param libInfo:libInfoList) {
            String libName = libInfo.getString(ProductLibEntity.Info.LIB_NAME);
            Param existInfo = Misc.getFirst(list, ProductLibEntity.Info.LIB_NAME, libName);
            if(!Str.isEmpty(existInfo)) {
                rt = Errno.ALREADY_EXISTED;
                throw new MgException(rt, "lib name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, libName);
            }
            //?????????id
            libId = creatAndSetId(aid, libInfo);
            //??????libId
            libIdsRef.add(libId);
        }

        //???????????????????????????libInfoList??????????????????null
        rt = m_daoCtrl.batchInsert(libInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product lib error;flow=%d;aid=%d;", m_flow, aid);

        }
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // ?????? backupFlag ?????????????????????????????????
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if (rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        //?????? backupIdFlag ???0????????????backupIdFlag???0 ??????????????????????????????????????????????????????
        ParamMatcher delMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
        rt = m_bakDao.delete(delMatcher);
        if (rt != Errno.OK) {
            throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }
    }

    public void backupData(int aid, int backupId, int backupFlag, Set<Integer> bakLibIds) {
        int rt;
        if (m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN, new FaiList<Integer>(bakLibIds));
        FaiList<Param> fromList = getListFromDb(aid, searchArg);

        // ????????????????????????????????????????????????????????????????????????
        Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1);
        FaiList<Calendar> updateTimeList = new FaiList<>();
        for (Param fromInfo : fromList) {
            fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
            newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
            updateTimeList.add(fromInfo.getCalendar(ProductLibEntity.Info.UPDATE_TIME));
        }

        // ????????????????????????????????????updateTime???????????????????????????
        SearchArg oldBakArg = new SearchArg();
        oldBakArg.matcher = searchArg.matcher.clone();
        oldBakArg.matcher.and(ProductLibEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
        FaiList<Param> oldBakList = getBakList(aid, oldBakArg);

        Set<String> oldBakUniqueKeySet = new HashSet<>((int) (oldBakList.size() / 0.75f) + 1);
        for (Param oldBak : oldBakList) {
            oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
        }
        // ???????????????????????????????????????????????????????????????
        oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);
        rt = updateBackup(aid, backupFlag, oldBakUniqueKeySet);
        if (rt != Errno.OK) {
            throw new MgException(rt, "merge bak update err;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
        }

        // ?????????????????????????????????????????????????????????????????????
        newBakUniqueKeySet.removeAll(oldBakUniqueKeySet);

        for (int j = fromList.size(); --j >= 0; ) {
            Param formInfo = fromList.get(j);
            if (newBakUniqueKeySet.contains(getBakUniqueKey(formInfo))) {
                // ????????????????????????
                formInfo.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                continue;
            }
            fromList.remove(j);
        }

        if (fromList.isEmpty()) {
            Log.logStd("backupData ok, need add bak empty;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
            return;
        }

        // ?????????????????????
        rt = m_bakDao.batchInsert(fromList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "batchInsert bak err;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
        }

        Log.logStd("backupData ok;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
    }


    public int updateBackup(int aid, int backupFlag, Collection<String> oldBakUniqueKeySet) {
        if (oldBakUniqueKeySet.isEmpty()) {
            return Errno.OK;
        }

        int rt;
        // ????????????
        ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);
        // ????????????
        ParamMatcher mergeMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, "?");
        mergeMatcher.and(ProductLibEntity.Info.LIB_ID, ParamMatcher.EQ, "?");
        mergeMatcher.and(ProductLibEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");
        FaiList<Param> dataList = new FaiList<Param>();
        for (String bakUniqueKey : oldBakUniqueKeySet) {
            String[] keys = bakUniqueKey.split(DELIMITER);
            Calendar updateTime = Calendar.getInstance();
            updateTime.setTimeInMillis(Long.parseLong(keys[1]));
            Param data = new Param();

            // mergeUpdater start
            data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
            // mergeUpdater end

            // mergeMatcher start
            data.setInt(ProductLibEntity.Info.AID, aid);
            data.setInt(ProductLibEntity.Info.LIB_ID, Integer.valueOf(keys[0]));
            data.setCalendar(ProductLibEntity.Info.UPDATE_TIME, updateTime);
            // mergeMatcher end

            dataList.add(data);
        }
        rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
        return rt;
    }

    public FaiList<Param> getBakList(int aid, SearchArg searchArg) {
        if(searchArg == null) {
            searchArg = new SearchArg();
        }
        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        searchArg.matcher.and(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_bakDao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        if (listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
        }
        return listRef.value;
    }

    private static String getBakUniqueKey(Param fromInfo) {
        return fromInfo.getInt(ProductLibEntity.Info.LIB_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductLibEntity.Info.UPDATE_TIME).getTimeInMillis();
    }
    private final static String DELIMITER = "-";

    public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // ?????????????????????
        ParamMatcher delMatcher = new ParamMatcher(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        delLibList(aid, delMatcher);

        // ??????????????????
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = getBakList(aid, bakSearchArg);

        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // ????????????
            rt = m_daoCtrl.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
            }
        }

        // ??????idBuilder
        m_daoCtrl.restoreMaxId(false);
        m_daoCtrl.clearIdBuilderCache(aid);
    }
}
