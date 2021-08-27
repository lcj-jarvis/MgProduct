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
     * 按照条件查询数据，默认是查询同一个aid下的全部数据.
     * @param searchArg 查询条件
     */
    public FaiList<Param> getListFromCacheOrDb(int aid, SearchArg searchArg) {
        FaiList<Param> list;
        // 从缓存获取数据
        list = ProductLibCache.getCacheList(aid);
        if (!Util.isEmptyList(list)) {
            return list;
        }
        //防止缓存穿透
        LockUtil.LibLock.readLock(aid);
        try {
            // check again
            list = ProductLibCache.getCacheList(aid);
            if (!Util.isEmptyList(list)) {
                return list;
            }
            //无缓存，查询db
            list = getListFromDb(aid, searchArg);
            //添加到缓存
            ProductLibCache.addCacheList(aid, list);
        } finally {
            LockUtil.LibLock.readUnLock(aid);
        }
        return list;
    }

    public FaiList<Param> getListFromDb(int aid, SearchArg searchArg) {
        FaiList<Param> list;
        //无searchArg
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        //有searchArg，无查询条件
        if (searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //如果查询过来的条件已经包含这两个查询条件,就先删除
        searchArg.matcher.remove(ProductLibEntity.Info.AID);
        //有searchArg，有查询条件，加多一个查询条件
        searchArg.matcher.and(ProductLibRelEntity.Info.AID, ParamMatcher.EQ, aid);

        //为了克隆需要，因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tableName（并不影响其他的业务）
        m_daoCtrl.setTableName(aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);

        //查完之后恢复最初的tableName
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
     * 修改库表（只修改部分字段）
     */
    public void setLibList(int aid, FaiList<ParamUpdater> libUpdaterList) {
        int rt;
        //入参校验
        for(ParamUpdater updater : libUpdaterList){
            Param updateInfo = updater.getData();
            String libName = updateInfo.getString(ProductLibEntity.Info.LIB_NAME);
            if(libName != null && !ProductLibCheck.isNameValid(libName)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, libName);
            }
        }

        //先获取到库表的所有的数据
        FaiList<Param> oldList = getListFromCacheOrDb(aid,null);
        //保存更新的数据
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : libUpdaterList){
            Param updateInfo = updater.getData();
            int libId = updateInfo.getInt(ProductLibEntity.Info.LIB_ID, 0);
            //获取到要修改的记录
            Param oldInfo = Misc.getFirstNullIsEmpty(oldList, ProductLibEntity.Info.LIB_ID, libId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            //保存修改的信息到oldInfo中
            oldInfo = updater.update(oldInfo, true);

            //只更新部分信息，和sql语句的参数一致
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

        //设置修改的条件
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductLibEntity.Info.LIB_ID, ParamMatcher.EQ, "?");
        //设置要修改的信息（这里没有修改库类型）
        Param item = new Param();
        item.setString(ProductLibEntity.Info.LIB_NAME, "?");
        item.setString(ProductLibEntity.Info.LIB_TYPE, "?");
        item.setString(ProductLibEntity.Info.FLAG, "?");
        item.setString(ProductLibEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        //注意，data中保存数据的顺序和个数要和sql语句入参的顺序一致
        rt = m_daoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product Lib error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);

        }
    }

    /**
     * 批量添加库表的数据
     * @param libInfoList 保存到库表的数据
     * @param libIdsRef 接收库id
     */
    public void addLibBatch(int aid, FaiList<Param> libInfoList, FaiList<Integer> libIdsRef) {
        int rt;
        if(Util.isEmptyList(libInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;libInfo=%s", m_flow, aid, libInfoList);
        }

        FaiList<Param> list = getListFromCacheOrDb(aid,null);
        int count = list.size();
        //判断是否超出数量限制
        boolean isOverLimit = (count >= ProductLibValObj.Limit.COUNT_MAX) ||
                              (count + libInfoList.size() >  ProductLibValObj.Limit.COUNT_MAX);
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;currentCount=%d;limit=%d;wantAddSize=%d;", m_flow, aid,
                    count, ProductLibValObj.Limit.COUNT_MAX, libInfoList.size());
        }

        int libId = 0;
        //检查名称是否合法
        for (Param libInfo:libInfoList) {
            String libName = libInfo.getString(ProductLibEntity.Info.LIB_NAME);
            Param existInfo = Misc.getFirst(list, ProductLibEntity.Info.LIB_NAME, libName);
            if(!Str.isEmpty(existInfo)) {
                rt = Errno.ALREADY_EXISTED;
                throw new MgException(rt, "lib name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, libName);
            }
            //自增库id
            libId = creatAndSetId(aid, libInfo);
            //保存libId
            libIdsRef.add(libId);
        }

        //批量插入，并且不将libInfoList的元素设置为null
        rt = m_daoCtrl.batchInsert(libInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product lib error;flow=%d;aid=%d;", m_flow, aid);

        }
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if (rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        //删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
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

        // 初始容量直接定为所需的最大容量，去掉不必要的扩容
        Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1);
        FaiList<Calendar> updateTimeList = new FaiList<>();
        for (Param fromInfo : fromList) {
            fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
            newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
            updateTimeList.add(fromInfo.getCalendar(ProductLibEntity.Info.UPDATE_TIME));
        }

        // 查出已有的备份数据，通过updateTime确定数据是否已备份
        SearchArg oldBakArg = new SearchArg();
        oldBakArg.matcher = searchArg.matcher.clone();
        oldBakArg.matcher.and(ProductLibEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
        FaiList<Param> oldBakList = getBakList(aid, oldBakArg);

        Set<String> oldBakUniqueKeySet = new HashSet<>((int) (oldBakList.size() / 0.75f) + 1);
        for (Param oldBak : oldBakList) {
            oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
        }
        // 获取交集，说明剩下的这些是要合并的备份数据
        oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);
        rt = updateBackup(aid, backupFlag, oldBakUniqueKeySet);
        if (rt != Errno.OK) {
            throw new MgException(rt, "merge bak update err;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
        }

        // 移除掉合并的数据，剩下的就是需要新增的备份数据
        newBakUniqueKeySet.removeAll(oldBakUniqueKeySet);

        for (int j = fromList.size(); --j >= 0; ) {
            Param formInfo = fromList.get(j);
            if (newBakUniqueKeySet.contains(getBakUniqueKey(formInfo))) {
                // 置起当前备份标识
                formInfo.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                continue;
            }
            fromList.remove(j);
        }

        if (fromList.isEmpty()) {
            Log.logStd("backupData ok, need add bak empty;aid=%d;bakLibIds=%s;backupId=%d;backupFlag=%d;", aid, bakLibIds, backupId, backupFlag);
            return;
        }

        // 批量插入备份表
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
        // 合并标记
        ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);
        // 合并条件
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

        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        delLibList(aid, delMatcher);

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = getBakList(aid, bakSearchArg);

        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_daoCtrl.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        m_daoCtrl.restoreMaxId(false);
        m_daoCtrl.clearIdBuilderCache(aid);
    }
}
