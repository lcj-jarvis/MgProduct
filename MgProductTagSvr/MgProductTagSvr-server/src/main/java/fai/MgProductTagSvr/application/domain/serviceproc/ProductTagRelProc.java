package fai.MgProductTagSvr.application.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagBakDaoCtrl;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagRelBakDaoCtrl;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagRelDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:03
 */
public class ProductTagRelProc {
    
    private int m_flow;
    private ProductTagRelDaoCtrl m_relDaoCtrl;
    private ProductTagRelBakDaoCtrl m_bakDao;

    public ProductTagRelProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        this.m_flow = flow;
        this.m_relDaoCtrl = ProductTagRelDaoCtrl.getInstance(flow, aid);
        this.m_bakDao = ProductTagRelBakDaoCtrl.getInstance(flow, aid);
        init(transactionCtrl);
    }

    private void init(TransactionCtrl transactionCtrl) {
        if (transactionCtrl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductTagDao err;");
        }
        if(!transactionCtrl.register(m_relDaoCtrl)) {
            throw new MgException("registered ProductTagDao err;");

        }
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
    }

    /**
     * 创建自增的标签id
     */
    private int createAndSetId(int aid, int unionPriId, Param relTagInfo) {
        Integer rlTagId = relTagInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
        if(rlTagId <= 0) {
            rlTagId = m_relDaoCtrl.buildId(aid, unionPriId, false);
            if (rlTagId == null) {
                throw new MgException(Errno.ERROR, "rlTagId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            rlTagId = m_relDaoCtrl.updateId(aid, unionPriId, rlTagId, false);
            if (rlTagId == null) {
                throw new MgException(Errno.ERROR, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        relTagInfo.setInt(ProductTagRelEntity.Info.RL_TAG_ID, rlTagId);

        return rlTagId;
    }

    /**
     * 获取最大的排序字段
     */
    public int getMaxSort(int aid, int unionPriId) {
        String sortCache = ProductTagRelCache.SortCache.get(aid, unionPriId);
        if(!Str.isEmpty(sortCache)) {
            return Parser.parseInt(sortCache, ProductTagRelValObj.Default.SORT);
        }

        // db中获取
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = m_relDaoCtrl.select(searchArg, listRef, "max(sort) as sort");
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return ProductTagRelValObj.Default.SORT;
        }

        Param info = listRef.value.get(0);
        int sort = info.getInt(ProductTagRelEntity.Info.SORT, ProductTagRelValObj.Default.SORT);
        // TODO 添加到缓存.后面删除掉，因为在调用了这个方法的方法里面的finally会处理。
        ProductTagRelCache.SortCache.set(aid, unionPriId, sort);
        return sort;
    }

    public void addTagRelBatch(int aid, int unionPriId, FaiList<Param> relTagInfoList, FaiList<Integer> relTagIds) {
        int rt;
        if(Util.isEmptyList(relTagInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, relTagInfoList is empty;flow=%d;aid=%d;relTagInfoList=%s", m_flow, aid, relTagInfoList);
        }

        //判断是否超出数量限制
        FaiList<Param> list = getListFromCacheOrDb(aid, unionPriId, null);
        int count = list.size();
        boolean isOverLimit = count + relTagInfoList.size() > ProductTagRelValObj.Limit.COUNT_MAX;
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;addSize=%d;", m_flow, aid, count,
                    ProductTagRelValObj.Limit.COUNT_MAX, relTagInfoList.size());
        }

        int relTagId = 0;
        for (Param relTagInfo:relTagInfoList) {
            //自增标签业务id
            relTagId = createAndSetId(aid, unionPriId, relTagInfo);
            //保存标签业务id
            relTagIds.add(relTagId);
        }

        //批量插入,并且不将relTagInfoList的元素设置为null
        rt = m_relDaoCtrl.batchInsert(relTagInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product tag rel error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    /**
     * 根据标签业务id获取所有的标签id
     */
    public FaiList<Integer> getTagIdsByRlTagIds(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        FaiList<Param> list = getListFromCacheOrDb(aid, unionPriId, null);
        FaiList<Integer> tagIdList = new FaiList<Integer>();
        if(list.isEmpty()) {
            return tagIdList;
        }
        list = Misc.getList(list, new ParamMatcher(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds));
        list.forEach(param -> tagIdList.add(param.getInt(ProductTagRelEntity.Info.TAG_ID)));
        return tagIdList;
    }
    
    /**
     * 按照条件查询数据，默认是查询同一个aid和unionPriId下的全部数据.如果没有缓存，再查db
     * @param searchArg 查询条件
     */
    public FaiList<Param> getListFromCacheOrDb(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list;
        // 从缓存获取数据
        list = ProductTagRelCache.InfoCache.getCacheList(aid, unionPriId);
        if (!Util.isEmptyList(list)) {
            return list;
        }

        LockUtil.TagRelLock.readLock(aid);
        try {
            // check again
            list = ProductTagRelCache.InfoCache.getCacheList(aid, unionPriId);
            if (!Util.isEmptyList(list)) {
                return list;
            }
            //没有缓存，去查db
            list = getListFromDb(aid, unionPriId, searchArg);
            //添加到缓存（直接查DB的不需要添加缓存）
            ProductTagRelCache.InfoCache.addCacheList(aid, unionPriId, list);
        } finally {
            LockUtil.TagRelLock.readUnLock(aid);
        }
        return list;
    }

    public FaiList<Param> getListFromDb(int aid, int unionPriId, SearchArg searchArg){
        FaiList<Param> list;
        //无searchArg
        if (searchArg == null) {
            searchArg = new SearchArg();
        }

        //有searchArg，无查询条件
        if (searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }

        //避免查询过来的条件已经包含这两个查询条件,就先删除，防止重复添加查询条件
        searchArg.matcher.remove(ProductTagRelEntity.Info.AID);
        searchArg.matcher.remove(ProductTagRelEntity.Info.UNION_PRI_ID);

        //有searchArg，有查询条件，加多两个查询条件
        searchArg.matcher.and(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

        //为了克隆需要,因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tableName（并不影响其他业务）
        m_relDaoCtrl.setTableName(aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_relDaoCtrl.select(searchArg, listRef);

        //恢复之前的表名
        m_relDaoCtrl.restoreTableName();

        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
        }
        list = listRef.value;
        if(list == null) {
            list = new FaiList<Param>();
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return list;
        }
        return list;
    }

    public void delRelTagList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "matcher is null;aid=%d;", aid);
        }
        matcher.and(ProductTagEntity.Info.AID, ParamMatcher.EQ, aid);

        rt = m_relDaoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delTagList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delTagList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
    }

    /**
     * 修改标签业务表(只修改部分字段)
     */
    public void setTagRelList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> tagUpdaterList) {
        int rt;
        // 保存修改之前标签表的数据
        FaiList<Param> oldInfoList = getListFromCacheOrDb(aid, unionPriId, null);
        // 保存修改的数据
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : updaterList){
            Param updateInfo = updater.getData();
            int rlTagId = updateInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
            //获取到reTagId属于oldInfoList的reTagIds中的数据
            Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductTagRelEntity.Info.RL_TAG_ID, rlTagId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            int tagId = oldInfo.getInt(ProductTagRelEntity.Info.TAG_ID);
            //将updater中保存的更新数据Param保存到oldInfo中
            oldInfo = updater.update(oldInfo, true);

            //保存修改的数据
            Param data = new Param();
            //只能修改rlFlag，sort，和updateTime
            int sort = oldInfo.getInt(ProductTagRelEntity.Info.SORT, 0);
            int rlFlag = oldInfo.getInt(ProductTagRelEntity.Info.RL_FLAG, 0);
            data.setInt(ProductTagRelEntity.Info.SORT, sort);
            data.setInt(ProductTagRelEntity.Info.RL_FLAG, rlFlag);
            data.setCalendar(ProductTagRelEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductTagRelEntity.Info.AID);
            data.assign(oldInfo, ProductTagRelEntity.Info.UNION_PRI_ID);
            data.assign(oldInfo, ProductTagRelEntity.Info.RL_TAG_ID);

            dataList.add(data);
            if(tagUpdaterList != null) {
                //保存关联的tagId
                updateInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagId);
                tagUpdaterList.add(updater);
            }
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList empty;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return;
        }

        //设置更新sql的where条件
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.EQ, "?");

        //设置更新的字段（这里没有修改标签类型）
        Param item = new Param();
        item.setString(ProductTagRelEntity.Info.SORT, "?");
        item.setString(ProductTagRelEntity.Info.RL_FLAG, "?");
        item.setString(ProductTagRelEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);

        //setNullList：sql入参的过程中，入参完成后，清空dataList的数据为null
        rt = m_relDaoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product tag error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    /**
     * 获取数据的状态
     */
    public Param getDataStatus(int aid, int unionPriId) {
        Param statusInfo = ProductTagRelCache.DataStatusCache.get(aid, unionPriId);
        if(!Str.isEmpty(statusInfo)) {
            // 获取数据，则更新数据过期时间
            ProductTagRelCache.DataStatusCache.expire(aid, unionPriId, 6*3600);
            return statusInfo;
        }
        long now = System.currentTimeMillis();
        statusInfo = new Param();
        int totalSize = getListFromCacheOrDb(aid, unionPriId, null).size();
        statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, totalSize);
        statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
        statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

        ProductTagRelCache.DataStatusCache.add(aid, unionPriId, statusInfo);
        return statusInfo;
    }

    /**
     * 标签业务表添加增量克隆的数据
     */
    public void addIncrementalClone(int aid, FaiList<Integer> unionPriIds, FaiList<Param> list) {
        if(Util.isEmptyList(list)) {
            Log.logStd("incrementalClone list is empty;aid=%d;uid=%s;", aid, unionPriIds);
            return;
        }
        int rt = m_relDaoCtrl.batchInsert(list, null, false);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert tag rel error;flow=%d;aid=%d;", m_flow, aid);
        }

        for(Integer unionPriId : unionPriIds) {
            rt = m_relDaoCtrl.restoreMaxId(unionPriId, false);
            if(rt != Errno.OK) {
                throw new MgException("restoreMaxId err;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            }
            m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
        }
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
        ParamMatcher delMatcher = new ParamMatcher(MgBackupEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
        rt = m_bakDao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        Log.logStd("del rel bak ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
    }

    public Set<Integer> backupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        Set<Integer> tagIds = new HashSet<>();
        for(int unionPriId : unionPriIds) {
            FaiList<Param> fromList = getListFromDb(aid, unionPriId, null);
            if(fromList.isEmpty()) {
                continue;
            }
            // 初始容量直接定为所需的最大容量，去掉不必要的扩容
            Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1);
            FaiList<Calendar> updateTimeList = new FaiList<>();
            for (Param fromInfo : fromList) {
                fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
                newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
                updateTimeList.add(fromInfo.getCalendar(ProductTagRelEntity.Info.UPDATE_TIME));
                tagIds.add(fromInfo.getInt(ProductTagRelEntity.Info.TAG_ID));
            }

            SearchArg oldBakArg = new SearchArg();
            oldBakArg.matcher = new ParamMatcher(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            oldBakArg.matcher = new ParamMatcher(ProductTagRelEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
            FaiList<Param> oldBakList = getBakList(aid, oldBakArg);

            Set<String> oldBakUniqueKeySet = new HashSet<>((int)(oldBakList.size()/0.75f)+1);
            for (Param oldBak : oldBakList) {
                oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
            }
            // 获取交集，说明剩下的这些是要合并的备份数据
            oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);
            rt = updateBackup(aid, backupFlag, oldBakUniqueKeySet);
            if(rt != Errno.OK) {
                throw new MgException(rt, "merge bak update err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
            }

            // 移除掉合并的数据，剩下的就是需要新增的备份数据
            newBakUniqueKeySet.removeAll(oldBakUniqueKeySet);

            for (int j = fromList.size(); --j >= 0;) {
                Param formInfo = fromList.get(j);
                if(newBakUniqueKeySet.contains(getBakUniqueKey(formInfo))){
                    // 置起当前备份标识
                    formInfo.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
                    continue;
                }
                fromList.remove(j);
            }

            if(fromList.isEmpty()) {
                continue;
            }
            // 批量插入备份表
            rt = m_bakDao.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "batchInsert bak err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
            }
        }
        Log.logStd("backupData ok;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);

        return tagIds;
    }

    /**
     * 解耦合，方便以后扩展
     * @param oldBakUniqueKeySet  由getBakUniqueKey(Param fromInfo)方法获得的bakUniqueKey组成的集合
     */
    public int updateBackup(int aid, int backupFlag, Collection<String> oldBakUniqueKeySet) {
        if (oldBakUniqueKeySet.isEmpty()) {
            return Errno.OK;
        }
        int rt;
        // 合并标记
        ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);

        // 合并条件
        ParamMatcher mergeMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, "?");
        mergeMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
        mergeMatcher.and(ProductTagRelEntity.Info.TAG_ID, ParamMatcher.EQ, "?");
        mergeMatcher.and(ProductTagRelEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

        FaiList<Param> dataList = new FaiList<Param>();
        for (String bakUniqueKey : oldBakUniqueKeySet) {
            String[] keys = bakUniqueKey.split(DELIMITER);
            Calendar updateTime = Calendar.getInstance();
            updateTime.setTimeInMillis(Long.parseLong(keys[2]));
            Param data = new Param();

            // mergeUpdater start
            data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
            // mergeUpdater end

            // mergeMatcher start
            data.setInt(ProductTagRelEntity.Info.AID, aid);
            data.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, Integer.valueOf(keys[0]));
            data.setInt(ProductTagRelEntity.Info.TAG_ID, Integer.valueOf(keys[1]));
            data.setCalendar(ProductTagRelEntity.Info.UPDATE_TIME, updateTime);
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
        searchArg.matcher.and(ProductTagEntity.Info.AID, ParamMatcher.EQ, aid);

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
        return fromInfo.getInt(ProductTagRelEntity.Info.UNION_PRI_ID) +
                DELIMITER +
                fromInfo.getInt(ProductTagRelEntity.Info.TAG_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductTagRelEntity.Info.UPDATE_TIME).getTimeInMillis();
    }

    private final static String DELIMITER = "-";

    public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_relDaoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "relDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
        }
        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        delRelTagList(aid, delMatcher);

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
        FaiList<Param> fromList = getBakList(aid, bakSearchArg);
        for(Param fromInfo : fromList) {
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
            fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
        }

        if(!fromList.isEmpty()) {
            // 批量插入
            rt = m_relDaoCtrl.batchInsert(fromList);
            if(rt != Errno.OK) {
                throw new MgException(rt, "restore insert err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }

        // 处理idBuilder
        for(int unionPriId : unionPriIds) {
            m_relDaoCtrl.restoreMaxId(unionPriId, false);
            m_relDaoCtrl.clearIdBuilderCache(aid, unionPriId);
        }
    }
}
