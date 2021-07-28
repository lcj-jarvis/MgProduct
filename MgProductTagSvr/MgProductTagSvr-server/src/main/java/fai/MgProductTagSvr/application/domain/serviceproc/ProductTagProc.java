package fai.MgProductTagSvr.application.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.common.ProductTagCheck;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagCache;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagBakDaoCtrl;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:03
 */
public class ProductTagProc {

    private int m_flow;
    private ProductTagDaoCtrl m_daoCtrl;
    private ProductTagBakDaoCtrl m_bakDao;

    public ProductTagProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductTagDaoCtrl.getInstance(flow, aid);
        this.m_bakDao = ProductTagBakDaoCtrl.getInstance(flow, aid);
        init(transactionCtrl);
    }

    private void init(TransactionCtrl transactionCtrl) {
        if (transactionCtrl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductTagDao err;");
        }
        if(!transactionCtrl.register(m_daoCtrl)) {
            throw new MgException("registered ProductTagDao err;");

        }
    }

    /**
     * 构建自增tagId
     * @param aid
     * @param tagInfo
     * @return
     */
    private int creatAndSetId(int aid, Param tagInfo) {
        Integer tagId = tagInfo.getInt(ProductTagEntity.Info.TAG_ID, 0);
        if(tagId <= 0) {
            tagId = m_daoCtrl.buildId(aid, false);
            if (tagId == null) {
                throw new MgException(Errno.ERROR, "tagId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            tagId = m_daoCtrl.updateId(aid, tagId, false);
            if (tagId == null) {
                throw new MgException(Errno.ERROR, "tagId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        tagInfo.setInt(ProductTagEntity.Info.TAG_ID, tagId);

        return tagId;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    /**
     * 批量添加标签表的数据
     * @param tagInfoList 保存到标签表的数据
     * @param tagIdsRef 接收标签id
     */
    public void addTagBatch(int aid, FaiList<Param> tagInfoList, FaiList<Integer> tagIdsRef) {
        int rt;
        if(Util.isEmptyList(tagInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;tagInfo=%s", m_flow, aid, tagInfoList);
        }

        //获取一个aid下的所有标签
        FaiList<Param> list = getListFromCacheOrDb(aid,null);
        int count = list.size();
        //判断是否超出数量限制
        boolean isOverLimit = count + tagInfoList.size() >  ProductTagValObj.Limit.COUNT_MAX;
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;currentCount=%d;limit=%d;wantAddSize=%d;", m_flow, aid,
                    count, ProductTagValObj.Limit.COUNT_MAX, tagInfoList.size());
        }

        int tagId = 0;
        //检查名称是否合法
        for (Param tagInfo:tagInfoList) {
            String tagName = tagInfo.getString(ProductTagEntity.Info.TAG_NAME);
            Param existInfo = Misc.getFirst(list, ProductTagEntity.Info.TAG_NAME, tagName);
            if(!Str.isEmpty(existInfo)) {
                rt = Errno.ALREADY_EXISTED;
                throw new MgException(rt, "tag name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, tagName);
            }
            //自增标签id
            tagId = creatAndSetId(aid, tagInfo);
            //保存tagId
            tagIdsRef.add(tagId);
        }

        //批量插入，并且不将tagInfoList的元素设置为null
        rt = m_daoCtrl.batchInsert(tagInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product tag error;flow=%d;aid=%d;", m_flow, aid);

        }
    }

    /**
     * 按照条件查询数据，默认是查询同一个aid下的全部数据.
     * (该方法方便后期扩展只查DB的情形)
     * @param searchArg 查询条件
     */
    public FaiList<Param> getListFromCacheOrDb(int aid, SearchArg searchArg) {
        FaiList<Param> list;
        // 从缓存获取数据
        list = ProductTagCache.getCacheList(aid);
        if (!Util.isEmptyList(list)) {
            return list;
        }
        LockUtil.TagLock.readLock(aid);

        try {
            // check again
            list = ProductTagCache.getCacheList(aid);
            if (!Util.isEmptyList(list)) {
                return list;
            }
            list = getListFromDb(aid, searchArg);
            //添加到缓存（直接查DB的不需要添加缓存）
            ProductTagCache.addCacheList(aid, list);
        }finally {
            LockUtil.TagLock.readUnLock(aid);
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
        //如果查询过来的条件已经包含这个查询条件,就先删除
        searchArg.matcher.remove(ProductTagEntity.Info.AID);
        //有searchArg，有查询条件，加多一个查询条件
        searchArg.matcher.and(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);

        //为了克隆需要，因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tableName(并不影响其他的业务)
        m_daoCtrl.setTableName(aid);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_daoCtrl.select(searchArg, listRef);

        //查完之后恢复最初的tableName
        m_daoCtrl.restoreTableName();

        //检查结果
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
        }
        list = listRef.value;
        if(list == null) {
            list = new FaiList<Param>();
        }
        if (list.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
            return list;
        }
        return list;
    }

    public void delTagList(int aid, ParamMatcher matcher) {
        int rt;
        if(matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "matcher is null;aid=%d;", aid);
        }
        matcher.and(ProductTagEntity.Info.AID, ParamMatcher.EQ, aid);

        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delTagList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
        }
        Log.logStd("delTagList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
    }

    /**
     * 修改标签表（只修改部分字段）
     */
    public void setTagList(int aid, FaiList<ParamUpdater> tagUpdaterList) {
        int rt;
        //入参校验
        for(ParamUpdater updater : tagUpdaterList){
            Param updateInfo = updater.getData();
            String tagName = updateInfo.getString(ProductTagEntity.Info.TAG_NAME);
            if(tagName != null && !ProductTagCheck.isNameValid(tagName)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, tagName);
            }
        }

        //先获取到标签表的所有的数据
        FaiList<Param> oldList = getListFromCacheOrDb(aid,null);
        //保存更新的数据
        FaiList<Param> dataList = new FaiList<Param>();
        Calendar now = Calendar.getInstance();
        for(ParamUpdater updater : tagUpdaterList){
            Param updateInfo = updater.getData();
            int tagId = updateInfo.getInt(ProductTagEntity.Info.TAG_ID, 0);
            //获取到要修改的记录
            Param oldInfo = Misc.getFirstNullIsEmpty(oldList, ProductTagEntity.Info.TAG_ID, tagId);
            if(Str.isEmpty(oldInfo)){
                continue;
            }
            //保存修改的信息到oldInfo中
            oldInfo = updater.update(oldInfo, true);

            //只更新部分信息，和sql语句的参数一致
            Param data = new Param();
            data.assign(oldInfo, ProductTagEntity.Info.TAG_NAME);
            data.assign(oldInfo, ProductTagEntity.Info.TAG_TYPE);
            data.assign(oldInfo, ProductTagEntity.Info.FLAG);
            data.setCalendar(ProductTagEntity.Info.UPDATE_TIME, now);
            data.assign(oldInfo, ProductTagEntity.Info.AID);
            data.assign(oldInfo, ProductTagEntity.Info.TAG_ID);
            dataList.add(data);
        }
        if(dataList.size() == 0){
            rt = Errno.OK;
            Log.logStd(rt, "dataList empty;flow=%d;aid=%d;", m_flow, aid);
            return;
        }

        //设置修改的条件
        ParamMatcher doBatchMatcher = new ParamMatcher(ProductTagEntity.Info.AID, ParamMatcher.EQ, "?");
        doBatchMatcher.and(ProductTagEntity.Info.TAG_ID, ParamMatcher.EQ, "?");
        //设置要修改的信息（这里没有修改标签类型）
        Param item = new Param();
        item.setString(ProductTagEntity.Info.TAG_NAME, "?");
        item.setString(ProductTagEntity.Info.TAG_TYPE, "?");
        item.setString(ProductTagEntity.Info.FLAG, "?");
        item.setString(ProductTagEntity.Info.UPDATE_TIME, "?");
        ParamUpdater doBatchUpdater = new ParamUpdater(item);
        //注意，data中保存数据的顺序和个数要和sql语句入参的顺序一致
        rt = m_daoCtrl.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
        if(rt != Errno.OK){
            throw new MgException(rt, "doBatchUpdate product Tag error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
        }
    }

    public void delBackupData(int aid, int backupId, int backupFlag) {
        ParamMatcher updateMatcher = new ParamMatcher(ProductTagEntity.Info.AID, ParamMatcher.EQ, aid);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
        updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

        // 先将 backupFlag 对应的备份数据取消置起
        ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
        int rt = m_bakDao.update(updater, updateMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        //删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
        ParamMatcher delMatcher = new ParamMatcher(ProductTagEntity.Info.AID, ParamMatcher.EQ, aid);
        delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
        rt = m_bakDao.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        Log.logStd("delete ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
    }

    public void backupData(int aid, int backupId, int backupFlag, Set<Integer> bakTagIds) {
        int rt;
        if(m_bakDao.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "bakDao is auto commit;aid=%d;bakTagIds=%s;backupId=%d;backupFlag=%d;", aid, bakTagIds, backupId, backupFlag);
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductTagEntity.Info.TAG_ID, ParamMatcher.IN, new FaiList<Integer>(bakTagIds));
        FaiList<Param> fromList = getListFromDb(aid, searchArg);

        // 初始容量直接定为所需的最大容量，去掉不必要的扩容
        Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1);
        FaiList<Calendar> updateTimeList = new FaiList<>();
        for (Param fromInfo : fromList) {
            fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
            newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
            updateTimeList.add(fromInfo.getCalendar(ProductTagEntity.Info.UPDATE_TIME));
        }

        // 查出已有的备份数据，通过updateTime确定数据是否已备份
        SearchArg oldBakArg = new SearchArg();
        oldBakArg.matcher = searchArg.matcher.clone();
        oldBakArg.matcher.and(ProductTagEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
        FaiList<Param> oldBakList = getBakList(aid, oldBakArg);

        Set<String> oldBakUniqueKeySet = new HashSet<>((int)(oldBakList.size()/0.75f)+1);
        for (Param oldBak : oldBakList) {
            oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
        }
        // 获取交集，说明剩下的这些是要合并的备份数据
        oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);
        if(!oldBakUniqueKeySet.isEmpty()){
            // 合并标记
            ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);

            // 合并条件
            ParamMatcher mergeMatcher = new ParamMatcher(ProductTagEntity.Info.AID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductTagEntity.Info.TAG_ID, ParamMatcher.EQ, "?");
            mergeMatcher.and(ProductTagEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

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
                data.setInt(ProductTagEntity.Info.AID, aid);
                data.setInt(ProductTagEntity.Info.TAG_ID, Integer.valueOf(keys[0]));
                data.setCalendar(ProductTagEntity.Info.UPDATE_TIME, updateTime);
                // mergeMatcher end

                dataList.add(data);
            }
            rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
            if(rt != Errno.OK) {
                throw new MgException(rt, "merge bak update err;aid=%d;bakTagIds=%s;backupId=%d;backupFlag=%d;", aid, bakTagIds, backupId, backupFlag);
            }
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
            Log.logStd("backupData ok, need add bak empty;aid=%d;bakTagIds=%s;backupId=%d;backupFlag=%d;", aid, bakTagIds, backupId, backupFlag);
            return;
        }

        // 批量插入备份表
        rt = m_bakDao.batchInsert(fromList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchInsert bak err;aid=%d;bakTagIds=%s;backupId=%d;backupFlag=%d;", aid, bakTagIds, backupId, backupFlag);
        }

        Log.logStd("backupData ok;aid=%d;bakTagIds=%s;backupId=%d;backupFlag=%d;", aid, bakTagIds, backupId, backupFlag);
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
        return fromInfo.getInt(ProductTagEntity.Info.TAG_ID) +
                DELIMITER +
                fromInfo.getCalendar(ProductTagEntity.Info.UPDATE_TIME).getTimeInMillis();
    }
    private final static String DELIMITER = "-";

    public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
        int rt;
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
        }

        // 先删除原表数据
        ParamMatcher delMatcher = new ParamMatcher(ProductTagEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
        delTagList(aid, delMatcher);

        // 查出备份数据
        SearchArg bakSearchArg = new SearchArg();
        bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
        bakSearchArg.matcher.and(ProductTagEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
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
