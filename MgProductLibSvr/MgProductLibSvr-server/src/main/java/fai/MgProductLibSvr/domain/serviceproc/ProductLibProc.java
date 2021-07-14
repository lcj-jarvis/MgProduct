package fai.MgProductLibSvr.domain.serviceproc;

import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.common.ProductLibCheck;
import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibCache;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.Map;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:23
 */
public class ProductLibProc {

    private int m_flow;
    private ProductLibDaoCtrl m_daoCtrl;

    public ProductLibProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductLibDaoCtrl.getInstance(flow, aid);
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

    public FaiList<Param> getLibList(int aid, SearchArg searchArg, boolean getFromCache) {
        return getListByConditions(aid, searchArg, getFromCache);
    }

    /**
     * 按照条件查询数据，默认是查询同一个aid下的全部数据.
     * (该方法方便后期扩展只查DB的情形)
     * @param searchArg 查询条件
     * @param getFromCache 是否需要从缓存中查询
     * @return
     */
    private FaiList<Param> getListByConditions(int aid, SearchArg searchArg, boolean getFromCache) {
        FaiList<Param> list;
        if (getFromCache) {
            // 从缓存获取数据
            list = ProductLibCache.getCacheList(aid);
            if(!Util.isEmptyList(list)) {
                return list;
            }
        }

        LockUtil.LibLock.readLock(aid);
        try {
            if (getFromCache) {
                // check again
                list = ProductLibCache.getCacheList(aid);
                if(!Util.isEmptyList(list)) {
                    return list;
                }
            }

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
            //添加到缓存（直接查DB的不需要添加缓存）
            if (getFromCache) {
                ProductLibCache.addCacheList(aid, list);
            }
        }finally {
            LockUtil.LibLock.readUnLock(aid);
        }
        return list;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    /**
     * 根据aid和libId删除库表的数据
     */
    public void delLibList(int aid, FaiList<Integer> libIdList) {
        int rt;
        if(libIdList == null || libIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, libIdList);
        }

        ParamMatcher matcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN, libIdList);
        rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            throw new MgException(rt, "delLibList error;flow=%d;aid=%d;libIdList=%s", m_flow, aid, libIdList);

        }
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
        FaiList<Param> oldList = getLibList(aid,null,true);
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

        FaiList<Param> list = getLibList(aid,null,true);
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

    /**
     * 从fromAid、fromUnionPriId中克隆数据到toAid、toUnionPriId下，并且设置toAid、toUnionPriId的自增id
     * @param toAid 克隆到哪个aid下
     * @param fromAid 从哪个aid下克隆
     * @param cloneUnionPriIds key：fromUnionPriId 从哪个uid下克隆
     *                         value: toUnionPriId 克隆到哪个uid下
     *
     */
    public void cloneData(int toAid, int fromAid, Map<Integer, Integer> cloneUnionPriIds) {
        int rt;
        if(cloneUnionPriIds == null || cloneUnionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "cloneUnionPriIds is null;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }
        if(m_daoCtrl.isAutoCommit()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "dao is auto commit;flow=%d;aid=%d;fromAid=%d;uids=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        FaiList<Integer> fromUnionPriIds = new FaiList<>(cloneUnionPriIds.keySet());
        ParamMatcher matcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, fromAid);
        matcher.and(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, fromUnionPriIds);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;

        // 根据fromAid设置表名，默认表名是根据aid生成的.查询出所有被克隆的数据
        m_daoCtrl.setTableName(fromAid);
        Ref<FaiList<Param>> dataListRef = new Ref<>();
        rt = m_daoCtrl.select(searchArg, dataListRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException("select clone data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        // 根据aid设置表名
        m_daoCtrl.setTableName(toAid);
        //删除掉已经存在的数据
        FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUnionPriIds.values());
        ParamMatcher delMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, toAid);
        delMatcher.and(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);
        rt = m_daoCtrl.delete(delMatcher);
        if(rt != Errno.OK) {
            throw new MgException("del old data err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }

        // 组装数据
        for(Param data : dataListRef.value) {
            int fromUnionPriId = data.getInt(ProductLibEntity.Info.SOURCE_UNIONPRIID);
            int toUnionPriId = cloneUnionPriIds.get(fromUnionPriId);
            data.setInt(ProductLibEntity.Info.AID, toAid);
            data.setInt(ProductLibEntity.Info.SOURCE_UNIONPRIID, toUnionPriId);
        }
        // 批量插入
        if(!dataListRef.value.isEmpty()) {
            rt = m_daoCtrl.batchInsert(dataListRef.value);
            if(rt != Errno.OK) {
                throw new MgException("batch insert err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
            }
        }

        //设置自增id
        rt = m_daoCtrl.restoreMaxId(false);
        if(rt != Errno.OK) {
            throw new MgException("restoreMaxId err;flow=%d;aid=%d;fromAid=%d;cloneUnionPriIds=%s;", m_flow, toAid, fromAid, cloneUnionPriIds);
        }
        m_daoCtrl.clearIdBuilderCache(toAid);
    }
}
