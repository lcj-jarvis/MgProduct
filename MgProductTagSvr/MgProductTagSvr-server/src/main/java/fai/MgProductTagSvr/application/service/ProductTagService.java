package fai.MgProductTagSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.common.ProductTagCheck;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.entity.ProductTagValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.CacheCtrl;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagCache;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagProc;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagRelProc;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.BackupStatusCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:51
 */
public class ProductTagService {

    /**
     * 批量添加商品标签
     * @param addInfoList  保存完整的标签信息（包含标签表和标签业务）
     * @param tagInfoList  用于保存要插入标签表的数据
     * @param relTagInfoList  用于保存要插入标签业务表的数据
     * @param relTagIds  保存插入成功后的标签业务id
     * @return
     */
    private int addTagBatch(int flow, int aid, int unionPriId, int tid,
                            ProductTagProc tagProc,
                            ProductTagRelProc relTagProc,
                            FaiList<Param> addInfoList,
                            FaiList<Param> tagInfoList,
                            FaiList<Param> relTagInfoList,
                            FaiList<Integer> relTagIds) {
        int rt;
        // 获取参数中最大的sort
        int maxSort = relTagProc.getMaxSort(aid, unionPriId);
        if(maxSort < 0) {
            rt = Errno.ERROR;
            Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }

        for (Param addInfo : addInfoList) {
            // 未设置排序则默认排序值+1
            Integer sort = addInfo.getInt(ProductTagRelEntity.Info.SORT);
            if (sort == null) {
                addInfo.setInt(ProductTagRelEntity.Info.SORT, ++maxSort);
            }

            Param tagInfo = new Param();
            Param relTagInfo = new Param();
            //装配标签表和标签业务表的数据
            assemblyTagInfo(flow, aid, unionPriId, tid, addInfo, tagInfo, relTagInfo);
            tagInfoList.add(tagInfo);
            relTagInfoList.add(relTagInfo);
        }

        //将事务设置为非自动提交
        /*if (transactionCtrl.isAutoCommit()) {
            transactionCtrl.setAutoCommit(false);
        }*/

        //保存tagId
        FaiList<Integer> tagIds = new FaiList<>();
        //批量添加标签表的数据
        tagProc.addTagBatch(aid, tagInfoList, tagIds);
        for (int i = 0; i < tagIds.size(); i++) {
            Param relTagInfo = relTagInfoList.get(i);
            //设置tagId
            relTagInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagIds.get(i));
        }
        //批量添加标签表的数据
        relTagProc.addTagRelBatch(aid, unionPriId, relTagInfoList, relTagIds);

        return maxSort;
    }

    /**
     * 添加单个标签
     */
    @SuccessRt(value = Errno.OK)
    public int addProductTag(FaiSession session, int flow, int aid, int unionPriId, int tid, Param info) throws IOException {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        FaiList<Param> addInfoList = new FaiList<>();
        addInfoList.add(info);
        FaiList<Param> tagInfoList = new FaiList<>();
        FaiList<Param> relTagInfoList = new FaiList<>();
        FaiList<Integer> relTagIds = new FaiList<>();

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);
            ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, aid, transactionCtrl);
            int maxSort = 0;
            try {
                maxSort = addTagBatch(flow, aid, unionPriId, tid, tagProc, tagRelProc, addInfoList,
                        tagInfoList, relTagInfoList, relTagIds);
                commit = true;
            } finally {
                if(commit) {
                    transactionCtrl.commit();
                    // 新增缓存
                    ProductTagCache.addCache(aid, tagInfoList.get(0));
                    ProductTagRelCache.InfoCache.addCache(aid, unionPriId, relTagInfoList.get(0));
                    ProductTagRelCache.SortCache.set(aid, unionPriId, maxSort);
                    ProductTagRelCache.DataStatusCache.update(aid, unionPriId, 1);
                }else {
                    transactionCtrl.rollback();
                    tagProc.clearIdBuilderCache(aid);
                    tagRelProc.clearIdBuilderCache(aid, unionPriId);
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        Param result = relTagInfoList.get(0);

        sendBuf.putInt(ProductTagRelDto.Key.RL_TAG_ID, relTagIds.get(0));
        sendBuf.putInt(ProductTagRelDto.Key.TAG_ID, result.getInt(ProductTagRelEntity.Info.TAG_ID));
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlTagId=%d;tagId=%d;", flow, aid, unionPriId,
                tid, relTagIds.get(0), result.getInt(ProductTagRelEntity.Info.TAG_ID));
        return rt;
    }

    /**
     * 根据标签业务id删除标签表和标签业务表的数据
     */
    @SuccessRt(value = Errno.OK)
    public int delTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlTagIds) throws IOException {
        int rt;
        if(Util.isEmptyList(rlTagIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlTagIds is not valid;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delTagIdList = null;
            try {
                transactionCtrl.setAutoCommit(false);
                ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, transactionCtrl);
                // 先获取要删除的标签id
                delTagIdList = relProc.getTagIdsByRlTagIds(aid, unionPriId, rlTagIds);

                // 删除标签业务表数据
                ParamMatcher matcher = new ParamMatcher(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.IN, rlTagIds);
                relProc.delRelTagList(aid, matcher);

                // 删除标签表数据
                ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);

                tagProc.delTagList(aid, new ParamMatcher(ProductTagEntity.Info.TAG_ID, ParamMatcher.IN, delTagIdList));

                commit = true;
                // commit之前设置10s过期时间，避免脏数据，保持一致性
                ProductTagRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductTagCache.setExpire(aid);
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductTagCache.delCacheList(aid, delTagIdList);
                    ProductTagRelCache.InfoCache.delCacheList(aid, unionPriId, rlTagIds);
                    ProductTagRelCache.DataStatusCache.update(aid, unionPriId, rlTagIds.size(), false);
                }else {
                    transactionCtrl.rollback();
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlTagIds);
        return rt;
    }

    /**
     * 先根据aid，uid，rlTagId修改标签业务表，后根据aid，tagId修改标签表
     */
    @SuccessRt(value = Errno.OK)
    public int setTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            FaiList<ParamUpdater> tagUpdaterList = new FaiList<ParamUpdater>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            try {
                transactionCtrl.setAutoCommit(false);
                // 修改标签业务表
                ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, transactionCtrl);
                relProc.setTagRelList(aid, unionPriId, updaterList, tagUpdaterList);

                // 修改标签表
                if(!tagUpdaterList.isEmpty()) {
                    ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);
                    tagProc.setTagList(aid, tagUpdaterList);
                }
                commit = true;
                // commit之前设置10s过期时间，避免脏数据，保持一致性
                if(!Util.isEmptyList(updaterList)) {
                    ProductTagRelCache.InfoCache.setExpire(aid, unionPriId);
                }
                if(!tagUpdaterList.isEmpty()) {
                    ProductTagCache.setExpire(aid);
                }
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductTagCache.updateCacheList(aid, tagUpdaterList);
                    if(!Util.isEmptyList(updaterList)) {
                        ProductTagRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                        // 修改数据，更新dataStatus 的管理态字段更新时间
                        ProductTagRelCache.DataStatusCache.update(aid, unionPriId);
                    }
                }else {
                    transactionCtrl.rollback();
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 查询所有的标签业务表的数据和所有的标签表的数据
     */
    @SuccessRt(value = Errno.OK)
    public int getTagList(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> relTagList;
        FaiList<Param> tagList;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductTagRelProc relTagProc = new ProductTagRelProc(flow, aid, transactionCtrl);
            //查询所有的标签业务表的数据
            relTagList = relTagProc.getListFromCacheOrDb(aid, unionPriId, null);

            ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);
            //查询所有的标签表的数据
            tagList = tagProc.getListFromCacheOrDb(aid,null);
        }finally {
            transactionCtrl.closeDao();
        }

        // 数据整合
        HashMap<Integer, Param> relMap = new HashMap<Integer, Param>();
        for(int i = 0; i < tagList.size(); i++) {
            Param info = tagList.get(i);
            Integer tagId = info.getInt(ProductTagEntity.Info.TAG_ID);
            relMap.put(tagId, info);
        }
        for(int i = 0; i < relTagList.size(); i++) {
            Param info = relTagList.get(i);
            Integer tagId = info.getInt(ProductTagRelEntity.Info.TAG_ID);

            Param tagInfo = relMap.get(tagId);
            if(tagInfo == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            info.assign(tagInfo);
        }

        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //先按sort字段排序，再按照rlTagId排序
        if(searchArg.cmpor == null) {
            searchArg.cmpor = new ParamComparator();
            searchArg.cmpor.addKey(ProductTagRelEntity.Info.SORT);
        }
        searchArg.cmpor.addKey(ProductTagRelEntity.Info.RL_TAG_ID);

        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> list = searcher.getParamList(relTagList);
        
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductTagRelDto.Key.INFO_LIST, ProductTagRelDto.getAllInfoDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductTagRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    /**
     * 获取所有的标签业务表的数据
     */
    @SuccessRt(value = Errno.OK)
    public int getAllTagRel(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        return getTagRelByConditions(session, flow, aid, unionPriId,null,true);
    }

    /**
     * 根据查询条件查询标签业务表的数据
     */
    @SuccessRt(value = Errno.OK)
    public int getTagRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        return getTagRelByConditions(session, flow, aid, unionPriId, searchArg, false);
    }

    /**
     * 根据条件查询标签业务表的数据
     * @param searchArg 查询的条件。为null的话，查询的条件就会是aid和unionPriId
     * @param getFromCache 是否需要查缓存
     */
    private int getTagRelByConditions(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, transactionCtrl);
            if (getFromCache) {
                list = relProc.getListFromCacheOrDb(aid, unionPriId, searchArg);
            } else {
                list = relProc.getListFromDb(aid, unionPriId, searchArg);
            }
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductTagRelDto.Key.INFO_LIST, ProductTagRelDto.getInfoDto());
        if (searchArg != null) {
            boolean needTotalSize = (searchArg.totalSize != null && searchArg.totalSize.value != null);
            if (needTotalSize) {
                sendBuf.putInt(ProductTagRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
            }
        }

        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getTagRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, transactionCtrl);
            info = relProc.getDataStatus(aid, unionPriId);
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductTagRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getTagRelDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 先删除，再修改，最后添加
     * @param addInfoList 要添加的标签（多个）
     * @param updaterList 要更新的标签（多个）
     * @param delRlTagIds 要删除的标签的标签业务id
     * @return
     * @throws IOException
     */
    @SuccessRt(value = Errno.OK)
    public int unionSetTagList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> addInfoList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delRlTagIds) throws IOException {
        int rt;
        //保存标签业务id
        FaiList<Integer> rlTagIds = new FaiList<>();
        int maxSort = 0;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            boolean commit = false;

            //保存标签表的信息
            FaiList<Param> tagInfoList = new FaiList<>();
            //保存标签业务表的信息
            FaiList<Param>  relInfoList = new FaiList<>();
            //保存要删除的标签的标签id
            FaiList<Integer> delTagIdList = null;
            //保存要更新的标签表信息
            FaiList<ParamUpdater> tagUpdaterList = new FaiList<>();

            ProductTagProc tagProc = new ProductTagProc(flow, aid, tc);
            ProductTagRelProc relTagProc = new ProductTagRelProc(flow, aid, tc);
            try {
                // 删除
                if (!Util.isEmptyList(delRlTagIds)) {
                    // 先获取要删除的标签id
                    delTagIdList = relTagProc.getTagIdsByRlTagIds(aid, unionPriId, delRlTagIds);

                    // 删除标签业务表数据
                    ParamMatcher matcher = new ParamMatcher(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    matcher.and(ProductTagRelEntity.Info.RL_TAG_ID, ParamMatcher.IN, delRlTagIds);
                    relTagProc.delRelTagList(aid, matcher);

                    // 删除标签表数据
                    tagProc.delTagList(aid, new ParamMatcher(ProductTagEntity.Info.TAG_ID, ParamMatcher.IN, delTagIdList));
                }

                // 修改
                if (!Util.isEmptyList(updaterList)) {
                    // 修改标签业务关系表
                    relTagProc.setTagRelList(aid, unionPriId, updaterList, tagUpdaterList);

                    // 修改标签表
                    if(!tagUpdaterList.isEmpty()) {
                        tagProc.setTagList(aid, tagUpdaterList);
                    }
                }

                // 添加
                if (!Util.isEmptyList(addInfoList)) {
                    maxSort = addTagBatch(flow, aid, unionPriId, tid, tagProc, relTagProc,
                            addInfoList, tagInfoList, relInfoList, rlTagIds);
                }

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                    tagProc.clearIdBuilderCache(aid);
                    relTagProc.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

            // 处理缓存
            if (!Util.isEmptyList(delRlTagIds)) {
                // 设置过期时间
                ProductTagCache.setExpire(aid);
                ProductTagRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductTagCache.delCacheList(aid, delTagIdList);
                ProductTagRelCache.InfoCache.delCacheList(aid, unionPriId, delRlTagIds);
                ProductTagRelCache.DataStatusCache.update(aid, unionPriId, delRlTagIds.size(), false);
            }

            if (!Util.isEmptyList(updaterList)) {
                // 设置过期时间
                ProductTagCache.setExpire(aid);
                ProductTagCache.updateCacheList(aid, tagUpdaterList);
                if(!Util.isEmptyList(updaterList)) {
                    ProductTagRelCache.InfoCache.setExpire(aid, unionPriId);
                    ProductTagRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                    // 修改数据，更新dataStatus 的管理态字段更新时间
                    ProductTagRelCache.DataStatusCache.update(aid, unionPriId);
                }
            }

            boolean isSuccess = !(Util.isEmptyList(tagInfoList) && Util.isEmptyList(relInfoList));
            if (isSuccess) {
                //设置过期时间
                ProductTagCache.setExpire(aid);
                ProductTagRelCache.InfoCache.setExpire(aid, unionPriId);

                //添加缓存
                ProductTagCache.addCacheList(aid, tagInfoList);
                ProductTagRelCache.InfoCache.addCacheList(aid, unionPriId, relInfoList);

                ProductTagRelCache.SortCache.set(aid, unionPriId, maxSort);
                ProductTagRelCache.DataStatusCache.update(aid, unionPriId, relInfoList.size(), true);
            }

        } finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);

        if (!Util.isEmptyList(addInfoList)) {
            rlTagIds.toBuffer(sendBuf, ProductTagRelDto.Key.RL_TAG_IDS);
        }
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;addTag=%s;", flow, aid, unionPriId, tid, addInfoList);
        return rt;
    }

    /**
     * 克隆标签表和标签业务表的数据
     */
    @SuccessRt(value = Errno.OK)
    public int cloneData(FaiSession session, int flow, int toAid, int fromAid, FaiList<Param> cloneUnionPriIds) throws IOException {
        int rt;
        doClone(flow, toAid, fromAid, cloneUnionPriIds, false);
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
        return rt;
    }

    /**
     * 增量克隆，即tagId是在已经存在的原来的tagId基础下自增的，不是直接克隆过来的
     */
    @SuccessRt(value = Errno.OK)
    public int incrementalClone(FaiSession session, int flow, int toAid, int toUnionPriId, int fromAid, int fromUnionPriId) throws IOException {
        int rt;
        Param clonePrimary = new Param();
        clonePrimary.setInt(CloneDef.Info.FROM_PRIMARY_KEY, fromUnionPriId);
        clonePrimary.setInt(CloneDef.Info.TO_PRIMARY_KEY, toUnionPriId);
        FaiList<Param> cloneUnionPriIds = new FaiList<>();
        cloneUnionPriIds.add(clonePrimary);

        doClone(flow, toAid, fromAid, cloneUnionPriIds, true);

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("incrementalClone ok;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
        return rt;
    }

    private void doClone(int flow, int toAid, int fromAid, FaiList<Param> cloneUnionPriIds, boolean incrementalClone) {
        int rt;
        if(Util.isEmptyList(cloneUnionPriIds)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args error, cloneUnionPriIds is empty;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
        }

        // 组装 fromUnionPriId -> toUnionPriId 映射关系
        Map<Integer, Integer> cloneUidMap = new HashMap<>();
        for(Param cloneUidInfo : cloneUnionPriIds) {
            Integer toUnionPriId = cloneUidInfo.getInt(CloneDef.Info.TO_PRIMARY_KEY);
            Integer fromUnionPriId = cloneUidInfo.getInt(CloneDef.Info.FROM_PRIMARY_KEY);
            if(toUnionPriId == null || fromUnionPriId == null) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args error, cloneUnionPriIds is error;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
            }
            cloneUidMap.put(fromUnionPriId, toUnionPriId);
        }

        Lock lock = LockUtil.getLock(toAid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            try {
                ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, toAid, tc);
                ProductTagProc tagProc = new ProductTagProc(flow, toAid, tc);

                if(!incrementalClone) {
                    FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUidMap.values());
                    // 删除原有的业务关系表数据
                    ParamMatcher delMatcher = new ParamMatcher(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, toAid);
                    delMatcher.and(ProductTagRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
                    tagRelProc.delRelTagList(toAid, delMatcher);

                    // 删除原有的标签表数据
                    delMatcher = new ParamMatcher(ProductTagEntity.Info.AID, ParamMatcher.EQ, toAid);
                    delMatcher.and(ProductTagEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);
                    tagProc.delTagList(toAid, delMatcher);
                }

                // TagId -> addRelList
                Map<Integer, FaiList<Param>> tagId_RelList = new HashMap<>();
                // TagId -> sourceUnionPriId
                Map<Integer, Integer> tagId_SourceUid = new HashMap<>();
                // 查出要克隆的业务关系表数据
                for(Map.Entry<Integer, Integer> entry: cloneUidMap.entrySet()) {
                    int fromUnionPriId = entry.getKey();
                    int toUnionPriId = entry.getValue();
                    FaiList<Param> fromRelList = tagRelProc.getListFromDb(fromAid, fromUnionPriId, null);

                    if(fromRelList.isEmpty()) {
                        continue;
                    }

                    // 查出已存在的数据 for 增量克隆
                    FaiList<Param> existedList = null;
                    if(incrementalClone) {
                        existedList = tagRelProc.getListFromDb(toAid, toUnionPriId, null);
                    }
                    for(Param fromInfo : fromRelList) {
                        int rlTagId = fromInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID);
                        // 如果是增量克隆，但是rlTagId已存在，则跳过
                        if(incrementalClone && Misc.getFirst(existedList, ProductTagRelEntity.Info.RL_TAG_ID, rlTagId) != null) {
                            continue;
                        }
                        Param data = fromInfo.clone();
                        data.setInt(ProductTagRelEntity.Info.AID, toAid);
                        data.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int tagId = data.getInt(ProductTagRelEntity.Info.TAG_ID);
                        FaiList<Param> addList = tagId_RelList.get(tagId);
                        if(addList == null) {
                            addList = new FaiList<>();
                            tagId_RelList.put(tagId, addList);
                            tagId_SourceUid.put(tagId, toUnionPriId);
                        }
                        addList.add(data);
                    }

                }

                // 没有要克隆的数据
                if(tagId_RelList.isEmpty()) {
                    return;
                }

                // 根据 fromAid 和 tagId 查出对应的标签表数据
                SearchArg tagSearch = new SearchArg();
                tagSearch.matcher = new ParamMatcher(ProductTagEntity.Info.TAG_ID, ParamMatcher.IN, new FaiList<>(tagId_RelList.keySet()));

                FaiList<Param> fromTagList = tagProc.getListFromDb(fromAid, tagSearch);
                // 这里必定是会查到数据才对
                if(fromTagList.isEmpty()) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "get from Tag list err;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
                }

                // 这里 保持 fromTagIds 和 addTagList的顺序一致，是为了后面能得到fromTagId 和 toTagId的映射关系
                FaiList<Integer> fromTagIds = new FaiList<>();
                FaiList<Param> addTagList = new FaiList<>();
                for(Param fromInfo : fromTagList) {
                    Param data = fromInfo.clone();
                    data.setInt(ProductTagEntity.Info.AID, toAid);
                    int fromTagId = (Integer) data.remove(ProductTagEntity.Info.TAG_ID);
                    int sourceUnionPriId = tagId_SourceUid.get(fromTagId);
                    data.setInt(ProductTagEntity.Info.SOURCE_UNIONPRIID, sourceUnionPriId);
                    fromTagIds.add(fromTagId);
                    addTagList.add(data);
                }

                Map<Integer, Integer> fromTagId_toTagId = new HashMap<>();
                FaiList<Integer> tagIds = new FaiList<>();
                tagProc.addTagBatch(toAid, addTagList, tagIds);
                // 组装fromTagId 和 toTagId的映射关系
                for(int i = 0; i < tagIds.size(); i++) {
                    fromTagId_toTagId.put(fromTagIds.get(i), tagIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toTagId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromTagId : tagId_RelList.keySet()) {
                    FaiList<Param> tmpAddList = tagId_RelList.get(fromTagId);
                    for(Param relInfo : tmpAddList) {
                        //新的自增的tagId
                        int tagId = fromTagId_toTagId.get(fromTagId);
                        relInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagId);
                        addRelList.add(relInfo);
                    }
                }
                // 插入业务关系表克隆数据
                tagRelProc.addIncrementalClone(toAid, new FaiList<>(cloneUidMap.values()), addRelList);

                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                }else {
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(toAid);
        }finally {
            lock.unlock();
        }

        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
    }

    /**
     * 装配标签表和标签业务表的数据
     */
    private void assemblyTagInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param  tagInfo, Param relTagInfo) {
        String tagName = recvInfo.getString(ProductTagEntity.Info.TAG_NAME, "");
        if(!ProductTagCheck.isNameValid(tagName)) {
            throw new MgException(Errno.ARGS_ERROR, "tagName is not valid;flow=%d;aid=%d;tagName=%d;", flow, aid, tagName);
        }

        //标签类型如果没有获取到，这里没有设置默认值
        int tagType = recvInfo.getInt(ProductTagEntity.Info.TAG_TYPE);

        int flag = recvInfo.getInt(ProductTagEntity.Info.FLAG, ProductTagValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductTagRelEntity.Info.SORT, ProductTagRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductTagRelEntity.Info.RL_FLAG, ProductTagRelValObj.Default.RL_FLAG);
        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductTagEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductTagEntity.Info.UPDATE_TIME, now);

        // 标签表数据
        tagInfo.setInt(ProductTagEntity.Info.AID, aid);
        tagInfo.setInt(ProductTagEntity.Info.SOURCE_TID, tid);
        tagInfo.setInt(ProductTagEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        tagInfo.setString(ProductTagEntity.Info.TAG_NAME, tagName);
        tagInfo.setInt(ProductTagEntity.Info.TAG_TYPE, tagType);
        tagInfo.setInt(ProductTagEntity.Info.FLAG, flag);
        tagInfo.setCalendar(ProductTagEntity.Info.CREATE_TIME, createTime);
        tagInfo.setCalendar(ProductTagEntity.Info.UPDATE_TIME, updateTime);

        // 标签业务表数据
        relTagInfo.setInt(ProductTagRelEntity.Info.AID, aid);
        relTagInfo.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, unionPriId);
        relTagInfo.setInt(ProductTagRelEntity.Info.SORT, sort);
        relTagInfo.setInt(ProductTagRelEntity.Info.RL_FLAG, rlFlag);
        relTagInfo.setCalendar(ProductTagRelEntity.Info.CREATE_TIME, createTime);
        relTagInfo.setCalendar(ProductTagRelEntity.Info.UPDATE_TIME, updateTime);
    }

    @SuccessRt(value = Errno.OK)
    public int backupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds, Param backupInfo) throws IOException {
        if (verifyBackupArgs(flow, aid, unionPriIds, backupInfo, true)) {
            return Errno.ARGS_ERROR;
        }

        int rt;
        LockUtil.BackupLock.lock(aid);
        try {
            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID);
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.BACKUP, aid, backupId)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, tc);
            ProductTagProc proc = new ProductTagProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 可能之前备份没有成功，先操作删除之前的备份
                deleteBackupData(relProc, proc, aid, backupId, backupFlag);

                // 备份业务关系表数据，返回需要备份的标签ids
                Set<Integer> bakTagIds = relProc.backupData(aid, unionPriIds, backupId, backupFlag);

                if(!bakTagIds.isEmpty()) {
                    // 备份标签表数据
                    proc.backupData(aid, backupId, backupFlag, bakTagIds);
                }
                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                    backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.BACKUP, aid, backupId);
                }else {
                    tc.rollback();
                    backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.BACKUP, aid, backupId);
                }
                tc.closeDao();
            }
        }finally {
            LockUtil.BackupLock.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("backupData ok;flow=%d;aid=%d;unionPriIds=%s;backupInfo=%s;", flow, aid, unionPriIds, backupInfo);
        return rt;
    }

    private void deleteBackupData(ProductTagRelProc relProc, ProductTagProc proc, int aid, int backupId, int backupFlag) {
        relProc.delBackupData(aid, backupId, backupFlag);
        proc.delBackupData(aid, backupId, backupFlag);
    }

    @SuccessRt(value = Errno.OK)
    public int restoreBackupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds, int restoreId, Param backupInfo) throws IOException {
        if (verifyBackupArgs(flow, aid, unionPriIds, backupInfo, true)) {
            return Errno.ARGS_ERROR;
        }
        if(restoreId <= 0) {
            Log.logErr("args error; restoreId err;flow=%d;aid=%d;unionPriIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPriIds, restoreId, backupInfo);
            return Errno.ARGS_ERROR;
        }
        int rt;
        LockUtil.BackupLock.lock(aid);
        try {
            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID);
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.RESTORE, aid, restoreId)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, tc);
            ProductTagProc proc = new ProductTagProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 还原关系表数据
                relProc.restoreBackupData(aid, unionPriIds, backupId, backupFlag);

                // 还原标签表数据
                proc.restoreBackupData(aid, unionPriIds, backupId, backupFlag);

                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                    backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
                }else {
                    tc.rollback();
                    backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(aid);
        }finally {
            LockUtil.BackupLock.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("restore backupData ok;flow=%d;aid=%d;unionPriIds=%s;backupInfo=%s;", flow, aid, unionPriIds, backupInfo);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delBackupData(FaiSession session, int flow, int aid, Param backupInfo) throws IOException {
        if (verifyBackupArgs(flow, aid, null, backupInfo, false)) {
            return Errno.ARGS_ERROR;
        }

        int rt;
        LockUtil.BackupLock.lock(aid);
        try {
            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID);
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.DELETE, aid, backupId)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductTagRelProc relProc = new ProductTagRelProc(flow, aid, tc);
            ProductTagProc proc = new ProductTagProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 删除备份
                deleteBackupData(relProc, proc, aid, backupId, backupFlag);

                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                    backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.DELETE, aid, backupId);
                }else {
                    tc.rollback();
                    backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.DELETE, aid, backupId);
                }
                tc.closeDao();
            }
        }finally {
            LockUtil.BackupLock.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del backupData ok;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
        return rt;
    }

    private boolean verifyBackupArgs(int flow, int aid, FaiList<Integer> unionPriIds, Param backupInfo, boolean needVerifyUnionPriIds) {
        if(aid <= 0 || Str.isEmpty(backupInfo)) {
            Log.logErr("args error;flow=%d;aid=%d;unionPriIds=%s;backupInfo=%s;", flow, aid, unionPriIds, backupInfo);
            return true;
        }

        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
        if(backupId == 0 || backupFlag == 0) {
            Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPriIds=%s;backupInfo=%s;", flow, aid, unionPriIds, backupInfo);
            return true;
        }

        if (needVerifyUnionPriIds) {
            if(Util.isEmptyList(unionPriIds)) {
                throw new MgException(Errno.ARGS_ERROR, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
            }
        }
        return false;
    }

    private boolean checkAndSetBackupStatus(int flow, BackupStatusCtrl.Action action, int aid, int actionId) {
        String backupStatus = backupStatusCtrl.getStatus(action, aid, actionId);
        if (backupStatus != null) {
            int rt = Errno.ALREADY_EXISTED;
            if (backupStatusCtrl.isDoing(backupStatus)) {
                throw new MgException(rt, action + " is doing;flow=%d;aid=%d;id=%s;", flow, aid, actionId);
            } else if (backupStatusCtrl.isFinish(backupStatus)) {
                rt = Errno.OK;
                Log.logStd(rt, action + " is already ok;flow=%d;aid=%d;id=%s;", flow, aid, actionId);
                return true;
            } else if (backupStatusCtrl.isFail(backupStatus)) {
                Log.logStd(rt, action + " is fail, going retry;flow=%d;aid=%d;id=%s;", flow, aid, actionId);
                return false;
            }
        }
        // 设置备份执行中
        backupStatusCtrl.setStatusIsDoing(action, aid, actionId);
        return false;
    }

    public void initBackupStatus(RedisCacheManager cache) {
        backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
    }

    private BackupStatusCtrl backupStatusCtrl;
    private static final String BAK_TYPE = "mgPdTag";
}
