package fai.MgProductTagSvr.application.service;

import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.common.ProductTagCheck;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.entity.ProductTagValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagCache;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagProc;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagRelProc;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.infutil.app.CloneDef;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
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
                            TransactionCtrl transactionCtrl,
                            ProductTagProc tagProc,
                            ProductTagRelProc relTagProc,
                            FaiList<Param> addInfoList,
                            FaiList<Param> tagInfoList,
                            FaiList<Param> relTagInfoList,
                            FaiList<Integer> relTagIds) {
        int rt;
        ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, aid, transactionCtrl);

        // 获取参数中最大的sort
        int maxSort = tagRelProc.getMaxSort(aid, unionPriId);
        if(maxSort < 0) {
            rt = Errno.ERROR;
            Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
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
        if (transactionCtrl.isAutoCommit()) {
            transactionCtrl.setAutoCommit(false);
        }

        //保存tagId
        FaiList<Integer> tagIds = new FaiList<>();
        if (tagProc == null) {
            tagProc = new ProductTagProc(flow, aid, transactionCtrl);
        }
        //批量添加标签表的数据
        tagProc.addTagBatch(aid, tagInfoList, tagIds);

        for (int i = 0; i < tagIds.size(); i++) {
            Param relTagInfo = relTagInfoList.get(i);
            //设置tagId
            relTagInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagIds.get(i));
        }
        if (relTagProc == null) {
            relTagProc = new ProductTagRelProc(flow, aid, transactionCtrl);
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
                maxSort = addTagBatch(flow, aid, unionPriId, tid, transactionCtrl, tagProc, tagRelProc, addInfoList,
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
                relProc.delRelTagList(aid, unionPriId, rlTagIds);

                // 删除标签表数据
                ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);
                tagProc.delTagList(aid, delTagIdList);

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

    @SuccessRt(value = Errno.OK)
    public int getAllTagRel(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        return getTagRelByConditions(session, flow, aid, unionPriId,null,true);
    }

    @SuccessRt(value = Errno.OK)
    public int getTagRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        return getTagRelByConditions(session, flow, aid, unionPriId, searchArg, false);
    }

    /**
     * 根据条件查询标签业务表的数据
     * @param searchArg 查询的条件。为null的话，查询的条件就会是aid和unionPriId
     * @param getFromCache 是否需要查缓存
     * @throws IOException
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
                    relTagProc.delRelTagList(aid, unionPriId, delRlTagIds);

                    // 删除标签表数据
                    tagProc.delTagList(aid, delTagIdList);
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
                    maxSort = addTagBatch(flow, aid, unionPriId, tid, tc, tagProc, relTagProc,
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
        if(Util.isEmptyList(cloneUnionPriIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, cloneUnionPriIds is empty;flow=%d;toAid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
            return rt;
        }
        
        //保存克隆关系的映射
        Map<Integer, Integer> cloneUidMap = new HashMap<>();
        for(Param cloneUidInfo : cloneUnionPriIds) {
            //克隆到哪个UnionPriId下
            Integer toUnionPriId = cloneUidInfo.getInt(CloneDef.Info.TO_UNIONPRIID);
            //从哪个UnionPriId下克隆
            Integer fromUnionPriId = cloneUidInfo.getInt(CloneDef.Info.FROM_UNIONPRIID);
            if(toUnionPriId == null || fromUnionPriId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, cloneUnionPriIds is error;flow=%d;toAid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
                return rt;
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
                // 业务关系数据克隆
                ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, toAid, tc);
                tagRelProc.cloneData(toAid, fromAid, cloneUidMap);

                // 标签数据克隆
                ProductTagProc tagProc = new ProductTagProc(flow, toAid, tc);
                tagProc.cloneData(toAid, fromAid, cloneUidMap);

                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                }else {
                    tc.rollback();
                }
                tc.closeDao();
            }
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("cloneData ok;flow=%d;toAid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
        return rt;
    }

    /**
     * 增量克隆，即tagId是自增的
     */
    @SuccessRt(value = Errno.OK)
    public int incrementalClone(FaiSession session, int flow, int toAid, int toUnionPriId, int fromAid, int fromUnionPriId) throws IOException {
        int rt;
        Lock lock = LockUtil.getLock(toAid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            // 业务关系数据克隆
            ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, toAid, tc);
            Map<Integer, Param> tagId_RelInfo = new HashMap<>();
            
            //查出要克隆的数据
            FaiList<Param> fromRelList = tagRelProc.getListFromDb(fromAid, fromUnionPriId, null);

            if(!fromRelList.isEmpty()) {
                // 查出已存在的数据
                FaiList<Param> existedList = tagRelProc.getListFromDb(toAid, toUnionPriId, null);
                for(Param fromInfo : fromRelList) {
                    int rlTagId = fromInfo.getInt(ProductTagRelEntity.Info.RL_TAG_ID);
                    boolean existed = Misc.getFirst(existedList, ProductTagRelEntity.Info.RL_TAG_ID, rlTagId) != null;
                    // 如果rlTagId尚未存在，则需做增量
                    if(!existed) {
                        Param data = fromInfo.clone();
                        data.setInt(ProductTagRelEntity.Info.AID, toAid);
                        data.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int tagId = data.getInt(ProductTagRelEntity.Info.TAG_ID);
                        tagId_RelInfo.put(tagId, data);
                    }
                }
            }
            // 没有需要做增量的数据
            if(tagId_RelInfo.isEmpty()) {
                rt = Errno.OK;
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                Log.logStd("ok, incremental is empty;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            //查出对应的标签表数据
            SearchArg tagSearch = new SearchArg();
            tagSearch.matcher = new ParamMatcher(ProductTagEntity.Info.TAG_ID, ParamMatcher.IN,
                    new FaiList<>(tagId_RelInfo.keySet()));
            ProductTagProc tagProc = new ProductTagProc(flow, toAid, tc);
            FaiList<Param> fromTagList = tagProc.getListFromDb(fromAid, tagSearch);
            // 这里必定是会查到数据才对
            if(fromTagList.isEmpty()) {
                rt = Errno.ERROR;
                Log.logErr(rt, "get from tag list err;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            // 这里保持 fromTagIds 和 addTagList的顺序一致，是为了后面能得到fromTagId 和 toTagId的映射关系
            FaiList<Integer> fromTagIds = new FaiList<>();
            FaiList<Param> addTagList = new FaiList<>();
            for(Param fromInfo : fromTagList) {
                Param data = fromInfo.clone();
                data.setInt(ProductTagEntity.Info.AID, toAid);
                //移除查出来的tagId，使用自增的tagId
                int fromTagId = (Integer) data.remove(ProductTagEntity.Info.TAG_ID);
                //保存fromTagId
                fromTagIds.add(fromTagId);
                addTagList.add(data);
            }
            tc.setAutoCommit(false);
            try {
                Map<Integer, Integer> fromTagId_toTagId = new HashMap<>();
                FaiList<Integer> tagIds = new FaiList<>();
                tagProc.addTagBatch(toAid, addTagList, tagIds);

                // 组装fromTagId 和 toTagId的映射关系
                for(int i = 0; i < tagIds.size(); i++) {
                    fromTagId_toTagId.put(fromTagIds.get(i), tagIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toTagId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromTagId : tagId_RelInfo.keySet()) {
                    Param relInfo = tagId_RelInfo.get(fromTagId);
                    int tagId = fromTagId_toTagId.get(fromTagId);
                    relInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagId);
                    addRelList.add(relInfo);
                }
                // 插入增量克隆数据
                tagRelProc.addIncrementalClone(toAid, toUnionPriId, addRelList);
                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                }else {
                    tc.rollback();
                }
                tc.closeDao();
            }
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("incrementalClone ok;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
        return rt;
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
}
