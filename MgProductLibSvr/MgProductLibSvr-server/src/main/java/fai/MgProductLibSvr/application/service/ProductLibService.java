package fai.MgProductLibSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.common.ProductLibCheck;
import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelValObj;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibCache;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibRelCache;
import fai.MgProductLibSvr.domain.serviceproc.ProductLibProc;
import fai.MgProductLibSvr.domain.serviceproc.ProductLibRelProc;
import fai.MgProductLibSvr.interfaces.dto.ProductLibRelDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.comm.middleground.app.CloneDef;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.BackupStatusCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:18
 */
public class ProductLibService {

    /**
     * 批量添加商品信息
     * @param addInfoList  保存完整的库信息（包含库表和库业务）
     * @param libInfoList  用于保存要插入库表的数据
     * @param relLibInfoList  用于保存要插入库业务表的数据
     * @param relLibIds  保存插入成功后的库业务id
     * @return
     */
    private int addLibBatch(int flow, int aid, int unionPriId, int tid,
                       ProductLibProc libProc,
                       ProductLibRelProc relLibProc,
                       FaiList<Param> addInfoList,
                       FaiList<Param> libInfoList,
                       FaiList<Param> relLibInfoList,
                       FaiList<Integer> relLibIds) {

        int rt;
        // 获取参数中最大的sort
        int maxSort = relLibProc.getMaxSort(aid, unionPriId);
        if(maxSort < 0) {
            rt = Errno.ERROR;
            Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }

        for (Param addInfo : addInfoList) {
            // 未设置排序则默认排序值+1
            Integer sort = addInfo.getInt(ProductLibRelEntity.Info.SORT);
            if (sort == null) {
                addInfo.setInt(ProductLibRelEntity.Info.SORT, ++maxSort);
            }

            Param libInfo = new Param();
            Param relLibInfo = new Param();
            //装配库表和库业务表的数据
            assemblyLibInfo(flow, aid, unionPriId, tid, addInfo, libInfo, relLibInfo);
            libInfoList.add(libInfo);
            relLibInfoList.add(relLibInfo);
        }

        //将事务设置为非自动提交
        /*if (transactionCtrl.isAutoCommit()) {
            transactionCtrl.setAutoCommit(false);
        }*/

        //保存libId
        FaiList<Integer> libIds = new FaiList<>();
        //批量添加库表的数据
        libProc.addLibBatch(aid, libInfoList, libIds);

        for (int i = 0; i < libIds.size(); i++) {
            Param relLibInfo = relLibInfoList.get(i);
            //设置libId
            relLibInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libIds.get(i));
        }
        //批量添加库表的数据
        relLibProc.addLibRelBatch(aid, unionPriId, relLibInfoList, relLibIds);

        return maxSort;
    }

    @SuccessRt(value = Errno.OK)
    public int addProductLib(FaiSession session, int flow, int aid, int unionPriId, int tid, Param info) throws IOException {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        FaiList<Param> addInfoList = new FaiList<>();
        addInfoList.add(info);
        FaiList<Param> libInfoList = new FaiList<>();
        FaiList<Param> relLibInfoList = new FaiList<>();
        FaiList<Integer> relLibIds = new FaiList<>();

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
            ProductLibRelProc libRelProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            int maxSort = 0;
            try {
                maxSort = addLibBatch(flow, aid, unionPriId, tid, libProc, libRelProc, addInfoList,
                                      libInfoList, relLibInfoList, relLibIds);
                commit = true;
            } finally {
                if(commit) {
                    transactionCtrl.commit();
                    // 新增缓存
                    ProductLibCache.addCache(aid, libInfoList.get(0));
                    ProductLibRelCache.InfoCache.addCache(aid, unionPriId, relLibInfoList.get(0));
                    ProductLibRelCache.SortCache.set(aid, unionPriId, maxSort);
                    ProductLibRelCache.DataStatusCache.update(aid, unionPriId, 1);
                }else {
                    transactionCtrl.rollback();
                    libProc.clearIdBuilderCache(aid);
                    libRelProc.clearIdBuilderCache(aid, unionPriId);
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        Param result = relLibInfoList.get(0);

        sendBuf.putInt(ProductLibRelDto.Key.RL_LIB_ID, relLibIds.get(0));
        sendBuf.putInt(ProductLibRelDto.Key.LIB_ID, result.getInt(ProductLibRelEntity.Info.LIB_ID));
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlLibId=%d;libId=%d;", flow, aid, unionPriId,
                tid, relLibIds.get(0), result.getInt(ProductLibRelEntity.Info.LIB_ID));
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delLibList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlLibIds) throws IOException {
        int rt;
        if(rlLibIds == null || rlLibIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlLibIds is not valid;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delLibIdList = null;
            try {
                transactionCtrl.setAutoCommit(false);
                ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, transactionCtrl);
                // 先获取要删除的库id
                delLibIdList = relProc.getLibIdsByRlLibIds(aid, unionPriId, rlLibIds);

                // 删除库业务表数据
                ParamMatcher matcher = new ParamMatcher(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                matcher.and(ProductLibRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, rlLibIds);
                relProc.delRelLibList(aid, matcher);

                // 删除库表数据
                ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
                libProc.delLibList(aid, new ParamMatcher(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN, delLibIdList));

                commit = true;
                // commit之前设置10s过期时间，避免脏数据，保持一致性
                ProductLibRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductLibCache.setExpire(aid);
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductLibCache.delCacheList(aid, delLibIdList);
                    ProductLibRelCache.InfoCache.delCacheList(aid, unionPriId, rlLibIds);
                    ProductLibRelCache.DataStatusCache.update(aid, unionPriId, rlLibIds.size(), false);
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
        Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlLibIds);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setLibList(FaiSession session, int flow, int aid, int unionPriId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            FaiList<ParamUpdater> libUpdaterList = new FaiList<ParamUpdater>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            try {
                transactionCtrl.setAutoCommit(false);
                // 修改库业务表
                ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, transactionCtrl);
                relProc.setLibRelList(aid, unionPriId, updaterList, libUpdaterList);

                // 修改库表
                if(!libUpdaterList.isEmpty()) {
                    ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
                    libProc.setLibList(aid, libUpdaterList);
                }
                commit = true;
                // commit之前设置10s过期时间，避免脏数据，保持一致性
                if(!Util.isEmptyList(updaterList)) {
                    ProductLibRelCache.InfoCache.setExpire(aid, unionPriId);
                }
                if(!libUpdaterList.isEmpty()) {
                    ProductLibCache.setExpire(aid);
                }
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductLibCache.updateCacheList(aid, libUpdaterList);
                    if(!Util.isEmptyList(updaterList)) {
                        ProductLibRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                        // 修改数据，更新dataStatus 的管理态字段更新时间
                        ProductLibRelCache.DataStatusCache.update(aid, unionPriId);
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

    @SuccessRt(value = Errno.OK)
    public int getLibList(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> relLibList;
        FaiList<Param> libList;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductLibRelProc relLibProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            //查询所有的库业务表的数据
            relLibList = relLibProc.getListFromCacheOrDb(aid, unionPriId, null);

            ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
            //查询所有的库表的数据
            libList = libProc.getListFromCacheOrDb(aid, null);
        }finally {
            transactionCtrl.closeDao();
        }

        // 数据整合
        HashMap<Integer, Param> relMap = new HashMap<Integer, Param>();
        for(int i = 0; i < libList.size(); i++) {
            Param info = libList.get(i);
            Integer libId = info.getInt(ProductLibEntity.Info.LIB_ID);
            relMap.put(libId, info);
        }
        for(int i = 0; i < relLibList.size(); i++) {
            Param info = relLibList.get(i);
            Integer libId = info.getInt(ProductLibRelEntity.Info.LIB_ID);

            Param libInfo = relMap.get(libId);
            if(libInfo == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            info.assign(libInfo);
        }

        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        //先按sort字段排序，再按照reLibId排序
        if(searchArg.cmpor == null) {
            searchArg.cmpor = new ParamComparator();
            searchArg.cmpor.addKey(ProductLibRelEntity.Info.SORT);
        }
        searchArg.cmpor.addKey(ProductLibRelEntity.Info.RL_LIB_ID);

        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> list = searcher.getParamList(relLibList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductLibRelDto.Key.INFO_LIST, ProductLibRelDto.getAllInfoDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductLibRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getAllLibRel(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        return getLibRelByConditions(session, flow, aid, unionPriId,null,true);
    }

    @SuccessRt(value = Errno.OK)
    public int getLibRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        return getLibRelByConditions(session, flow, aid, unionPriId, searchArg, false);
    }

    /**
     * 根据条件查询库业务表的数据
     * @param searchArg 查询的条件。为null的话，查询的条件就会是aid和unionPriId
     * @param getFromCache 是否需要查缓存
     */
    private int getLibRelByConditions(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg, boolean getFromCache) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            if (getFromCache) {
                list = relProc.getListFromCacheOrDb(aid, unionPriId, searchArg);
            } else {
                list = relProc.getListFromDb(aid, unionPriId, searchArg);
            }
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductLibRelDto.Key.INFO_LIST, ProductLibRelDto.getInfoDto());
        if (searchArg != null) {
            boolean needTotalSize = searchArg.totalSize != null && searchArg.totalSize.value != null;
            if (needTotalSize) {
                sendBuf.putInt(ProductLibRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
            }
        }

        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getLibRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            info = relProc.getDataStatus(aid, unionPriId);
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductLibRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getLibRelDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 装配库表和库业务表的数据
     */
    private void assemblyLibInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param  libInfo, Param relLibInfo) {

        String libName = recvInfo.getString(ProductLibEntity.Info.LIB_NAME, "");
        if(!ProductLibCheck.isNameValid(libName)) {
            throw new MgException(Errno.ARGS_ERROR, "libName is not valid;flow=%d;aid=%d;libName=%d;", flow, aid, libName);
        }

        //库类型如果没有获取到，这里没有设置默认值
        int libType = recvInfo.getInt(ProductLibEntity.Info.LIB_TYPE);

        int flag = recvInfo.getInt(ProductLibEntity.Info.FLAG, ProductLibValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductLibRelEntity.Info.SORT, ProductLibRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductLibRelEntity.Info.RL_FLAG, ProductLibRelValObj.Default.RL_FLAG);
        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductLibEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductLibEntity.Info.UPDATE_TIME, now);

        // 库表数据
        libInfo.setInt(ProductLibEntity.Info.AID, aid);
        libInfo.setInt(ProductLibEntity.Info.SOURCE_TID, tid);
        libInfo.setInt(ProductLibEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        libInfo.setString(ProductLibEntity.Info.LIB_NAME, libName);
        libInfo.setInt(ProductLibEntity.Info.LIB_TYPE, libType);
        libInfo.setInt(ProductLibEntity.Info.FLAG, flag);
        libInfo.setCalendar(ProductLibEntity.Info.CREATE_TIME, createTime);
        libInfo.setCalendar(ProductLibEntity.Info.UPDATE_TIME, updateTime);

        // 库业务表数据
        relLibInfo.setInt(ProductLibRelEntity.Info.AID, aid);
        relLibInfo.setInt(ProductLibRelEntity.Info.UNION_PRI_ID, unionPriId);
        relLibInfo.setInt(ProductLibRelEntity.Info.LIB_TYPE, libType);
        relLibInfo.setInt(ProductLibRelEntity.Info.SORT, sort);
        relLibInfo.setInt(ProductLibRelEntity.Info.RL_FLAG, rlFlag);
        relLibInfo.setCalendar(ProductLibRelEntity.Info.CREATE_TIME, createTime);
        relLibInfo.setCalendar(ProductLibRelEntity.Info.UPDATE_TIME, updateTime);

    }

    /**
     * 先删除，再修改，最后添加
     * @param addInfoList 要添加的库（多个）
     * @param updaterList 要更新的库（多个）
     * @param delRlLibIds 要删除的库的库业务id
     * @return
     * @throws IOException
     */
    @SuccessRt(value = Errno.OK)
    public int unionSetLibList(FaiSession session, int flow, int aid, int unionPriId, int tid,
                               FaiList<Param> addInfoList,
                               FaiList<ParamUpdater> updaterList,
                               FaiList<Integer> delRlLibIds) throws IOException {
        int rt;
        //保存库业务id
        FaiList<Integer> rlLibIds = new FaiList<>();
        int maxSort = 0;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            boolean commit = false;

            //保存库表的信息
            FaiList<Param> libInfoList = new FaiList<>();
            //保存库业务表的信息
            FaiList<Param>  relInfoList = new FaiList<>();
            //保存要删除的库的库id
            FaiList<Integer> delLibIdList = null;
            //保存要更新的库表信息
            FaiList<ParamUpdater> libUpdaterList = new FaiList<>();

            ProductLibProc libProc = new ProductLibProc(flow, aid, tc);
            ProductLibRelProc relLibProc = new ProductLibRelProc(flow, aid, tc);
            try {
                // 删除
                if (!Util.isEmptyList(delRlLibIds)) {
                    // 先获取要删除的库id
                    delLibIdList = relLibProc.getLibIdsByRlLibIds(aid, unionPriId, delRlLibIds);

                    // 删除库业务表数据
                    ParamMatcher matcher = new ParamMatcher(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    matcher.and(ProductLibRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, delRlLibIds);
                    relLibProc.delRelLibList(aid, matcher);

                    // 删除库表数据
                    libProc.delLibList(aid, new ParamMatcher(new ParamMatcher(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN, delLibIdList)));
                }

                // 修改
                if (!Util.isEmptyList(updaterList)) {
                    // 修改库业务关系表
                    relLibProc.setLibRelList(aid, unionPriId, updaterList, libUpdaterList);

                    // 修改库表
                    if(!libUpdaterList.isEmpty()) {
                        libProc.setLibList(aid, libUpdaterList);
                    }
                }

                // 添加
                if (!Util.isEmptyList(addInfoList)) {
                   maxSort = addLibBatch(flow, aid, unionPriId, tid, libProc, relLibProc,
                                addInfoList, libInfoList, relInfoList, rlLibIds);
                }

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                    libProc.clearIdBuilderCache(aid);
                    relLibProc.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

            // 处理缓存
            if (!Util.isEmptyList(delRlLibIds)) {
                // 设置过期时间
                ProductLibCache.setExpire(aid);
                ProductLibRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductLibCache.delCacheList(aid, delLibIdList);
                ProductLibRelCache.InfoCache.delCacheList(aid, unionPriId, delRlLibIds);
                ProductLibRelCache.DataStatusCache.update(aid, unionPriId, delRlLibIds.size(), false);
            }

            if (!Util.isEmptyList(updaterList)) {
                // 设置过期时间
                ProductLibCache.setExpire(aid);
                ProductLibCache.updateCacheList(aid, libUpdaterList);
                if(!Util.isEmptyList(updaterList)) {
                    ProductLibRelCache.InfoCache.setExpire(aid, unionPriId);
                    ProductLibRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                    // 修改数据，更新dataStatus 的管理态字段更新时间
                    ProductLibRelCache.DataStatusCache.update(aid, unionPriId);
                }
            }

            boolean isSuccess = !(Util.isEmptyList(libInfoList) && Util.isEmptyList(relInfoList));
            if (isSuccess) {
                //设置过期时间
                ProductLibCache.setExpire(aid);
                ProductLibRelCache.InfoCache.setExpire(aid, unionPriId);

                //添加缓存
                ProductLibCache.addCacheList(aid, libInfoList);
                ProductLibRelCache.InfoCache.addCacheList(aid, unionPriId, relInfoList);

                ProductLibRelCache.SortCache.set(aid, unionPriId, maxSort);
                ProductLibRelCache.DataStatusCache.update(aid, unionPriId, relInfoList.size(), true);
            }

        } finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);

        if (!Util.isEmptyList(addInfoList)) {
           rlLibIds.toBuffer(sendBuf, ProductLibRelDto.Key.RL_LIB_IDS);
        }
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;addLib=%s;", flow, aid, unionPriId, tid, addInfoList);
        return rt;
    }

    /**
     * 克隆库表和库业务表的数据
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
     * 增量克隆，即libId是自增的
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
                ProductLibRelProc libRelProc = new ProductLibRelProc(flow, toAid, tc);
                ProductLibProc libProc = new ProductLibProc(flow, toAid, tc);

                if(!incrementalClone) {
                    FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUidMap.values());
                    // 删除原有的业务关系表数据
                    ParamMatcher delMatcher = new ParamMatcher(ProductLibRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
                    libRelProc.delRelLibList(toAid, delMatcher);

                    // 删除原有的库表数据
                    delMatcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, toAid);
                    delMatcher.and(ProductLibEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);
                    libProc.delLibList(toAid, delMatcher);
                }

                // LibId -> addRelList
                Map<Integer, FaiList<Param>> libId_RelList = new HashMap<>();
                // LibId -> sourceUnionPriId
                Map<Integer, Integer> libId_SourceUid = new HashMap<>();
                // 查出要克隆的业务关系表数据
                for(Map.Entry<Integer, Integer> entry: cloneUidMap.entrySet()) {
                    int fromUnionPriId = entry.getKey();
                    int toUnionPriId = entry.getValue();
                    FaiList<Param> fromRelList = libRelProc.getListFromDb(fromAid, fromUnionPriId, null);

                    if(fromRelList.isEmpty()) {
                        continue;
                    }

                    // 查出已存在的数据 for 增量克隆
                    FaiList<Param> existedList = null;
                    if(incrementalClone) {
                        existedList = libRelProc.getListFromDb(toAid, toUnionPriId, null);
                    }
                    for(Param fromInfo : fromRelList) {
                        int rlLibId = fromInfo.getInt(ProductLibRelEntity.Info.RL_LIB_ID);
                        // 如果是增量克隆，但是rlLibId已存在，则跳过
                        if(incrementalClone && Misc.getFirst(existedList, ProductLibRelEntity.Info.RL_LIB_ID, rlLibId) != null) {
                            continue;
                        }
                        Param data = fromInfo.clone();
                        data.setInt(ProductLibRelEntity.Info.AID, toAid);
                        data.setInt(ProductLibRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int libId = data.getInt(ProductLibRelEntity.Info.LIB_ID);
                        FaiList<Param> addList = libId_RelList.get(libId);
                        if(addList == null) {
                            addList = new FaiList<>();
                            libId_RelList.put(libId, addList);
                            libId_SourceUid.put(libId, toUnionPriId);
                        }
                        addList.add(data);
                    }

                }

                // 没有要克隆的数据
                if(libId_RelList.isEmpty()) {
                    return;
                }

                // 根据 fromAid 和 libId 查出对应的库表数据
                SearchArg libSearch = new SearchArg();
                libSearch.matcher = new ParamMatcher(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN, new FaiList<>(libId_RelList.keySet()));

                FaiList<Param> fromLibList = libProc.getListFromDb(fromAid, libSearch);
                // 这里必定是会查到数据才对
                if(fromLibList.isEmpty()) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "get from Lib list err;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
                }

                // 这里 保持 fromLibIds 和 addLibList的顺序一致，是为了后面能得到fromLibId 和 toLibId的映射关系
                FaiList<Integer> fromLibIds = new FaiList<>();
                FaiList<Param> addLibList = new FaiList<>();
                for(Param fromInfo : fromLibList) {
                    Param data = fromInfo.clone();
                    data.setInt(ProductLibEntity.Info.AID, toAid);
                    int fromLibId = (Integer) data.remove(ProductLibEntity.Info.LIB_ID);
                    int sourceUnionPriId = libId_SourceUid.get(fromLibId);
                    data.setInt(ProductLibEntity.Info.SOURCE_UNIONPRIID, sourceUnionPriId);
                    fromLibIds.add(fromLibId);
                    addLibList.add(data);
                }

                Map<Integer, Integer> fromLibId_toLibId = new HashMap<>();
                FaiList<Integer> libIds = new FaiList<>();
                libProc.addLibBatch(toAid, addLibList, libIds);
                // 组装fromLibId 和 toLibId的映射关系
                for(int i = 0; i < libIds.size(); i++) {
                    fromLibId_toLibId.put(fromLibIds.get(i), libIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toLibId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromLibId : libId_RelList.keySet()) {
                    FaiList<Param> tmpAddList = libId_RelList.get(fromLibId);
                    for(Param relInfo : tmpAddList) {
                        //新的自增的libId
                        int libId = fromLibId_toLibId.get(fromLibId);
                        relInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libId);
                        addRelList.add(relInfo);
                    }
                }
                // 插入业务关系表克隆数据
                libRelProc.addIncrementalClone(toAid, new FaiList<>(cloneUidMap.values()), addRelList);

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

        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
    }

    @SuccessRt(value = Errno.OK)
    public int backupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds, Param backupInfo) throws IOException {
        if (verifyBackupArgs(flow, aid, unionPriIds, backupInfo, true)) {
            return Errno.ARGS_ERROR;
        }

        int rt;
        LockUtil.BackupLock.lock(aid);
        try {
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.BACKUP, aid, backupInfo)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, tc);
            ProductLibProc proc = new ProductLibProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 可能之前备份没有成功，先操作删除之前的备份
                deleteBackupData(relProc, proc, aid, backupId, backupFlag);

                // 备份业务关系表数据，返回需要备份的库ids
                Set<Integer> bakLibIds = relProc.backupData(aid, unionPriIds, backupId, backupFlag);

                if(!bakLibIds.isEmpty()) {
                    // 备份库表数据
                    proc.backupData(aid, backupId, backupFlag, bakLibIds);
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

    private void deleteBackupData(ProductLibRelProc relProc, ProductLibProc proc, int aid, int backupId, int backupFlag) {
        relProc.delBackupData(aid, backupId, backupFlag);
        proc.delBackupData(aid, backupId, backupFlag);
    }

    @SuccessRt(value = Errno.OK)
    public int restoreBackupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds, Param backupInfo) throws IOException {
        if (verifyBackupArgs(flow, aid, unionPriIds, backupInfo, true)) {
            return Errno.ARGS_ERROR;
        }
        int rt;
        LockUtil.BackupLock.lock(aid);
        try {
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.RESTORE, aid, backupInfo)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, tc);
            ProductLibProc proc = new ProductLibProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 还原关系表数据
                relProc.restoreBackupData(aid, unionPriIds, backupId, backupFlag);

                // 还原库表数据
                proc.restoreBackupData(aid, unionPriIds, backupId, backupFlag);

                commit = true;
            }finally {
                if(commit) {
                    tc.commit();
                    backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.RESTORE, aid, backupId);
                }else {
                    tc.rollback();
                    backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.RESTORE, aid, backupId);
                }
                tc.closeDao();
            }
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
            if (checkAndSetBackupStatus(flow, BackupStatusCtrl.Action.DELETE, aid, backupInfo)) {
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                return Errno.OK;
            }

            int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
            int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
            TransactionCtrl tc = new TransactionCtrl();
            ProductLibRelProc relProc = new ProductLibRelProc(flow, aid, tc);
            ProductLibProc proc = new ProductLibProc(flow, aid, tc);
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

    private boolean checkAndSetBackupStatus(int flow, BackupStatusCtrl.Action action, int aid, Param backupInfo) {
        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        String backupStatus = backupStatusCtrl.getStatus(action, aid, backupId);
        if (backupStatus != null) {
            int rt = Errno.ALREADY_EXISTED;
            if (backupStatusCtrl.isDoing(backupStatus)) {
                throw new MgException(rt, action + " is doing;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
            } else if (backupStatusCtrl.isFinish(backupStatus)) {
                rt = Errno.OK;
                Log.logStd(rt, action + " is already ok;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
                return true;
            } else if (backupStatusCtrl.isFail(backupStatus)) {
                Log.logStd(rt, action + " is fail, going retry;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
                return false;
            }
        }
        // 设置备份执行中
        backupStatusCtrl.setStatusIsDoing(action, aid, backupId);
        return false;
    }

    public void initBackupStatus(RedisCacheManager cache) {
        backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
    }

    private BackupStatusCtrl backupStatusCtrl;
    private static final String BAK_TYPE = "mgPdLib";
}
