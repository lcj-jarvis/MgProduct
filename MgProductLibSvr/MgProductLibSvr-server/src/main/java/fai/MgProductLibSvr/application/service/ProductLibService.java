package fai.MgProductLibSvr.application.service;

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
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.CloneDef;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
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
                       TransactionCtrl transactionCtrl,
                       ProductLibProc libProc,
                       ProductLibRelProc relLibProc,
                       FaiList<Param> addInfoList,
                       FaiList<Param> libInfoList,
                       FaiList<Param> relLibInfoList,
                       FaiList<Integer> relLibIds) {

        int rt;
        ProductLibRelProc libRelProc = new ProductLibRelProc(flow, aid, transactionCtrl);

        // 获取参数中最大的sort
        int maxSort = libRelProc.getMaxSort(aid, unionPriId);
        if(maxSort < 0) {
            rt = Errno.ERROR;
            Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
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
        if (transactionCtrl.isAutoCommit()) {
            transactionCtrl.setAutoCommit(false);
        }

        //保存libId
        FaiList<Integer> libIds = new FaiList<>();
        if (libProc == null) {
            libProc = new ProductLibProc(flow, aid, transactionCtrl);
        }
        //批量添加库表的数据
        libProc.addLibBatch(aid, libInfoList, libIds);

        for (int i = 0; i < libIds.size(); i++) {
            Param relLibInfo = relLibInfoList.get(i);
            //设置libId
            relLibInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libIds.get(i));
        }
        if (relLibProc == null) {
            relLibProc = new ProductLibRelProc(flow, aid, transactionCtrl);
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
                maxSort = addLibBatch(flow, aid, unionPriId, tid, transactionCtrl, libProc, libRelProc, addInfoList,
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
                relProc.delRelLibList(aid, unionPriId, rlLibIds);

                // 删除库表数据
                ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
                libProc.delLibList(aid, delLibIdList);

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
            relLibList = relLibProc.getLibRelList(aid, unionPriId, null,true);

            ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
            //查询所有的库表的数据
            libList = libProc.getLibList(aid,null, true);
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

    @SuccessRt(value = Errno.OK)
    public int getLibRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        return getLibRelByConditions(session, flow, aid, unionPriId, searchArg, false);
    }

    /**
     * 根据条件查询库业务表的数据
     * @param searchArg 查询的条件。为null的话，查询的条件就会是aid和unionPriId
     * @param getFromCache 是否需要查缓存
     * @return
     * @throws IOException
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
            list = relProc.getLibRelList(aid, unionPriId, searchArg, getFromCache);
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
                    relLibProc.delRelLibList(aid, unionPriId, delRlLibIds);

                    // 删除库表数据
                    libProc.delLibList(aid, delLibIdList);
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
                   maxSort = addLibBatch(flow, aid, unionPriId, tid, tc, libProc, relLibProc,
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
                ProductLibRelProc libRelProc = new ProductLibRelProc(flow, toAid, tc);
                libRelProc.cloneData(toAid, fromAid, cloneUidMap);

                // 库数据克隆
                ProductLibProc libProc = new ProductLibProc(flow, toAid, tc);
                libProc.cloneData(toAid, fromAid, cloneUidMap);

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
     * 增量克隆，即libId是自增的
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
            ProductLibRelProc libRelProc = new ProductLibRelProc(flow, toAid, tc);
            Map<Integer, Param> libId_RelInfo = new HashMap<>();

            //查出要克隆的数据
            FaiList<Param> fromRelList = libRelProc.getLibRelList(fromAid, fromUnionPriId, null, false);

            if(!fromRelList.isEmpty()) {
                // 查出已存在的数据
                FaiList<Param> existedList = libRelProc.getLibRelList(toAid, toUnionPriId, null, false);
                for(Param fromInfo : fromRelList) {
                    int rlLibId = fromInfo.getInt(ProductLibRelEntity.Info.RL_LIB_ID);
                    boolean existed = Misc.getFirst(existedList, ProductLibRelEntity.Info.RL_LIB_ID, rlLibId) != null;
                    // 如果rlLibId尚未存在，则需做增量
                    if(!existed) {
                        Param data = fromInfo.clone();
                        data.setInt(ProductLibRelEntity.Info.AID, toAid);
                        data.setInt(ProductLibRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int libId = data.getInt(ProductLibRelEntity.Info.LIB_ID);
                        libId_RelInfo.put(libId, data);
                    }
                }
            }
            // 没有需要做增量的数据
            if(libId_RelInfo.isEmpty()) {
                rt = Errno.OK;
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                Log.logStd("ok, incremental is empty;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            //查出对应的标签表数据
            SearchArg libSearch = new SearchArg();
            libSearch.matcher = new ParamMatcher(ProductLibEntity.Info.LIB_ID, ParamMatcher.IN,
                    new FaiList<>(libId_RelInfo.keySet()));
            ProductLibProc libProc = new ProductLibProc(flow, toAid, tc);
            FaiList<Param> fromLibList = libProc.getLibList(fromAid, libSearch, false);
            // 这里必定是会查到数据才对
            if(fromLibList.isEmpty()) {
                rt = Errno.ERROR;
                Log.logErr(rt, "get from lib list err;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            // 这里保持 fromLibIds 和 addLibList的顺序一致，是为了后面能得到fromLibId 和 toLibId的映射关系
            FaiList<Integer> fromLibIds = new FaiList<>();
            FaiList<Param> addLibList = new FaiList<>();
            for(Param fromInfo : fromLibList) {
                Param data = fromInfo.clone();
                data.setInt(ProductLibEntity.Info.AID, toAid);
                //移除查出来的libId，使用自增的libId
                int fromLibId = (Integer) data.remove(ProductLibEntity.Info.LIB_ID);
                //保存fromLibId
                fromLibIds.add(fromLibId);
                addLibList.add(data);
            }
            tc.setAutoCommit(false);
            try {
                Map<Integer, Integer> fromLibId_toLibId = new HashMap<>();
                FaiList<Integer> libIds = new FaiList<>();
                libProc.addLibBatch(toAid, addLibList, libIds);

                // 组装fromLibId 和 toLibId的映射关系
                for(int i = 0; i < libIds.size(); i++) {
                    fromLibId_toLibId.put(fromLibIds.get(i), libIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toLibId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromLibId : libId_RelInfo.keySet()) {
                    Param relInfo = libId_RelInfo.get(fromLibId);
                    int libId = fromLibId_toLibId.get(fromLibId);
                    relInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libId);
                    addRelList.add(relInfo);
                }
                // 插入增量克隆数据
                libRelProc.addIncrementalClone(toAid, toUnionPriId, addRelList);
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

}
