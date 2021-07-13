package fai.MgProductGroupSvr.application.service;

import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.common.ProductGroupCheck;
import fai.MgProductGroupSvr.domain.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelEntity;
import fai.MgProductGroupSvr.domain.entity.ProductGroupRelValObj;
import fai.MgProductGroupSvr.domain.entity.ProductGroupValObj;
import fai.MgProductGroupSvr.domain.repository.CacheCtrl;
import fai.MgProductGroupSvr.domain.repository.ProductGroupCache;
import fai.MgProductGroupSvr.domain.repository.ProductGroupRelCache;
import fai.MgProductGroupSvr.domain.serviceproc.ProductGroupProc;
import fai.MgProductGroupSvr.domain.serviceproc.ProductGroupRelProc;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.CloneDef;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class ProductGroupService extends ServicePub {

    @SuccessRt(value = Errno.OK)
    public int addGroupInfo(FaiSession session, int flow, int aid, int unionPriId, int tid, Param info) throws IOException {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        int groupId = 0;
        int rlGroupId = 0;
        int maxSort = 0;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            ProductGroupRelProc groupRelProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
            // 获取参数中最大的sort
            maxSort = groupRelProc.getMaxSort(aid, unionPriId);
            if(maxSort < 0) {
                rt = Errno.ERROR;
                Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            // 未设置排序则默认排序值+1
            Integer sort = info.getInt(ProductGroupRelEntity.Info.SORT);
            if(sort == null) {
                info.setInt(ProductGroupRelEntity.Info.SORT, ++maxSort);
            }
            Param groupInfo = new Param();
            Param relInfo = new Param();
            assemblyGroupInfo(flow, aid, unionPriId, tid, info, groupInfo, relInfo);

            boolean commit = false;
            transactionCtrl.setAutoCommit(false);
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
            try {
                groupId = groupProc.addGroup(aid, groupInfo);
                relInfo.setInt(ProductGroupRelEntity.Info.GROUP_ID, groupId);

                rlGroupId = groupRelProc.addGroupRelInfo(aid, unionPriId, relInfo);
                commit = true;
            } finally {
                if(commit) {
                    transactionCtrl.commit();
                    // 新增缓存
                    ProductGroupCache.addCache(aid, groupInfo);
                    ProductGroupRelCache.InfoCache.addCache(aid, unionPriId, relInfo);
                    ProductGroupRelCache.SortCache.set(aid, unionPriId, maxSort);
                    ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, 1);
                }else {
                    transactionCtrl.rollback();
                    groupProc.clearIdBuilderCache(aid);
                    groupRelProc.clearIdBuilderCache(aid, unionPriId);
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductGroupRelDto.Key.RL_GROUP_ID, rlGroupId);
        sendBuf.putInt(ProductGroupRelDto.Key.GROUP_ID, groupId);
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlGroupId=%d;groupId=%d;", flow, aid, unionPriId, tid, rlGroupId, groupId);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getGroupList(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> relList;
        FaiList<Param> groupList;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
            relList = relProc.getGroupRelList(aid, unionPriId);

            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
            groupList = groupProc.getGroupList(aid);
        }finally {
            transactionCtrl.closeDao();
        }

        // 数据整合
        HashMap<Integer, Param> relMap = new HashMap<Integer, Param>();
        for(int i = 0; i < groupList.size(); i++) {
            Param info = groupList.get(i);
            Integer groupId = info.getInt(ProductGroupEntity.Info.GROUP_ID);
            relMap.put(groupId, info);
        }
        for(int i = 0; i < relList.size(); i++) {
            Param info = relList.get(i);
            Integer groupId = info.getInt(ProductGroupRelEntity.Info.GROUP_ID);
            Param groupInfo = relMap.get(groupId);
            if(groupInfo == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            info.assign(groupInfo);
        }

        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        if(searchArg.cmpor == null) {
            searchArg.cmpor = new ParamComparator();
            searchArg.cmpor.addKey(ProductGroupRelEntity.Info.SORT);
        }
        searchArg.cmpor.addKey(ProductGroupRelEntity.Info.RL_GROUP_ID);

        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> list = searcher.getParamList(relList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductGroupRelDto.Key.INFO_LIST, ProductGroupRelDto.getAllInfoDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductGroupRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getGroupRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
            info = relProc.getDataStatus(aid, unionPriId);
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductGroupRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getGroupRelDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllGroupRel(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
            list = relProc.getGroupRelList(aid, unionPriId);
        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductGroupRelDto.Key.INFO_LIST, ProductGroupRelDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int searchGroupRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
            list = relProc.searchFromDb(aid, unionPriId, searchArg);

        }finally {
            transactionCtrl.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductGroupRelDto.Key.INFO_LIST, ProductGroupRelDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductGroupRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setGroupList(FaiSession session, int flow, int aid, int unionPriId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            FaiList<ParamUpdater> groupUpdaterList = new FaiList<ParamUpdater>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            try {
                transactionCtrl.setAutoCommit(false);
                // 修改分类业务关系表
                ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
                relProc.setGroupRelList(aid, unionPriId, updaterList, groupUpdaterList);

                // 修改分类表
                if(!groupUpdaterList.isEmpty()) {
                    ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
                    groupProc.setGroupList(aid, groupUpdaterList);
                }
                commit = true;
                // commit之前设置10s过期时间，避免脏数据
                if(updaterList != null && !updaterList.isEmpty()) {
                    ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                }
                if(!groupUpdaterList.isEmpty()) {
                    ProductGroupCache.setExpire(aid);
                }
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductGroupCache.updateCacheList(aid, groupUpdaterList);
                    if(!Util.isEmptyList(updaterList)) {
                        ProductGroupRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                        // 修改数据，更新dataStatus 的管理态字段更新时间
                        ProductGroupRelCache.DataStatusCache.update(aid, unionPriId);
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
    public int delGroupList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlGroupIdList) throws IOException {
        int rt;
        if(rlGroupIdList == null || rlGroupIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlGroupIdList is not valid;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delGroupIdList = null;
            try {
                transactionCtrl.setAutoCommit(false);
                ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
                // 先获取要删除的分类id
                delGroupIdList = relProc.getIdsByRlIds(aid, unionPriId, rlGroupIdList);

                // 删除分类业务表数据
                relProc.delGroupList(aid, unionPriId, rlGroupIdList);

                // 删除分类表数据
                ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
                groupProc.delGroupList(aid, delGroupIdList);

                commit = true;
                // commit之前设置10s过期时间，避免脏数据
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupCache.setExpire(aid);
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductGroupCache.delCacheList(aid, delGroupIdList);
                    ProductGroupRelCache.InfoCache.delCacheList(aid, unionPriId, rlGroupIdList);
                    ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, rlGroupIdList.size(), false);
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
        Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlGroupIdList);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int unionSetGroupList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList) throws IOException {
        int rt;
        FaiList<Integer> rlGroupIds = new FaiList<>();
        int maxSort = 0;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            boolean commit = false;
            FaiList<Param> groupInfoList = new FaiList<>();
            FaiList<Param> relInfoList = new FaiList<>();
            FaiList<Integer> delGroupIdList = null;
            FaiList<ParamUpdater> groupUpdaterList = new FaiList<>();
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, tc);
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            try {
                // 删除
                if (!Util.isEmptyList(delList)) {
                    // 先获取要删除的分类id
                    delGroupIdList = relProc.getIdsByRlIds(aid, unionPriId, delList);

                    // 删除分类业务表数据
                    relProc.delGroupList(aid, unionPriId, delList);

                    // 删除分类表数据
                    groupProc.delGroupList(aid, delGroupIdList);
                }
                // 修改
                if (!Util.isEmptyList(updaterList)) {
                    // 修改分类业务关系表
                    relProc.setGroupRelList(aid, unionPriId, updaterList, groupUpdaterList);
                    // 修改分类表
                    if(!groupUpdaterList.isEmpty()) {
                        groupProc.setGroupList(aid, groupUpdaterList);
                    }
                }
                // 添加
                if (!Util.isEmptyList(addList)) {
                    maxSort = addGroupList(flow, aid, unionPriId, tid, addList, tc, groupInfoList, relInfoList, rlGroupIds);
                }

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                    groupProc.clearIdBuilderCache(aid);
                    relProc.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }
            // 处理缓存
            if (!Util.isEmptyList(delList)) {
                // 设置过期时间
                ProductGroupCache.setExpire(aid);
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupCache.delCacheList(aid, delGroupIdList);
                ProductGroupRelCache.InfoCache.delCacheList(aid, unionPriId, delList);
                ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, delList.size(), false);
            }
            if (!Util.isEmptyList(updaterList)) {
                if (!groupUpdaterList.isEmpty()) {
                    // 设置过期时间
                    ProductGroupCache.setExpire(aid);
                    ProductGroupCache.updateCacheList(aid, groupUpdaterList);
                }
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupRelCache.InfoCache.updateCacheList(aid, unionPriId, updaterList);
                // 修改数据，更新dataStatus 的管理态字段更新时间
                ProductGroupRelCache.DataStatusCache.update(aid, unionPriId);
            }
            if (!Util.isEmptyList(addList)) {
                // 设置过期时间
                ProductGroupCache.setExpire(aid);
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupCache.addCacheList(aid, groupInfoList);
                ProductGroupRelCache.InfoCache.addCacheList(aid, unionPriId, relInfoList);
                ProductGroupRelCache.SortCache.set(aid, unionPriId, maxSort);
                ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, groupInfoList.size());
            }
        } finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        if (!Util.isEmptyList(rlGroupIds)) {
            rlGroupIds.toBuffer(sendBuf, ProductGroupRelDto.Key.RL_GROUP_IDS);
        }
        session.write(sendBuf);
        Log.logStd("unionSetGroupList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlGroupId=%s;", flow, aid, unionPriId, tid, rlGroupIds);
        return rt;
    }

    private int addGroupList(int flow, int aid, int unionPriId, int tid, FaiList<Param> addList, TransactionCtrl tc, FaiList<Param> groupInfoList, FaiList<Param> relInfoList, FaiList<Integer> rlGroupIds) {
        int rt;
        ProductGroupRelProc groupRelProc = new ProductGroupRelProc(flow, aid, tc);
        int maxSort = groupRelProc.getMaxSort(aid, unionPriId);
        for (Param addInfo : addList) {
            // 获取参数中最大的sort
            if(maxSort < 0) {
                rt = Errno.ERROR;
                Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            // 未设置排序则默认排序值+1
            Integer sort = addInfo.getInt(ProductGroupRelEntity.Info.SORT);
            if(sort == null) {
                addInfo.setInt(ProductGroupRelEntity.Info.SORT, ++maxSort);
            }
            Param groupInfo = new Param();
            Param relInfo = new Param();
            assemblyGroupInfo(flow, aid, unionPriId, tid, addInfo, groupInfo, relInfo);
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, tc);
            int groupId = groupProc.addGroup(aid, groupInfo);
            relInfo.setInt(ProductGroupRelEntity.Info.GROUP_ID, groupId);

            groupInfoList.add(groupInfo);
            relInfoList.add(relInfo);
            rlGroupIds.add(groupRelProc.addGroupRelInfo(aid, unionPriId, relInfo));
        }

        return maxSort;
    }

    @SuccessRt(value = Errno.OK)
    public int cloneData(FaiSession session, int flow, int aid, int fromAid, FaiList<Param> cloneUnionPriIds) throws IOException {
        int rt;
        if(Util.isEmptyList(cloneUnionPriIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, cloneUnionPriIds is empty;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, aid, fromAid, cloneUnionPriIds);
            return rt;
        }

        Map<Integer, Integer> cloneUidMap = new HashMap<>();
        for(Param cloneUidInfo : cloneUnionPriIds) {
            Integer toUnionPriId = cloneUidInfo.getInt(CloneDef.Info.TO_UNIONPRIID);
            Integer fromUnionPriId = cloneUidInfo.getInt(CloneDef.Info.FROM_UNIONPRIID);
            if(toUnionPriId == null || fromUnionPriId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, cloneUnionPriIds is error;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, aid, fromAid, cloneUnionPriIds);
                return rt;
            }
            cloneUidMap.put(fromUnionPriId, toUnionPriId);
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            try {
                // 业务关系数据克隆
                ProductGroupRelProc groupRelProc = new ProductGroupRelProc(flow, aid, tc);
                groupRelProc.cloneData(aid, fromAid, cloneUidMap);

                // 分类数据克隆
                ProductGroupProc groupProc = new ProductGroupProc(flow, aid, tc);
                groupProc.cloneData(aid, fromAid, cloneUidMap);

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
        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, aid, fromAid, cloneUnionPriIds);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int incrementalClone(FaiSession session, int flow, int toAid, int toUnionPriId, int fromAid, int fromUnionPriId) throws IOException {
        int rt;

        Lock lock = LockUtil.getLock(toAid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            // 业务关系数据克隆
            ProductGroupRelProc groupRelProc = new ProductGroupRelProc(flow, toAid, tc);
            Map<Integer, Param> groupId_RelInfo = new HashMap<>();

            // 查出要克隆的数据
            FaiList<Param> fromRelList = groupRelProc.searchFromDb(fromAid, fromUnionPriId, null);
            if(!fromRelList.isEmpty()) {
                // 查出已存在的数据
                FaiList<Param> existedList = groupRelProc.searchFromDb(toAid, toUnionPriId, null);
                for(Param fromInfo : fromRelList) {
                    int rlGroupId = fromInfo.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID);
                    boolean existed = Misc.getFirst(existedList, ProductGroupRelEntity.Info.RL_GROUP_ID, rlGroupId) != null;
                    // 如果rlGroupId尚未存在，则需做增量
                    if(!existed) {
                        Param data = fromInfo.clone();
                        data.setInt(ProductGroupRelEntity.Info.AID, toAid);
                        data.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int groupId = data.getInt(ProductGroupRelEntity.Info.GROUP_ID);
                        groupId_RelInfo.put(groupId, data);
                    }
                }
            }
            // 没有需要做增量的数据
            if(groupId_RelInfo.isEmpty()) {
                rt = Errno.OK;
                FaiBuffer sendBuf = new FaiBuffer(true);
                session.write(sendBuf);
                Log.logStd("ok, incremental is empty;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            //查出对应的分类表数据
            SearchArg groupSearch = new SearchArg();
            groupSearch.matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, new FaiList<>(groupId_RelInfo.keySet() ));
            ProductGroupProc groupProc = new ProductGroupProc(flow, toAid, tc);
            FaiList<Param> fromGroupList = groupProc.searchFromDb(fromAid, groupSearch);
            // 这里必定是会查到数据才对
            if(fromGroupList.isEmpty()) {
                rt = Errno.ERROR;
                Log.logErr(rt, "get from group list err;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
                return rt;
            }

            // 这里 保持 fromGroupIds 和 addGroupList的顺序一致，是为了后面能得到fromGroupId 和 toGroupId的映射关系
            FaiList<Integer> fromGroupIds = new FaiList<>();
            FaiList<Param> addGroupList = new FaiList<>();
            for(Param fromInfo : fromGroupList) {
                Param data = fromInfo.clone();
                data.setInt(ProductGroupEntity.Info.AID, toAid);
                int fromGroupId = (Integer) data.remove(ProductGroupEntity.Info.GROUP_ID);
                fromGroupIds.add(fromGroupId);
                addGroupList.add(data);
            }
            tc.setAutoCommit(false);
            try {
                Map<Integer, Integer> fromGroupId_toGroupId = new HashMap<>();
                FaiList<Integer> groupIds = groupProc.batchAddGroup(toAid, addGroupList);
                // 组装fromGroupId 和 toGroupId的映射关系
                for(int i = 0; i < groupIds.size(); i++) {
                    fromGroupId_toGroupId.put(fromGroupIds.get(i), groupIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toGroupId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromGroupId : groupId_RelInfo.keySet()) {
                    Param relInfo = groupId_RelInfo.get(fromGroupId);
                    int groupId = fromGroupId_toGroupId.get(fromGroupId);
                    relInfo.setInt(ProductGroupRelEntity.Info.GROUP_ID, groupId);
                    addRelList.add(relInfo);
                }
                // 插入增量克隆数据
                groupRelProc.insert4IncrementalClone(toAid, toUnionPriId, addRelList);

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

    @SuccessRt(value = Errno.OK)
    public int clearCache(FaiSession session, int flow, int aid) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            CacheCtrl.clearCacheVersion(aid);
            ProductGroupCache.delCache(aid);
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clearCache ok;flow=%d;aid=%d;", flow, aid);
        return rt;
    }

    private void assemblyGroupInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param groupInfo, Param relInfo) {
        String groupName = recvInfo.getString(ProductGroupEntity.Info.GROUP_NAME, "");
        if(!ProductGroupCheck.isNameValid(groupName)) {
            throw new MgException(Errno.ARGS_ERROR, "groupName is not valid;flow=%d;aid=%d;groupName=%d;", flow, aid, groupName);
        }

        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductGroupEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductGroupEntity.Info.UPDATE_TIME, now);
        int parentId = recvInfo.getInt(ProductGroupEntity.Info.PARENT_ID, ProductGroupValObj.Default.PARENT_ID);
        String iconList = recvInfo.getString(ProductGroupEntity.Info.ICON_LIST, ProductGroupValObj.Default.ICON_LIST);
        int flag = recvInfo.getInt(ProductGroupEntity.Info.FLAG, ProductGroupValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductGroupRelEntity.Info.SORT, ProductGroupRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductGroupRelEntity.Info.RL_FLAG, ProductGroupRelValObj.Default.RL_FLAG);

        // 分类表数据
        groupInfo.setInt(ProductGroupEntity.Info.AID, aid);
        groupInfo.setInt(ProductGroupEntity.Info.SOURCE_TID, tid);
        groupInfo.setInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        groupInfo.setString(ProductGroupEntity.Info.GROUP_NAME, groupName);
        groupInfo.setCalendar(ProductGroupEntity.Info.CREATE_TIME, createTime);
        groupInfo.setCalendar(ProductGroupEntity.Info.UPDATE_TIME, updateTime);
        groupInfo.setInt(ProductGroupEntity.Info.PARENT_ID, parentId);
        groupInfo.setString(ProductGroupEntity.Info.ICON_LIST, iconList);
        groupInfo.setInt(ProductGroupEntity.Info.FLAG, flag);

        // 分类业务关系表数据
        relInfo.setInt(ProductGroupRelEntity.Info.AID, aid);
        relInfo.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, unionPriId);
        relInfo.setCalendar(ProductGroupRelEntity.Info.CREATE_TIME, createTime);
        relInfo.setCalendar(ProductGroupRelEntity.Info.UPDATE_TIME, updateTime);
        relInfo.setInt(ProductGroupRelEntity.Info.SORT, sort);
        relInfo.setInt(ProductGroupRelEntity.Info.RL_FLAG, rlFlag);
    }
}
