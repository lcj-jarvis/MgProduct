package fai.MgProductGroupSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
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
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.BackupStatusCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

        LockUtil.lock(aid);
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
                groupId = groupProc.addGroup(aid, groupInfo, unionPriId, tid);
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
            LockUtil.unlock(aid);
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

        LockUtil.lock(aid);
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
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setAllGroupList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<ParamUpdater> updaterList, int sysType, int groupLevel, boolean softDel) throws IOException {
        int rt;
        FaiList<Integer> rlGroupIds = new FaiList<>();
        // 先校验层级
        rt = checkLevel(updaterList, groupLevel);
        if (rt != Errno.OK) {
            Log.logErr("setAllGroupList err;groupLevel exceeds limit;flow=%d;aid=%d", flow, aid);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            int maxSort = 0;
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delGroupIdList = new FaiList<>();
            FaiList<Param> groupInfoList = new FaiList<>();
            FaiList<Param> relInfoList = new FaiList<>();
            FaiList<Param> addList = new FaiList<>();
            FaiList<ParamUpdater> newUpdaterList = new FaiList<>();
            FaiList<ParamUpdater> groupUpdaterList = new FaiList<>();
            FaiList<Integer> delRlGroupIds;
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, tc);
            try {
                tc.setAutoCommit(false);
                // 读取老数据
                FaiList<Param> list = relProc.getGroupRelList(aid, unionPriId);
                // 根据 sysType 筛选
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
                Searcher searcher = new Searcher(searchArg);
                FaiList<Param> oldList = searcher.getParamList(list);
                // 根据 rlGroupId 作为 key ，info 作为 value
                Map<Integer, Param> map = Utils.getMap(oldList, ProductGroupRelEntity.Info.RL_GROUP_ID);

                // 区分数据 （新增，删除，修改）
                for (ParamUpdater updater : updaterList) {
                    Param data = updater.getData();
                    Integer rlGroupId = data.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID);
                    if (rlGroupId == null) {
                        addList.add(data);
                        continue;
                    }
                    // 判断旧数据 map 中是否有数据，有则需要从 map 中 remove 掉，同时加入修改 list 中
                    if (map != null && map.get(rlGroupId) != null) {
                        map.remove(rlGroupId);
                        newUpdaterList.add(new ParamUpdater(data));
                    }
                }
                // map 剩下的数据就是要删除的
                delRlGroupIds = new FaiList<>(map.keySet());
                if (!delRlGroupIds.isEmpty()) {
                    // 先获取要删除的分类id
                    delGroupIdList = relProc.getIdsByRlIds(aid, unionPriId, delRlGroupIds, sysType);
                    // 删除分类业务表数据
                    relProc.delGroupList(aid, unionPriId, delGroupIdList, sysType, softDel);
                    // 删除分类表数据
                    groupProc.delGroupList(aid, delGroupIdList, softDel);
                }

                if (!newUpdaterList.isEmpty()) {
                    // 修改分类业务关系表
                    relProc.setGroupRelList(aid, unionPriId, newUpdaterList, groupUpdaterList);
                    // 修改分类表
                    if(!groupUpdaterList.isEmpty()) {
                        groupProc.setGroupList(aid, groupUpdaterList);
                    }
                }

                if (!addList.isEmpty()) {
                    maxSort = addGroupList(flow, aid, unionPriId, tid, addList, tc, groupInfoList, relInfoList, rlGroupIds);
                }

                commit = true;
            } finally {
                if (commit) {
                    tc.commit();
                } else {
                    tc.rollback();
                    groupProc.clearIdBuilderCache(aid);
                    relProc.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

            // 处理缓存
            if (!Util.isEmptyList(delRlGroupIds)) {
                // 设置过期时间
                ProductGroupCache.setExpire(aid);
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupCache.delCacheList(aid, delGroupIdList);
                ProductGroupRelCache.InfoCache.delCacheList(aid, unionPriId, delGroupIdList);
                ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, delRlGroupIds.size(), false);
            }
            if (!Util.isEmptyList(newUpdaterList)) {
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
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        if (!Util.isEmptyList(rlGroupIds)) {
            rlGroupIds.toBuffer(sendBuf, ProductGroupRelDto.Key.RL_GROUP_IDS);
        }
        session.write(sendBuf);
        Log.logStd("setAllGroupList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlGroupId=%s;", flow, aid, unionPriId, tid, rlGroupIds);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delGroupList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlGroupIdList, int sysType, boolean softDel) throws IOException {
        int rt;
        if(rlGroupIdList == null || rlGroupIdList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlGroupIdList is not valid;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
        }

        LockUtil.lock(aid);
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delGroupIdList = null;
            try {
                transactionCtrl.setAutoCommit(false);
                ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);
                // 先获取要删除的分类id
                delGroupIdList = relProc.getIdsByRlIds(aid, unionPriId, rlGroupIdList, sysType);

                // 删除分类业务表数据
                relProc.delGroupList(aid, unionPriId, rlGroupIdList, sysType, softDel);

                // 删除分类表数据
                ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
                groupProc.delGroupList(aid, delGroupIdList, softDel);

                commit = true;
                // commit之前设置10s过期时间，避免脏数据
                ProductGroupRelCache.InfoCache.setExpire(aid, unionPriId);
                ProductGroupCache.setExpire(aid);
            }finally {
                if(commit) {
                    transactionCtrl.commit();
                    ProductGroupCache.delCacheList(aid, delGroupIdList);
                    ProductGroupRelCache.InfoCache.delCacheList(aid, unionPriId, delGroupIdList);
                    ProductGroupRelCache.DataStatusCache.update(aid, unionPriId, delGroupIdList.size(), false);
                }else {
                    transactionCtrl.rollback();
                }
                transactionCtrl.closeDao();
            }
        }finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlGroupIdList);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int unionSetGroupList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList, int sysType, boolean softDel) throws IOException {
        int rt;
        FaiList<Integer> rlGroupIds = new FaiList<>();
        int maxSort = 0;
        LockUtil.lock(aid);
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
                    delGroupIdList = relProc.getIdsByRlIds(aid, unionPriId, delList, sysType);

                    // 删除分类业务表数据
                    relProc.delGroupList(aid, unionPriId, delList, sysType, softDel);

                    // 删除分类表数据
                    groupProc.delGroupList(aid, delGroupIdList, softDel);
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
                ProductGroupRelCache.InfoCache.delCacheList(aid, unionPriId, delGroupIdList);
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
            LockUtil.unlock(aid);
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
            int groupId = groupProc.addGroup(aid, groupInfo, unionPriId, tid);
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

        doClone(flow, aid, fromAid, cloneUnionPriIds, false);

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, aid, fromAid, cloneUnionPriIds);
        return rt;
    }

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

        LockUtil.lock(toAid);
        try {
            boolean commit = false;
            TransactionCtrl tc = new TransactionCtrl();
            tc.setAutoCommit(false);
            try {
                ProductGroupRelProc groupRelProc = new ProductGroupRelProc(flow, toAid, tc);
                ProductGroupProc groupProc = new ProductGroupProc(flow, toAid, tc);

                if(!incrementalClone) {
                    FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUidMap.values());
                    // 删除原有的业务关系表数据
                    ParamMatcher delMatcher = new ParamMatcher(ProductGroupRelEntity.Info.AID, ParamMatcher.EQ, toAid);
                    delMatcher.and(ProductGroupRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
                    groupRelProc.delGroupRelList(toAid, delMatcher);

                    // 删除原有的分类表数据
                    delMatcher = new ParamMatcher(ProductGroupEntity.Info.AID, ParamMatcher.EQ, toAid);
                    delMatcher.and(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);
                    groupProc.delGroupList(toAid, delMatcher);
                }

                // groupId -> addRelList
                Map<Integer, FaiList<Param>> groupId_RelList = new HashMap<>();
                // groupId -> sourceUnionPriId
                Map<Integer, Integer> groupId_SourceUid = new HashMap<>();
                // 查出要克隆的业务关系表数据
                for(Map.Entry<Integer, Integer> entry: cloneUidMap.entrySet()) {
                    int fromUnionPriId = entry.getKey();
                    int toUnionPriId = entry.getValue();
                    FaiList<Param> fromRelList = groupRelProc.searchFromDb(fromAid, fromUnionPriId, null);

                    if(fromRelList.isEmpty()) {
                        continue;
                    }

                    // 查出已存在的数据 for 增量克隆
                    FaiList<Param> existedList = null;
                    if(incrementalClone) {
                        existedList = groupRelProc.searchFromDb(toAid, toUnionPriId, null);
                    }
                    for(Param fromInfo : fromRelList) {
                        int rlGroupId = fromInfo.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID);
                        // 如果是增量克隆，但是rlGroupId已存在，则跳过
                        if(incrementalClone && Misc.getFirst(existedList, ProductGroupRelEntity.Info.RL_GROUP_ID, rlGroupId) != null) {
                            continue;
                        }
                        Param data = fromInfo.clone();
                        data.setInt(ProductGroupRelEntity.Info.AID, toAid);
                        data.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, toUnionPriId);
                        int groupId = data.getInt(ProductGroupRelEntity.Info.GROUP_ID);
                        FaiList<Param> addList = groupId_RelList.get(groupId);
                        if(addList == null) {
                            addList = new FaiList<>();
                            groupId_RelList.put(groupId, addList);
                            groupId_SourceUid.put(groupId, toUnionPriId);
                        }
                        addList.add(data);
                    }

                }

                // 没有要克隆的数据
                if(groupId_RelList.isEmpty()) {
                    return;
                }

                // 根据 fromAid 和 groupId 查出对应的分类表数据
                SearchArg groupSearch = new SearchArg();
                groupSearch.matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, new FaiList<>(groupId_RelList.keySet()));

                FaiList<Param> fromGroupList = groupProc.searchFromDb(fromAid, groupSearch);
                // 这里必定是会查到数据才对
                if(fromGroupList.isEmpty()) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "get from group list err;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
                }

                // 这里 保持 fromGroupIds 和 addGroupList的顺序一致，是为了后面能得到fromGroupId 和 toGroupId的映射关系
                FaiList<Integer> fromGroupIds = new FaiList<>();
                FaiList<Param> addGroupList = new FaiList<>();
                for(Param fromInfo : fromGroupList) {
                    Param data = fromInfo.clone();
                    data.setInt(ProductGroupEntity.Info.AID, toAid);
                    int fromGroupId = (Integer) data.remove(ProductGroupEntity.Info.GROUP_ID);
                    int sourceUnionPriId = groupId_SourceUid.get(fromGroupId);
                    data.setInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID, sourceUnionPriId);
                    fromGroupIds.add(fromGroupId);
                    addGroupList.add(data);
                }

                Map<Integer, Integer> fromGroupId_toGroupId = new HashMap<>();
                FaiList<Integer> groupIds = groupProc.batchAddGroup(toAid, addGroupList);
                // 组装fromGroupId 和 toGroupId的映射关系
                for(int i = 0; i < groupIds.size(); i++) {
                    fromGroupId_toGroupId.put(fromGroupIds.get(i), groupIds.get(i));
                }

                // 组装业务关系表增量克隆数据，设置toGroupId
                FaiList<Param> addRelList = new FaiList<>();
                for(Integer fromGroupId : groupId_RelList.keySet()) {
                    FaiList<Param> tmpAddList = groupId_RelList.get(fromGroupId);
                    for(Param relInfo : tmpAddList) {
                        int groupId = fromGroupId_toGroupId.get(fromGroupId);
                        relInfo.setInt(ProductGroupRelEntity.Info.GROUP_ID, groupId);
                        addRelList.add(relInfo);
                    }
                }
                // 插入业务关系表克隆数据
                groupRelProc.insert4Clone(toAid, new FaiList<>(cloneUidMap.values()), addRelList);

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
            LockUtil.unlock(toAid);
        }

        Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
    }

    @SuccessRt(value = Errno.OK)
    public int backupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPirIds, Param backupInfo) throws IOException {
        int rt;
        if(aid <= 0 || Str.isEmpty(backupInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
            return rt;
        }

        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
        if(backupId == 0 || backupFlag == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
            return rt;
        }

        LockUtil.BackupLock.lock(aid);
        try {
            String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.BACKUP, aid, backupId);
            if(backupStatus != null) {
                if(backupStatusCtrl.isDoing(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    throw new MgException(rt, "backup is doing;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                }else if(backupStatusCtrl.isFinish(backupStatus)) {
                    rt = Errno.OK;
                    Log.logStd(rt, "backup is already ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                    FaiBuffer sendBuf = new FaiBuffer(true);
                    session.write(sendBuf);
                    return Errno.OK;
                }else if(backupStatusCtrl.isFail(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    Log.logStd(rt, "backup is fail, going retry;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                }
            }
            // 设置备份执行中
            backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.BACKUP, aid, backupId);

            TransactionCtrl tc = new TransactionCtrl();
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            ProductGroupProc proc = new ProductGroupProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 可能之前备份没有成功，先操作删除之前的备份
                deleteBackupData(relProc, proc, aid, backupId, backupFlag);

                // 备份业务关系表数据，返回需要备份的分类ids
                Set<Integer> bakGroupIds = relProc.backupData(aid, unionPirIds, backupId, backupFlag);
            
                if(!bakGroupIds.isEmpty()) {
                    // 备份分类表数据
                    proc.backupData(aid, backupId, backupFlag, bakGroupIds);
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
        Log.logStd("backupData ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
        return rt;
    }

    @SuccessRt(Errno.OK)
    public int restoreBackupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPirIds, Param backupInfo) throws IOException {
        int rt;
        if(aid <= 0 || Str.isEmpty(backupInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
            return rt;
        }

        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
        if(backupId == 0 || backupFlag == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
            return rt;
        }

        LockUtil.BackupLock.lock(aid);
        try {
            String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.RESTORE, aid, backupId);
            if(backupStatus != null) {
                if(backupStatusCtrl.isDoing(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    throw new MgException(rt, "restore is doing;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                }else if(backupStatusCtrl.isFinish(backupStatus)) {
                    rt = Errno.OK;
                    Log.logStd(rt, "restore is already ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                    FaiBuffer sendBuf = new FaiBuffer(true);
                    session.write(sendBuf);
                    return Errno.OK;
                }else if(backupStatusCtrl.isFail(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    Log.logStd(rt, "restore is fail, going retry;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
                }
            }
            // 设置备份执行中
            backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.RESTORE, aid, backupId);

            TransactionCtrl tc = new TransactionCtrl();
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            ProductGroupProc proc = new ProductGroupProc(flow, aid, tc);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 还原关系表数据
                relProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

                // 还原分类表数据
                proc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

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
        Log.logStd("restore backupData ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
        return rt;
    }

    @SuccessRt(Errno.OK)
    public int delBackupData(FaiSession session, int flow, int aid, Param backupInfo) throws IOException {
        int rt;
        if(aid <= 0 || Str.isEmpty(backupInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
            return rt;
        }

        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
        if(backupId == 0 || backupFlag == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, backupInfo error;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
            return rt;
        }

        LockUtil.BackupLock.lock(aid);
        try {
            String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.DELETE, aid, backupId);
            if(backupStatus != null) {
                if(backupStatusCtrl.isDoing(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    throw new MgException(rt, "del backup is doing;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
                }else if(backupStatusCtrl.isFinish(backupStatus)) {
                    rt = Errno.OK;
                    Log.logStd(rt, "del backup is already ok;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
                    FaiBuffer sendBuf = new FaiBuffer(true);
                    session.write(sendBuf);
                    return Errno.OK;
                }else if(backupStatusCtrl.isFail(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    Log.logStd(rt, "del backup is fail, going retry;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
                }
            }
            // 设置备份执行中
            backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.DELETE, aid, backupId);

            TransactionCtrl tc = new TransactionCtrl();
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            ProductGroupProc proc = new ProductGroupProc(flow, aid, tc);
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

    private void deleteBackupData(ProductGroupRelProc relProc, ProductGroupProc proc, int aid, int backupId, int backupFlag) {

        relProc.delBackupData(aid, backupId, backupFlag);

        proc.delBackupData(aid, backupId, backupFlag);
    }

    @SuccessRt(value = Errno.OK)
    public int clearCache(FaiSession session, int flow, int aid) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            CacheCtrl.clearCacheVersion(aid);
            ProductGroupCache.delCache(aid);
        }finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clearCache ok;flow=%d;aid=%d;", flow, aid);
        return rt;
    }

    /**
     * 校验层级
     *
     * @param dataList 数据集合
     * @param groupLevel 层级限制
     * @return {@link Errno}
     */
    private int checkLevel(FaiList<ParamUpdater> dataList, int groupLevel) {
        int rt;
        // 为空直接返回 OK
        if (dataList == null || dataList.isEmpty()) {
            return Errno.OK;
        }
        // 用来标识新增的数据，作为 key
        int temp = -1;
        // 放入 <rlGroupId, parentId> 的 map 中，根据 map 查出层级
        HashMap<Integer, Integer> rlGroupIdAndParentIdMap = new HashMap<>();
        for (ParamUpdater updater : dataList) {
            Param data = updater.getData();
            int rlGroupId = data.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
            int parentId = data.getInt(ProductGroupRelEntity.Info.PARENT_ID, 0);
            if (rlGroupId == 0) {
                rlGroupIdAndParentIdMap.put(temp, parentId);
                data.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, temp--);
            } else {
                rlGroupIdAndParentIdMap.put(rlGroupId, parentId);
            }
        }
        Ref<Boolean> isLegal = new Ref<>();
        isLegal.value = true;
        for (ParamUpdater updater : dataList) {
            Param data = updater.getData();
            Integer rlGroupId = data.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID);
            int times = 0;
            // 递归寻找层级
            recursiveQuery(rlGroupId, rlGroupIdAndParentIdMap, times, groupLevel, isLegal);
            if (!isLegal.value) {
                rt = Errno.ERROR;
                return rt;
            }
            // 如果 rlGroupId 是负数 要将 这个 key remove 掉，这是之前插入的临时标志
            if (rlGroupId < 0) {
                data.remove(ProductGroupRelEntity.Info.RL_GROUP_ID);
            }
        }
        rt = Errno.OK;
        return rt;
    }

    /**
     * 递归查询层级，如果超出层级限制，isLegal 设置为 false
     *
     * @param rlGroupId 业务分类id
     * @param map rlGroupId 和 parentId 的映射
     * @param times 递归次数
     * @param level 层级限制
     * @param isLegal 是否合法，默认为 true
     */
    private void recursiveQuery(int rlGroupId, Map<Integer, Integer> map, int times, int level, Ref<Boolean> isLegal) {
        if (times >= level) {
            isLegal.value = false;
            return;
        }
        Integer parentId = map.get(rlGroupId);
        if (parentId != null) {
            // 如果父id 已经是一级分类,那就直接返回，减少一次递归
            if (parentId == 0) {
                return;
            }
            recursiveQuery(parentId, map, ++times, level, isLegal);
        }
    }

    private void assemblyGroupInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param groupInfo, Param relInfo) {
        String groupName = recvInfo.getString(ProductGroupEntity.Info.GROUP_NAME, "");
        if(!ProductGroupCheck.isNameValid(groupName)) {
            throw new MgException(Errno.ARGS_ERROR, "groupName is not valid;flow=%d;aid=%d;groupName=%d;", flow, aid, groupName);
        }

        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductGroupEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductGroupEntity.Info.UPDATE_TIME, now);
        int parentId = recvInfo.getInt(ProductGroupRelEntity.Info.PARENT_ID, ProductGroupRelValObj.Default.PARENT_ID);
        String iconList = recvInfo.getString(ProductGroupEntity.Info.ICON_LIST, ProductGroupValObj.Default.ICON_LIST);
        int flag = recvInfo.getInt(ProductGroupEntity.Info.FLAG, ProductGroupValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductGroupRelEntity.Info.SORT, ProductGroupRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductGroupRelEntity.Info.RL_FLAG, ProductGroupRelValObj.Default.RL_FLAG);
        int sysType = recvInfo.getInt(ProductGroupRelEntity.Info.SYS_TYPE, ProductGroupRelValObj.SysType.PRODUCT);
        int status = recvInfo.getInt(ProductGroupRelEntity.Info.STATUS, ProductGroupRelValObj.Status.DEFAULT);
        int groupId = recvInfo.getInt(ProductGroupEntity.Info.GROUP_ID, 0);
        int rlGroupId = recvInfo.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);

        // 分类表数据
        groupInfo.setInt(ProductGroupEntity.Info.AID, aid);
        groupInfo.setInt(ProductGroupEntity.Info.GROUP_ID, groupId);
        groupInfo.setInt(ProductGroupEntity.Info.SOURCE_TID, tid);
        groupInfo.setInt(ProductGroupEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        groupInfo.setString(ProductGroupEntity.Info.GROUP_NAME, groupName);
        groupInfo.setInt(ProductGroupEntity.Info.SYS_TYPE, sysType);
        groupInfo.setCalendar(ProductGroupEntity.Info.CREATE_TIME, createTime);
        groupInfo.setCalendar(ProductGroupEntity.Info.UPDATE_TIME, updateTime);
        groupInfo.setString(ProductGroupEntity.Info.ICON_LIST, iconList);
        groupInfo.setInt(ProductGroupEntity.Info.FLAG, flag);
        groupInfo.setInt(ProductGroupEntity.Info.STATUS, status);

        // 分类业务关系表数据
        relInfo.setInt(ProductGroupRelEntity.Info.AID, aid);
        relInfo.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, rlGroupId);
        relInfo.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, unionPriId);
        relInfo.setCalendar(ProductGroupRelEntity.Info.CREATE_TIME, createTime);
        relInfo.setCalendar(ProductGroupRelEntity.Info.UPDATE_TIME, updateTime);
        relInfo.setInt(ProductGroupRelEntity.Info.SORT, sort);
        relInfo.setInt(ProductGroupRelEntity.Info.RL_FLAG, rlFlag);
        relInfo.setInt(ProductGroupRelEntity.Info.SYS_TYPE, sysType);
        relInfo.setInt(ProductGroupRelEntity.Info.STATUS, status);
        relInfo.setInt(ProductGroupRelEntity.Info.PARENT_ID, parentId);
    }

    public void initBackupStatus(RedisCacheManager cache) {
        backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
    }

    private BackupStatusCtrl backupStatusCtrl;
    private static final String BAK_TYPE = "mgPdGroup";
}
