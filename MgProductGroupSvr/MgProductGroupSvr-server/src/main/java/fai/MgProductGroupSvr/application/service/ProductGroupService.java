package fai.MgProductGroupSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.common.ProductGroupCheck;
import fai.MgProductGroupSvr.domain.entity.*;
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
import java.util.*;
import java.util.stream.Collectors;

public class ProductGroupService extends ServicePub {

    @SuccessRt(value = Errno.OK)
    public int addGroupInfo(FaiSession session, int flow, int aid, int unionPriId, int tid, int sysType, Param info) throws IOException {
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
            assemblyGroupInfo(flow, aid, unionPriId, tid, sysType, info, groupInfo, relInfo);

            boolean commit = false;
            transactionCtrl.setAutoCommit(false);
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
            try {
                // 判断是否需要校验名称
                // 根据 tid 获取业务名称
                String businessName = BusinessMapping.getName(tid);
                // 通过读取配置文件，判断是否进行分类名称的校验
                boolean isCheck = isCheckGroupName(businessName);

                if (isCheck) {
                    // 搜索旧数据中是否存在当前名称
                    String name = info.getString(ProductGroupEntity.Info.GROUP_NAME);
                    ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, unionPriId);
                    matcher.and(ProductGroupEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
                    matcher.and(ProductGroupEntity.Info.GROUP_NAME, ParamMatcher.EQ, name);

                    FaiList<Param> groupList = getGroupList(aid, matcher, groupProc);
                    if (!Utils.isEmptyList(groupList)) {
                        rt = Errno.ALREADY_EXISTED;
                        throw new MgException(rt, "name is existed;flow=%d;aid=%d;name=%s", flow, aid, name);
                    }
                }

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
    public int setGroupList(FaiSession session, int flow, int aid, int tid, int unionPriId, int sysType, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            FaiList<ParamUpdater> groupUpdaterList = new FaiList<>();
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            boolean commit = false;
            try {
                transactionCtrl.setAutoCommit(false);
                ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, transactionCtrl);

                // 读取旧数据，作为修改时的参照
                FaiList<Param> oldRelList = getRelList(aid, unionPriId, sysType, null, relProc);

                // 修改分类业务关系表
                relProc.setGroupRelList(aid, unionPriId, oldRelList, updaterList, groupUpdaterList);
                // 修改分类表
                if(!groupUpdaterList.isEmpty()) {
                    // 判断是否需要校验名称, 根据 tid 获取业务名称
                    String businessName = BusinessMapping.getName(tid);
                    // 通过读取配置文件，判断是否进行分类名称的校验
                    boolean isCheck = isCheckGroupName(businessName);

                    // 找出旧数据当中的 groupId 集合，用于查询基础表的旧数据信息
                    List<Integer> oldGroupIds = oldRelList.stream().map(o -> o.getInt(ProductGroupEntity.Info.GROUP_ID)).collect(Collectors.toList());
                    ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, oldGroupIds);
                    ProductGroupProc groupProc = new ProductGroupProc(flow, aid, transactionCtrl);
                    // 获取基础表旧数据
                    FaiList<Param> oldList = getGroupList(aid, matcher, groupProc);
                    groupProc.setGroupList(aid, oldList, groupUpdaterList, isCheck);
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
    public int setAllGroupList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> treeDataList, int sysType, int groupLevel, boolean softDel) throws IOException {
        int rt;
        FaiList<Param> addList = new FaiList<>();
        FaiList<ParamUpdater> modifyList = new FaiList<>();
        // 校验层级
        checkLevel(flow, aid, treeDataList, groupLevel);

        // 判断是否需要校验名称, 根据 tid 获取业务名称
        String businessName = BusinessMapping.getName(tid);
        // 通过读取配置文件，判断是否进行分类名称的校验
        boolean isCheck = isCheckGroupName(businessName);

        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            FaiList<Integer> delGroupIdList = new FaiList<>();
            FaiList<Param> groupInfoList = new FaiList<>();
            FaiList<Param> relInfoList = new FaiList<>();
            FaiList<ParamUpdater> groupUpdaterList = new FaiList<>();
            FaiList<Integer> delRlGroupIds = new FaiList<>();
            ProductGroupRelProc relProc = new ProductGroupRelProc(flow, aid, tc);
            ProductGroupProc groupProc = new ProductGroupProc(flow, aid, tc);
            try {
                tc.setAutoCommit(false);
                // 读取业务表老数据
                FaiList<Param> oldRelList = getRelList(aid, unionPriId, sysType, null, relProc);

                // 旧数据的 rlGroupId 与 groupId 映射集合
                Map<Integer, Integer> map = oldRelList.stream().collect(Collectors.toMap(o -> o.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID), o -> o.getInt(ProductGroupRelEntity.Info.GROUP_ID)));
                // 获取旧数据中全部的 groupId，用于之后查询基础表信息
                FaiList<Integer> oldGroupIds = new FaiList<>(map.values());

                // 从 id_builder 表中查到当前的 rlGroupId，用于递归时对新增数据设置 rlGroupId
                Integer curRlGroupId = relProc.getId(aid, unionPriId);
                if (curRlGroupId == null) {
                    throw new MgException("get rlGroupId err;flow=%d;aid=%d", flow, aid);
                }

                // 将 curRlGroupId 设置到 tempId，递归时通过 tempId 来作为新增数据的 rlGroupId
                setTempId(curRlGroupId);

                // 递归处理树结构数据：
                // 1、梳理树结构，根据树结构添加 parentId 2、为新增数据添加上 rlGroupId 3、做数据区分，区分出新增、修改、删除的数据，map 剩下的数据就是需要删除的
                dataProcessing(treeDataList, map, 0, addList, modifyList);

                if (!map.isEmpty()) {
                    // 要删除的分类业务id
                    delRlGroupIds = new FaiList<>(map.keySet());
                    // 获取要删除的分类id
                    delGroupIdList = new FaiList<>(map.values());
                    // 删除分类业务表数据
                    relProc.delGroupList(aid, unionPriId, delRlGroupIds, sysType, softDel);
                    // 删除分类表数据
                    groupProc.delGroupList(aid, delGroupIdList, softDel);
                }

                FaiList<Param> oldList = new FaiList<>();
                if (!modifyList.isEmpty() || !addList.isEmpty()) {
                    // 排除之前删除的 groupId，通过剩下的 groupId 去查询基础表信息，这样就可以通过查出来的集合去做修改，以及名称上的校验
                    oldGroupIds.removeAll(delGroupIdList);
                    // 获取旧基础表数据
                    ParamMatcher matcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, oldGroupIds);
                    oldList = getGroupList(aid, matcher, groupProc);
                }

                if (!modifyList.isEmpty()) {
                    // 修改分类业务关系表
                    relProc.setGroupRelList(aid, unionPriId, oldRelList, modifyList, groupUpdaterList);
                    // 修改分类表
                    if(!groupUpdaterList.isEmpty()) {
                        groupProc.setGroupList(aid, oldList, groupUpdaterList, isCheck);
                    }
                }

                if (!addList.isEmpty()) {
                    addGroupList(flow, aid, unionPriId, tid, sysType, oldList, addList, groupProc, relProc, groupInfoList, relInfoList, null, isCheck);
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
            // 直接清除缓存
            if (!Utils.isEmptyList(delRlGroupIds) || !Utils.isEmptyList(modifyList) || !Utils.isEmptyList(addList)) {
                ProductGroupCache.delCache(aid);
                ProductGroupRelCache.InfoCache.delCache(aid, unionPriId);
                ProductGroupRelCache.SortCache.del(aid, unionPriId);
                ProductGroupRelCache.DataStatusCache.delCache(aid, unionPriId);
                CacheCtrl.clearCacheVersion(aid);
            }
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("setAllGroupList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
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
        // 判断是否需要校验名称, 根据 tid 获取业务名称
        String businessName = BusinessMapping.getName(tid);
        // 通过读取配置文件，判断是否进行分类名称的校验
        boolean isCheck = isCheckGroupName(businessName);
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
                ParamMatcher matcher = new ParamMatcher(ProductGroupRelEntity.Info.RL_GROUP_ID, ParamMatcher.NOT_IN, delList);
                // 获取未被删除的旧业务表数据
                FaiList<Param> oldRelList = getRelList(aid, unionPriId, sysType, matcher, relProc);
                FaiList<Param> oldList = new FaiList<>();
                if (!updaterList.isEmpty() || !addList.isEmpty()) {
                    // 获取groupIds集合
                    List<Integer> oldGroupIds = oldRelList.stream().map(o -> o.getInt(ProductGroupEntity.Info.GROUP_ID)).collect(Collectors.toList());
                    // 获取旧基础表数据
                    ParamMatcher groupMatcher = new ParamMatcher(ProductGroupEntity.Info.GROUP_ID, ParamMatcher.IN, oldGroupIds);
                    oldList = getGroupList(aid, groupMatcher, groupProc);
                }
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
                    relProc.setGroupRelList(aid, unionPriId, oldRelList, updaterList, groupUpdaterList);
                    // 修改分类表
                    if(!groupUpdaterList.isEmpty()) {
                        groupProc.setGroupList(aid, oldList, groupUpdaterList, isCheck);
                    }
                }
                // 添加
                if (!Util.isEmptyList(addList)) {
                    maxSort = addGroupList(flow, aid, unionPriId, tid, sysType, oldList, addList, groupProc, relProc, groupInfoList, relInfoList, rlGroupIds, isCheck);
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

    private int addGroupList(int flow, int aid, int unionPriId, int tid, int sysType, FaiList<Param> oldList, FaiList<Param> addList, ProductGroupProc groupProc, ProductGroupRelProc groupRelProc,
                             FaiList<Param> groupList, FaiList<Param> relList, FaiList<Integer> rlGroupIdList, boolean check) {
        int rt;
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
            assemblyGroupInfo(flow, aid, unionPriId, tid, sysType, addInfo, groupInfo, relInfo);
            groupList.add(groupInfo);
            relList.add(relInfo);
        }
        // 批量新增基础信息表数据
        FaiList<Integer> groupIds = groupProc.batchAddGroup(aid, oldList, groupList, check);
        for (int i = 0; i < relList.size(); i++) {
            Param param = relList.get(i);
            Integer groupId = groupIds.get(i);
            param.setInt(ProductGroupEntity.Info.GROUP_ID, groupId);
        }
        FaiList<Integer> rlGroupIds = groupRelProc.batchAddGroupRel(aid, unionPriId, relList);
        if (rlGroupIdList != null) {
            rlGroupIdList.addAll(rlGroupIds);
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
     */
    private void checkLevel(int flow, int aid, FaiList<Param> dataList, int groupLevel) {
        // 为空直接返回 OK
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        Ref<Boolean> isLegal = new Ref<>(true);
        // 递归检查层级
        recursiveQuery(dataList, 0, groupLevel, isLegal);
        if (!isLegal.value) {
            throw new MgException(Errno.ARGS_ERROR, "setAllGroupList err;groupLevel exceeds limit;flow=%d;aid=%d", flow, aid);
        }
    }

    /**
     * 递归查询层级，如果超出层级限制，isLegal 设置为 false
     *
     * @param dataList 业务分类id
     * @param times 递归次数
     * @param level 层级限制
     * @param isLegal 是否合法，默认为 true
     */
    private void recursiveQuery(FaiList<Param> dataList, int times, int level, Ref<Boolean> isLegal) {
        if (times >= level) {
            isLegal.value = false;
            return;
        }
        for (Param data : dataList) {
            // 先判断一次 isLegal 是否为 true , 如果不是应该跳出
            if (!isLegal.value) {
                break;
            }
            // 递归查询 children
            FaiList<Param> children = data.getList(ProductGroupEntity.Info.CHILDREN);
            if (!Utils.isEmptyList(children)) {
                // 递归次数 + 1
                recursiveQuery(children, times + 1, level, isLegal);
            }
        }
    }

    /**
     * 数据处理 ：
     *      1、去除掉 children 字段 (为了添加到 addList 或者 modifyList 的时候不带上 children)，同时根据或者的 rlGroupId 区分数据
     *      2、根据 treeDataList 存在的 rlGroupId 移除 map 中的数据，map 剩下数据就是需要删除的
     *      2、将新增数据的 rlGroupId 和 parentId 设置
     * @param treeDataList 树形数据
     * @param map 旧数据 rlGroupId 和 rlGroupId 映射集合
     * @param parentId 父类id
     * @param addList 需要添加的数据集合
     * @param modifyList 需要修改的数据集合
     */
    private void dataProcessing(FaiList<Param> treeDataList, Map<Integer, Integer> map, int parentId, FaiList<Param> addList, FaiList<ParamUpdater> modifyList) {
        if (Utils.isEmptyList(treeDataList)) {
            return;
        }
        for (Param data : treeDataList) {
            FaiList<Param> children = data.getList(ProductGroupEntity.Info.CHILDREN);
            data.remove(ProductGroupEntity.Info.CHILDREN);
            // 获取当前的 rlGroupId ，如果为空，加入添加列表
            Integer curRlGroupId = data.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID);
            // 双写时根据 isAdd 判断是否为添加数据
            boolean isAdd = data.getBoolean(ProductGroupEntity.Info.IS_ADD, false);
            if (isAdd) {
                addList.add(data);
            } else if (curRlGroupId == null) {
                addList.add(data);
                curRlGroupId = getAndIncTempId();
            } else {
                modifyList.add(new ParamUpdater(data));
            }
            if (map != null && map.size() != 0) {
                map.remove(curRlGroupId);
            }
            // 重新设置 rlGroupId 与 parentId
            data.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, curRlGroupId);
            data.setInt(ProductGroupRelEntity.Info.PARENT_ID, parentId);
            // 获取 children 作为下次递归的条件，同时将 parentId 设置为当前的 rlGroupId
            dataProcessing(children, map, curRlGroupId, addList, modifyList);
        }
    }

    private void assemblyGroupInfo(int flow, int aid, int unionPriId, int tid, int sysType, Param recvInfo, Param groupInfo, Param relInfo) {
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

    public Integer getAndIncTempId() {
        return ++tempId;
    }

    public void setTempId(Integer tempId) {
        this.tempId = tempId;
    }

    public void initBackupStatus(RedisCacheManager cache) {
        backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
    }

    /**
     * 获取配置文件，是否检查分类名称重复
     * 门店通反馈不需要做groupName校验
     * 暂时先不拿配置文件，直接返回false了
     * 考虑后面废弃校验name相关代码逻辑
     *
     * @param name 业务名称 eg: YK , SITE
     * @return boolean 是否检查
     */
    private boolean isCheckGroupName(String name) {
        /*if (Str.isEmpty(name)) {
            throw new MgException(Errno.ERROR, "tid is illegal;flow=%d;");
        }
        Param conf = MgConfPool.getEnvConf("MgPdCheckGroupNameSwitch");
        if (Str.isEmpty(conf)) {
            return false;
        }
        return conf.getBoolean(name, false);*/
        return false;
    }

    /**
     * 获取业务表数据
     */
    private FaiList<Param> getRelList(int aid, int unionPriId, int sysType, ParamMatcher matcher, ProductGroupRelProc relProc) {
        // 读取业务表老数据
        FaiList<Param> list = relProc.getGroupRelList(aid, unionPriId);
        // 根据 sysType 筛选
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductGroupRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        if (matcher != null && !matcher.isEmpty()) {
            searchArg.matcher.and(matcher);
        }
        Searcher searcher = new Searcher(searchArg);
        return searcher.getParamList(list);
    }

    /**
     * 获取基础表数据
     */
    private FaiList<Param> getGroupList(int aid, ParamMatcher matcher, ProductGroupProc groupProc) {
        // 读取数据
        FaiList<Param> groupList = groupProc.getGroupList(aid);
        if (matcher != null && !matcher.isEmpty()) {
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = matcher;
            Searcher groupSearcher = new Searcher(searchArg);
            return groupSearcher.getParamList(groupList);
        }
        return groupList;
    }

    private BackupStatusCtrl backupStatusCtrl;
    private static final String BAK_TYPE = "mgPdGroup";
    private int tempId;
}
