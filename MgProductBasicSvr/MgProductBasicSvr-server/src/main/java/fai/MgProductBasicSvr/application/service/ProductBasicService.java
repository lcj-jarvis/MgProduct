package fai.MgProductBasicSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.common.SagaRollback;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.*;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.MgProductBasicSvr.domain.serviceproc.*;
import fai.MgProductBasicSvr.interfaces.dto.ProductDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.app.DocOplogDef;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.infutil.MgConfPool;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.BackupStatusCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 操作商品基础数据
 */
public class ProductBasicService extends BasicParentService {

    @SuccessRt(Errno.OK)
    public int getPdBindBizInfo(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;uid=%d;sysType=%d;rlPdIds=%s;", flow, aid, unionPriId, sysType, rlPdIds);
            return rt;
        }
        if(!MgProductCheck.RequestLimit.checkReadSize(aid, rlPdIds)) {
            return Errno.SIZE_LIMIT;
        }

        FaiList<Param> result;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            Map<Integer, Integer> pdIdMaps = relProc.getPdIdRelMap(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
            if(pdIdMaps == null || pdIdMaps.isEmpty()) {
                rt = Errno.NOT_FOUND;
                Log.logErr(rt, "not found;aid=%d;uid=%s;sysType=%s;rlPdIds=%s;", aid, flow, unionPriId, sysType, rlPdIds);
                return rt;
            }

            Map<Integer, FaiList<Integer>> map = getBoundUniPriIds(flow, aid, new FaiList<>(pdIdMaps.keySet()));
            result = new FaiList<>();
            for(Integer pdId : map.keySet()) {
                FaiList<Integer> unionPriIds = map.get(pdId);
                Param resInfo = new Param();
                resInfo.setInt(ProductRelEntity.Info.RL_PD_ID, pdIdMaps.get(pdId));
                resInfo.setList(ProductRelEntity.Info.BIND_LIST, unionPriIds);
                result.add(resInfo);
            }
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        result.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getPdBindBizDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getPdBindBizInfo ok;flow=%d;aid=%d;unionPriId=%d;sysType=%s;rlPdIds=%s;size=%d;", flow, aid, unionPriId, sysType, rlPdIds, result.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getProductInfo(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlPdId) throws IOException {
        int rt;
        if(aid <= 0 || rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;uid=%d;sysType=%d;rlPdId=%s;", flow, aid, unionPriId, sysType, rlPdId);
            return rt;
        }

        Param result = new Param();
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            Integer pdId = relProc.getPdId(aid, unionPriId, sysType, rlPdId);
            if(pdId == null) {
                return Errno.NOT_FOUND;
            }
            // 获取商品业务关系表数据
            Param relInfo = relProc.getProductRel(aid, unionPriId, pdId);

            // 获取商品表数据
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            Param pdInfo = pdProc.getProductInfo(aid, pdId);

            result.assign(pdInfo);
            result.assign(relInfo);

            // 获取绑定分类
            if(useProductGroup()) {
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                FaiList<Param> bindGroups = bindGroupProc.getPdBindGroupList(aid, unionPriId, new FaiList<>(Arrays.asList(pdId)));
                FaiList<Integer> bindGroupIds = Utils.getValList(bindGroups, ProductBindGroupEntity.Info.RL_GROUP_ID);
                result.setList(ProductRelEntity.Info.RL_GROUP_IDS, bindGroupIds);
            }

            // 获取绑定标签
            if(useProductTag()) {
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                FaiList<Param> bindTags = bindTagProc.getPdBindTagList(aid, unionPriId, new FaiList<>(Arrays.asList(pdId)));
                FaiList<Integer> bindTagIds = Utils.getValList(bindTags, ProductBindTagEntity.Info.RL_TAG_ID);
                result.setList(ProductRelEntity.Info.RL_TAG_IDS, bindTagIds);
            }

            // 获取绑定参数
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            FaiList<Param> bindProps = bindPropProc.getPdBindPropList(aid, unionPriId, sysType, rlPdId);
            result.setList(ProductRelEntity.Info.RL_PROPS, bindProps);

        } finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        result.toBuffer(sendBuf, ProductRelDto.Key.INFO, ProductRelDto.getRelAndPdDto());
        session.write(sendBuf);
        Log.logDbg("get info ok;flow=%d;aid=%d;uid=%d;sysType=%s;rlPdId=%s;", flow, aid, unionPriId, sysType, rlPdId);
        return Errno.OK;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getProductList(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) throws IOException {
        int rt;
        if(!MgProductCheck.RequestLimit.checkReadSize(aid, rlPdIds)) {
            return Errno.SIZE_LIMIT;
        }

        FaiList<Param> relList;
        FaiList<Param> pdList;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            FaiList<Integer> pdIds = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
            // 获取商品业务关系表数据
            relList = relProc.getProductRelList(aid, unionPriId, pdIds);
            if(Utils.isEmptyList(relList)) {
                return Errno.NOT_FOUND;
            }
            // 获取商品表数据
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            pdList = pdProc.getProductList(aid, pdIds);
        } finally {
            tc.closeDao();
        }
        if(Utils.isEmptyList(pdList)) {
            rt = Errno.NOT_FOUND;
            Log.logErr("get pd list err;pdList is empty;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        Map<Integer, Param> pdMap = new HashMap<>(pdList.size());
        for (int i = 0; i < pdList.size(); i++) {
            Param info = pdList.get(i);
            int pdId = info.getInt(ProductEntity.Info.PD_ID);
            pdMap.put(pdId, info);
        }
        // 数据整合
        for (int i = 0; i < relList.size(); i++) {
            Param relInfo = relList.get(i);
            int pdId = relInfo.getInt(ProductRelEntity.Info.PD_ID);
            Param pdInfo = pdMap.get(pdId);
            if(pdInfo == null) {
                int rlPdId = relInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;flow=%d;aid=%d;uid=%d;pdId=%d;rlPdId=%d;", flow, aid, unionPriId, pdId, rlPdId);
                return rt;
            }
            pdInfo.remove(ProductEntity.Info.STATUS);
            relInfo.assign(pdInfo);
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        relList.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getRelAndPdDto());
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        return Errno.OK;
    }

    /**
     * 给悦客提供的批量修改接口
     * 可同时修改多个unionPriId * 多个rlPdId的数据
     * 目前仅支持修改status 和 绑定分类
     *
     * 修改status时，若对应unionPriId下没有数据，则克隆一份总店数据到对应unionPriId下
     */
    @SuccessRt(Errno.OK)
    public int batchSet4YK(FaiSession session, int flow, int aid, String xid, int ownUnionPriId, int sysType, FaiList<Integer> unionPriIds, FaiList<Integer> rlPdIds, ParamUpdater updater) throws IOException {
        int rt;
        if(aid <= 0 || ownUnionPriId <= 0 || Utils.isEmptyList(unionPriIds) || Utils.isEmptyList(rlPdIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;ownUid=%d;uids=%s;rlPdIds=%s;updater=%s;", flow, aid, ownUnionPriId, unionPriIds, rlPdIds, updater.toJson());
            return rt;
        }
        Param updateInfo = updater.getData();
        if(Str.isEmpty(updateInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, update is empty;flow=%d;aid=%d;ownUid=%d;uids=%s;rlPdIds=%s;updater=%s;", flow, aid, ownUnionPriId, unionPriIds, rlPdIds, updater.toJson());
            return rt;
        }

        Integer status = updateInfo.getInt(ProductRelEntity.Info.STATUS);
        FaiList<Integer> rlGroupIds = updateInfo.getList(ProductRelEntity.Info.RL_GROUP_IDS);
        if(status == null && rlGroupIds == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, update is empty;flow=%d;aid=%d;ownUid=%d;uids=%s;rlPdIds=%s;updater=%s;", flow, aid, ownUnionPriId, unionPriIds, rlPdIds, updater.toJson());
            return rt;
        }
        FaiList<Param> addList = new FaiList<>();

        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid);
                }

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, true);
                FaiList<Integer> pdIds = relProc.getPdIds(aid, ownUnionPriId, sysType, new HashSet<>(rlPdIds));
                if(status != null) {
                    addList = relProc.setPdStatus(aid, ownUnionPriId, unionPriIds, pdIds, status);
                }

                // 更新分类绑定
                if(rlGroupIds != null) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, true);
                    for(int unionPirId : unionPriIds) {
                        FaiList<Param> relList = relProc.getProductRelList(aid, unionPirId, pdIds);
                        Map<Integer, Integer> pdId_rlPdId = Utils.getMap(relList, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
                        // 组装新设置的分类绑定关系
                        FaiList<Param> newBindGroupList = new FaiList<>();
                        for(int pdId : pdIds) {
                            int rlPdId = pdId_rlPdId.get(pdId);
                            for(int rlGroupId : rlGroupIds) {
                                Param newBind = new Param();
                                newBind.setInt(ProductBindGroupEntity.Info.SYS_TYPE, sysType);
                                newBind.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
                                newBind.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
                                newBind.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
                                newBindGroupList.add(newBind);
                            }
                        }
                        bindGroupProc.updateBindGroupList(aid, unionPirId, pdIds, newBindGroupList);
                    }
                }

                // 提交前设置下临时过期时间
                CacheCtrl.setExpire(aid);
                commit = true;
            }finally {
                if(!commit) {
                    tc.rollback();
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }

            // 删除缓存
            CacheCtrl.clearCacheVersion(aid);

            // 同步修改给es
            ESUtil.commitPre(flow, aid);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        addList.toBuffer(sendBuf, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
        session.write(sendBuf);
        Log.logStd("setPdStatus ok;flow=%d;aid=%d;ownUid=%d;uids=%s;rlPdIds=%s;updater=%s;", flow, aid, ownUnionPriId, unionPriIds, rlPdIds, updater.toJson());
        return rt;
    }

    @SuccessRt(Errno.OK)
    public int batchSet4YKRollback(FaiSession session, int flow, int aid, String xid, long branchId) throws IOException {
        SagaRollback sagaRollback = (tc) -> {
            // 回滚商品业务关系表数据
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, false);
            relProc.rollback4Saga(aid, branchId);

            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, false);
            bindGroupProc.rollback4Saga(aid, branchId);
        };

        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    @SuccessRt(Errno.OK)
    public int cloneBizBind(FaiSession session, int flow, int aid, int fromUnionPriId, int toUnionPriId) throws IOException {
        int rt;
        if(aid <= 0 || fromUnionPriId <= 0 || toUnionPriId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error;flow=%d;aid=%d;fromUid=%d;toUid=%s;", flow, aid, fromUnionPriId, toUnionPriId);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // 克隆业务表数据
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                relProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId);

                // 克隆分类关联数据
                if(useProductGroup()) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                    bindGroupProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId);
                }

                // 克隆标签关联数据
                if(useProductTag()) {
                    ProductBindTagProc bindGroupProc = new ProductBindTagProc(flow, aid, tc);
                    bindGroupProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId);
                }

                // 克隆参数关联数据
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                bindPropProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId);

                commit = true;
            }finally {
                if(!commit) {
                    tc.rollback();
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }

            // 删除缓存
            CacheCtrl.clearCacheVersion(aid);
            // 同步修改给es
            ESUtil.commitPre(flow, aid);

        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("cloneBizBind ok;flow=%d;aid=%d;fromUid=%s;toUid=%s;", flow, aid, fromUnionPriId, toUnionPriId);
        return rt;
    }

    /**
     * 取消商品业务关联
     * softDel: 是否软删除，软删除实际只是置起标志位
     */
    @SuccessRt(value = Errno.OK)
    public int batchDelPdRelBind(FaiSession session, int flow ,int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) throws IOException {
        int rt;
        if(!MgProductCheck.RequestLimit.checkWriteSize(aid, rlPdIds)) {
            return Errno.SIZE_LIMIT;
        }
        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            int delCount = 0;
            int delGroupCount = 0;
            int delTagCount = 0;
            int delPropCount = 0;
            FaiList<Integer> delPdIds;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                delPdIds = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
                if(Utils.isEmptyList(delPdIds)) {
                    rt = Errno.NOT_FOUND;
                    Log.logErr(rt, "delete data not found;aid=%d;uid=%d;sysType=%d;rlPdIds=%s;", aid, unionPriId, sysType, rlPdIds);
                    return rt;
                }
                ProductRelCacheCtrl.InfoCache.setExpire(aid, unionPriId);
                delCount = relProc.delProductRel(aid, unionPriId, delPdIds, softDel);
                // 删除参数、分类、标签关联数据
                if(!softDel) {
                    if(useProductGroup()) {
                        ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                        delGroupCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, delPdIds);
                    }

                    if(useProductTag()) {
                        ProductBindTagProc bindGroupProc = new ProductBindTagProc(flow, aid, tc);
                        ProductBindTagCache.setExpire(aid, unionPriId, delPdIds);
                        delGroupCount = bindGroupProc.delPdBindTagList(aid, unionPriId, delPdIds);
                    }

                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
                    matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
                    matcher.and(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
                    delPropCount = bindPropProc.delPdBindProp(aid, matcher);
                }
                commit = true;

            }finally {
                if(!commit) {
                    tc.rollback();
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }

            // 同步到es
            ESUtil.batchLogDocId(flow, aid, unionPriId, delPdIds, DocOplogDef.Operation.DELETE_ONE);

            // 删除缓存
            ProductRelCacheCtrl.PdIdCache.delCacheList(aid, unionPriId, sysType, rlPdIds);
            ProductRelCacheCtrl.InfoCache.delCacheList(aid, unionPriId, delPdIds);
            ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, -delCount); // 更新数据状态缓存
            if(!softDel) {
                // 处理商品分类关联数据缓存
                ProductBindGroupCache.delCacheList(aid, unionPriId, delPdIds);
                ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, -delGroupCount);
                // 处理商品标签关联数据缓存
                ProductBindTagCache.delCacheList(aid, unionPriId, delPdIds);
                ProductBindTagCache.DataStatusCache.update(aid, unionPriId, -delTagCount);
                // 处理商品参数关联数据缓存
                ProductBindPropCache.delCacheList(aid, unionPriId, sysType, rlPdIds);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, -delPropCount);
            }

        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("delProductRels ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        return rt;
    }

    /**
     * 删除商品数据，同时删除所有相关业务关联数据
     */
    @SuccessRt(value = Errno.OK)
    public int delProductList(FaiSession session, int flow ,int aid, String xid, int tid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        if(!MgProductCheck.RequestLimit.checkWriteSize(aid, rlPdIds)) {
            return Errno.SIZE_LIMIT;
        }
        LockUtil.lock(aid);
        try {
            int delPdCount = 0;
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            FaiList<Integer> pdIdList = new FaiList<>();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid);
                }

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, true);
                pdIdList = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
                if(Utils.isEmptyList(pdIdList)) {
                    rt = Errno.NOT_FOUND;
                    Log.logErr(rt, "getIdRelList isEmpty;flow=%d;aid=%d;uid=%d;sysType=%s;rlPdIds=%s;", flow, aid, unionPriId, sysType, rlPdIds);
                    return rt;
                }
                // 删除pdIdList的所有业务关联数据
                relProc.delProductRelByPdId(aid, pdIdList, softDel);

                ProductProc pdProc = new ProductProc(flow, aid, tc, xid, true);
                // 删除商品数据
                pdProc.deleteProductList(aid, tid, pdIdList, softDel);

                if(!softDel) {
                    // 删除参数关联
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, true);
                    bindPropProc.delPdBindProp(aid, pdIdList);

                    if(useProductGroup()) {
                        // 删除分类关联
                        ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, true);
                        bindGroupProc.delPdBindGroupList(aid, pdIdList);
                    }

                    if(useProductTag()) {
                        // 删除标签关联
                        ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, true);
                        bindTagProc.delPdBindTagList(aid, pdIdList);
                    }
                }

                relProc.end(aid);

                commit = true;
                tc.commit();
            }finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 清缓存
            ProductRelCacheCtrl.PdIdCache.delCacheList(aid, unionPriId, sysType, rlPdIds);
            ProductRelCacheCtrl.InfoCache.delCacheList(aid, unionPriId, pdIdList);
            ProductCacheCtrl.InfoCache.delCacheList(aid, pdIdList);

            CacheCtrl.clearCacheVersion(aid);
            // 同步数据到es
            ESUtil.batchLogDocId(flow, aid, unionPriId, pdIdList, DocOplogDef.Operation.DELETE_ONE);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("delProductList ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delProductListRollback(FaiSession session, int flow, int aid, String xid, long branchId) throws IOException {
        SagaRollback sagaRollback = (tc) -> {
            // 回滚商品业务关系表数据
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, false);
            relProc.rollback4Saga(aid, branchId);

            // 回滚商品表数据
            ProductProc proc = new ProductProc(flow, aid, tc, xid, false);
            proc.rollback4Saga(aid, branchId, relProc);

            // 回滚绑定参数
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, false);
            bindPropProc.rollback4Saga(aid, branchId);

            // 回滚绑定分类
            if(useProductGroup()) {
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, false);
                bindGroupProc.rollback4Saga(aid, branchId);
            }

            // 回滚绑定标签
            if(useProductTag()) {
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, false);
                bindTagProc.rollback4Saga(aid, branchId);
            }
        };

        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    /**
     * 删除商品数据关联数据
     */
    @SuccessRt(value = Errno.OK)
    public int clearRelData(FaiSession session, int flow ,int aid, int unionPriId, boolean softDel) throws IOException {
        int rt;

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                relProc.clearData(aid, unionPriId, softDel);

                if(!softDel) {
                    // 删除参数关联
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    // 删除当前unionPriId的数据
                    ParamMatcher delMatcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
                    delMatcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    bindPropProc.delPdBindProp(aid, delMatcher);

                    if(useProductGroup()) {
                        // 删除分类关联
                        ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                        // 删除当前unionPriId的数据
                        bindGroupProc.delPdBindGroup(aid, unionPriId, null);
                    }

                    if(useProductTag()) {
                        // 删除标签关联
                        ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                        // 删除当前unionPriId的数据
                        bindTagProc.delPdBindTag(aid, unionPriId, null);
                    }
                }
                commit = true;
            }finally {
                if(!commit){
                    tc.rollback();
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(aid);
            // 同步修改给es
            ESUtil.commitPre(flow, aid);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("delProductList ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);

        return rt;
    }

    /**
     * 删除商品数据
     */
    @SuccessRt(value = Errno.OK)
    public int clearAcct(FaiSession session, int flow ,int aid, FaiList<Integer> unionPriIds) throws IOException {
        int rt;
        if(Utils.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, unionPriIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, unionPriIds);
            return rt;
        }

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 删除商品业务关系数据
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                relProc.clearAcct(aid, unionPriIds);

                // 删除商品基础数据
                ProductProc productProc = new ProductProc(flow, aid, tc);
                productProc.clearAcct(aid, unionPriIds);

                // 删除参数关联
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                bindPropProc.clearAcct(aid, unionPriIds);

                // 删除分类关联
                if(useProductGroup()) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                    bindGroupProc.clearAcct(aid, unionPriIds);
                }

                //删除标签关联
                if(useProductTag()) {
                    // 删除标签关联
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                    // 删除当前unionPriId的数据
                    bindTagProc.clearAcct(aid, unionPriIds);
                }

                commit = true;
            }finally {
                if(!commit){
                    tc.rollback();
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(aid);

            // 同步数据给es
            ESUtil.commitPre(flow, aid);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clear acct ok;flow=%d;aid=%d;unionPriIds=%s;", flow, aid, unionPriIds);

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setSingle(FaiSession session, int flow, int aid, String xid, int unionPriId, int sysType, int rlPdId, ParamUpdater recvUpdater) throws IOException {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlPdId is not valid;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            return rt;
        }
        if(recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvUpdater is empty;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            return rt;
        }
        ParamUpdater relUpdate = ProductRelProc.assignUpdate(flow, aid, recvUpdater);
        ParamUpdater pdUpdate = ProductProc.assignUpdate(flow, aid, recvUpdater);
        Param upData = recvUpdater.getData();
        FaiList<Integer> rlGroupIds = null;
        FaiList<Integer> rlTagIds = null;
        FaiList<Param> rlProps = null;
        if(!Str.isEmpty(upData)) {
            rlGroupIds = upData.getList(ProductRelEntity.Info.RL_GROUP_IDS);
            rlTagIds = upData.getList(ProductRelEntity.Info.RL_TAG_IDS);
            rlProps = upData.getList(ProductRelEntity.Info.RL_PROPS);
        }
        if(relUpdate == null && pdUpdate == null && rlGroupIds == null && rlTagIds == null && rlProps == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvUpdater is not valid;aid=%d;uid=%d;rlPdId=%d;updater=%s;", aid, unionPriId, rlPdId, recvUpdater.toJson());
            return rt;
        }
        LockUtil.lock(aid);
        try {
            Integer pdId;
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid);
                }

                // 修改业务关系表
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, true);
                pdId = relProc.getPdId(aid, unionPriId, sysType, rlPdId);
                if(pdId == null) {
                    throw new MgException("args error;get pdId is null;aid=%d;uid=%d;sysType=%d;rlPdId=%d;", aid, unionPriId, sysType, rlPdId);
                }

                if(relUpdate != null) {
                    // 如果修改置顶时间，则将sort置为最大
                    Param updateInfo = relUpdate.getData();
                    Calendar top = updateInfo.getCalendar(ProductRelEntity.Info.TOP);
                    Integer sort = updateInfo.getInt(ProductRelEntity.Info.SORT);
                    if(top != null && sort == null) {
                        int maxSort = relProc.getMaxSort(aid, unionPriId);
                        updateInfo.setInt(ProductRelEntity.Info.SORT, ++maxSort);
                    }
                    ProductRelCacheCtrl.InfoCache.setExpire(aid, unionPriId); // 设置过期时间，最大努力的避免脏数据
                    relProc.setSingle(aid, unionPriId, pdId, relUpdate);
                }

                // 修改商品表
                if(pdUpdate != null) {
                    ProductCacheCtrl.InfoCache.setExpire(aid); // 设置过期时间，最大努力的避免脏数据

                    ProductProc pdProc = new ProductProc(flow, aid, tc, xid, true);
                    pdProc.setSingle(aid, pdId, pdUpdate);
                }

                // 修改绑定的商品分类
                if(rlGroupIds != null) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, true);
                    bindGroupProc.updateBindGroupList(aid, unionPriId, sysType, rlPdId, pdId, rlGroupIds);
                }

                // 修改绑定的标签
                if(rlTagIds != null) {
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, true);
                    bindTagProc.updateBindTagList(aid, unionPriId, sysType, rlPdId, pdId, rlTagIds);
                }

                // 修改绑定的参数
                if(rlProps != null) {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, true);
                    bindPropProc.updatePdBindProp(aid, unionPriId, sysType, rlPdId, pdId, rlProps);
                }
                relProc.end(aid);
                commit = true;
                tc.commit();
            } finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 处理缓存
            if(relUpdate != null) {
                ProductRelCacheCtrl.InfoCache.updateCache(aid, unionPriId, pdId, relUpdate);
            }
            if(pdUpdate != null) {
                ProductCacheCtrl.InfoCache.updateCache(aid, pdId, pdUpdate);
            }
            ProductBindPropCache.delCache(aid, unionPriId, sysType, rlPdId);
            ProductBindGroupCache.delCache(aid, unionPriId, pdId);
            ProductBindTagCache.delCache(aid, unionPriId, pdId);
            ProductRelCacheCtrl.SortCache.del(aid, unionPriId); // sort缓存

            // 同步数据给es
            ESUtil.logDocId(flow, aid, pdId, unionPriId, DocOplogDef.Operation.UPDATE_ONE);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set single ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setSinglePdRollback(FaiSession session, int flow, int aid, String xid, long branchId) throws IOException {
        SagaRollback sagaRollback = (tc) -> {
            // 回滚商品业务关系表数据
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, false);
            relProc.rollback4Saga(aid, branchId);

            // 回滚商品表数据
            ProductProc proc = new ProductProc(flow, aid, tc, xid, false);
            proc.rollback4Saga(aid, branchId, relProc);

            // 回滚绑定参数
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, false);
            bindPropProc.rollback4Saga(aid, branchId);

            // 回滚绑定分类
            if(useProductGroup()) {
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, false);
                bindGroupProc.rollback4Saga(aid, branchId);
            }

            // 回滚绑定标签
            if(useProductTag()) {
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, false);
                bindTagProc.rollback4Saga(aid, branchId);
            }
        };

        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int setProducts(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, ParamUpdater recvUpdater) throws IOException {
        int rt;
        if(!MgProductCheck.RequestLimit.checkWriteSize(aid, rlPdIds)) {
            rt = Errno.SIZE_LIMIT;
            return rt;
        }
        if(recvUpdater == null || recvUpdater.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvUpdater is empty;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        ParamUpdater relUpdate = ProductRelProc.assignUpdate(flow, aid, recvUpdater);
        ParamUpdater pdUpdate = ProductProc.assignUpdate(flow, aid, recvUpdater);
        if(relUpdate == null && pdUpdate == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvUpdater is not valid;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            FaiList<Integer> pdIdList = new FaiList<>();
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                pdIdList = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
                if(Utils.isEmptyList(pdIdList)) {
                    rt = Errno.NOT_FOUND;
                    Log.logErr(rt, "getRelList isEmpty;flow=%d;aid=%d;uid=%d;sysType=%s;rlPdIds=%s;", flow, aid, unionPriId, sysType, rlPdIds);
                    return rt;
                }
                if(relUpdate != null) {
                    relProc.setPdRels(aid, unionPriId, pdIdList, relUpdate);
                }
                if(pdUpdate != null) {
                    ProductProc pdProc = new ProductProc(flow, aid, tc);
                    pdProc.setProducts(aid, pdIdList, pdUpdate);
                }
                commit = true;
                tc.commit();
            } finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 处理缓存
            if(relUpdate != null) {
                ProductRelCacheCtrl.InfoCache.delCacheList(aid, unionPriId, pdIdList);
            }
            if(pdUpdate != null) {
                ProductCacheCtrl.InfoCache.delCacheList(aid, pdIdList);
            }

            // 同步数据给es
            ESUtil.batchLogDocId(flow, aid, unionPriId, pdIdList, DocOplogDef.Operation.UPDATE_ONE);
        }finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set products ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        return rt;
    }

    /**
     * 根据商品id，获取商品数据
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getInfoByPdId(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt;
        if(pdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlPdId is not valid;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, pdId);
            return rt;
        }
        Param info = new Param();
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            Param pdInfo = pdProc.getProductInfo(aid, pdId);
            if(Str.isEmpty(pdInfo)) {
                return Errno.NOT_FOUND;
            }
            info.assign(pdInfo);
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            Param relInfo = relProc.getProductRel(aid, unionPriId, pdId);
            if(Str.isEmpty(relInfo)) {
                return Errno.NOT_FOUND;
            }
            info.assign(relInfo);

        } finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductRelDto.Key.INFO, ProductRelDto.getRelAndPdDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;pdId=%d;", flow, aid, unionPriId, pdId);

        return rt;
    }

    /**
     * 根据商品id集合，获取商品数据
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdListByPdIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> pdIds) throws IOException {
        int rt;
        if(!MgProductCheck.RequestLimit.checkReadSize(aid, pdIds)) {
            return Errno.SIZE_LIMIT;
        }
        FaiList<Param> list = new FaiList<>();
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            // 业务关系表
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            FaiList<Param> relList = relProc.getProductRelList(aid, unionPriId, pdIds);
            if(Utils.isEmptyList(relList)) {
                return Errno.NOT_FOUND;
            }

            // 基础表
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            FaiList<Param> basicList = pdProc.getProductList(aid, pdIds);
            if(Utils.isEmptyList(basicList)) {
                return Errno.NOT_FOUND;
            }

            // 绑定分类
            Map<Integer, List<Param>> bindGroupMap = new HashMap<>();
            if(useProductGroup()) {
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                FaiList<Param> bindGroups = bindGroupProc.getPdBindGroupList(aid, unionPriId, pdIds);
                if(!Utils.isEmptyList(bindGroups)) {
                    bindGroupMap = bindGroups.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductBindGroupEntity.Info.PD_ID)));
                }
            }

            // 数据整合
            Map<Integer, Param> relMap = Utils.getMap(relList, ProductRelEntity.Info.PD_ID);
            Map<Integer, Param> basicMap = Utils.getMap(relList, ProductEntity.Info.PD_ID);
            for(Map.Entry<Integer, Param> entry : relMap.entrySet()) {
                Integer pdId = entry.getKey();
                Param info = entry.getValue();
                Param basicInfo = basicMap.get(pdId);
                // 业务表和基础表都有status字段，业务方感知的应该是relInfo中的status
                basicInfo.remove(ProductEntity.Info.STATUS);
                // 整合基础表数据
                info.assign(basicInfo);
                // 整合绑定分类表数据
                List<Param> bindGroupsTemp = bindGroupMap.get(pdId);
                FaiList<Param> bindGroups = new FaiList<>(bindGroupsTemp);
                FaiList<Integer> rlGroupIds = Utils.getValList(bindGroups, ProductBindGroupEntity.Info.RL_GROUP_ID);
                info.setList(ProductRelEntity.Info.RL_GROUP_IDS, rlGroupIds);

                list.add(info);
            }
        } finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getRelAndPdDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;pdIds=%s;", flow, aid, unionPriId, pdIds);

        return rt;
    }

    /**
     * 根据业务商品id，获取商品业务关系数据
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRelInfoByRlId(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlPdId) throws IOException {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlPdId is not valid;aid=%d;uid=%d;sysType=%d;rlPdId=%d;", aid, unionPriId, sysType, rlPdId);
            return rt;
        }
        Param relInfo = new Param();
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            Integer pdId = relProc.getPdId(aid, unionPriId, sysType, rlPdId);
            if(pdId == null) {
                return Errno.NOT_FOUND;
            }
            relInfo = relProc.getProductRel(aid, unionPriId, pdId);
            if(Str.isEmpty(relInfo)) {
                return Errno.NOT_FOUND;
            }
        } finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        relInfo.toBuffer(sendBuf, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);

        return rt;
    }

    /**
     * 根据业务商品id集合，获取商品业务关系数据集合
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRelListByRlIds(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) throws IOException {
        int rt;
        if(!MgProductCheck.RequestLimit.checkReadSize(aid, rlPdIds)) {
            return Errno.SIZE_LIMIT;
        }

        FaiList<Param> list;
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            FaiList<Integer> pdIds = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
            if(Utils.isEmptyList(pdIds)) {
                return Errno.NOT_FOUND;
            }
            list = relProc.getProductRelList(aid, unionPriId, pdIds);
        } finally {
            tc.closeDao();
        }
        if(list == null || list.isEmpty()) {
            return Errno.NOT_FOUND;
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        return rt;
    }

    /**
     * 根据pdIds获取业务关联数据，仅获取有限的字段，aid+unionPriId+pdId+rlPdId
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getReducedRelsByPdIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> pdIds) throws IOException {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            throw new MgException(Errno.ARGS_ERROR, "args error, pdIds is empty;aid=%d;pdIds=%s;", aid, pdIds);
        }

        FaiList<Param> list;
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            list = relProc.getProductRelListWithDel(aid, unionPriId, pdIds);
        } finally {
            tc.closeDao();
        }
        if(list == null || list.isEmpty()) {
            return Errno.NOT_FOUND;
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;pdIds=%s;", flow, aid, unionPriId, pdIds);

        return rt;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     * 1. 添加商品数据
     * 2. 添加业务关系表数据
     * 3. 添加商品参数绑定关系
     * 4. 添加商品分类绑定关系
     * 5. 添加商品标签绑定关系
     */
    @SuccessRt(value = Errno.OK)
    public int addProductAndRel(FaiSession session, int flow, int aid, String xid, int tid, int unionPriId, Param info) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 商品参数绑定关系
        FaiList<Param> bindProps = null;
        if(info.containsKey(ProductRelEntity.Info.RL_PROPS)) {
            bindProps = (FaiList<Param>)info.remove(ProductRelEntity.Info.RL_PROPS);
        }

        // 商品分类绑定关系
        FaiList<Integer> rlGroupIds = null;
        if(info.containsKey(ProductRelEntity.Info.RL_GROUP_IDS)) {
            rlGroupIds = (FaiList<Integer>)info.remove(ProductRelEntity.Info.RL_GROUP_IDS);
        }

        // 商品标签绑定关系
        FaiList<Integer> rlTagIds = null;
        if(info.containsKey(ProductRelEntity.Info.RL_TAG_IDS)) {
            rlTagIds = (FaiList<Integer>)info.remove(ProductRelEntity.Info.RL_TAG_IDS);
        }

        Param pdData = new Param();
        Param relData = new Param();

        rt = assemblyInfo(flow, aid, tid, unionPriId, info, relData, pdData);
        if(rt != Errno.OK) {
            return rt;
        }

        Integer rlPdId = 0;
        Integer pdId = 0;
        int maxSort = 0;

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid);
                }

                // 新增商品数据
                ProductProc pdProc = new ProductProc(flow, aid, tc, xid, true);
                pdId = pdProc.addProduct(aid, pdData);

                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                // 新增业务关系
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, true);
                Integer sort = relData.getInt(ProductRelEntity.Info.SORT);
                if(sort == null) {
                    maxSort = relProc.getMaxSort(aid, unionPriId);
                    relData.setInt(ProductRelEntity.Info.SORT, ++maxSort);
                }

                rlPdId = relProc.addProductRel(aid, tid, unionPriId, relData);

                int sysType = relData.getInt(ProductRelEntity.Info.SYS_TYPE);

                // 新增商品参数绑定关系
                if(!Utils.isEmptyList(bindProps)) {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, true);
                    bindPropProc.addPdBindPropList(aid, unionPriId, sysType, rlPdId, pdId, bindProps);
                }

                // 新增商品分类绑定关系
                if(useProductGroup() && !Utils.isEmptyList(rlGroupIds)) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, true);
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, sysType, rlPdId, pdId, rlGroupIds);
                    Log.logStd("add bind groupIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlGroupIds);
                }

                // 新增商品标签绑定关系
                if(useProductTag() && !Utils.isEmptyList(rlTagIds)) {
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, true);
                    bindTagProc.addPdBindTagList(aid, unionPriId, sysType, rlPdId, pdId, rlTagIds);
                    Log.logStd("add bind rlTagIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlTagIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlTagIds);
                }
                relProc.end(aid);

                commit = true;
            } finally {
                if(commit) {
                    tc.commit();
                }else {
                    tc.rollback();
                    ProductDaoCtrl.clearIdBuilderCache(aid);
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }
            // 更新缓存
            ProductCacheCtrl.InfoCache.addCache(aid, pdData);
            ProductRelCacheCtrl.InfoCache.addCache(aid, unionPriId, relData);
            ProductCacheCtrl.DataStatusCache.update(aid, 1); // 更新数据状态缓存
            ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, 1); // 更新数据状态缓存
            ProductRelCacheCtrl.SortCache.set(aid, unionPriId, maxSort); // sort缓存

            // 同步数据给es
            ESUtil.logDocId(flow, aid, pdId, unionPriId, DocOplogDef.Operation.UPDATE_ONE);
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        Log.logStd("addProductAndRel ok;flow=%d;aid=%d;uid=%d;tid=%s;", flow, aid, unionPriId, tid);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
        sendBuf.putInt(ProductRelDto.Key.PD_ID, pdId);
        session.write(sendBuf);
        return rt;
    }

    /**
     * 新增商品业务关联-补偿
     */
    @SuccessRt(value = Errno.OK)
    public int addProductAndRelRollback(FaiSession session, int flow, int aid, String xid, long branchId) throws IOException {
        SagaRollback sagaRollback = getAddRollback(flow, aid, xid, branchId, false);

        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    /**
     * 批量添加商品，并添加与当前unionPriId的关联
     */
    @SuccessRt(value = Errno.OK)
    public int batchAddProductAndRel(FaiSession session, int flow, int aid, int tid, int unionPriId, FaiList<Param> addList) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 因为新增数据没有id标识
        // 所有数据按顺序存放，方便后面整合数据
        // 商品参数绑定关系
        FaiList<FaiList<Param>> bindProps = new FaiList<>();
        FaiList<FaiList<Integer>> rlGroupIds = new FaiList<>();
        FaiList<FaiList<Integer>> rlTagIds = new FaiList<>();
        FaiList<Param> relDataList = new FaiList<>();
        FaiList<Param> pdDataList = new FaiList<>();
        for(int i = 0;i < addList.size(); i++) {
            Param pdData = new Param();
            Param relData = new Param();
            Param info = addList.get(i);
            rt = assemblyInfo(flow, aid, tid, unionPriId, info, relData, pdData);
            if(rt != Errno.OK) {
                return rt;
            }
            relDataList.add(relData);
            pdDataList.add(pdData);
            rlGroupIds.add(info.getListNullIsEmpty(ProductRelEntity.Info.RL_GROUP_IDS));
            rlTagIds.add(info.getListNullIsEmpty(ProductRelEntity.Info.RL_TAG_IDS));
            bindProps.add(info.getListNullIsEmpty(ProductRelEntity.Info.RL_PROPS));
        }

        int maxSort = 0;

        FaiList<Param> bindRlProps = new FaiList<>();
        FaiList<Param> bindRlGroups = new FaiList<>();
        FaiList<Param> bindRlTags = new FaiList<>();
        LockUtil.lock(aid);
        try {
            FaiList<Integer> pdIdList;
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // 新增商品数据
                ProductProc pdProc = new ProductProc(flow, aid, tc);
                pdIdList = pdProc.batchAddProduct(aid, pdDataList);

                // 新增业务关系
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                maxSort = relProc.getMaxSort(aid, unionPriId);
                for(int i = 0;i < relDataList.size(); i++) {
                    Param relData = relDataList.get(i);
                    relData.setInt(ProductRelEntity.Info.PD_ID, pdIdList.get(i));
                    Integer sort = relData.getInt(ProductRelEntity.Info.SORT);
                    if(sort == null) {
                        relData.setInt(ProductRelEntity.Info.SORT, ++maxSort);
                    }
                }
                relProc.batchAddProductRel(aid, tid, unionPriId, relDataList);

                // 新增绑定关系
                for(int i = 0;i < relDataList.size(); i++) {
                    Param relData = relDataList.get(i);
                    int pdId = relData.getInt(ProductRelEntity.Info.PD_ID);
                    int rlPdId = relData.getInt(ProductRelEntity.Info.RL_PD_ID);
                    int sysType = relData.getInt(ProductRelEntity.Info.SYS_TYPE);
                    FaiList<Integer> curGroupIds = rlGroupIds.get(i);
                    if(!curGroupIds.isEmpty()) {
                        for(Integer rlGroupId : curGroupIds) {
                            Param bindGroup = new Param();
                            bindGroup.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.SYS_TYPE, sysType);
                            bindRlGroups.add(bindGroup);
                        }
                    }

                    FaiList<Integer> curTagIds = rlTagIds.get(i);
                    if(!curTagIds.isEmpty()) {
                        for(Integer rlTagId : curTagIds) {
                            Param bindTag = new Param();
                            bindTag.setInt(ProductBindTagEntity.Info.RL_TAG_ID, rlTagId);
                            bindTag.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
                            bindTag.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
                            bindTag.setInt(ProductBindTagEntity.Info.SYS_TYPE, sysType);
                            bindRlTags.add(bindTag);
                        }
                    }

                    FaiList<Param> curProps = bindProps.get(i);
                    if(!curProps.isEmpty()) {
                        for(Param prop : curProps) {
                            prop.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
                            prop.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
                            prop.setInt(ProductBindTagEntity.Info.SYS_TYPE, sysType);
                            bindRlProps.add(prop);
                        }
                    }
                }

                // 新增分类绑定
                if(!bindRlGroups.isEmpty()) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                    bindGroupProc.batchBindGroupList(aid, unionPriId, bindRlGroups);
                }

                // 新增标签绑定
                if(!bindRlTags.isEmpty()) {
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                    bindTagProc.batchBindTagList(aid, unionPriId, bindRlTags);
                }

                // 新增参数绑定
                if(!bindRlProps.isEmpty()) {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    bindPropProc.batchBindPropList(aid, unionPriId, bindRlProps);
                }

                commit = true;
            } finally {
                if(!commit) {
                    tc.rollback();
                    ProductDaoCtrl.clearIdBuilderCache(aid);
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }

            // 更新缓存
            if(!Utils.isEmptyList(pdDataList)) {
                ProductCacheCtrl.InfoCache.addCacheList(aid, pdDataList);
                ProductCacheCtrl.DataStatusCache.update(aid, pdDataList.size()); // 更新数据状态缓存
            }
            if(!Utils.isEmptyList(relDataList)) {
                ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, relDataList);
                ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, relDataList.size()); // 更新数据状态缓存
                ProductRelCacheCtrl.SortCache.set(aid, unionPriId, maxSort); // 更新sort缓存
            }
            if(!bindRlProps.isEmpty()) {
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, bindRlProps.size());
            }
            if(!bindRlGroups.isEmpty()) {
                ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, bindRlGroups.size());
            }
            if(!bindRlTags.isEmpty()) {
                ProductBindTagCache.DataStatusCache.update(aid, unionPriId, bindRlTags.size());
            }


            // 同步数据给es
            ESUtil.batchLogDocId(flow, aid, unionPriId, pdIdList, DocOplogDef.Operation.UPDATE_ONE);
        } finally {
            LockUtil.unlock(aid);
        }
        FaiList<Param> idInfoList = new FaiList<Param>();
        for(int i = 0;i < relDataList.size(); i++) {
            Param relData = relDataList.get(i);
            Param idInfo = new Param();
            idInfo.assign(relData, ProductRelEntity.Info.PD_ID);
            idInfo.assign(relData, ProductRelEntity.Info.RL_PD_ID);
            idInfoList.add(idInfo);
        }
        rt = Errno.OK;
        Log.logStd("batchAddProductAndRel ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        FaiBuffer sendBuf = new FaiBuffer(true);
        idInfoList.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getInfoDto());
        session.write(sendBuf);
        return rt;
    }

    private int assemblyInfo(int flow, int aid, int tid, int unionPriId, Param info, Param relData, Param pdData) {
        int rt = Errno.OK;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error info is empty;flow=%d;aid=%d;uid=%d;info=%s", flow, aid, unionPriId, info);
            return rt;
        }
        // 是否需要校验数据，初步接入中台，一些非必要数据可能存在需要添加空数据场景
        boolean infoCheck = info.getBoolean(ProductRelEntity.Info.INFO_CHECK, true);

        Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
        if(addedSid == null && !infoCheck) {
            addedSid = 0;
        }
        if(infoCheck && addedSid == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, addedSid is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }

        String name = info.getString(ProductEntity.Info.NAME);
        if(name == null && !infoCheck) {
            name = "";
        }
        if(infoCheck && !MgProductCheck.checkProductName(name)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, name is unvalid;flow=%d;aid=%d;uid=%d;name=%s;", flow, aid, unionPriId, name);
            return rt;
        }

        int rlLibId = info.getInt(ProductRelEntity.Info.RL_LIB_ID, 1);
        if(rlLibId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlLibId is empty;flow=%d;aid=%d;uid=%d;rlLibId=%s", flow, aid, unionPriId, rlLibId);
            return rt;
        }

        int sourceTid = info.getInt(ProductRelEntity.Info.SOURCE_TID, tid);
        if(!FaiValObj.TermId.isValidTid(sourceTid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, sourceTid is unvalid;flow=%d;aid=%d;uid=%d;sourceTid=%d;", flow, aid, unionPriId, sourceTid);
            return rt;
        }
        int sysType = info.getInt(ProductRelEntity.Info.SYS_TYPE, ProductRelValObj.SysType.DEFAULT);

        int sourceUnionPriId = info.getInt(ProductEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        Calendar now = Calendar.getInstance();
        Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
        Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
        Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        relData.setInt(ProductRelEntity.Info.AID, aid);
        relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
        relData.setInt(ProductRelEntity.Info.SYS_TYPE, sysType);
        relData.setInt(ProductRelEntity.Info.RL_LIB_ID, rlLibId);
        relData.setInt(ProductRelEntity.Info.SOURCE_TID, sourceTid);
        relData.setCalendar(ProductRelEntity.Info.ADD_TIME, addedTime);
        relData.setInt(ProductRelEntity.Info.ADD_SID, addedSid);
        relData.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, lastUpdateTime);
        relData.setCalendar(ProductRelEntity.Info.CREATE_TIME, sysCreateTime);
        relData.setCalendar(ProductRelEntity.Info.UPDATE_TIME, sysUpdateTime);

        relData.assign(info, ProductRelEntity.Info.RL_PD_ID);
        relData.assign(info, ProductRelEntity.Info.LAST_SID);
        relData.assign(info, ProductRelEntity.Info.STATUS);
        relData.assign(info, ProductRelEntity.Info.UP_SALE_TIME);
        relData.assign(info, ProductRelEntity.Info.FLAG);
        relData.assign(info, ProductRelEntity.Info.SORT);
        relData.assign(info, ProductRelEntity.Info.PD_TYPE);
        relData.assign(info, ProductRelEntity.Info.TOP);

        pdData.setInt(ProductEntity.Info.AID, aid);
        pdData.setInt(ProductEntity.Info.SOURCE_TID, sourceTid);
        pdData.setInt(ProductEntity.Info.SOURCE_UNIONPRIID, sourceUnionPriId);
        pdData.setString(ProductEntity.Info.NAME, name);
        pdData.setCalendar(ProductEntity.Info.CREATE_TIME, sysCreateTime);
        pdData.setCalendar(ProductEntity.Info.UPDATE_TIME, sysUpdateTime);

        pdData.assign(info, ProductEntity.Info.PD_TYPE);
        pdData.assign(info, ProductEntity.Info.IMG_LIST);
        pdData.assign(info, ProductEntity.Info.VIDEO_LIST);
        pdData.assign(info, ProductEntity.Info.UNIT);
        pdData.assign(info, ProductEntity.Info.FLAG);
        pdData.assign(info, ProductEntity.Info.FLAG1);
        pdData.assign(info, ProductEntity.Info.KEEP_PROP1);
        pdData.assign(info, ProductEntity.Info.KEEP_PROP2);
        pdData.assign(info, ProductEntity.Info.KEEP_PROP3);
        pdData.assign(info, ProductEntity.Info.KEEP_INT_PROP1);
        pdData.assign(info, ProductEntity.Info.KEEP_INT_PROP2);
        pdData.assign(info, ProductEntity.Info.STATUS);

        return rt;
    }

    /**
     * 新增商品业务关联
     */
    @SuccessRt(value = Errno.OK)
    public int bindProductRel(FaiSession session, int flow, int aid, String xid, int tid, int unionPriId, Param bindRlPdInfo, Param info) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        if(Str.isEmpty(bindRlPdInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindRlPdInfo is empty;flow=%d;aid=%d;uid=%d;bindRlPdInfo=%s", flow, aid, unionPriId, bindRlPdInfo);
            return rt;
        }
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;uid=%d;info=%s", flow, aid, unionPriId, info);
            return rt;
        }
        // 是否需要校验数据，初步接入中台，一些非必要数据可能存在需要添加空数据场景
        boolean infoCheck = info.getBoolean(ProductRelEntity.Info.INFO_CHECK, true);

        // 校验被关联的商品业务数据
        Integer bindRlPdId = bindRlPdInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
        if(bindRlPdId == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindRlPdId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        Integer bindUniPriId = bindRlPdInfo.getInt(ProductRelEntity.Info.UNION_PRI_ID);
        if(bindUniPriId == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindUniPriId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 校验新增的商品业务数据
        Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
        if(addedSid == null && !infoCheck) {
            addedSid = 0;
        }
        if(infoCheck && addedSid == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, addedSid is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }

        int rlLibId = info.getInt(ProductRelEntity.Info.RL_LIB_ID, 1);
        if(rlLibId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlLibId is empty;flow=%d;aid=%d;uid=%d;rlLibId=%s", flow, aid, unionPriId, rlLibId);
            return rt;
        }

        int sourceTid = info.getInt(ProductRelEntity.Info.SOURCE_TID, tid);
        if(!FaiValObj.TermId.isValidTid(sourceTid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, sourceTid is unvalid;flow=%d;aid=%d;uid=%d;sourceTid=%d;", flow, aid, unionPriId, sourceTid);
            return rt;
        }

        // 系统类型，默认为0
        int sysType = info.getInt(ProductRelEntity.Info.SYS_TYPE, ProductRelValObj.SysType.DEFAULT);

        Calendar now = Calendar.getInstance();
        Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
        Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
        Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        Param relData = new Param();
        relData.setInt(ProductRelEntity.Info.AID, aid);
        relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
        relData.setInt(ProductRelEntity.Info.SYS_TYPE, sysType);
        relData.setInt(ProductRelEntity.Info.RL_LIB_ID, rlLibId);
        relData.setInt(ProductRelEntity.Info.SOURCE_TID, sourceTid);
        relData.setCalendar(ProductRelEntity.Info.ADD_TIME, addedTime);
        relData.setInt(ProductRelEntity.Info.ADD_SID, addedSid);
        relData.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, lastUpdateTime);
        relData.setCalendar(ProductRelEntity.Info.CREATE_TIME, sysCreateTime);
        relData.setCalendar(ProductRelEntity.Info.UPDATE_TIME, sysUpdateTime);

        relData.assign(info, ProductRelEntity.Info.RL_PD_ID);
        relData.assign(info, ProductRelEntity.Info.LAST_SID);
        relData.assign(info, ProductRelEntity.Info.STATUS);
        relData.assign(info, ProductRelEntity.Info.UP_SALE_TIME);
        relData.assign(info, ProductRelEntity.Info.FLAG);
        relData.assign(info, ProductRelEntity.Info.SORT);
        relData.assign(info, ProductRelEntity.Info.TOP);

        Integer rlPdId;
        Integer pdId;
        int maxSort = 0;

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid);
                }

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, true);
                pdId = relProc.getPdId(aid, bindUniPriId, sysType, bindRlPdId);
                if(pdId == null) {
                    Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    rt = Errno.ERROR;
                    return rt;
                }

                if(!relData.containsKey(ProductRelEntity.Info.SORT)) {
                    maxSort = relProc.getMaxSort(aid, unionPriId);
                    relData.setInt(ProductRelEntity.Info.SORT, maxSort);
                }

                Param bindInfo = relProc.getProductRel(aid, bindUniPriId, pdId);
                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                relData.assign(bindInfo, ProductRelEntity.Info.PD_TYPE);

                // 新增商品业务关系
                rlPdId = relProc.addProductRel(aid, tid, unionPriId, relData);

                // 新增商品参数绑定关系
                FaiList<Param> bindProps = info.getList(ProductRelEntity.Info.RL_PROPS);
                if(!Utils.isEmptyList(bindProps)) {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, true);
                    bindPropProc.addPdBindPropList(aid, unionPriId, sysType, rlPdId, pdId, bindProps);
                }

                // 新增商品分类绑定关系
                FaiList<Integer> rlGroupIds = info.getList(ProductRelEntity.Info.RL_GROUP_IDS);
                if(useProductGroup() && !Utils.isEmptyList(rlGroupIds)) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, true);
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, sysType, rlPdId, pdId, rlGroupIds);
                    Log.logStd("add bind groupIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlGroupIds);
                }

                // 新增商品标签绑定关系
                FaiList<Integer> rlTagIds = info.getList(ProductRelEntity.Info.RL_TAG_IDS);
                if(useProductTag() && !Utils.isEmptyList(rlTagIds)) {
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, true);
                    bindTagProc.addPdBindTagList(aid, unionPriId, sysType, rlPdId, pdId, rlTagIds);
                    Log.logStd("add bind rlTagIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlTagIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlTagIds);
                }
                relProc.end(aid);

                commit = true;
            } finally {
                if(!commit) {
                    tc.rollback();
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }else {
                    tc.commit();
                    // 更新缓存
                    ProductRelCacheCtrl.InfoCache.addCache(aid, unionPriId, relData);
                    ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, 1); //更新数据状态缓存
                }
                tc.closeDao();
            }
            // 同步数据给es
            ESUtil.logDocId(flow, aid, pdId, unionPriId, DocOplogDef.Operation.UPDATE_ONE);
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        Log.logStd("bindProductRel ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;info=%s;", flow, aid, unionPriId, rlPdId, pdId, info);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
        sendBuf.putInt(ProductRelDto.Key.PD_ID, pdId);
        session.write(sendBuf);
        return rt;
    }

    /**
     * 新增商品业务关联-补偿
     */
    @SuccessRt(value = Errno.OK)
    public int bindProductRelRollback(FaiSession session, int flow, int aid, String xid, long branchId) throws IOException {
        SagaRollback sagaRollback = getAddRollback(flow, aid, xid, branchId, true);

        int branchStatus = doRollback(flow, aid, xid, branchId, sagaRollback);
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, branchStatus);
        session.write(sendBuf);
        return Errno.OK;
    }

    /**
     * 批量新增商品业务关联
     */
    @SuccessRt(value = Errno.OK)
    public int batchBindProductRel(FaiSession session, int flow, int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        if(Str.isEmpty(bindRlPdInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindRlPdInfo is empty;flow=%d;aid=%d;tid=%d;infoList=%s", flow, aid, tid, infoList);
            return rt;
        }
        if(infoList == null || infoList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, infoList is empty;flow=%d;aid=%d;tid=%d;infoList=%s", flow, aid, tid, infoList);
            return rt;
        }

        Integer bindRlPdId = bindRlPdInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
        if(bindRlPdId == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindRlPdId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        Integer bindUniPriId = bindRlPdInfo.getInt(ProductRelEntity.Info.UNION_PRI_ID);
        if(bindUniPriId == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, bindUniPriId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        // 系统类型，默认为0
        int sysType = bindRlPdInfo.getInt(ProductRelEntity.Info.SYS_TYPE, ProductRelValObj.SysType.DEFAULT);

        HashMap<Integer, FaiList<Param>> listOfUid = new HashMap<>();
        for(Param info : infoList) {
            // 是否需要校验数据，初步接入中台，一些非必要数据可能存在需要添加空数据场景
            boolean infoCheck = info.getBoolean(ProductRelEntity.Info.INFO_CHECK, true);

            Integer unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            if(unionPriId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, unionPriId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
            if(addedSid == null && !infoCheck) {
                addedSid = 0;
            }
            if(infoCheck && addedSid == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, addedSid is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return rt;
            }

            int rlLibId = info.getInt(ProductRelEntity.Info.RL_LIB_ID, 1);
            if(rlLibId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, rlLibId is empty;flow=%d;aid=%d;uid=%d;rlLibId=%s", flow, aid, unionPriId, rlLibId);
                return rt;
            }

            int sourceTid = info.getInt(ProductRelEntity.Info.SOURCE_TID, tid);
            if(!FaiValObj.TermId.isValidTid(sourceTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, sourceTid is unvalid;flow=%d;aid=%d;uid=%d;sourceTid=%d;", flow, aid, unionPriId, sourceTid);
                return rt;
            }
            Calendar now = Calendar.getInstance();
            Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
            Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
            Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
            Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

            Param relData = new Param();
            relData.setInt(ProductRelEntity.Info.AID, aid);
            relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            relData.setInt(ProductRelEntity.Info.SYS_TYPE, sysType);
            relData.setInt(ProductRelEntity.Info.RL_LIB_ID, rlLibId);
            relData.setInt(ProductRelEntity.Info.SOURCE_TID, sourceTid);
            relData.setCalendar(ProductRelEntity.Info.ADD_TIME, addedTime);
            relData.setInt(ProductRelEntity.Info.ADD_SID, addedSid);
            relData.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, lastUpdateTime);
            relData.setCalendar(ProductRelEntity.Info.CREATE_TIME, sysCreateTime);
            relData.setCalendar(ProductRelEntity.Info.UPDATE_TIME, sysUpdateTime);

            relData.assign(info, ProductRelEntity.Info.RL_PD_ID);
            relData.assign(info, ProductRelEntity.Info.LAST_SID);
            relData.assign(info, ProductRelEntity.Info.STATUS);
            relData.assign(info, ProductRelEntity.Info.UP_SALE_TIME);
            relData.assign(info, ProductRelEntity.Info.FLAG);
            relData.assign(info, ProductRelEntity.Info.SORT);
            relData.assign(info, ProductRelEntity.Info.TOP);

            FaiList<Param> curUidList = listOfUid.get(unionPriId);
            if(curUidList == null) {
                curUidList = new FaiList<>();
                listOfUid.put(unionPriId, curUidList);
            }
            curUidList.add(relData);
        }

        Set<Integer> unionPriIds = listOfUid.keySet();

        FaiList<Integer> rlPdIds = new FaiList<Integer>();

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 先校验商品数据是否存在
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                Integer pdId = relProc.getPdId(aid, bindUniPriId, sysType, bindRlPdId);
                if(pdId == null) {
                    Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    rt = Errno.ERROR;
                    return rt;
                }
                Param bindRel = relProc.getProductRel(aid, bindUniPriId, pdId);

                // 新增商品业务关系
                for(Integer unionPriId : unionPriIds) {
                    FaiList<Param> curList = listOfUid.get(unionPriId);
                    if(Utils.isEmptyList(curList)) {
                        continue;
                    }
                    int maxSort = relProc.getMaxSort(aid, unionPriId);
                    for(Param info : curList) {
                        info.assign(bindRel, ProductRelEntity.Info.PD_ID);
                        info.assign(bindRel, ProductRelEntity.Info.PD_TYPE);
                        Integer sort = info.getInt(ProductRelEntity.Info.SORT);
                        if(sort == null) {
                            info.setInt(ProductRelEntity.Info.SORT, ++maxSort);
                        }
                    }
                    FaiList<Integer> tmpRlIds = relProc.batchAddProductRel(aid, tid, unionPriId, curList);
                    rlPdIds.addAll(tmpRlIds);

                    // 记录要同步给es的数据
                    ESUtil.preLog(aid, pdId, unionPriId, DocOplogDef.Operation.UPDATE_ONE);
                }

                commit = true;
            } finally {
                if(!commit) {
                    tc.rollback();
                    for(Integer unionPriId : unionPriIds) {
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }

            // 清缓存
            CacheCtrl.clearCacheVersion(aid);

            // 同步数据给es
            ESUtil.commitPre(flow, aid);
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductRelDto.Key.RL_PD_IDS);
        session.write(sendBuf);
        Log.logStd("batchBindProductRel ok;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        return rt;
    }

    /**
     * 批量新增商品业务关联，同时绑定多个产品数据，给悦客接入进销存中心临时使用的
     * 接入完成后，废除，该接口禁止对外开放
     */
    @SuccessRt(value = Errno.OK)
    public int batchBindProductsRel(FaiSession session, int flow, int aid, int tid, FaiList<Param> recvList) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        if(recvList == null || recvList.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvList is empty;flow=%d;aid=%d;tid=%d;recvList=%s", flow, aid, tid, recvList);
            return rt;
        }
        FaiList<Integer> bindPdIds = new FaiList<Integer>();
        for(Param recvInfo : recvList) {
            Integer pdId = recvInfo.getInt(ProductRelEntity.Info.PD_ID);
            if(pdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, pdId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            bindPdIds.add(pdId);
        }
        // 根据要绑定的pdId集合，获取对应已绑定的unionPriId
        HashMap<Integer, FaiList<Integer>> pdRels = getBoundUniPriIds(flow, aid, bindPdIds);

        HashMap<Integer, FaiList<Param>> listOfUid = new HashMap<>();
        for(Param recvInfo : recvList) {
            Integer pdId = recvInfo.getInt(ProductRelEntity.Info.PD_ID);
            if(pdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, pdId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            FaiList<Integer> boundUniPriIds = pdRels.get(pdId);
            if(boundUniPriIds == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "bind pdId is not exist;flow=%d;aid=%d;tid=%d;pdId=%d;", flow, aid, tid, pdId);
                return rt;
            }
            // 要绑定pdId的数据集合
            FaiList<Param> infoList = recvInfo.getList(ProductRelEntity.Info.BIND_LIST);
            if(infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "bind list is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            for(Param info : infoList) {
                Integer unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                if(unionPriId == null) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, unionPriId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    return rt;
                }
                // 该unionPriId已经绑定该pdId，则跳过
                if(boundUniPriIds.contains(unionPriId)) {
                    continue;
                }
                // 是否需要校验数据，初步接入中台，一些非必要数据可能存在需要添加空数据场景
                boolean infoCheck = info.getBoolean(ProductRelEntity.Info.INFO_CHECK, true);
                Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
                if(addedSid == null && !infoCheck) {
                    addedSid = 0;
                }
                if(infoCheck && addedSid == null) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, addedSid is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                    return rt;
                }

                int rlLibId = info.getInt(ProductRelEntity.Info.RL_LIB_ID, 1);
                if(rlLibId <= 0) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, rlLibId is empty;flow=%d;aid=%d;uid=%d;rlLibId=%s", flow, aid, unionPriId, rlLibId);
                    return rt;
                }

                int sourceTid = info.getInt(ProductRelEntity.Info.SOURCE_TID, tid);
                if(!FaiValObj.TermId.isValidTid(sourceTid)) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, sourceTid is unvalid;flow=%d;aid=%d;uid=%d;sourceTid=%d;", flow, aid, unionPriId, sourceTid);
                    return rt;
                }
                int sysType = info.getInt(ProductRelEntity.Info.SYS_TYPE, ProductRelValObj.SysType.DEFAULT);

                Calendar now = Calendar.getInstance();
                Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
                Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
                Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
                Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

                Param relData = new Param();
                relData.setInt(ProductRelEntity.Info.AID, aid);
                relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
                relData.setInt(ProductRelEntity.Info.SYS_TYPE, sysType);
                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                relData.setInt(ProductRelEntity.Info.RL_LIB_ID, rlLibId);
                relData.setInt(ProductRelEntity.Info.SOURCE_TID, sourceTid);
                relData.setCalendar(ProductRelEntity.Info.ADD_TIME, addedTime);
                relData.setInt(ProductRelEntity.Info.ADD_SID, addedSid);
                relData.setCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, lastUpdateTime);
                relData.setCalendar(ProductRelEntity.Info.CREATE_TIME, sysCreateTime);
                relData.setCalendar(ProductRelEntity.Info.UPDATE_TIME, sysUpdateTime);

                relData.assign(info, ProductRelEntity.Info.RL_PD_ID);
                relData.assign(info, ProductRelEntity.Info.LAST_SID);
                relData.assign(info, ProductRelEntity.Info.STATUS);
                relData.assign(info, ProductRelEntity.Info.UP_SALE_TIME);
                relData.assign(info, ProductRelEntity.Info.FLAG);
                relData.assign(info, ProductRelEntity.Info.SORT);
                relData.assign(info, ProductRelEntity.Info.PD_TYPE);
                relData.assign(info, ProductRelEntity.Info.TOP);

                FaiList<Param> curUidList = listOfUid.get(unionPriId);
                if(curUidList == null) {
                    curUidList = new FaiList<>();
                    listOfUid.put(unionPriId, curUidList);
                }
                curUidList.add(relData);
            }
        }
        if(listOfUid.isEmpty()) {
            rt = Errno.OK;
            Log.logDbg("need bind rel data is empty;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            return rt;
        }
        Set<Integer> unionPriIds = listOfUid.keySet();
        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                // 新增商品业务关系
                for(Integer unionPriId : unionPriIds) {
                    FaiList<Param> list = listOfUid.get(unionPriId);
                    if(Utils.isEmptyList(list)) {
                        continue;
                    }

                    int maxSort = relProc.getMaxSort(aid, unionPriId);
                    for(Param info : list) {
                        Integer sort = info.getInt(ProductRelEntity.Info.SORT);
                        if(sort == null) {
                            info.setInt(ProductRelEntity.Info.SORT, ++maxSort);
                        }
                    }
                    relProc.batchAddProductRel(aid, tid, unionPriId, list);
                    // 记录要同步给es 的数据
                    ESUtil.batchPreLog(aid, list, DocOplogDef.Operation.UPDATE_ONE);
                }

                commit = true;
            } finally {
                if(!commit) {
                    tc.rollback();
                    for(Integer unionPriId : unionPriIds) {
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }
                }else {
                    tc.commit();
                }
                tc.closeDao();
            }
            // 更新缓存
            CacheCtrl.clearCacheVersion(aid);

            // 同步数据给es
            ESUtil.commitPre(flow, aid);
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("batch add ok;flow=%d;aid=%d;uids=%s;", flow, aid, unionPriIds);
        return rt;
    }

    /**
     * 获取产品数据data status
     */
    @SuccessRt(value = Errno.OK)
    public int getProductDataStatus(FaiSession session, int flow, int aid) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductProc productProc = new ProductProc(flow, aid, tc);
            info = productProc.getDataStatus(aid);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getProductDataStatus ok;flow=%d;aid=%d;", flow, aid);
        return rt;
    }

    /**
     * 从db搜索aid下的商品数据
     * 只获取部分字段：ProductEntity.MANAGE_FIELDS
     * 目前提供给搜索服务使用
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int searchProductFromDb(FaiSession session, int flow, int aid, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            list = pdProc.searchFromDb(aid, searchArg, ProductEntity.MANAGE_FIELDS);

        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductDto.Key.INFO_LIST, ProductDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("search from db ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());

        return rt;
    }

    /**
     * 从db获取aid下所有的商品数据
     * 只获取部分字段：ProductEntity.MANAGE_FIELDS
     * 目前提供给搜索服务使用
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllProduct(FaiSession session, int flow, int aid) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            SearchArg searchArg = new SearchArg(); // 获取所有数据
            list = pdProc.searchFromDb(aid, searchArg, ProductEntity.MANAGE_FIELDS);

        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductDto.Key.INFO_LIST, ProductDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get all from db ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());

        return rt;
    }

    /**
     * 获取商品关联数据data status
     */
    @SuccessRt(value = Errno.OK)
    public int getProductRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            info = relProc.getDataStatus(aid, unionPriId);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getProductRelDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllProductRel(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            // 查aid + unionPriId 下所有数据
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = relProc.searchFromDbWithDel(aid, searchArg, ProductRelEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int searchProductRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = relProc.searchFromDbWithDel(aid, searchArg, ProductRelEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
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
            ProductCacheCtrl.clearAllCache(aid);
            ProductDaoCtrl.clearIdBuilderCache(aid);
        }finally {
            LockUtil.unlock(aid);
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logStd("clear cache ok;flow=%d;aid=%d;", flow, aid);

        return rt;
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
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, true);
            ProductProc proc = new ProductProc(flow, aid, tc, true);
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, true);
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, true);
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, true);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 可能之前备份没有成功，先操作删除之前的备份
                deleteBackupData(relProc, proc, bindGroupProc, bindTagProc, bindPropProc, aid, backupId, backupFlag);

                // 备份业务关系表数据
                relProc.backupData(aid, unionPirIds, backupId, backupFlag);

                // 备份分类表数据
                proc.backupData(aid, unionPirIds, backupId, backupFlag);

                // 备份绑定分类数据
                bindGroupProc.backupData(aid, unionPirIds, backupId, backupFlag);

                // 备份绑定标签数据
                bindTagProc.backupData(aid, unionPirIds, backupId, backupFlag);

                // 备份绑定参数数据
                bindPropProc.backupData(aid, unionPirIds, backupId, backupFlag);
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
    public int restoreBackupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPirIds, int restoreId, Param backupInfo) throws IOException {
        int rt;
        if(aid <= 0 || Str.isEmpty(backupInfo)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
            return rt;
        }

        int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
        int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
        if(backupId == 0 || backupFlag == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
            return rt;
        }

        LockUtil.BackupLock.lock(aid);
        try {
            String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
            if(backupStatus != null) {
                if(backupStatusCtrl.isDoing(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    throw new MgException(rt, "restore is doing;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
                }else if(backupStatusCtrl.isFinish(backupStatus)) {
                    rt = Errno.OK;
                    Log.logStd(rt, "restore is already ok;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
                    FaiBuffer sendBuf = new FaiBuffer(true);
                    session.write(sendBuf);
                    return Errno.OK;
                }else if(backupStatusCtrl.isFail(backupStatus)) {
                    rt = Errno.ALREADY_EXISTED;
                    Log.logStd(rt, "restore is fail, going retry;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
                }
            }
            // 设置备份执行中
            backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.RESTORE, aid, restoreId);

            TransactionCtrl tc = new TransactionCtrl();
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, true);
            ProductProc proc = new ProductProc(flow, aid, tc, true);
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, true);
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, true);
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, true);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 还原关系表数据
                relProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

                // 还原商品表数据
                proc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

                // 还原绑定分类数据
                bindGroupProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

                // 还原绑定标签数据
                bindTagProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

                // 还原绑定参数数据
                bindPropProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

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
        }finally {
            LockUtil.BackupLock.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("restore backupData ok;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
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
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, true);
            ProductProc proc = new ProductProc(flow, aid, tc, true);
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, true);
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, true);
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, true);
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 删除备份
                deleteBackupData(relProc, proc, bindGroupProc, bindTagProc, bindPropProc, aid, backupId, backupFlag);

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

    private void deleteBackupData(ProductRelProc relProc, ProductProc proc, ProductBindGroupProc bindGroupProc, ProductBindTagProc bindTagProc, ProductBindPropProc bindPropProc, int aid, int backupId, int backupFlag) {

        relProc.delBackupData(aid, backupId, backupFlag);

        proc.delBackupData(aid, backupId, backupFlag);

        bindGroupProc.delBackupData(aid, backupId, backupFlag);

        bindTagProc.delBackupData(aid, backupId, backupFlag);

        bindPropProc.delBackupData(aid, backupId, backupFlag);
    }

    private SagaRollback getAddRollback(int flow, int aid, String xid, long branchId, boolean isBindRel) {
        SagaRollback sagaRollback = (tc) -> {
            // 回滚商品业务关系表数据
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc, xid, false);
            relProc.rollback4Saga(aid, branchId);

            // 回滚商品表数据
            if(!isBindRel) {
                ProductProc proc = new ProductProc(flow, aid, tc, xid, false);
                proc.rollback4Saga(aid, branchId, relProc);
            }

            // 回滚绑定参数
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc, xid, false);
            bindPropProc.rollback4Saga(aid, branchId);

            // 回滚绑定分类
            if(useProductGroup()) {
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc, xid, false);
                bindGroupProc.rollback4Saga(aid, branchId);
            }

            // 回滚绑定标签
            if(useProductTag()) {
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc, xid, false);
                bindTagProc.rollback4Saga(aid, branchId);
            }
        };

        return sagaRollback;
    }

    public static boolean useProductGroup() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        boolean useProductGroup = mgSwitch.getBoolean("useProductBindGroup", false);
        return useProductGroup;
    }

    public static boolean useProductTag() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        boolean useProductGroup = mgSwitch.getBoolean("useProductTag", false);
        return useProductGroup;
    }

    private HashMap<Integer, FaiList<Integer>> getBoundUniPriIds(int flow, int aid, FaiList<Integer> pdIds) {
        HashMap<Integer, FaiList<Integer>> pdRels = new HashMap<>();
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            FaiList<Param> list = relProc.getBoundUniPriIds(aid, pdIds);
            for(int i = 0; i < list.size(); i++) {
                Param info = list.get(i);
                Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
                Integer unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                FaiList<Integer> unionPriIds = pdRels.get(pdId);
                if(unionPriIds == null) {
                    unionPriIds = new FaiList<>();
                    pdRels.put(pdId, unionPriIds);
                }
                unionPriIds.add(unionPriId);
            }
        }finally {
            tc.closeDao();
        }
        return pdRels;
    }

    public void initBackupStatus(RedisCacheManager cache) {
        backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
    }

    private static final String BAK_TYPE = "mgPdBasic";
    private BackupStatusCtrl backupStatusCtrl;
}
