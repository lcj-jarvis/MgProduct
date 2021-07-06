package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindPropProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.domain.serviceproc.SagaProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;

/**
 * 操作商品与商品参数关联数据
 */
public class ProductBindPropService extends ServicePub {

    /**
     * 获取指定商品设置的商品参数信息
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        FaiList<Param> list;
        try {
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            list = bindPropProc.getPdBindPropList(aid, unionPriId, rlPdId);
        }finally {
            // 关闭dao
            tc.closeDao();
        }
        if(Util.isEmptyList(list)) {
            return Errno.NOT_FOUND;
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindPropDto.Key.INFO_LIST, ProductBindPropDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
        return rt;
    }

    /**
     * 修改指定商品设置的商品参数信息
     */
    @SuccessRt(value = Errno.OK)
    public int setPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                int addCount = 0;
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                // 添加数据
                if(addList != null && !addList.isEmpty()) {
                    ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                    Param pdRelInfo = pdRelProc.getProductRel(aid, unionPriId, rlPdId);
                    /*if(Str.isEmpty(pdRelInfo)) {
                        Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                        return Errno.NOT_FOUND;
                    }
                    int pdId = pdRelInfo.getInt(ProductBindGroupEntity.Info.PD_ID);*/
                    int pdId = 0;
                    if(!Str.isEmpty(pdRelInfo)) {
                        pdId = pdRelInfo.getInt(ProductBindGroupEntity.Info.PD_ID);
                    }
                    // 目前商品数据还在业务方，这边先设置商品id为0
                    bindPropProc.addPdBindPropList(aid, unionPriId, rlPdId, pdId, addList);
                    addCount += addList.size();
                }
                // 删除数据
                if(delList != null && !delList.isEmpty()) {
                    int delCount = bindPropProc.delPdBindPropList(aid, unionPriId, rlPdId, delList);
                    addCount -= delCount;
                }

                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindPropCache.delCache(aid, unionPriId, rlPdId);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        } finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("setPdBindProp ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * Fseata分布式事务
     */
    @SuccessRt(value = Errno.OK)
    public int transactionSetPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId, String xid, FaiList<Param> addList, FaiList<Param> delList) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        // 加锁
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            // 本地事务控制
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                int addCount = 0;
                int pdId = 0;
                ProductBindPropProc propProc = new ProductBindPropProc(flow, aid, tc);

                // 删除数据
                if (!Util.isEmptyList(delList)) {
                    int delCount = propProc.delPdBindPropList(aid, unionPriId, rlPdId, delList);
                    addCount -= delCount;
                }

                // 添加数据
                if (!Util.isEmptyList(addList)) {
                    ProductRelProc productRelProc = new ProductRelProc(flow, aid, tc);
                    Param productRel = productRelProc.getProductRel(aid, unionPriId, rlPdId);
                    if(!Str.isEmpty(productRel)) {
                        pdId = productRel.getInt(ProductBindGroupEntity.Info.PD_ID);
                    }
                    // 目前商品数据还在业务方，这边先设置商品id为0
                    propProc.addPdBindPropList(aid, unionPriId, rlPdId, pdId, addList);
                    addCount += addList.size();
                }

                // 定义补偿信息
                Param rollbackInfo = new Param();
                rollbackInfo.setInt(ProductEntity.Business.ADD_COUNT, addCount);
                rollbackInfo.setInt(ProductBindPropEntity.Info.RL_PD_ID, rlPdId);
                rollbackInfo.setInt(ProductBindPropEntity.Info.PD_ID, pdId);
                rollbackInfo.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, unionPriId);
                rollbackInfo.setList(ProductBindPropEntity.BUSINESS.ADD_LIST, addList);
                rollbackInfo.setList(ProductBindPropEntity.BUSINESS.DEL_LIST, delList);

                SagaProc rollbackProc = new SagaProc(flow, aid, tc);
                rollbackProc.addInfo(aid, xid, rollbackInfo);

                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindPropCache.delCache(aid, unionPriId, rlPdId);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        } finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("transactionSetPdBindProp ok;flow=%d;aid=%d;uid=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
        return rt;
    }

    /**
     * transactionSetPdBindProp 的补偿方法
     */
    public int setPdBindPropRollback(FaiSession session, int aid, int flow, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            int addCount = 0;
            int unionPriId = 0;
            try {
                tc.setAutoCommit(false);
                // 获取 saga 表中的数据
                SagaProc sagaProc = new SagaProc(flow, aid, tc);
                Param sagaInfo = sagaProc.getInfoWithAdd(xid, branchId);
                if (sagaInfo == null) {
                    return Errno.OK;
                }

                // 获取补偿信息
                Param rollbackInfo = Param.parseParam(sagaInfo.getString(BasicSagaEntity.Info.PROP));
                int status = sagaInfo.getInt(BasicSagaEntity.Info.STATUS);
                // 幂等性保证
                if (status == BasicSagaValObj.Status.ROLLBACK_OK) {
                    return rt;
                }

                int pdId = rollbackInfo.getInt(ProductBindGroupEntity.Info.PD_ID);
                unionPriId = rollbackInfo.getInt(ProductBindGroupEntity.Info.UNION_PRI_ID);
                int rlPdId = rollbackInfo.getInt(ProductBindGroupEntity.Info.RL_PD_ID);
                addCount = rollbackInfo.getInt(ProductEntity.Business.ADD_COUNT);
                // 之前添加的参数绑定信息
                FaiList<Param> addList = rollbackInfo.getList(ProductBindPropEntity.BUSINESS.ADD_LIST);
                // 之前删除的参数绑定信息
                FaiList<Param> delList = rollbackInfo.getList(ProductBindPropEntity.BUSINESS.DEL_LIST);

                // 执行补偿
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                if (!Util.isEmptyList(addList)) {
                    bindPropProc.delPdBindPropList(aid, unionPriId, rlPdId, addList);
                }
                if (!Util.isEmptyList(delList)) {
                    bindPropProc.addPdBindPropList(aid, unionPriId, rlPdId, pdId, delList);
                }

                // 修改 saga 状态
                sagaProc.setStatus(xid, branchId, status);

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                } else {
                    // 告知数据状态发生变化，由于之前的逻辑是修改完后直接删除整个缓存并没有更新缓存，所以在这里不做参数绑定的缓存补偿
                    ProductBindPropCache.DataStatusCache.update(aid, unionPriId, -addCount);
                }
                tc.closeDao();
            }
        } finally {
            lock.unlock();

            FaiBuffer sendBuf = new FaiBuffer(true);
            // 判断是否需要重试
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
            session.write(rt);
        }
        return rt;
    }

    /**
     * 根据参数id+参数值ids 删除关联的商品参数信息
     */
    @SuccessRt(value = Errno.OK)
    public int delPdBindPropByValId(FaiSession session, int flow, int aid, int unionPriId, int rlPropId, FaiList<Integer> delPropValIds) throws IOException {
        int rt;
        if(aid < 0 || Util.isEmptyList(delPropValIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, delPropValIds);

        rt = delPdBindProp(flow, aid, unionPriId, matcher);
        if(rt != Errno.OK) {
            Log.logErr("del by valIds error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del by valIds ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 根据参数ids 删除关联的商品参数信息
     */
    @SuccessRt(value = Errno.OK)
    public int delPdBindPropByPropId(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPropIds) throws IOException {
        int rt;
        if(aid < 0 || Util.isEmptyList(rlPropIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlPropIds);

        rt = delPdBindProp(flow, aid, unionPriId, matcher);
        if(rt != Errno.OK) {
            Log.logErr("del by propIds error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del by propIds ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 根据商品参数id和商品参数值id，返回商品业务id集合
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRlPdByPropVal(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> proIdsAndValIds) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        if(proIdsAndValIds == null || proIdsAndValIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error proIdsAndValIds is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Integer> rlPdIds;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            rlPdIds = bindPropProc.getRlPdByPropVal(aid, unionPriId, proIdsAndValIds);
        } finally {
            tc.closeDao();
        }

        if(rlPdIds.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logDbg("not found;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
            return rt;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductBindPropDto.Key.RL_PD_IDS);
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getBindPropDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            info = bindPropProc.getDataStatus(aid, unionPriId);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductBindPropDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getBindPropDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllBindProp(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            // 查aid + unionPriId 下所有数据，传入空的searchArg
            SearchArg searchArg = new SearchArg();
            list = bindPropProc.searchFromDb(aid, unionPriId, searchArg, ProductBindPropEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindPropDto.Key.INFO_LIST, ProductBindPropDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int searchBindPropFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
            list = bindPropProc.searchFromDb(aid, unionPriId, searchArg, ProductBindPropEntity.MANAGE_FIELDS);

        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindPropDto.Key.INFO_LIST, ProductBindPropDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductBindPropDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    private int delPdBindProp(int flow, int aid, int unionPriId, ParamMatcher matcher) {
        int rt;
        if(aid < 0 || matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            try {
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                FaiList<String> selectFields = new FaiList<>();
                selectFields.add(ProductBindPropEntity.Info.RL_PD_ID);
                FaiList<Param> list = bindPropProc.searchFromDb(aid, unionPriId, searchArg, selectFields);
                if(Util.isEmptyList(list)) {
                    Log.logStd("del data not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                    return Errno.OK;
                }
                // 删除数据
                int delCount = bindPropProc.delPdBindProp(aid, unionPriId, matcher);
                int addCount = 0 - delCount;

                HashSet<Integer> rlPdIds = new HashSet<>();
                // 删除缓存
                for (int i = 0; i < list.size(); i++) {
                    Param info = list.get(i);
                    rlPdIds.add(info.getInt(ProductBindPropEntity.Info.RL_PD_ID));
                }
                ProductBindPropCache.delCacheList(aid, unionPriId, rlPdIds);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                tc.closeDao();
            }
        } finally {
            lock.unlock();
        }

        rt = Errno.OK;
        return rt;
    }
}
