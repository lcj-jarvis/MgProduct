package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.BasicSagaEntity;
import fai.MgProductBasicSvr.domain.entity.BasicSagaValObj;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindTagCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindTagProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.domain.serviceproc.SagaProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindTagDto;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-07-14 14:32
 */
public class ProductBindTagService extends ServicePub {

    /**
     * 根据aid，unionPriId，rlPdIds获取到商品和标签关联的数据。
     * 先从缓存中获取，再从db中获取剩下的rlPdIds(缓存中不存在)的商品关联标签的数据
     */
    @SuccessRt(value = Errno.OK)
    public int getPdBindTag(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        if(Util.isEmptyList(rlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        TransactionCtrl tc = new TransactionCtrl();
        FaiList<Param> list;
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            list = bindTagProc.getPdBindTagList(aid, unionPriId, rlPdIds);

        }finally {
            tc.closeDao();
        }
        if(Util.isEmptyList(list)) {
            return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlPdIds);
        return Errno.OK;
    }

    /**
     * 先根据aid，unionPriId，delRlTagIds删除商品关联标签的数据，
     * 再根据addRlTagIds添加新的商品关联标签的数据
     * @param addRlTagIds 要删除的多个rlTagId
     * @param delRlTagIds 要添加的多个rlTagId
     */
    @SuccessRt(value = Errno.OK)
    public int setPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int rlPdId,
                            FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) throws IOException {
        boolean isEmpty = Util.isEmptyList(addRlTagIds) && Util.isEmptyList(delRlTagIds);
        if(isEmpty) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addRlTagIds and delRlTagIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                int addCount = 0;

                //先删除
                if(!Util.isEmptyList(delRlTagIds)) {
                    // 删除数据
                    int delCount = bindTagProc.delPdBindTagList(aid, unionPriId, rlPdId, delRlTagIds);
                    addCount -= delCount;
                }

                //后添加
                if(!Util.isEmptyList(addRlTagIds)) {
                    ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                    Param pdRelInfo = pdRelProc.getProductRel(aid, unionPriId, rlPdId);
                    if(Str.isEmpty(pdRelInfo)) {
                        Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                        return Errno.NOT_FOUND;
                    }
                    int pdId = pdRelInfo.getInt(ProductBindTagEntity.Info.PD_ID);
                    // 添加数据
                    bindTagProc.addPdBindTagList(aid, unionPriId, rlPdId, pdId, addRlTagIds);
                    addCount += addRlTagIds.size();
                }
                commit = true;
                tc.commit();

                // 删除缓存
                FaiList<Integer> rlPdIds = new FaiList<>();
                rlPdIds.add(rlPdId);
                ProductBindTagCache.delCacheList(aid, unionPriId, rlPdIds);
                ProductBindTagCache.DataStatusCache.update(aid, unionPriId, addCount);
            }finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        }finally {
            lock.unlock();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set ok;flow=%d;aid=%d;uid=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
        return Errno.OK;
    }

    /**
     * 分布式事务Saga模式，先删除后添加标签
     */
    @SuccessRt(value = Errno.OK)
    public int transactionSetPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, String xid, FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) throws IOException {
        if(Util.isEmptyList(addRlTagIds) && Util.isEmptyList(delRlTagIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addRlTagIds and delRlTagIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindTagProc bindRlTagProc = new ProductBindTagProc(flow, aid, tc);
                int addCount = 0;
                int pdId = 0;

                if(!Util.isEmptyList(delRlTagIds)) {
                    // 删除数据
                    int delCount = bindRlTagProc.delPdBindTagList(aid, unionPriId, rlPdId, delRlTagIds);
                    addCount -= delCount;
                }

                if(!Util.isEmptyList(addRlTagIds)) {
                    ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                    Param pdRelInfo = pdRelProc.getProductRel(aid, unionPriId, rlPdId);
                    if(Str.isEmpty(pdRelInfo)) {
                        Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                        return Errno.NOT_FOUND;
                    }
                    pdId = pdRelInfo.getInt(ProductBindTagEntity.Info.PD_ID);
                    // 添加数据
                    bindRlTagProc.addPdBindTagList(aid, unionPriId, rlPdId, pdId, addRlTagIds);
                    addCount += addRlTagIds.size();
                }

                // 记录修改的数据，作为补偿
                Param rollbackInfo = new Param();
                rollbackInfo.setInt(ProductBindTagEntity.Info.AID, aid);
                rollbackInfo.setInt(ProductBindTagEntity.Info.PD_ID, pdId);
                rollbackInfo.setInt(ProductBindTagEntity.Info.UNION_PRI_ID, unionPriId);
                rollbackInfo.setInt(ProductBindTagEntity.Info.RL_PD_ID, rlPdId);
                rollbackInfo.setInt(ProductEntity.Business.ADD_COUNT, addCount);
                rollbackInfo.setList(ProductBindTagEntity.Business.ADD_TAG_IDS, addRlTagIds);
                rollbackInfo.setList(ProductBindTagEntity.Business.DEL_TAG_IDS, delRlTagIds);

                SagaProc sagaProc = new SagaProc(flow, aid, tc);
                sagaProc.addInfo(aid, xid, rollbackInfo);

                commit = true;
                tc.commit();
                // 删除缓存
                FaiList<Integer> rlPdIds = new FaiList<>();
                rlPdIds.add(rlPdId);
                ProductBindTagCache.delCacheList(aid, unionPriId, rlPdIds);
                ProductBindTagCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                if (!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        } finally {
            lock.unlock();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("transactionSetPdBindRlTag ok;flow=%d;aid=%d;uid=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
        return Errno.OK;
    }

    /**
     * 分布式事务transactionSetPdBindTag方法对应的回滚的方法
     */
    @SuccessRt(value = Errno.OK)
    public int setPdBindTagRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            // 标签绑定数目的改变
            int addCount = 0;
            int unionPriId = 0;
            try {
                tc.setAutoCommit(false);
                SagaProc sagaProc = new SagaProc(flow, aid, tc);
                // 获取 saga 表中的数据
                Param sagaInfo = sagaProc.getInfoWithAdd(xid, branchId);
                if (sagaInfo == null) {
                    rt = Errno.OK;
                    return rt;
                }

                // 获取补偿信息
                Param rollbackInfo = Param.parseParam(sagaInfo.getString(BasicSagaEntity.Info.PROP));
                int status = sagaInfo.getInt(BasicSagaEntity.Info.STATUS);
                // 幂等性保证
                if (status == BasicSagaValObj.Status.ROLLBACK_OK) {
                    return rt;
                }

                /* 获取补偿信息 start */
                int pdId = rollbackInfo.getInt(ProductBindTagEntity.Info.PD_ID);
                unionPriId = rollbackInfo.getInt(ProductBindTagEntity.Info.UNION_PRI_ID);
                int curAid = sagaInfo.getInt(ProductBindTagEntity.Info.AID);
                int rlPdId = rollbackInfo.getInt(ProductBindTagEntity.Info.RL_PD_ID);
                addCount = rollbackInfo.getInt(ProductEntity.Business.ADD_COUNT);
                // 之前添加的标签ids
                FaiList<Integer> addRlTagIds = rollbackInfo.getList(ProductBindTagEntity.Business.ADD_TAG_IDS);
                // 之前删除的标签ids
                FaiList<Integer> delRlTagList = rollbackInfo.getList(ProductBindTagEntity.Business.DEL_TAG_IDS);
                /* 获取补偿信息 end */

                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                // 补偿，删除之前添加的标签绑定
                if (!Util.isEmptyList(addRlTagIds)) {
                    bindTagProc.delPdBindTagList(curAid, unionPriId, pdId, addRlTagIds);
                }
                // 补偿，添加之前删除的标签绑定
                if (!Util.isEmptyList(delRlTagList)) {
                    bindTagProc.addPdBindTagList(curAid, unionPriId, rlPdId, pdId, delRlTagList);
                }
                // 修改Saga状态
                sagaProc.setStatus(xid, branchId, BasicSagaValObj.Status.ROLLBACK_OK);

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                } else {
                    // 告知数据状态发生变化，由于之前的逻辑是修改完后直接删除整个缓存并没有更新缓存，所以在这里不做标签绑定的缓存补偿
                    ProductBindTagCache.DataStatusCache.update(aid, unionPriId, -addCount);
                }
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
     * 根据rlPdIds删除商品和标签的关联数据
     * @param  delRlPdIds 要删除的多个rlPdId
     */
    @SuccessRt(value = Errno.OK)
    public int delBindTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> delRlPdIds) throws IOException {
        if(Util.isEmptyList(delRlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlTagIds is empty;aid=%d;unionPriId=%d;rlTagIds=%s;", aid, unionPriId, delRlPdIds);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            int delCount = 0;
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                delCount = bindTagProc.delPdBindTagList(aid, unionPriId, delRlPdIds);
                commit = true;

                //commit之前设置10s过期时间，避免脏数据，保持一致性
                ProductBindTagCache.setExpire(aid, unionPriId);
            }finally {
                if (commit) {
                    // 删除缓存
                    ProductBindTagCache.delCacheList(aid, unionPriId, delRlPdIds);
                    ProductBindTagCache.DataStatusCache.update(aid, unionPriId, -delCount);
                } else {
                    tc.rollback();
                }
                tc.closeDao();
            }
        } finally {
           lock.unlock();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logDbg("del ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, delRlPdIds);
        return Errno.OK;
    }

    /**
     * 根据rlTagIds查询RlPdIds
     */
    @SuccessRt(value = Errno.OK)
    public int getRlPdIdsByRlTagIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlTagIds) throws IOException {
        if(Util.isEmptyList(rlTagIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlTagIds is empty;aid=%d;unionPriId=%d;rlTagIds=%s;", aid, unionPriId, rlTagIds);
            return rt;
        }
        FaiList<Integer> rlPdIds;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            rlPdIds = bindTagProc.getRlPdIdsByTagIds(aid, unionPriId, rlTagIds);
            if(Util.isEmptyList(rlPdIds)) {
                Log.logDbg("not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return Errno.NOT_FOUND;
            }
        }finally {
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductBindTagDto.Key.RL_PD_IDS);
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;uid=%d;rlTagIds=%s;", flow, aid, unionPriId, rlTagIds);
        return Errno.OK;
    }

    /**
     * 获取标签的数据状态
     */
    @SuccessRt(value = Errno.OK)
    public int getBindTagDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc tagProc = new ProductBindTagProc(flow, aid, tc);
            info = tagProc.getDataStatus(aid, unionPriId);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductBindTagDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getBindTagDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 获取aid和unionPriId下的所有的商品和标签的关联数据
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllPdBindTag(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            // 查aid + unionPriId 下所有数据
            list = bindTagProc.getListByConditions(aid, unionPriId, null, ProductBindTagEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    /**
     * 根据aid，unionPriId，searchArg从db中查询商品和标签的关联数据
     * @param searchArg 查询的条件
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getBindTagFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            list = bindTagProc.getListByConditions(aid, unionPriId, searchArg, ProductBindTagEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductBindTagDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }
}
