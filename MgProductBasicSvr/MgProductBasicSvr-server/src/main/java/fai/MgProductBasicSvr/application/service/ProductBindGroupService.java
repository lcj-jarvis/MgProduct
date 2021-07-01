package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductSagaEntity;
import fai.MgProductBasicSvr.domain.entity.ProductSagaValObj;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindGroupProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRollbackProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
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

public class ProductBindGroupService extends ServicePub {

    /**
     * 获取指定商品关联的商品分类信息
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdsBindGroup(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        if(Util.isEmptyList(rlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        TransactionCtrl tc = new TransactionCtrl();
        FaiList<Param> list;
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            list = bindGroupProc.getPdBindGroupList(aid, unionPriId, rlPdIds);
        }finally {
            tc.closeDao();
        }
        if(list == null || list.isEmpty()) {
            return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindGroupDto.Key.INFO_LIST, ProductBindGroupDto.getInfoDto());
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlPdIds);
        return Errno.OK;
    }

    /**
     * 修改指定商品设置的商品分类关联
     */
    @SuccessRt(value = Errno.OK)
    public int setPdBindGroup(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) throws IOException {
        if(Util.isEmptyList(addGroupIds) && Util.isEmptyList(delGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addGroupIds and delGroupIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                int addCount = 0;
                if(!Util.isEmptyList(addGroupIds)) {
                    ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                    Param pdRelInfo = pdRelProc.getProductRel(aid, unionPriId, rlPdId);
                    if(Str.isEmpty(pdRelInfo)) {
                        Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                        return Errno.NOT_FOUND;
                    }
                    int pdId = pdRelInfo.getInt(ProductBindGroupEntity.Info.PD_ID);
                    // 添加数据
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, rlPdId, pdId, addGroupIds);
                    addCount += addGroupIds.size();
                }

                if(!Util.isEmptyList(delGroupIds)) {
                    // 删除数据
                    int delCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, rlPdId, delGroupIds);
                    addCount -= delCount;
                }
                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindGroupCache.delCache(aid, unionPriId);
                ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, addCount);
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
     * Saga模式，修改绑定分类信息
     */
    @SuccessRt(value = Errno.OK)
    public int transactionSetPdBindGroup(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, String xid, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) throws IOException {
        if(Util.isEmptyList(addGroupIds) && Util.isEmptyList(delGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addGroupIds and delGroupIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                int addCount = 0;
                int pdId = 0;

                if(!Util.isEmptyList(delGroupIds)) {
                    // 删除数据
                    int delCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, rlPdId, delGroupIds);
                    addCount -= delCount;
                }

                if(!Util.isEmptyList(addGroupIds)) {
                    ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                    Param pdRelInfo = pdRelProc.getProductRel(aid, unionPriId, rlPdId);
                    if(Str.isEmpty(pdRelInfo)) {
                        Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                        return Errno.NOT_FOUND;
                    }
                    pdId = pdRelInfo.getInt(ProductBindGroupEntity.Info.PD_ID);
                    // 添加数据
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, rlPdId, pdId, addGroupIds);
                    addCount += addGroupIds.size();
                }

                // 记录修改的数据，作为补偿
                Param rollbackInfo = new Param();
                rollbackInfo.setInt(ProductBindGroupEntity.Info.AID, aid);
                rollbackInfo.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
                rollbackInfo.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);
                rollbackInfo.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
                rollbackInfo.setInt(ProductEntity.Business.ADD_COUNT, addCount);
                rollbackInfo.setList(ProductBindGroupEntity.Business.ADD_GROUP_IDS, addGroupIds);
                rollbackInfo.setList(ProductBindGroupEntity.Business.DEL_GROUP_IDS, delGroupIds);

                ProductRollbackProc rollbackProc = new ProductRollbackProc(flow, aid, tc);
                rollbackProc.addInfo(aid, xid, rollbackInfo);

                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindGroupCache.delCache(aid, unionPriId);
                ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, addCount);
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
        Log.logStd("set ok;flow=%d;aid=%d;uid=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
        return Errno.OK;
    }

    /**
     * transactionSetPdBindGroup 的补偿方法
     */
    @SuccessRt(value = Errno.OK)
    public int setPdBindGroupRollback(FaiSession session, int flow, int aid, String xid, Long branchId) throws IOException {
        int rt = Errno.ERROR;
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            // 分类绑定数目的改变
            int addCount = 0;
            int unionPriId = 0;
            try {
                tc.setAutoCommit(false);
                ProductRollbackProc rollbackProc = new ProductRollbackProc(flow, aid, tc);
                Ref<Param> sagaInfoRef = new Ref<>();
                // 获取 saga 表中的数据
                rt = rollbackProc.getInfo(xid, branchId, sagaInfoRef);
                if (rt != Errno.OK) {
                    // 如果是 NOT_FOUND，要允许空补偿和防悬挂控制，在getInfo中已经做到，插入一条saga记录作为空补偿
                    if (rt == Errno.NOT_FOUND) {
                        rt = Errno.OK;
                        Log.reportErr(flow, rt,"get SagaInfo not found; xid=%s, branchId=%s", xid, branchId);
                    }
                    return rt;
                }

                // 获取补偿信息
                Param sagaInfo = Param.parseParam(sagaInfoRef.value.getString(ProductSagaEntity.Info.ROLLBACK_INFO));
                int status = sagaInfo.getInt(ProductSagaEntity.Info.STATUS);
                // 幂等性保证
                if (status == ProductSagaValObj.Status.ROLLBACK_OK) {
                    return rt;
                }

                /* 获取补偿信息 start */
                int pdId = sagaInfo.getInt(ProductBindGroupEntity.Info.PD_ID);
                unionPriId = sagaInfo.getInt(ProductBindGroupEntity.Info.UNION_PRI_ID);
                int curAid = sagaInfo.getInt(ProductBindGroupEntity.Info.AID);
                int rlPdId = sagaInfo.getInt(ProductBindGroupEntity.Info.RL_PD_ID);
                addCount = sagaInfo.getInt(ProductEntity.Business.ADD_COUNT);
                // 之前添加的分类ids
                FaiList<Integer> addGroupIds = sagaInfo.getList(ProductBindGroupEntity.Business.ADD_GROUP_IDS);
                // 之前删除的分类ids
                FaiList<Integer> delGroupList = sagaInfo.getList(ProductBindGroupEntity.Business.DEL_GROUP_IDS);
                /* 获取补偿信息 end */

                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                // 补偿，删除之前添加的分类绑定
                if (!Util.isEmptyList(addGroupIds)) {
                    bindGroupProc.delPdBindGroupList(curAid, unionPriId, pdId, addGroupIds);
                }
                // 补偿，添加之前删除的分类绑定
                if (!Util.isEmptyList(delGroupList)) {
                    bindGroupProc.addPdBindGroupList(curAid, unionPriId, rlPdId, pdId, delGroupList);
                }
                // 修改Saga状态
                rt = rollbackProc.setStatus(xid, branchId, ProductSagaValObj.Status.ROLLBACK_OK);
                if (rt != Errno.OK) {
                    return rt;
                }

                commit = true;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                } else {
                    // 告知数据状态发生变化，由于之前的逻辑是修改完后直接删除整个缓存并没有更新缓存，所以在这里不做分类绑定的缓存补偿
                    ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, -addCount);
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
     * 查询指定分类id下的商品业务id
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRlPdByRlGroupId(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlGroupIds) throws IOException {
        if(Util.isEmptyList(rlGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlGroupIds is empty;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
            return rt;
        }
        FaiList<Integer> rlPdIds = new FaiList<>();
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            rlPdIds = bindGroupProc.getRlPdIdsByGroupId(aid, unionPriId, rlGroupIds);
            if(Util.isEmptyList(rlPdIds)) {
                Log.logDbg("not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return Errno.NOT_FOUND;
            }
        }finally {
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductBindGroupDto.Key.INFO_LIST, ProductBindGroupDto.getInfoDto());
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;uid=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlGroupIds);
        return Errno.OK;
    }

    /**
     * 删除指定分类业务id的关联数据
     */
    @SuccessRt(value = Errno.OK)
    public int delBindGroupList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlGroupIds) throws IOException {
        if(Util.isEmptyList(rlGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlGroupIds is empty;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
            return rt;
        }
        int delCount = 0;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            delCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, rlGroupIds);
        }finally {
            tc.closeDao();
        }
        // 删除缓存
        ProductBindGroupCache.delCache(aid, unionPriId);
        ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, -delCount);

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logDbg("del ok;flow=%d;aid=%d;uid=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlGroupIds);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int getBindGroupDataStatus(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        Param info;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc groupProc = new ProductBindGroupProc(flow, aid, tc);
            info = groupProc.getDataStatus(aid, unionPriId);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, ProductBindGroupDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("getBindGroupDataStatus ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllBindGroup(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            // 查aid + unionPriId 下所有数据，传入空的searchArg
            SearchArg searchArg = new SearchArg();
            list = bindGroupProc.searchFromDb(aid, unionPriId, searchArg, ProductBindGroupEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindGroupDto.Key.INFO_LIST, ProductBindGroupDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int searchBindGroupFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            list = bindGroupProc.searchFromDb(aid, unionPriId, searchArg, ProductBindGroupEntity.MANAGE_FIELDS);

        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindGroupDto.Key.INFO_LIST, ProductBindGroupDto.getInfoDto());
        if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductBindGroupDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }
}
