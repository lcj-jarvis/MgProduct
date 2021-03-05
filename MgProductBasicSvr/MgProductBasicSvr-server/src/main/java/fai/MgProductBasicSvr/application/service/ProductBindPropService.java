package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindPropProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
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
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                // 添加数据
                if(addList != null && !addList.isEmpty()) {
                    // 目前商品数据还在业务方，这边先设置商品id为0
                    bindPropProc.addPdBindPropList(aid, unionPriId, rlPdId, 0, addList);
                }
                // 删除数据
                if(delList != null && !delList.isEmpty()) {
                    bindPropProc.delPdBindPropList(aid, unionPriId, rlPdId, delList);
                }

                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindPropCache.delCache(aid, unionPriId, rlPdId);
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
}
