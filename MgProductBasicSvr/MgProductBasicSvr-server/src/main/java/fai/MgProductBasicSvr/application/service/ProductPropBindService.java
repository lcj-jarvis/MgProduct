package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.repository.*;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindPropProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

public class ProductPropBindService extends ServicePub {

    /**
     * 获取指定商品设置的商品参数信息
     */
    public int getPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
            //统一控制事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            transactionCtrl.register(bindPropDao);
            Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
            try {
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, bindPropDao);
                rt = bindPropProc.getPdBindPropList(aid, unionPriId, rlPdId, listRef);
                if(rt != Errno.OK) {
                    if(rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "getPdBindProp error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                    }
                    return rt;
                }
            }finally {
                // 关闭dao
                transactionCtrl.closeDao();
            }
            FaiList<Param> list = listRef.value;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductBindPropDto.Key.INFO_LIST, ProductBindPropDto.getInfoDto());
            session.write(sendBuf);
            rt = Errno.OK;
            Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 修改指定商品设置的商品参数信息
     */
    public int setPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
                //统一控制事务
                TransactionCtrl transactionCtrl = new TransactionCtrl();
                transactionCtrl.register(bindPropDao);
                transactionCtrl.setAutoCommit(false);

                try {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, bindPropDao);
                    // 添加数据
                    if(addList != null && !addList.isEmpty()) {
                        // 目前商品数据还在业务方，这边先设置商品id为0
                        rt = bindPropProc.addPdBindPropList(aid, unionPriId, rlPdId, 0, addList);
                        if(rt != Errno.OK) {
                            Log.logErr(rt, "add bind prop list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPdId=%d;", flow, aid, unionPriId, tid, rlPdId);
                            return rt;
                        }
                    }
                    // 删除数据
                    if(delList != null && !delList.isEmpty()) {
                        rt = bindPropProc.delPdBindPropList(aid, rlPdId, delList);
                        if(rt != Errno.OK) {
                            Log.logErr(rt, "del bind prop list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPdId=%d;", flow, aid, unionPriId, tid, rlPdId);
                            return rt;
                        }
                    }
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
                    }else {
                        transactionCtrl.commit();
                        // 删除缓存
                        ProductBindPropCache.delCache(aid, unionPriId, rlPdId);
                    }
                    transactionCtrl.closeDao();
                }
            } finally {
                lock.unlock();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("setPdBindProp ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 根据商品参数id和商品参数值id，返回商品业务id集合
     */
    public int getRlPdByPropVal(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> proIdsAndValIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
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
            FaiList<Integer> rlPdIds = new FaiList<Integer>();
            ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(flow, aid);
            try {
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, bindPropDao);
                rt = bindPropProc.getRlPdByPropVal(aid, unionPriId, proIdsAndValIds, rlPdIds);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                    Log.logErr(rt, "getRlPdByPropVal error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                    return rt;
                }
            } finally {
                bindPropDao.closeDao();
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
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
}
