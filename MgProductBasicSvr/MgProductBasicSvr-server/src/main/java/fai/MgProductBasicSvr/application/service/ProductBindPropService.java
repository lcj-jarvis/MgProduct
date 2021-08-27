package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindPropCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindPropProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.HashSet;

/**
 * 操作商品与商品参数关联数据
 */
public class ProductBindPropService extends ServicePub {

    /**
     * 获取指定商品设置的商品参数信息
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int sysType, int tid, int rlPdId) throws IOException {
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
            list = bindPropProc.getPdBindPropList(aid, unionPriId, sysType, rlPdId);
        }finally {
            // 关闭dao
            tc.closeDao();
        }
        if(Utils.isEmptyList(list)) {
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
    public int setPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int sysType, int tid, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        LockUtil.lock(aid);
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
                    Integer pdId = pdRelProc.getPdId(aid, unionPriId, sysType, rlPdId);
                    if(pdId == null) {
                        if(tid != FaiValObj.TermId.TS) {
                            Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                            return Errno.NOT_FOUND;
                        }
                        // 目前探鼠商品数据还在业务方，这边先设置商品id为0
                        pdId = 0;

                    }

                    bindPropProc.addPdBindPropList(aid, unionPriId, sysType, rlPdId, pdId, addList);
                    addCount += addList.size();
                }
                // 删除数据
                if(delList != null && !delList.isEmpty()) {
                    int delCount = bindPropProc.delPdBindPropList(aid, unionPriId, sysType, rlPdId, delList, false);
                    addCount -= delCount;
                }

                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindPropCache.delCache(aid, unionPriId, sysType, rlPdId);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        } finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("setPdBindProp ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return rt;
    }

    /**
     * 根据参数id+参数值ids 删除关联的商品参数信息
     */
    @SuccessRt(value = Errno.OK)
    public int delPdBindPropByValId(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlPropId, FaiList<Integer> delPropValIds) throws IOException {
        int rt;
        if(aid < 0 || Utils.isEmptyList(delPropValIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
        matcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, delPropValIds);
        matcher.and(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);

        rt = delPdBindProp(flow, aid, unionPriId, sysType, matcher);
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
    public int delPdBindPropByPropId(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPropIds) throws IOException {
        int rt;
        if(aid < 0 || Utils.isEmptyList(rlPropIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }
        ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(ProductBindPropEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
        matcher.and(ProductBindPropEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlPropIds);

        rt = delPdBindProp(flow, aid, unionPriId, sysType, matcher);
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
    public int getRlPdByPropVal(FaiSession session, int flow, int aid, int unionPriId, int sysType, int tid, FaiList<Param> proIdsAndValIds) throws IOException {
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
            rlPdIds = bindPropProc.getRlPdByPropVal(aid, unionPriId, sysType, proIdsAndValIds);
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
            // 查aid + unionPriId 下所有数据
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindPropEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = bindPropProc.searchFromDb(aid, searchArg, ProductBindPropEntity.MANAGE_FIELDS);
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
            searchArg.matcher.and(ProductBindPropEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = bindPropProc.searchFromDb(aid, searchArg, ProductBindPropEntity.MANAGE_FIELDS);

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

    private int delPdBindProp(int flow, int aid, int unionPriId, int sysType, ParamMatcher matcher) {
        int rt;
        if(aid < 0 || matcher == null || matcher.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
            return rt;
        }

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            try {
                ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                FaiList<String> selectFields = new FaiList<>();
                selectFields.add(ProductBindPropEntity.Info.RL_PD_ID);
                FaiList<Param> list = bindPropProc.searchFromDb(aid, searchArg, selectFields);
                if(Utils.isEmptyList(list)) {
                    Log.logStd("del data not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                    return Errno.OK;
                }
                // 删除数据
                int delCount = bindPropProc.delPdBindProp(aid, matcher);
                int addCount = 0 - delCount;

                FaiList<Integer> rlPdIds = new FaiList<>();
                // 删除缓存
                for (int i = 0; i < list.size(); i++) {
                    Param info = list.get(i);
                    rlPdIds.add(info.getInt(ProductBindPropEntity.Info.RL_PD_ID));
                }
                ProductBindPropCache.delCacheList(aid, unionPriId, sysType, rlPdIds);
                ProductBindPropCache.DataStatusCache.update(aid, unionPriId, addCount);
            } finally {
                tc.closeDao();
            }
        } finally {
            LockUtil.unlock(aid);
        }

        rt = Errno.OK;
        return rt;
    }
}
