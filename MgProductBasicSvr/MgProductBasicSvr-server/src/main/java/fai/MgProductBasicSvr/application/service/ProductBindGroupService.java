package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindGroupCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindGroupProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.HashSet;

public class ProductBindGroupService extends ServicePub {

    /**
     * 获取指定商品关联的商品分类信息
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdsBindGroup(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) throws IOException {
        if(Utils.isEmptyList(rlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        TransactionCtrl tc = new TransactionCtrl();
        FaiList<Param> list;
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            FaiList<Integer> pdIds = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(rlPdIds));
            if(Utils.isEmptyList(pdIds)) {
                return Errno.NOT_FOUND;
            }
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            list = bindGroupProc.getPdBindGroupList(aid, unionPriId, pdIds);
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
    public int setPdBindGroup(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlPdId, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) throws IOException {
        if(Utils.isEmptyList(addGroupIds) && Utils.isEmptyList(delGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addGroupIds and delGroupIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }

        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                int addCount = 0;

                ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                Integer pdId = pdRelProc.getPdId(aid, unionPriId, sysType, rlPdId);
                if(pdId == null) {
                    Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);
                    return Errno.NOT_FOUND;
                }

                if(!Utils.isEmptyList(addGroupIds)) {
                    // 添加数据
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, sysType, rlPdId, pdId, addGroupIds);
                    addCount += addGroupIds.size();
                }

                if(!Utils.isEmptyList(delGroupIds)) {
                    // 删除数据
                    int delCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, pdId, delGroupIds);
                    addCount -= delCount;
                }
                commit = true;
                tc.commit();
                // 删除缓存
                ProductBindGroupCache.delCache(aid, unionPriId, pdId);
                ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, addCount);
                ProductBindGroupCache.EmptyCache.delCache(aid, unionPriId, pdId); // 删除绑定分类数据空缓存
            }finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        }finally {
            LockUtil.unlock(aid);
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set ok;flow=%d;aid=%d;uid=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
        return Errno.OK;
    }


    /**
     * 查询指定分类id下的商品业务id
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRlPdByRlGroupId(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds) throws IOException {
        if(Utils.isEmptyList(rlGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlGroupIds is empty;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
            return rt;
        }
        FaiList<Integer> rlPdIds = new FaiList<>();
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            rlPdIds = bindGroupProc.getRlPdIdsByGroupId(aid, unionPriId, sysType, rlGroupIds);
            if(Utils.isEmptyList(rlPdIds)) {
                Log.logDbg("not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return Errno.NOT_FOUND;
            }
        }finally {
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductBindGroupDto.Key.RL_PD_IDS);
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;uid=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlGroupIds);
        return Errno.OK;
    }

    /**
     * 删除指定分类业务id的关联数据
     */
    @SuccessRt(value = Errno.OK)
    public int delBindGroupList(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds) throws IOException {
        if(Utils.isEmptyList(rlGroupIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlGroupIds is empty;aid=%d;unionPriId=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
            return rt;
        }
        int delCount = 0;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            delCount = bindGroupProc.delPdBindGroupListByRlGroupIds(aid, unionPriId, sysType, rlGroupIds);

        }finally {
            tc.closeDao();
        }
        // 删除缓存
        ProductBindGroupCache.clearCacheVersion(aid);
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
        long begin = System.currentTimeMillis();
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
            searchArg.matcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = bindGroupProc.searchFromDb(aid, searchArg, ProductBindGroupEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindGroupDto.Key.INFO_LIST, ProductBindGroupDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        long end = System.currentTimeMillis();
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;consume=%d", flow, aid, unionPriId, list.size(), end - begin);

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
            searchArg.matcher = searchArg.matcher == null ? new ParamMatcher() : searchArg.matcher;
            searchArg.matcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = bindGroupProc.searchFromDb(aid, searchArg, ProductBindGroupEntity.MANAGE_FIELDS);

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
