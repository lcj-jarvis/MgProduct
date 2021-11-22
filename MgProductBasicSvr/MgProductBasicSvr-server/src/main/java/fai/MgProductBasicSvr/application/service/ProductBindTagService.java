package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.repository.cache.ProductBindTagCache;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindTagProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindTagDto;
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
    public int getPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) throws IOException {
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
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            list = bindTagProc.getPdBindTagList(aid, unionPriId, pdIds);

        }finally {
            tc.closeDao();
        }
        if(Utils.isEmptyList(list)) {
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
    public int setPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlPdId,
                            FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) throws IOException {
        if(Utils.isEmptyList(addRlTagIds) && Utils.isEmptyList(delRlTagIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, addRlTagIds and delRlTagIds is empty;aid=%d;unionPriId=%d;rlPdId=%s;", aid, unionPriId, rlPdId);
            return rt;
        }

        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                int addCount = 0;

                ProductRelProc pdRelProc = new ProductRelProc(flow, aid, tc);
                Integer pdId = pdRelProc.getPdId(aid, unionPriId, sysType, rlPdId);
                if(pdId == null) {
                    Log.logErr("pd rel info is not exist;flow=%d;aid=%d;uid=%d;sysType=%d;rlPdId=%d;", flow, aid, unionPriId, sysType, rlPdId);
                    return Errno.NOT_FOUND;
                }

                //先删除
                if(!Utils.isEmptyList(delRlTagIds)) {
                    // 删除数据
                    int delCount = bindTagProc.delPdBindTagList(aid, unionPriId, pdId, delRlTagIds);
                    addCount -= delCount;
                }

                //后添加
                if(!Utils.isEmptyList(addRlTagIds)) {
                    // 添加数据
                    bindTagProc.addPdBindTagList(aid, unionPriId, sysType, rlPdId, pdId, addRlTagIds);
                    addCount += addRlTagIds.size();
                }
                commit = true;
                tc.commit();

                // 删除缓存
                ProductBindTagCache.delCache(aid, unionPriId, pdId);
                ProductBindTagCache.DataStatusCache.update(aid, unionPriId, addCount);
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
     * 根据rlPdIds删除商品和标签的关联数据
     * @param  delRlPdIds 要删除的多个rlPdId
     */
    @SuccessRt(value = Errno.OK)
    public int delBindTagList(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> delRlPdIds) throws IOException {
        if(Utils.isEmptyList(delRlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlTagIds is empty;aid=%d;unionPriId=%d;rlTagIds=%s;", aid, unionPriId, delRlPdIds);
            return rt;
        }
        LockUtil.lock(aid);
        try {
            FaiList<Integer> delPdIds = null;
            int delCount = 0;
            TransactionCtrl tc = new TransactionCtrl();
            try {
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                delPdIds = relProc.getPdIds(aid, unionPriId, sysType, new HashSet<>(delRlPdIds));

                ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                //修改之前设置10s过期时间，避免脏数据，保持一致性
                ProductBindTagCache.setExpire(aid, unionPriId, delPdIds);

                delCount = bindTagProc.delPdBindTagList(aid, unionPriId, delPdIds);
            }finally {
                tc.closeDao();
            }
            // 删除缓存
            ProductBindTagCache.delCacheList(aid, unionPriId, delPdIds);
            ProductBindTagCache.DataStatusCache.update(aid, unionPriId, -delCount);
        } finally {
            LockUtil.unlock(aid);
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
    public int getRlPdIdsByRlTagIds(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlTagIds) throws IOException {
        if(Util.isEmptyList(rlTagIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlTagIds is empty;aid=%d;unionPriId=%d;rlTagIds=%s;", aid, unionPriId, rlTagIds);
            return rt;
        }
        FaiList<Integer> rlPdIds;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            rlPdIds = bindTagProc.getRlPdIdsByTagIds(aid, unionPriId, sysType, rlTagIds);
            if(Utils.isEmptyList(rlPdIds)) {
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
        long begin = System.currentTimeMillis();
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
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            list = bindTagProc.getListByConditions(aid, searchArg, ProductBindTagEntity.MANAGE_FIELDS);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        long end = System.currentTimeMillis();
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;consume=%d", flow, aid, unionPriId, list.size(), end - begin);

        return rt;
    }

    /**
     * 根据aid，unionPriId，searchArg从db中查询商品和标签的关联数据
     * @param searchArg 查询的条件
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getBindTagFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
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
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            searchArg.matcher = searchArg.matcher == null ? new ParamMatcher() : searchArg.matcher;
            searchArg.matcher.and(ProductBindTagEntity.Info.AID, ParamMatcher.EQ, aid);
            searchArg.matcher.and(ProductBindTagEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

            list = bindTagProc.getListByConditions(aid, searchArg, ProductBindTagEntity.MANAGE_FIELDS);
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
        long end = System.currentTimeMillis();
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;consume=%d", flow, aid, unionPriId, list.size(), end - begin);

        return rt;
    }
}
