package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.*;
import fai.MgProductBasicSvr.domain.repository.cache.*;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.MgProductBasicSvr.domain.serviceproc.*;
import fai.MgProductBasicSvr.interfaces.dto.ProductDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.infutil.MgConfPool;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

/**
 * 操作商品基础数据
 */
public class ProductBasicService extends ServicePub {

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getProductList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error rlPdIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, rlPdIds);
            return rt;
        }

        FaiList<Param> relList = null;
        FaiList<Param> pdList = null;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            // 获取商品业务关系表数据
            relList = relProc.getProductRelList(aid, unionPriId, rlPdIds);
            if(Util.isEmptyList(relList)) {
                return Errno.NOT_FOUND;
            }
            FaiList<Integer> pdIds = relList.stream().map(info -> info.getInt(ProductRelEntity.Info.PD_ID)).collect(Collectors.toCollection(FaiList::new));
            // 获取商品表数据
            ProductProc pdProc = new ProductProc(flow, aid, tc);
            pdList = pdProc.getProductList(aid, pdIds);
        } finally {
            tc.closeDao();
        }
        if(Util.isEmptyList(pdList)) {
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
            relInfo.assign(pdInfo);
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        relList.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getRelAndPdDto());
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        return Errno.OK;
    }

    /**
     * 取消商品业务关联
     * softDel: 是否软删除，软删除实际只是置起标志位
     */
    @SuccessRt(value = Errno.OK)
    public int batchDelPdRelBind(FaiSession session, int flow ,int aid, int unionPriId, FaiList<Integer> rlPdIds, boolean softDel) throws IOException {
        int rt;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error rlPdIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, rlPdIds);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                ProductRelCacheCtrl.InfoCache.setExpire(aid, unionPriId);
                int delCount = relProc.delProductRel(aid, unionPriId, rlPdIds, softDel);
                // 删除参数、分类、标签关联数据
                int delGroupCount = 0;
                int delTagCount = 0;
                int delPropCount = 0;
                if(!softDel) {
                    if(useProductGroup()) {
                        ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                        ProductBindGroupCache.setExpire(aid, unionPriId);
                        delGroupCount = bindGroupProc.delPdBindGroupList(aid, unionPriId, rlPdIds);
                    }

                    if(useProductTag()) {
                        ProductBindTagProc bindGroupProc = new ProductBindTagProc(flow, aid, tc);
                        ProductBindTagCache.setExpire(aid, unionPriId);
                        delGroupCount = bindGroupProc.delPdBindTagList(aid, unionPriId, rlPdIds);
                    }

                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    ParamMatcher matcher = new ParamMatcher(ProductBindPropEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIds);
                    delPropCount = bindPropProc.delPdBindProp(aid, unionPriId, matcher);
                }
                commit = true;
                tc.commit();

                // 删除缓存
                ProductRelCacheCtrl.InfoCache.delCacheList(aid, unionPriId, rlPdIds);
                ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, -delCount); // 更新数据状态缓存
                if(!softDel) {
                    // 处理商品分类关联数据缓存
                    ProductBindGroupCache.delCache(aid, unionPriId);
                    ProductBindGroupCache.DataStatusCache.update(aid, unionPriId, -delGroupCount);
                    // 处理商品标签关联数据缓存
                    HashSet<Integer> cacheRlPdIds = new HashSet<>(rlPdIds);
                    rlPdIds = new FaiList<>(cacheRlPdIds);
                    ProductBindTagCache.delCacheList(aid, unionPriId, rlPdIds);
                    ProductBindTagCache.DataStatusCache.update(aid, unionPriId, -delTagCount);
                    // 处理商品参数关联数据缓存
                    ProductBindPropCache.delCacheList(aid, unionPriId, cacheRlPdIds);
                    ProductBindPropCache.DataStatusCache.update(aid, unionPriId, -delPropCount);
                }
            }finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
        }finally {
            lock.unlock();
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
    public int delProductList(FaiSession session, int flow ,int aid, int tid, int unionPriId, FaiList<Integer> rlPdIds, boolean softDel) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlPdIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, rlPdIds);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            int delPdCount = 0;
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            FaiList<Integer> pdIdList = new FaiList<Integer>();
            FaiList<Param> delRelInfos = new FaiList<>();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                FaiList<Param> relList = relProc.getProductRelList(aid, unionPriId, rlPdIds);
                if(relList == null || relList.isEmpty()) {
                    rt = Errno.NOT_FOUND;
                    Log.logErr(rt, "getIdRelList isEmpty;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
                    return rt;
                }
                for(Param idRel : relList) {
                    int pdId = idRel.getInt(ProductRelEntity.Info.PD_ID);
                    pdIdList.add(pdId);
                }
                // 删除pdIdList的所有业务关联数据
                delRelInfos = relProc.delProductRelByPdId(aid, pdIdList, softDel, true);

                ProductProc pdProc = new ProductProc(flow, aid, tc);
                // 删除商品数据
                delPdCount = pdProc.deleteProductList(aid, tid, pdIdList, softDel);

                if(!softDel) {
                    // 删除参数关联
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    bindPropProc.delPdBindProp(aid, pdIdList);

                    if(useProductGroup()) {
                        // 删除分类关联
                        ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                        bindGroupProc.delPdBindGroupList(aid, pdIdList);
                    }

                    if(useProductTag()) {
                        // 删除标签关联
                        ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                        bindTagProc.delPdBindTagList(aid, pdIdList);
                    }
                }

                commit = true;
                tc.commit();
            }finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 清缓存
            if(!Util.isEmptyList(delRelInfos)) {
                HashSet<Integer> uids = new HashSet<>();
                for(Param info : delRelInfos) {
                    FaiList<Integer> rlPdIdList = new FaiList<>();
                    int curUnionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                    int curRlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                    uids.add(curUnionPriId);
                    rlPdIdList.add(curRlPdId);
                    // mgProductRel 缓存
                    ProductRelCacheCtrl.InfoCache.delCache(aid, curUnionPriId, curRlPdId);
                    // cache: aid+unionPriId+pdId -> rlPdId
                    ProductRelCacheCtrl.RlIdRelCache.delCache(aid, curUnionPriId, pdIdList);
                    // 参数关联缓存
                    ProductBindPropCache.delCache(aid, curUnionPriId, curRlPdId);
                    // 分类关联缓存
                    ProductBindGroupCache.delCache(aid, curUnionPriId);
                    //标签关联缓存
                    ProductBindTagCache.delCacheList(aid, curUnionPriId, rlPdIdList);
                }

                // 删除 数据状态dataStatus 缓存
                ProductRelCacheCtrl.DataStatusCache.del(aid, uids);
                ProductBindPropCache.DataStatusCache.del(aid, uids);
                ProductBindGroupCache.DataStatusCache.del(aid, uids);
                ProductBindTagCache.DataStatusCache.del(aid, uids);
            }
            ProductCacheCtrl.InfoCache.delCacheList(aid, pdIdList);
            ProductCacheCtrl.DataStatusCache.update(aid, -delPdCount); // 更新数据状态缓存
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("delProductList ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        return rt;
    }

    /**
     * 删除商品数据关联数据
     */
    @SuccessRt(value = Errno.OK)
    public int clearRelData(FaiSession session, int flow ,int aid, int unionPriId, boolean softDel) throws IOException {
        int rt;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
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
                    bindPropProc.delPdBindProp(aid, unionPriId, null);

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
                tc.commit();
            }finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(aid);
        }finally {
            lock.unlock();
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
        if(Util.isEmptyList(unionPriIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, unionPriIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, unionPriIds);
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
                tc.commit();
            }finally {
                if(!commit){
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 清缓存
            CacheCtrl.clearCacheVersion(aid);

        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clear acct ok;flow=%d;aid=%d;unionPriIds=%s;", flow, aid, unionPriIds);

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setSingle(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, ParamUpdater recvUpdater) throws IOException {
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
        if(relUpdate == null && pdUpdate == null) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, recvUpdater is not valid;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            return rt;
        }
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            int pdId = 0;
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                if(relUpdate != null) {
                    ProductRelCacheCtrl.InfoCache.setExpire(aid, unionPriId); // 设置过期时间，最大努力的避免脏数据
                    relProc.setSingle(aid, unionPriId, rlPdId, relUpdate);
                }
                if(pdUpdate != null) {
                    ProductCacheCtrl.InfoCache.setExpire(aid); // 设置过期时间，最大努力的避免脏数据
                    Param relInfo = relProc.getProductRel(aid, unionPriId, rlPdId);
                    if(Str.isEmpty(relInfo)) {
                        rt = Errno.NOT_FOUND;
                        Log.logErr(rt, "getIdRel isEmpty;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdId);
                        return rt;
                    }
                    pdId = relInfo.getInt(ProductRelEntity.Info.PD_ID);
                    ProductProc pdProc = new ProductProc(flow, aid, tc);
                    pdProc.setSingle(aid, pdId, pdUpdate);
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
                ProductRelCacheCtrl.InfoCache.updateCache(aid, unionPriId, rlPdId, relUpdate);
            }
            if(pdUpdate != null) {
                ProductCacheCtrl.InfoCache.updateCache(aid, pdId, relUpdate);
            }
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set single ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int setProducts(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds, ParamUpdater recvUpdater) throws IOException {
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
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            FaiList<Integer> pdIdList = new FaiList<>();
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                if(relUpdate != null) {
                    relProc.setPdRels(aid, unionPriId, rlPdIds, relUpdate);
                }
                if(pdUpdate != null) {
                    FaiList<Param> relList = relProc.getProductRelList(aid, unionPriId, rlPdIds);
                    if(relList == null || relList.isEmpty()) {
                        rt = Errno.NOT_FOUND;
                        Log.logErr(rt, "getRelList isEmpty;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
                        return rt;
                    }
                    for(Param idRel : relList) {
                        int pdId = idRel.getInt(ProductRelEntity.Info.PD_ID);
                        pdIdList.add(pdId);
                    }
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
                ProductRelCacheCtrl.InfoCache.delCacheList(aid, unionPriId, rlPdIds);
            }
            if(pdUpdate != null) {
                ProductCacheCtrl.InfoCache.delCacheList(aid, pdIdList);
            }
        }finally {
            lock.unlock();
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set products ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        return rt;
    }

    /**
     * 根据业务商品id，获取商品业务关系数据
     */
    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getRelInfoByRlId(FaiSession session, int flow, int aid, int unionPriId, int rlPdId) throws IOException {
        int rt;
        if(rlPdId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, rlPdId is not valid;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
            return rt;
        }
        Param relInfo = new Param();
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            relInfo = relProc.getProductRel(aid, unionPriId, rlPdId);
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
    public int getRelListByRlIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, pdIds is empty;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }

        FaiList<Param> list;
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            list = relProc.getProductRelList(aid, unionPriId, rlPdIds);
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
        int rt = Errno.ERROR;
        if(pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, pdIds is empty;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
            return rt;
        }

        FaiList<Param> list;
        //统一控制事务
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
            list = relProc.getRlPdIdList(aid, unionPriId, pdIds);
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
     */
    @SuccessRt(value = Errno.OK)
    public int addProductAndRel(FaiSession session, int flow, int aid, int tid, int unionPriId, Param info) throws IOException {
        int rt = Errno.ERROR;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        Param pdData = new Param();
        Param relData = new Param();

        rt = assemblyInfo(flow, aid, tid, unionPriId, info, relData, pdData);
        if(rt != Errno.OK) {
            return rt;
        }

        Integer rlPdId = 0;
        Integer pdId = 0;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // 新增商品数据
                ProductProc pdProc = new ProductProc(flow, aid, tc);
                pdId = pdProc.addProduct(aid, pdData);

                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                // 新增业务关系
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                rlPdId = relProc.addProductRel(aid, unionPriId, relData);

                commit = true;
                tc.commit();
                // 更新缓存
                ProductCacheCtrl.InfoCache.addCache(aid, pdData);
                ProductRelCacheCtrl.InfoCache.addCache(aid, unionPriId, relData);
                ProductCacheCtrl.DataStatusCache.update(aid, 1); // 更新数据状态缓存
                ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, 1); // 更新数据状态缓存
            } finally {
                if(!commit) {
                    tc.rollback();
                    ProductDaoCtrl.clearIdBuilderCache(aid);
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

        } finally {
            lock.unlock();
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
        FaiList<Param> relDataList = new FaiList<Param>();
        FaiList<Param> pdDataList = new FaiList<Param>();
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
        }

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                // 新增商品数据
                ProductProc pdProc = new ProductProc(flow, aid, tc);
                FaiList<Integer> pdIdList = pdProc.batchAddProduct(aid, pdDataList);

                for(int i = 0;i < relDataList.size(); i++) {
                    Param relData = relDataList.get(i);
                    relData.setInt(ProductRelEntity.Info.PD_ID, pdIdList.get(i));
                }
                // 新增业务关系
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                relProc.batchAddProductRel(aid, unionPriId, null, relDataList);

                commit = true;
                tc.commit();
                // 更新缓存
                if(!Util.isEmptyList(pdDataList)) {
                    ProductCacheCtrl.InfoCache.addCacheList(aid, pdDataList);
                    ProductCacheCtrl.DataStatusCache.update(aid, pdDataList.size()); // 更新数据状态缓存
                }
                if(!Util.isEmptyList(relDataList)) {
                    ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, relDataList);
                    ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, relDataList.size()); // 更新数据状态缓存
                }
            } finally {
                if(!commit) {
                    tc.rollback();
                    ProductDaoCtrl.clearIdBuilderCache(aid);
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

        } finally {
            lock.unlock();
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
        int sourceUnionPriId = info.getInt(ProductEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        Calendar now = Calendar.getInstance();
        Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
        Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
        Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        relData.setInt(ProductRelEntity.Info.AID, aid);
        relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
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
        relData.assign(info, ProductRelEntity.Info.PD_TYPE);

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
        Calendar now = Calendar.getInstance();
        Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
        Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
        Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
        Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

        Param relData = new Param();
        relData.setInt(ProductRelEntity.Info.AID, aid);
        relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
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

        Integer rlPdId = 0;
        Integer pdId = 0;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                Param bindRel = relProc.getProductRel(aid, bindUniPriId, bindRlPdId);
                if(Str.isEmpty(bindRel)) {
                    Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    rt = Errno.ERROR;
                    return rt;
                }
                pdId = bindRel.getInt(ProductRelEntity.Info.PD_ID);
                relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                relData.assign(bindRel, ProductRelEntity.Info.PD_TYPE);

                // 新增商品业务关系
                rlPdId = relProc.addProductRel(aid, unionPriId, relData);

                // 新增商品参数绑定关系
                FaiList<Param> bindProps = info.getList(ProductRelEntity.Info.RL_PROPS);
                if(!Util.isEmptyList(bindProps)) {
                    ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                    bindPropProc.addPdBindPropList(aid, unionPriId, rlPdId, pdId, bindProps);
                }

                // 新增商品分类绑定关系
                FaiList<Integer> rlGroupIds = info.getList(ProductRelEntity.Info.RL_GROUP_IDS);
                if(useProductGroup() && !Util.isEmptyList(rlGroupIds)) {
                    ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                    bindGroupProc.addPdBindGroupList(aid, unionPriId, rlPdId, pdId, rlGroupIds);
                    Log.logStd("add bind groupIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlGroupIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlGroupIds);
                }

                // 新增商品分类绑定关系
                FaiList<Integer> rlTagIds = info.getList(ProductRelEntity.Info.RL_TAG_IDS);
                if(useProductTag() && !Util.isEmptyList(rlTagIds)) {
                    ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
                    bindTagProc.addPdBindTagList(aid, unionPriId, rlPdId, pdId, rlTagIds);
                    Log.logStd("add bind rlTagIds ok;flow=%d;aid=%d;uid=%d;rlPdId=%d;pdId=%d;rlTagIds=%s;", flow, aid, unionPriId, rlPdId, pdId, rlTagIds);
                }

                // xid不为空，则开启了分布式事务，saga添加一条记录
                if(!Str.isEmpty(xid)) {
                    // saga
                    Param prop = new Param();
                    prop.setInt(BasicSagaEntity.PropInfo.UNION_PRI_ID, unionPriId);
                    prop.setInt(BasicSagaEntity.PropInfo.RL_PD_ID, rlPdId);
                    prop.assign(info, BasicSagaEntity.PropInfo.RL_PROPS);
                    prop.assign(info, BasicSagaEntity.PropInfo.RL_GROUP_IDS);
                    SagaProc sagaProc = new SagaProc(flow, aid, tc);
                    sagaProc.addInfo(aid, xid, prop);
                }

                commit = true;
                tc.commit();
                // 更新缓存
                ProductRelCacheCtrl.InfoCache.addCache(aid, unionPriId, relData);
                ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, 1); //更新数据状态缓存
            } finally {
                if(!commit) {
                    tc.rollback();
                    ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                }
                tc.closeDao();
            }

        } finally {
            lock.unlock();
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
        int rt = Errno.ERROR;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                SagaProc sagaProc = new SagaProc(flow, aid, tc);
                Param sagaInfo = sagaProc.getInfoWithAdd(xid, branchId);
                if(sagaInfo == null) {
                    commit = true;
                    rt = Errno.OK;
                    return rt;
                }

                int status = sagaInfo.getInt(BasicSagaEntity.Info.STATUS);
                if(status == BasicSagaValObj.Status.ROLLBACK_OK) {
                    commit = true;
                    rt = Errno.OK;
                    Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                    return rt;
                }

                Param prop = Param.parseParam(sagaInfo.getString(BasicSagaEntity.Info.PROP));
                // 执行回滚逻辑
                if(!Str.isEmpty(prop)) {
                    int rlPdId = prop.getInt(BasicSagaEntity.PropInfo.RL_PD_ID);
                    int unionPriId = prop.getInt(BasicSagaEntity.PropInfo.UNION_PRI_ID);
                    ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                    relProc.delProductRel(aid, unionPriId, new FaiList<>(Arrays.asList(rlPdId)), false);

                    // 注意！！！ 这里是restoreMaxId，不是回退为之前的id
                    // 因为saga模式是不保证事务隔离性的，很有可能已经有其他请求添加数据了
                    relProc.restoreMaxId(aid, unionPriId, false);

                    FaiList<Param> rlProps = prop.getList(BasicSagaEntity.PropInfo.RL_PROPS);
                    if(!Util.isEmptyList(rlProps)) {
                        ProductBindPropProc bindPropProc = new ProductBindPropProc(flow, aid, tc);
                        bindPropProc.delPdBindPropList(aid, unionPriId, rlPdId, rlProps);
                    }

                    if(useProductGroup()) {
                        FaiList<Integer> rlGroupIds = prop.getList(BasicSagaEntity.PropInfo.RL_GROUP_IDS);
                        if(!Util.isEmptyList(rlProps)) {
                            ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                            bindGroupProc.delPdBindGroupList(aid, unionPriId, rlPdId, rlGroupIds);
                        }
                    }
                }

                // 更新saga记录status
                sagaProc.setStatus(xid, branchId, BasicSagaValObj.Status.ROLLBACK_OK);

                rt = Errno.OK;
                commit = true;
                tc.commit();
            } finally {
                if(!commit) {
                    tc.rollback();
                }
                tc.closeDao();
            }
            // 更新缓存
            CacheCtrl.clearCacheVersion(aid);
        } finally {
            lock.unlock();
            FaiBuffer sendBuf = new FaiBuffer(true);
            if (rt != Errno.OK) {
                //失败，需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode());
            }else{
                //成功，不需要重试
                sendBuf.putInt(CommDef.Protocol.Key.BRANCH_STATUS, BranchStatus.PhaseTwo_Rollbacked.getCode());
            }
            session.write(sendBuf);
        }
        rt = Errno.OK;
        return rt;
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

            FaiList<Param> curUidList = listOfUid.get(unionPriId);
            if(curUidList == null) {
                curUidList = new FaiList<>();
                listOfUid.put(unionPriId, curUidList);
            }
            curUidList.add(relData);
        }

        Set<Integer> unionPriIds = listOfUid.keySet();

        FaiList<Integer> rlPdIds = new FaiList<Integer>();

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);

                // 先校验商品数据是否存在
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                Param bindRel = relProc.getProductRel(aid, bindUniPriId, bindRlPdId);
                if(Str.isEmpty(bindRel)) {
                    Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    rt = Errno.ERROR;
                    return rt;
                }

                // 新增商品业务关系
                for(Integer unionPriId : unionPriIds) {
                    FaiList<Param> curList = listOfUid.get(unionPriId);
                    if(Util.isEmptyList(curList)) {
                        continue;
                    }
                    FaiList<Integer> tmpRlIds = relProc.batchAddProductRel(aid, unionPriId, bindRel, curList);
                    rlPdIds.addAll(tmpRlIds);
                }

                commit = true;
                tc.commit();
                for(Integer unionPriId : unionPriIds) {
                    FaiList<Param> curList = listOfUid.get(unionPriId);
                    if(Util.isEmptyList(curList)) {
                        continue;
                    }
                    ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, curList);
                    ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, curList.size()); // 更新数据状态缓存
                }
                // 删除缓存
            } finally {
                if(!commit) {
                    tc.rollback();
                    for(Integer unionPriId : unionPriIds) {
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }
                }
                tc.closeDao();
            }

        } finally {
            lock.unlock();
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
                Calendar now = Calendar.getInstance();
                Calendar addedTime = info.getCalendar(ProductRelEntity.Info.ADD_TIME, now);
                Calendar lastUpdateTime = info.getCalendar(ProductRelEntity.Info.LAST_UPDATE_TIME, now);
                Calendar sysCreateTime = info.getCalendar(ProductRelEntity.Info.CREATE_TIME, now);
                Calendar sysUpdateTime = info.getCalendar(ProductRelEntity.Info.UPDATE_TIME, now);

                Param relData = new Param();
                relData.setInt(ProductRelEntity.Info.AID, aid);
                relData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
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
                relData.assign(info, ProductRelEntity.Info.PD_TYPE);

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
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
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
                    if(Util.isEmptyList(list)) {
                        continue;
                    }
                    relProc.batchAddProductRel(aid, unionPriId, null, list);
                }

                commit = true;
                tc.commit();
                // 更新缓存
                for(Integer unionPriId : unionPriIds) {
                    FaiList<Param> list = listOfUid.get(unionPriId);
                    if(!Util.isEmptyList(list)) {
                        continue;
                    }
                    ProductRelCacheCtrl.InfoCache.addCacheList(aid, unionPriId, list);
                    ProductRelCacheCtrl.DataStatusCache.update(aid, unionPriId, list.size()); // 更新数据状态缓存
                }
            } finally {
                if(!commit) {
                    tc.rollback();
                    for(Integer unionPriId : unionPriIds) {
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }
                }
                tc.closeDao();
            }

        } finally {
            lock.unlock();
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
            // 查aid + unionPriId 下所有数据，传入空的searchArg
            SearchArg searchArg = new SearchArg();
            list = relProc.searchFromDb(aid, unionPriId, searchArg, ProductRelEntity.MANAGE_FIELDS);
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
            list = relProc.searchFromDb(aid, unionPriId, searchArg, ProductRelEntity.MANAGE_FIELDS);
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
        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            CacheCtrl.clearCacheVersion(aid);
            ProductCacheCtrl.clearAllCache(aid);
        }finally {
            lock.unlock();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logStd("clear cache ok;flow=%d;aid=%d;", flow, aid);

        return rt;
    }

    public static boolean useProductGroup() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        boolean useProductGroup = mgSwitch.getBoolean("useProductGroup", false);
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
                    unionPriIds = new FaiList<Integer>();
                    pdRels.put(pdId, unionPriIds);
                }
                unionPriIds.add(unionPriId);
            }
        }finally {
            tc.closeDao();
        }
        return pdRels;
    }
}
