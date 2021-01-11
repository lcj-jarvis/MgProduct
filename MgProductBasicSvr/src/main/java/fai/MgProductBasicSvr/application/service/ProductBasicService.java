package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.repository.*;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindPropProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.repository.TransactionCtrl;
import fai.comm.middleground.service.ServicePub;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;

public class ProductBasicService extends ServicePub {

    public int getPdBindProp(FaiSession session, int flow, int aid, int unionPriId, int tid, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(session);
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
                ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(session);
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
            ProductBindPropDaoCtrl bindPropDao = ProductBindPropDaoCtrl.getInstance(session);
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

    /**
     * 取消商品业务关联
     */
    public int batchDelPdRelBind(FaiSession session, int flow ,int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error rlPdIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, rlPdIds);
                return rt;
            }

            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                try {
                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    ProductRelCacheCtrl.setExpire(aid, unionPriId);
                    rt = relProc.delProductRel(aid, unionPriId, rlPdIds);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "del product rels error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%d;", flow, aid, unionPriId, rlPdIds);
                        return rt;
                    }
                    // 删除缓存
                    ProductRelCacheCtrl.delCacheList(aid, unionPriId, rlPdIds);
                }finally {
                    relDao.closeDao();
                }
            }finally {
                lock.unlock();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("delProductRels ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 删除商品数据，同时删除所有相关业务关联数据
     */
    public int delProductList(FaiSession session, int flow ,int aid, int tid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            if(rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error rlPdIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, rlPdIds);
                return rt;
            }
            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                //统一控制事务
                TransactionCtrl transactionCtrl = new TransactionCtrl();
                FaiList<Integer> pdIdList = new FaiList<Integer>();
                FaiList<Integer> returnUids = new FaiList<Integer>();
                try {
                    ProductDaoCtrl pdDao = ProductDaoCtrl.getInstance(session);
                    ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                    transactionCtrl.register(pdDao);
                    transactionCtrl.register(relDao);
                    transactionCtrl.setAutoCommit(false);

                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<FaiList<Param>> relListRef = new Ref<FaiList<Param>>();
                    rt = relProc.getProductRelList(aid, unionPriId, rlPdIds, relListRef);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "getIdRelList error;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
                        return rt;
                    }
                    FaiList<Param> relList = relListRef.value;
                    for(Param idRel : relList) {
                        int pdId = idRel.getInt(ProductRelEntity.Info.PD_ID);
                        pdIdList.add(pdId);
                    }
                    // 删除pdIdList的所有业务关联数据
                    rt = relProc.delProductRelByPdId(aid, pdIdList, returnUids);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "del product rel by pdIds error;flow=%d;aid=%d;pdids=%s;", aid, pdIdList);
                        return rt;
                    }
                    ProductProc pdProc = new ProductProc(flow, pdDao);
                    // 删除商品数据
                    rt = pdProc.deleteProductList(aid, tid, pdIdList);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "del product list error;flow=%d;aid=%d;unionPriId=%d;pdIdList=%d;", flow, aid, unionPriId, pdIdList);
                        return rt;
                    }
                }finally {
                    if(rt == Errno.OK) {
                        transactionCtrl.commit();
                        // 清缓存
                        if(!returnUids.isEmpty()) {
                            for(Integer curUnionPriId : returnUids) {
                                // 删除 mgProductRel 缓存
                                ProductRelCacheCtrl.delCacheList(aid, curUnionPriId, rlPdIds);
                                // 删除 cache: aid+unionPriId+pdId -> rlPdId
                                ProductRelCacheCtrl.delRlIdRelCache(aid, curUnionPriId, pdIdList);
                            }
                        }
                        ProductCacheCtrl.delCacheList(aid, pdIdList);
                    }else {
                        transactionCtrl.rollback();
                    }
                    transactionCtrl.closeDao();
                }
            }finally {
                lock.unlock();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("delProductList ok;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }

        return rt;
    }

    /**
     * 根据业务商品id，获取商品业务关系数据
     */
    public int getRelInfoByRlId(FaiSession session, int flow, int aid, int unionPriId, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(rlPdId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, rlPdId is not valid;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                return rt;
            }
            Ref<Param> relInfoRef = new Ref<Param>();
            //统一控制事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductRelDaoCtrl relDaoCtrl = ProductRelDaoCtrl.getInstance(session);
                transactionCtrl.register(relDaoCtrl);
                ProductRelProc relProc = new ProductRelProc(flow, relDaoCtrl);
                rt = relProc.getProductRel(aid, unionPriId, rlPdId, relInfoRef);
                if(rt != Errno.OK) {
                    if(rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "get product rel info error;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                    }
                    return rt;
                }
            } finally {
                transactionCtrl.closeDao();
            }
            Param relInfo = relInfoRef.value;
            FaiBuffer sendBuf = new FaiBuffer(true);
            relInfo.toBuffer(sendBuf, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
            session.write(sendBuf);
            rt = Errno.OK;
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdId);

        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据业务商品id集合，获取商品业务关系数据集合
     */
    public int getRelListByRlIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, pdIds is empty;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
                return rt;
            }

            Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
            //统一控制事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductRelDaoCtrl relDaoCtrl = ProductRelDaoCtrl.getInstance(session);
                transactionCtrl.register(relDaoCtrl);
                ProductRelProc relProc = new ProductRelProc(flow, relDaoCtrl);
                rt = relProc.getProductRelList(aid, unionPriId, rlPdIds, listRef);
                if(rt != Errno.OK) {
                    if(rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "get product rel info error;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
                    }
                    return rt;
                }
            } finally {
                transactionCtrl.closeDao();
            }
            FaiList<Param> list = listRef.value;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductRelDto.Key.INFO_LIST, ProductRelDto.getInfoDto());
            session.write(sendBuf);
            rt = Errno.OK;
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);

        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 根据pdIds获取业务关联数据，仅获取有限的字段，aid+unionPriId+pdId+rlPdId
     */
    public int getReducedRelsByPdIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> pdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(pdIds == null || pdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, pdIds is empty;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
                return rt;
            }

            Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
            //统一控制事务
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                ProductRelDaoCtrl relDaoCtrl = ProductRelDaoCtrl.getInstance(session);
                transactionCtrl.register(relDaoCtrl);
                ProductRelProc relProc = new ProductRelProc(flow, relDaoCtrl);
                rt = relProc.getRlPdIdList(aid, unionPriId, pdIds, listRef);
                if(rt != Errno.OK) {
                    if(rt != Errno.NOT_FOUND) {
                        Log.logErr(rt, "get product rel info error;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
                    }
                    return rt;
                }
            } finally {
                transactionCtrl.closeDao();
            }
            FaiList<Param> list = listRef.value;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
            session.write(sendBuf);
            rt = Errno.OK;
            Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;pdIds=%s;", flow, aid, unionPriId, pdIds);

        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     */
    public int addProductAndRel(FaiSession session, int flow, int aid, int tid, int unionPriId, Param info) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
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

            Param pdData = new Param();
            pdData.setInt(ProductEntity.Info.AID, aid);
            pdData.setInt(ProductEntity.Info.SOURCE_TID, sourceTid);
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

            Integer rlPdId = 0;
            Integer pdId = 0;

            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                //统一控制事务
                TransactionCtrl transactionCtrl = new TransactionCtrl();
                try {
                    ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                    ProductDaoCtrl pdDao = ProductDaoCtrl.getInstance(session);
                    transactionCtrl.register(relDao);
                    transactionCtrl.register(pdDao);
                    transactionCtrl.setAutoCommit(false);
                    // 新增商品数据
                    ProductProc pdProc = new ProductProc(flow, pdDao);
                    Ref<Integer> pdIdRef = new Ref<Integer>();
                    rt = pdProc.addProduct(aid, pdData, pdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    pdId = pdIdRef.value;

                    relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                    // 新增业务关系
                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<Integer> rlPdIdRef = new Ref<Integer>();
                    rt = relProc.addProductRel(aid, unionPriId, relData, rlPdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    rlPdId = rlPdIdRef.value;
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
                        ProductDaoCtrl.clearIdBuilderCache(aid);
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }else {
                        transactionCtrl.commit();
                        // 更新缓存
                        ProductCacheCtrl.addCache(aid, pdData);
                        ProductRelCacheCtrl.addCache(aid, unionPriId, relData);
                    }
                    transactionCtrl.closeDao();
                }

            } finally {
                lock.unlock();
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            sendBuf.putInt(ProductRelDto.Key.PD_ID, pdId);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 新增商品业务关联
     */
    public int bindProductRel(FaiSession session, int flow, int aid, int tid, int unionPriId, Param bindRlPdInfo, Param info) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
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

            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                //统一控制事务
                TransactionCtrl transactionCtrl = new TransactionCtrl();
                try {
                    ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                    transactionCtrl.register(relDao);
                    transactionCtrl.setAutoCommit(false);

                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<Param> bindRelRef = new Ref<Param>();
                    rt = relProc.getProductRel(aid, bindUniPriId, bindRlPdId, bindRelRef);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                        rt = Errno.ERROR;
                        return rt;
                    }
                    int pdId = bindRelRef.value.getInt(ProductRelEntity.Info.PD_ID);
                    relData.setInt(ProductRelEntity.Info.PD_ID, pdId);

                    // 新增商品业务关系
                    Ref<Integer> rlPdIdRef = new Ref<Integer>();
                    rt = relProc.addProductRel(aid, unionPriId, relData, rlPdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    rlPdId = rlPdIdRef.value;
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
                        ProductDaoCtrl.clearIdBuilderCache(aid);
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }else {
                        transactionCtrl.commit();
                        // 更新缓存
                        ProductRelCacheCtrl.addCache(aid, unionPriId, relData);
                    }
                    transactionCtrl.closeDao();
                }

            } finally {
                lock.unlock();
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(FaiSession session, int flow, int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
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

            FaiList<Param> relDataList = new FaiList<Param>();
            HashSet<Integer> unionPriIds = new HashSet<Integer>();
            for(Param info : infoList) {
                Integer unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                if(unionPriId == null) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, unionPriId is null;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    return rt;
                }
                unionPriIds.add(unionPriId);
                Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
                if(addedSid == null) {
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

                relDataList.add(relData);
            }

            FaiList<Integer> rlPdIds = new FaiList<Integer>();

            Lock lock = LockUtil.getLock(aid);
            lock.lock();
            try {
                //统一控制事务
                TransactionCtrl transactionCtrl = new TransactionCtrl();
                try {
                    ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                    transactionCtrl.register(relDao);
                    transactionCtrl.setAutoCommit(false);

                    // 先校验商品数据是否存在
                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<Param> bindRelRef = new Ref<Param>();
                    rt = relProc.getProductRel(aid, bindUniPriId, bindRlPdId, bindRelRef);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "get bind pd rel info fail;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                        rt = Errno.ERROR;
                        return rt;
                    }
                    int pdId = bindRelRef.value.getInt(ProductRelEntity.Info.PD_ID);

                    // 新增商品业务关系
                    Ref<FaiList<Integer>> rlPdIdsRef = new Ref<FaiList<Integer>>();
                    rt = relProc.batchAddProductRel(aid, pdId, relDataList.clone(), rlPdIdsRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    rlPdIds = rlPdIdsRef.value;
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
                        for(Integer unionPriId : unionPriIds) {
                            ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                        }
                    }else {
                        transactionCtrl.commit();
                        // 删除缓存
                        ProductRelCacheCtrl.delCacheByUids(aid, unionPriIds);
                    }
                    transactionCtrl.closeDao();
                }

            } finally {
                lock.unlock();
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            rlPdIds.toBuffer(sendBuf, ProductRelDto.Key.RL_PD_IDS);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
