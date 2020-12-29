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
                FaiList<Param> idRelList = null;
                FaiList<Integer> pdIdList = new FaiList<>();
                try {
                    ProductDaoCtrl pdDao = ProductDaoCtrl.getInstance(session);
                    ProductRelDaoCtrl relDao = ProductRelDaoCtrl.getInstance(session);
                    transactionCtrl.register(pdDao);
                    transactionCtrl.register(relDao);
                    transactionCtrl.setAutoCommit(false);

                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<FaiList<Param>> idRelListRef = new Ref<>();
                    rt = relProc.getIdRelList(aid, rlPdIds, idRelListRef);
                    if(rt != Errno.OK) {
                        Log.logErr(rt, "getIdRelList error;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", flow, aid, unionPriId, rlPdIds);
                        return rt;
                    }
                    idRelList = idRelListRef.value;
                    for(Param idRel : idRelList) {
                        int pdId = idRel.getInt(ProductRelEntity.Info.PD_ID);
                        pdIdList.add(pdId);
                    }
                    // 删除pdIdList的所有业务关联数据
                    rt = relProc.delProductRelByPdId(aid, pdIdList);
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
                        if(idRelList != null && !idRelList.isEmpty()) {
                            for(Param idRel : idRelList) {
                                int rlPdId = idRel.getInt(ProductRelEntity.Info.RL_PD_ID);
                                int curUnionPriId = idRel.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                                ProductRelCacheCtrl.delCache(aid, curUnionPriId, rlPdId);
                            }
                        }
                        ProductRelCacheCtrl.delIdRelCache(aid, rlPdIds);
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
            Ref<Param> relInfoRef = new Ref<>();
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
            Integer addedSid = info.getInt(ProductRelEntity.Info.ADD_SID);
            if(addedSid == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, addedSid is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return rt;
            }

            String name = info.getString(ProductEntity.Info.NAME);
            if(!MgProductCheck.checkProductName(name)) {
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
                    Ref<Integer> pdIdRef = new Ref<>();
                    rt = pdProc.addProduct(aid, relData, pdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    pdId = pdIdRef.value;

                    relData.setInt(ProductRelEntity.Info.PD_ID, pdId);
                    // 新增业务关系
                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<Integer> rlPdIdRef = new Ref<>();
                    rt = relProc.addProductRel(aid, unionPriId, relData, rlPdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    rlPdId = rlPdIdRef.value;
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
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
    public int bindProductRel(FaiSession session, int flow, int aid, int tid, int unionPriId, Param info) throws IOException {
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
            Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            if(pdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, pdId is null;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return rt;
            }
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

            Integer rlPdId = 0;

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

                    // 先校验商品数据是否存在
                    ProductProc pdProc = new ProductProc(flow, pdDao);
                    Ref<Param> pdInfoRef = new Ref<>();
                    rt = pdProc.getProductInfo(aid, pdId, pdInfoRef);
                    if(rt != Errno.OK || Str.isEmpty(pdInfoRef.value)) {
                        Log.logErr(rt, "get pd info error;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                        return rt;
                    }

                    // 新增商品业务关系
                    ProductRelProc relProc = new ProductRelProc(flow, relDao);
                    Ref<Integer> rlPdIdRef = new Ref<>();
                    rt = relProc.addProductRel(aid, unionPriId, relData, rlPdIdRef);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    rlPdId = rlPdIdRef.value;
                } finally {
                    if(rt != Errno.OK) {
                        transactionCtrl.rollback();
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
}
