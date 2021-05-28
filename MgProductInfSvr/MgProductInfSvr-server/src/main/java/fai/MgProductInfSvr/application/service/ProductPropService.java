package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductPropProc;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import org.apache.tools.ant.taskdefs.condition.Os;

import java.io.IOException;

public class ProductPropService extends MgProductInfService {

    /**
     * 添加商品参数以及参数值
     */
    public int addPropInfoWithVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> proValList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            Ref<Integer> rlPropIdRef = new Ref<Integer>();
            //添加商品参数数据
            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.addPropInfoWithVal(aid, tid, unionPriId, libId, propInfo, proValList, rlPropIdRef);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropIdRef.value);
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改商品参数以及参数值
     * 参数值的修改包括 增删改
     */
    public int setPropAndVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, ParamUpdater propUpdater, FaiList<Param> addValList, FaiList<ParamUpdater> setValList, FaiList<Integer> delValIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            boolean argsError = true;

            ProductPropProc productPropProc = new ProductPropProc(flow);
            if(propUpdater != null && !propUpdater.isEmpty()) {
                argsError = false;
                rt = productPropProc.setPropInfo(aid, tid, unionPriId, libId, rlPropId, propUpdater);
                if(rt != Errno.OK) {
                    return rt;
                }
            }

            if(addValList == null) {
                addValList = new FaiList<Param>();
            }
            if(setValList == null) {
                setValList = new FaiList<ParamUpdater>();
            }
            if(delValIds == null) {
                delValIds = new FaiList<Integer>();
            }
            if(!addValList.isEmpty() || !setValList.isEmpty() || !delValIds.isEmpty()) {
                argsError = false;
                rt = productPropProc.setPropValList(aid, tid, unionPriId, libId, rlPropId, addValList, setValList, delValIds);
                if(rt != Errno.OK) {
                    return rt;
                }
                if(!delValIds.isEmpty()) {
                    ProductBasicProc basicProc = new ProductBasicProc(flow);
                    rt = basicProc.delPdBindProp(aid, unionPriId, rlPropId, delValIds);
                    if(rt != Errno.OK) {
                        Oss.logAlarm("del pd bind prop err;aid=" + aid);
                        Log.logErr("del pd bind prop err;aid=%d;uid=%d;rlPropId=%d;delValIds=%s;", aid, unionPriId, rlPropId, delValIds);
                        return rt;
                    }
                }
            }
            if(argsError) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "all arg is null error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, SearchArg searchArg) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductPropProc productPropProc = new ProductPropProc(flow);
            FaiList<Param> list = new FaiList<Param>();
            rt = productPropProc.getPropList(aid, tid, unionPriId, libId, searchArg, list);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductPropDto.Key.PROP_LIST, ProductPropDto.getPropInfoDto());
            if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
                sendBuf.putInt(ProductPropDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
            }
            session.write(sendBuf);
            Log.logDbg("get list ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int addPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> list) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(list == null || list.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            Ref<FaiList<Integer>> rlPropIdRef = new Ref<FaiList<Integer>>();
            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.addPropList(aid, tid, unionPriId, libId, list, rlPropIdRef);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPropIds = rlPropIdRef.value;

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            if(rlPropIds != null) {
                rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
            }
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int setPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.setPropList(aid, tid, unionPriId, libId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int delPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> idList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(idList == null || idList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.delPropList(aid, tid, unionPriId, libId, idList);
            if(rt != Errno.OK) {
                return rt;
            }

            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.delPdBindProp(aid, unionPriId, idList);
            if(rt != Errno.OK) {
                Oss.logAlarm("del pd bind prop err;aid=" + aid);
                Log.logErr("del pd bind prop err;aid=%d;uid=%d;delPropIds=%s;", aid, unionPriId, idList);
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;idList=%s;", flow, aid, unionPriId, idList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 合并 商品参数（增、删、改） 接口
     */
    public int unionSetPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            Ref<FaiList<Integer>> idsRef = new Ref<>();
            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.unionSetPropList(aid, tid, unionPriId, libId, addList, updaterList, delList, idsRef);
            if (rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPropIds = idsRef.value;

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            if(rlPropIds != null) {
                rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
            }
            session.write(sendBuf);
            Log.logStd("unionSetPropList ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }


    /**
     * 根据商品参数业务id，获取商品参数值列表
     */
    public int getPropValList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> rlPropIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(rlPropIds == null || rlPropIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductPropProc productPropProc = new ProductPropProc(flow);
            FaiList<Param> list = new FaiList<Param>();
            rt = productPropProc.getPropValList(aid, tid, unionPriId, libId, rlPropIds, list);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 批量修改(包括增、删、改)指定商品库的商品参数值列表
     */
    public int setPropValList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, FaiList<Param> addValList, FaiList<ParamUpdater> setValList, FaiList<Integer> delValIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(addValList == null) {
                addValList = new FaiList<Param>();
            }
            if(setValList == null) {
                setValList = new FaiList<ParamUpdater>();
            }
            if(delValIds == null) {
                delValIds = new FaiList<Integer>();
            }
            if(addValList.isEmpty() && setValList.isEmpty() && delValIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "all arg is null error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductPropProc productPropProc = new ProductPropProc(flow);
            rt = productPropProc.setPropValList(aid, tid, unionPriId, libId, rlPropId, addValList, setValList, delValIds);
            if(rt != Errno.OK) {
                return rt;
            }

            if(!delValIds.isEmpty()) {
                ProductBasicProc basicProc = new ProductBasicProc(flow);
                rt = basicProc.delPdBindProp(aid, unionPriId, rlPropId, delValIds);
                if(rt != Errno.OK) {
                    Oss.logAlarm("del pd bind prop err;aid=" + aid);
                    Log.logErr("del pd bind prop err;aid=%d;uid=%d;rlPropId=%d;delValIds=%s;", aid, unionPriId, rlPropId, delValIds);
                    return rt;
                }
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
