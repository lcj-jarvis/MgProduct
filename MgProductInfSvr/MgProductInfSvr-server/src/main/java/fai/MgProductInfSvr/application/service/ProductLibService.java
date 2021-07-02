package fai.MgProductInfSvr.application.service;

import fai.MgProductInfSvr.domain.serviceproc.ProductLibProc;
import fai.MgProductInfSvr.interfaces.dto.ProductLibDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;

import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-01 11:25
 */
public class ProductLibService extends MgProductInfService{

    @SuccessRt(value = Errno.OK)
    public int addProductLib(FaiSession session, int flow, int aid, int tid, int siteId,
                             int lgId, int keepPriId1, Param info) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(Errno.ARGS_ERROR, "args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // 添加商品分类数据,获取新增商品的库id和库业务id
        ProductLibProc libProc = new ProductLibProc(flow);
        Ref<Integer> libIdRef = new Ref<>();
        Ref<Integer> rlLibIdRef = new Ref<>();
        libProc.addProductLib(aid, tid, unionPriId, info, libIdRef, rlLibIdRef);

        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductLibDto.Key.LIB_ID, libIdRef.value);
        sendBuf.putInt(ProductLibDto.Key.RL_LIB_ID, rlLibIdRef.value);
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int delPdLibList(FaiSession session, int flow, int aid, int tid, int siteId,
                              int lgId, int keepPriId1, FaiList<Integer> rlLibIds) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductLibProc libProc = new ProductLibProc(flow);
        libProc.delLibList(aid, unionPriId, rlLibIds);

        /*ProductBasicProc basicProc = new ProductBasicProc(flow);
        int rt = basicProc.delPdBindGroup(aid, unionPriId, rlLibIds);
        if(rt != Errno.OK) {
            Oss.logAlarm("del pd bind group err;aid=" + aid);
            Log.logErr("del pd bind group err;aid=%d;uid=%d;rlLibIds=%s;", aid, unionPriId, rlLibIds);
            return rt;
        }*/

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int setPdLibList(FaiSession session, int flow, int aid, int tid, int siteId,
                            int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductLibProc libProc = new ProductLibProc(flow);
        libProc.setLibList(aid, unionPriId, updaterList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int getPdLibList(FaiSession session, int flow, int aid, int tid, int siteId,
                            int lgId, int keepPriId1, SearchArg searchArg) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductLibProc libProc = new ProductLibProc(flow);
        FaiList<Param> list = libProc.getLibList(aid, unionPriId, searchArg);
        if(list.isEmpty()) {
            return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductLibDto.Key.INFO_LIST, ProductLibDto.getPdLibDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductLibDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;size=%d;", flow, aid, unionPriId, list.size());
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int unionSetLibList(FaiSession session, int flow, int aid, int tid, int siteId,
                               int lgId, int keepPriId1,
                               FaiList<Param> addInfoList,
                               FaiList<ParamUpdater> updaterList,
                               FaiList<Integer> delRlLibIds) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
        ProductLibProc libProc = new ProductLibProc(flow);

        FaiList<Integer> rlLibIdsRef = libProc.unionSetLibList(aid, tid, unionPriId, addInfoList, updaterList, delRlLibIds);
        FaiBuffer sendBuf = new FaiBuffer(true);

        if (!Util.isEmptyList(rlLibIdsRef)) {
            rlLibIdsRef.toBuffer(sendBuf, ProductLibDto.Key.RL_LIB_IDS);
        }
        session.write(sendBuf);
        Log.logStd("unionSetLibList ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    /**
     * 获取所有的库业务表的数据
     */
    public int getPdRelLibList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
        ProductLibProc libProc = new ProductLibProc(flow);

        FaiList<Param> list = libProc.getRelLibList(aid, unionPriId);
        if(list.isEmpty()) {
            return Errno.NOT_FOUND;
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductLibDto.Key.INFO_LIST, ProductLibDto.getPdRelLibDto());
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;size=%d;", flow, aid, unionPriId, list.size());
        return Errno.OK;
    }
}
