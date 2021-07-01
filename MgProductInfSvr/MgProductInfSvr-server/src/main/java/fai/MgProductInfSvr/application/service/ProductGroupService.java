package fai.MgProductInfSvr.application.service;

import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductGroupProc;
import fai.MgProductInfSvr.interfaces.dto.ProductGroupDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;

import java.io.IOException;

public class ProductGroupService extends MgProductInfService {

    @SuccessRt(value = Errno.OK)
    public int addProductGroup(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Param info) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(Errno.ARGS_ERROR, "args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // 添加商品分类数据
        ProductGroupProc groupProc = new ProductGroupProc(flow);
        int rlGroupId = groupProc.addProductGroup(aid, tid, unionPriId, info);

        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductGroupDto.Key.RL_GROUP_ID, rlGroupId);
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getPdGroupList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, SearchArg searchArg) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductGroupProc groupProc = new ProductGroupProc(flow);
        FaiList<Param> list = groupProc.getGroupList(aid, unionPriId, searchArg);
        if(list.isEmpty()) {
           return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductGroupDto.Key.INFO_LIST, ProductGroupDto.getPdGroupDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductGroupDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;size=%d;", flow, aid, unionPriId, list.size());
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int setPdGroupList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.setGroupList(aid, unionPriId, updaterList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int delPdGroupList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlGroupIds) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.delGroupList(aid, unionPriId, rlGroupIds);

        ProductBasicProc basicProc = new ProductBasicProc(flow);
        int rt = basicProc.delPdBindGroup(aid, unionPriId, rlGroupIds);
        if(rt != Errno.OK) {
            Oss.logAlarm("del pd bind group err;aid=" + aid);
            Log.logErr("del pd bind group err;aid=%d;uid=%d;rlGroupIds=%s;", aid, unionPriId, rlGroupIds);
            return rt;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int unionSetGroupList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
        ProductGroupProc groupProc = new ProductGroupProc(flow);

        FaiList<Integer> rlGroupIds = groupProc.unionSetGroupList(aid, tid, unionPriId, addList, updaterList, delList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        if (!Util.isEmptyList(rlGroupIds)) {
            rlGroupIds.toBuffer(sendBuf, ProductGroupDto.Key.RL_GROUP_IDS);
        }
        session.write(sendBuf);
        Log.logStd("unionSetGroupList ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }
}
