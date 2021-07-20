package fai.MgProductInfSvr.application.service;

import fai.MgProductInfSvr.domain.serviceproc.ProductTagProc;
import fai.MgProductInfSvr.interfaces.dto.ProductTagDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;

import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-19 12:03
 */
public class ProductTagService extends MgProductInfService {

    @SuccessRt(value = Errno.OK)
    public int addProductTag(FaiSession session, int flow, int aid, int tid, int siteId, 
                             int lgid, int keepPriId1, Param addInfo) throws IOException {
        int rt;
        if(!FaiValObj.TermId.isValidTid(tid)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(Errno.ARGS_ERROR, "args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);

        // 添加商品标签数据,获取新增商品的库id和库业务id
        ProductTagProc tagProc = new ProductTagProc(flow);
        Ref<Integer> tagIdRef = new Ref<>();
        Ref<Integer> rlTagIdRef = new Ref<>();
        tagProc.addProductTag(aid, tid, unionPriId, addInfo, tagIdRef, rlTagIdRef);

        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductTagDto.Key.TAG_ID, tagIdRef.value);
        sendBuf.putInt(ProductTagDto.Key.RL_TAG_ID, rlTagIdRef.value);
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int delPdTagList(FaiSession session, int flow, int aid, int tid, int siteId, int lgid, int keepPriId1, FaiList<Integer> rlTagIds) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);
        ProductTagProc tagProc = new ProductTagProc(flow);
        tagProc.delTagList(aid, unionPriId, rlTagIds);
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("delete list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }
    
    @SuccessRt(value = Errno.OK)
    public int setPdTagList(FaiSession session, int flow, int aid, int tid, int siteId, int lgid, int keepPriId1, FaiList<ParamUpdater> updaterList) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);
        ProductTagProc tagProc = new ProductTagProc(flow);
        tagProc.setTagList(aid, unionPriId, updaterList);
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("set list ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int getPdTagList(FaiSession session, int flow, int aid, int tid, int siteId, int lgid, int keepPriId1, SearchArg searchArg) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);

        ProductTagProc tagProc = new ProductTagProc(flow);
        FaiList<Param> list = tagProc.getTagList(aid, unionPriId, searchArg);
        if(list.isEmpty()) {
            return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductTagDto.Key.INFO_LIST, ProductTagDto.getPdTagDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductTagDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;size=%d;", flow, aid, unionPriId, list.size());
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int unionSetTagList(FaiSession session, int flow, int aid, int tid, int siteId, int lgid, int keepPriId1, FaiList<Param> addInfoList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delRlTagIds) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);
        ProductTagProc tagProc = new ProductTagProc(flow);

        FaiList<Integer> rlTagIdsRef = tagProc.unionSetTagList(aid, tid, unionPriId, addInfoList, updaterList, delRlTagIds);
        FaiBuffer sendBuf = new FaiBuffer(true);

        if (!Util.isEmptyList(rlTagIdsRef)) {
            rlTagIdsRef.toBuffer(sendBuf, ProductTagDto.Key.RL_TAG_IDS);
        }
        session.write(sendBuf);
        Log.logStd("unionSetTagList ok;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int getPdRlTagList(FaiSession session, int flow, int aid, int tid,
                              int siteId, int lgid, int keepPriId1, SearchArg searchArg) throws IOException {
        // 获取unionPriId
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgid, keepPriId1);
        ProductTagProc tagProc = new ProductTagProc(flow);
        FaiList<Param> list = tagProc.getRlTagList(aid, unionPriId, searchArg);
        if(list.isEmpty()) {
            return Errno.NOT_FOUND;
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductTagDto.Key.INFO_LIST, ProductTagDto.getPdRelTagDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductTagDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        Log.logDbg("get list ok;flow=%d;aid=%d;uid=%d;size=%d;", flow, aid, unionPriId, list.size());
        return Errno.OK;
    }
}
