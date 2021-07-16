package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.entity.ProductBindTagEntity;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindTagProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindTagProc;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindTagDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindTagDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-14 14:32
 */
public class ProductBindTagService extends ServicePub {

    @SuccessRt(value = Errno.OK)
    public int getPdsBindTag(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlPdIds) throws IOException {
        if(Util.isEmptyList(rlPdIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlPdIds is empty;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            return rt;
        }
        TransactionCtrl tc = new TransactionCtrl();
        FaiList<Param> list;
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            list = bindTagProc.getPdBindTagList(aid, unionPriId, rlPdIds);
        }finally {
            tc.closeDao();
        }
        if(Util.isEmptyList(list)) {
            return Errno.NOT_FOUND;
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;unionPriId=%d;ids=%s;", flow, aid, unionPriId, rlPdIds);
        return Errno.OK;
    }

    @SuccessRt(value = Errno.OK)
    public int setPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, FaiList<Integer> addTagIds, FaiList<Integer> delTagIds) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int transactionSetPdBindTag(FaiSession session, int flow, int aid, int unionPriId, int rlPdId, String xid, FaiList<Integer> addTagIds, FaiList<Integer> delTagIds) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int setPdBindTagRollback(FaiSession session, int flow, int aid, String xid, Long branchId) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int delBindTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> delTagIds) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int getRlPdIdsByRlTagIds(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlTagIds) throws IOException {
        if(Util.isEmptyList(rlTagIds)) {
            int rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "args error, rlTagIds is empty;aid=%d;unionPriId=%d;rlTagIds=%s;", aid, unionPriId, rlTagIds);
            return rt;
        }
        FaiList<Integer> rlPdIds;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            rlPdIds = bindTagProc.getRlPdIdsByTagId(aid, unionPriId, rlTagIds);
            if(Util.isEmptyList(rlPdIds)) {
                Log.logDbg("not found;flow=%d;aid=%d;uid=%d;", flow, aid, unionPriId);
                return Errno.NOT_FOUND;
            }
        }finally {
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        rlPdIds.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        Log.logDbg("get ok;flow=%d;aid=%d;uid=%d;rlTagIds=%s;", flow, aid, unionPriId, rlTagIds);
        return Errno.OK;
    }

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

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getAllPdBindTag(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
        int rt;
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
            list = bindTagProc.getListByConditions(aid, unionPriId, null, ProductBindTagEntity.MANAGE_FIELDS, null);
        }finally {
            tc.closeDao();
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductBindTagDto.Key.INFO_LIST, ProductBindTagDto.getInfoDto());
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getBindTagFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> list;
        TransactionCtrl tc = new TransactionCtrl();
        try {
            ProductBindTagProc bindTagProc = new ProductBindTagProc(flow, aid, tc);
            list = bindTagProc.getListByConditions(aid, unionPriId, searchArg, ProductBindTagEntity.MANAGE_FIELDS, null);
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
        Log.logDbg("search from db ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }


}
