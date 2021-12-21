package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgRichTextInfSvr.interfaces.cli.MgRichTextInfCli;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.MgRichTextInfSvr.interfaces.utils.MgRichTextArg;
import fai.MgRichTextInfSvr.interfaces.utils.MgRichTextSearch;
import fai.comm.util.Errno;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.util.Arrays;

public class RichTextProc {
    private int m_flow;
    private MgRichTextInfCli m_cli;

    public RichTextProc(int flow) {
        this.m_flow = flow;
        m_cli = createCli();
    }

    public int addPdRichTexts(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, int pdId, FaiList<Param> richList) {
        int rt;
        if(pdId <= 0 || Utils.isEmptyList(richList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("addPdRichTexts arg err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
            return rt;
        }
        for(Param rich : richList) {
            rich.setInt(MgRichTextEntity.Info.RLID, pdId);
        }
        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid,  tid, siteId, lgId, keepPriId1)
                .setInfoList(richList)
                .setXid(xid)
                .build();
        rt = m_cli.batchAddRichText(mgRichTextArg);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRichText err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
        }
        return rt;
    }

    public void batchAdd(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> richList) {
        int rt;
        if(Utils.isEmptyList(richList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "batchAddRich arg err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;", aid, tid, siteId, lgId, keepPriId1);
        }
        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid,  tid, siteId, lgId, keepPriId1)
                .setInfoList(richList)
                .setXid(xid)
                .build();
        rt = m_cli.batchAddRichText(mgRichTextArg);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRichText err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;", aid, tid, siteId, lgId, keepPriId1);
        }
    }

    public int batchDel(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> pdIds) {
        int rt;
        if(Utils.isEmptyList(pdIds)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("batchDel arg err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdIds=%s;", aid, tid, siteId, lgId, keepPriId1, pdIds);
            return rt;
        }

        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid,  tid, siteId, lgId, keepPriId1)
                .setBizList(new FaiList<>(Arrays.asList(MgRichTextValObj.Biz.PRODUCT)))
                .setRlIdList(pdIds)
                .setXid(xid)
                .build();
        rt = m_cli.delRichText(mgRichTextArg);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchDel err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdIds=%s;", aid, tid, siteId, lgId, keepPriId1, pdIds);
        }
        return rt;
    }
    public int migrateDel(int aid, int tid, int siteId, int lgId, int keepPriId1) {
        int rt;

        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid,  tid, siteId, lgId, keepPriId1)
                .setBizList(new FaiList<>(Arrays.asList(MgRichTextValObj.Biz.PRODUCT)))
                .setXid("")
                .build();
        rt = m_cli.delRichText(mgRichTextArg);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchDel err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;", aid, tid, siteId, lgId, keepPriId1);
        }
        return rt;
    }

    public FaiList<Param> getPdRichText(int aid, int tid, int siteId, int lgId, int keepPriId1, int pdId) {
        int rt;
        MgRichTextSearch search = new MgRichTextSearch();
        search.setBiz(MgRichTextValObj.Biz.PRODUCT);
        search.setRlId(pdId);
        search.setTypeList(new FaiList<>());

        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setMgRichTextSearch(search)
                .build();

        FaiList<Param> list = new FaiList<>();
        rt = m_cli.getRichText(mgRichTextArg, list);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getRichText err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
        }
        return list;
    }

    public int updatePdRichText(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, int pdId, FaiList<Param> richTextList) {
        int rt;
        if(pdId <= 0 || Utils.isEmptyList(richTextList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("updatePdRichText arg err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
            return rt;
        }
        for(Param rich : richTextList) {
            rich.setInt(MgRichTextEntity.Info.RLID, pdId);
        }
        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid,  tid, siteId, lgId, keepPriId1)
                .setInfoList(richTextList)
                .setXid(xid)
                .build();
        rt = m_cli.batchSetRichText(mgRichTextArg);
        if(rt != Errno.OK) {
            throw new MgException(rt, "updatePdRichText err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
        }
        return rt;
    }

    private MgRichTextInfCli createCli() {
        MgRichTextInfCli cli = new MgRichTextInfCli(m_flow);
        if(!cli.init()) {
            throw new MgException(Errno.ERROR, "init MgRichTextInfCli error;flow=%d;", m_flow);
        }
        return cli;
    }
}
