package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductInfSvr.domain.comm.gfw.GfwUtil;
import fai.MgRichTextInfSvr.interfaces.cli.MgRichTextInfCli;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.MgRichTextInfSvr.interfaces.utils.MgRichTextArg;
import fai.MgRichTextInfSvr.interfaces.utils.MgRichTextSearch;
import fai.comm.util.*;
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

    public int addPdRichTexts(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int pdId, FaiList<Param> richList) {
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
        GfwUtil.writeRichGfwLog(aid, tid, siteId, rlPdId, richList);
        return rt;
    }

    // 迁移门店通数据用，不做gfw上报，如后续对外开放，需做上报处理
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

    public int checkoutAndAdd(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int pdId, FaiList<Param> richTextList) {
        int rt;
        if(pdId <= 0 || Utils.isEmptyList(richTextList)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("updatePdRichText arg err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;", aid, tid, siteId, lgId, keepPriId1, pdId);
            return rt;
        }
        FaiList<Param> existList = getPdRichText(aid, tid, siteId, lgId, keepPriId1, pdId);
        FaiList<Integer> existTypes = Utils.getValList(existList, MgRichTextEntity.Info.TYPE);
        if(existTypes == null) {
            existTypes = new FaiList<>();
        }
        FaiList<Param> addList = new FaiList<>();
        for(Param richInfo : richTextList) {
            int type = richInfo.getInt(MgRichTextEntity.Info.TYPE);
            if(!existTypes.contains(type)) {
                addList.add(richInfo);
            }
        }
        if(addList.isEmpty()) {
            return Errno.OK;
        }
        rt = addPdRichTexts(xid, aid, tid, siteId, lgId, keepPriId1, rlPdId, pdId, addList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "addPdRichTexts err;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;addList=%s;", aid, tid, siteId, lgId, keepPriId1, pdId, addList);
            return rt;
        }
        Log.logStd("checkoutAndAdd ok;aid=%d;tid=%d;siteId=%d;lgId=%d;keepId=%d;pdId=%d;addList=%s;", aid, tid, siteId, lgId, keepPriId1, pdId, addList);
        return rt;
    }

    public int updatePdRichText(String xid, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int pdId, FaiList<Param> richTextList) {
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
        GfwUtil.writeRichGfwLog(aid, tid, siteId, rlPdId, richTextList);
        return rt;
    }

    public int clearAcct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> primaryKeys) {
        int rt;
        if (aid <= 0 || Utils.isEmptyList(primaryKeys)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg error;flow=%d;aid=%d;primaryKeys=%s", m_flow, aid, primaryKeys);
            return rt;
        }
        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setPrimaryKeys(primaryKeys)
                .build();
        rt = m_cli.clearAcct(mgRichTextArg);
        if (rt != Errno.OK) {
            Log.logErr(rt, "clearAcct error;flow=%d;aid=%d;primaryKeys=%s", m_flow, aid, primaryKeys);
            return rt;
        }
        return rt;
    }

    public void incCloneRichText(int aid, Integer toTid, Integer toSiteId, Integer toLgId, Integer toKeepPriId1, int toBiz, int fromAid, Param fromPrimaryKey, int fromBiz, FaiList<Param> pdIdMapList) {
        int rt;
        if (aid <= 0 || Str.isEmpty(fromPrimaryKey) || Utils.isEmptyList(pdIdMapList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "arg error;flow=%d;aid=%d;fromPrimaryKey=%s;pdIdMapList=%s", m_flow, aid, fromPrimaryKey, pdIdMapList);
        }
        MgRichTextArg mgRichTextArg = new MgRichTextArg.Builder(aid, toTid, toSiteId, toLgId, toKeepPriId1)
                .setFromAid(fromAid)
                .setToBiz(toBiz)
                .setFromBiz(fromBiz)
                .setFromPrimaryKey(fromPrimaryKey)
                .setRlIdMapList(pdIdMapList)
                .build();
        rt = m_cli.incCloneByIdMap(mgRichTextArg);
        if (rt != Errno.OK) {
            throw new MgException(rt, "incCloneRichText error;flow=%d;aid=%d;fromAid=%d;pdIdMapList=%s", m_flow, aid, fromAid, pdIdMapList);
        }
    }

    private MgRichTextInfCli createCli() {
        MgRichTextInfCli cli = new MgRichTextInfCli(m_flow);
        if(!cli.init()) {
            throw new MgException(Errno.ERROR, "init MgRichTextInfCli error;flow=%d;", m_flow);
        }
        return cli;
    }
}
