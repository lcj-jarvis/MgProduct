package fai.MgProductBasicSvr.domain.common;

import fai.MgProductBasicSvr.domain.common.gfw.MgGfw;
import fai.MgProductBasicSvr.domain.common.gfw.SiteGfw;
import fai.MgProductBasicSvr.domain.common.gfw.YkGfw;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;

public class GfwUtil {

    public static void clear() {
        MgGfw.clear();
    }

    private static MgGfw getGfw(int tid) {
        switch (tid) {
            case FaiValObj.TermId.YK:
                return YkGfw.getInstance();
            case FaiValObj.TermId.SITE:
                return SiteGfw.getInstance();
            default:
                return null;
        }
    }

    public static void preWriteGfwLog(int aid, int tid, int siteId, int pdId, Param info) {
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;tid=%s;siteId=%s;info=%s", tid, siteId, info);
            return;
        }
        info = info.clone();
        info.setInt(ProductEntity.Info.PD_ID, pdId);
        mgGfw.preWriteGfwLog(aid, siteId, info);
    }

    public static void preWriteGfwLog(int aid, int tid, int siteId, FaiList<Integer> pdIds, Param info) {
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;tid=%s;siteId=%s;info=%s", tid, siteId, info);
            return;
        }
        FaiList<Param> list = new FaiList<>();
        for(int pdId : pdIds) {
            Param data = info.clone();
            data.setInt(ProductEntity.Info.PD_ID, pdId);
            list.add(data);
        }
        mgGfw.preWriteGfwLog(aid, siteId, list);
    }

    public static void preWriteGfwLog(int aid, int tid, int siteId, Param info) {
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;tid=%s;siteId=%s;info=%s", tid, siteId, info);
            return;
        }
        mgGfw.preWriteGfwLog(aid, siteId, info);
    }

    public static void preWriteGfwLog(int aid, int tid, int siteId, FaiList<Param> list) {
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;tid=%s;siteId=%s;list=%s", tid, siteId, list);
            return;
        }
        mgGfw.preWriteGfwLog(aid, siteId, list);
    }

    public static void commitPre(int tid) {
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;tid=%s;", tid);
            return;
        }
        mgGfw.commitPre();
    }
}
