package fai.MgProductInfSvr.domain.comm.gfw;

import fai.comm.middleground.FaiValObj;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.middleground.svrutil.misc.Utils;

public class GfwUtil {
    private static MgGfw getGfw(int tid) {
        switch (tid) {
            case FaiValObj.TermId.YK:
                return YkGfw.getInstance();
            default:
                return null;
        }
    }

    public static void writeRichGfwLog(int aid, int tid, int siteId, int rlPdId, FaiList<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        MgGfw mgGfw = getGfw(tid);
        if(mgGfw == null) {
            Log.logErr("get mg gfw error;aid=%s;tid=%s;siteId=%s;rlPdId=%s;list=%s", aid, tid, siteId, rlPdId, list);
            return;
        }
        mgGfw.writeRichLog(aid, siteId, rlPdId, list);
    }
}
