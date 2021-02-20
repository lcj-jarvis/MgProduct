package fai.MgProductSearchSvr.application.service;

import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Str;

import java.io.IOException;

public class MgProductSearchService {

    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamString=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);

        Param searchParam = Param.parseParam(searchParamString);
        if(Str.isEmpty(searchParam)){
            rt = Errno.ARGS_ERROR;
            return rt;
        }
        MgProductSearch mgProductSearch = new MgProductSearch();
        mgProductSearch.initProductSearch(searchParam);    // 初始化 ProductSearch
        Log.logDbg("searchParam=%s;", searchParam.toJson());
        Log.logDbg("mgProductSearch=%s;", mgProductSearch.getParamMatcher(null).isEmpty());
        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;mgProductSearch=%s;", aid, unionPriId, tid, productCount, flow, mgProductSearch.getSearArg(null).matcher.getSql());
        rt = Errno.OK;
        session.write(rt);
        return rt;
    }
}
