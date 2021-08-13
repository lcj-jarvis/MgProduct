package fai.MgProductSearchSvr.application;

import java.io.IOException;

import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.application.service.MgProductSearchService;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSearchSvr.interfaces.cmd.MgProductSearchCmd;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.util.*;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;


public class MgProductSearchHandler extends MiddleGroundHandler {

    public MgProductSearchHandler(FaiServer server) {
        super(server);
    }

    @Cmd(MgProductSearchCmd.SearchCmd.SEARCH_LIST)
    public int searchList(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(MgProductSearchDto.Key.UNION_PRI_ID) int unionPriId,
                           @ArgBodyInteger(MgProductSearchDto.Key.TID) int tid,
                           @ArgBodyInteger(MgProductSearchDto.Key.PRODUCT_COUNT) int productCount,
                           @ArgBodyString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING) String esSearchParamString,
                           @ArgBodyString(MgProductSearchDto.Key.SEARCH_PARAM_STRING) String searchParamString) throws IOException {
        return searchService.searchList(session, flow, aid, unionPriId, tid, productCount, esSearchParamString, searchParamString);
    }

    private final MgProductSearchService searchService = ServiceProxy.create(new MgProductSearchService());
}
