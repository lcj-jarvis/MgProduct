package fai.MgProductInfSvr.application.service;

import fai.MgProductSearchSvr.interfaces.cli.MgProductSearchCli;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;

import java.io.IOException;


public class ProductSearchService extends MgProductInfService {
    public int searchList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            MgProductSearchCli mgProductSearchCli = createSearchCli(flow);
            int productCount = 100;    // 可以发包拿数据，然后根据商品的数据 走不同的 集群，待实现
            Param searchReult = new Param();
            rt = mgProductSearchCli.searchList(aid, tid, unionPriId, productCount, searchParamString, searchReult);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            searchReult.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    private MgProductSearchCli createSearchCli(int flow) {
        MgProductSearchCli cli = new MgProductSearchCli(flow);
        if (!cli.init()) {
            throw new RuntimeException("MgProductBasicCli init error, flow="+ flow);
        }
        return cli;
    }

}
