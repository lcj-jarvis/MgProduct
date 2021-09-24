package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.serviceProc.SpuBizSummaryProc;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;

public class MigrateService {
    public int migrate(FaiSession session, int flow, int aid, FaiList<Param> spuList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Utils.isEmptyList(spuList)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;aid=%s;spuList=%s;",aid, spuList);
                return rt;
            }

            TransactionCtrl tc = new TransactionCtrl();
            LockUtil.lock(aid);
            try {

                SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
                try {
                    tc.setAutoCommit(false);

                    rt = spuBizSummaryProc.migrate(aid, spuList);
                    if(rt != Errno.OK) {
                        return rt;
                    }

                }finally {
                    if(rt != Errno.OK){
                        tc.rollback();
                        return rt;
                    }
                    // 事务提交前先设置一个较短的过期时间
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    tc.commit();
                    // 提交成功再删除缓存
                    spuBizSummaryProc.deleteDirtyCache(aid);
                }
            }finally {
                LockUtil.unlock(aid);
                tc.closeDao();
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("migrate ok;aid=%s;", aid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
