package fai.MgProductStoreSvr.application.service;

public class MigrateService {
    /*public int migrate(FaiSession session, int flow, int aid, FaiList<Param> spuList) throws IOException {
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
                    // 事务提交前先设置一个较短的过期时间
                    spuBizSummaryProc.setDirtyCacheEx(aid);
                    tc.commit();
                    // 提交成功再删除缓存
                    spuBizSummaryProc.deleteDirtyCache(aid);
                }finally {
                    if(rt != Errno.OK){
                        tc.rollback();
                        return rt;
                    }
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
    }*/

    /*public int migrateYKService(FaiSession session, int flow, int aid, FaiList<Param> spuList) throws IOException {
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

                    rt = spuBizSummaryProc.migrateYKService(aid, spuList);
                    if(rt != Errno.OK) {
                        return rt;
                    }

                    tc.commit();
                }finally {
                    if(rt != Errno.OK){
                        tc.rollback();
                        return rt;
                    }
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
    }*/

    /*@SuccessRt(Errno.OK)
    public int migrateYKServiceDel(FaiSession session, int flow, int aid, FaiList<Integer> pdIds) throws IOException {
        int rt = Errno.ERROR;
        if (pdIds == null || pdIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "pdIds is empty;flow=%d;aid=%d", flow, aid);
        }
        TransactionCtrl tc = new TransactionCtrl();
        LockUtil.lock(aid);
        try {
            tc.setAutoCommit(false);
            StoreSalesSkuProc storeSalesSkuProc = new StoreSalesSkuProc(flow, aid, tc);
            SpuBizSummaryProc spuBizSummaryProc = new SpuBizSummaryProc(flow, aid, tc);
            SpuSummaryProc spuSummaryProc = new SpuSummaryProc(flow, aid, tc);
            SkuSummaryProc skuSummaryProc = new SkuSummaryProc(flow, aid, tc);
            boolean commit = false;
            try {

                storeSalesSkuProc.migrateYKDel(aid, pdIds);
                spuBizSummaryProc.migrateYKDel(aid, pdIds);
                spuSummaryProc.migrateYKDel(aid, pdIds);
                skuSummaryProc.migrateYKDel(aid, pdIds);

                commit = true;
                rt = Errno.OK;
                tc.commit();
            } finally {
                if (!commit) {
                    tc.rollback();
                    return rt;
                }
            }
        } finally {
            LockUtil.unlock(aid);
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        return rt;
    }*/
}
