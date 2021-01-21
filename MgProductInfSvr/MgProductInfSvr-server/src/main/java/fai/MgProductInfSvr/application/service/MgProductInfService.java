package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.comm.middleground.service.ServicePub;
import fai.comm.util.*;

/**
 * 维护接口服务各个service共用的方法
 */
public class MgProductInfService extends ServicePub {

    /**
     * 获取unionPriId
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Ref<Integer> idRef) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error");
            return rt;
        }

        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, idRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        return rt;
    }

    protected int getPrimaryKeyList(int flow, int aid, FaiList<Param> searchArgList, FaiList<Param> list) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        rt = cli.getPrimaryKeyList(aid, searchArgList, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPrimaryKeyList error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        return rt;
    }

    protected int getPdIdWithAdd(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, true);
    }
    /**
     * 获取PdId
     */
    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, false);
    }

    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef, boolean withAdd) {
        int rt = Errno.ERROR;
        MgProductBasicCli mgProductBasicCli = new MgProductBasicCli(flow);
        if(!mgProductBasicCli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgProductBasicCli error");
            return rt;
        }

        Param pdRelInfo = new Param();
        rt = mgProductBasicCli.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            if(withAdd && (rt == Errno.NOT_FOUND)){
                rt = mgProductBasicCli.addProductAndRel(aid, tid, unionPriId, new Param()
                                .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                        , idRef, new Ref<>());
                if(rt != Errno.OK) {
                    Log.logErr(rt, "addProductAndRel error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
                    return rt;
                }
            }
            Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
            return rt;
        }
        idRef.value = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
        return rt;
    }
}
