package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.comm.util.*;

/**
 * 商品基础信息服务
 */
public class ProductBasicService {
    public ProductBasicService(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductBasicCli(flow);
        if(!m_cli.init()) {
            m_cli = null;
        }
    }

    public int getPdBindPropInfo(int aid, int tid, int unionPriId, int rlPdId, FaiList<Param> bindPropList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        rt = m_cli.getPdBindProp(aid, tid, unionPriId, rlPdId, bindPropList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPdBindProp error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int setPdBindPropInfo(int aid, int tid, int unionPriId, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdBindProp(aid, tid, unionPriId, rlPdId, addList, delList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindProp error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            return rt;
        }

        return rt;
    }


    private int m_flow;
    private MgProductBasicCli m_cli;
}
