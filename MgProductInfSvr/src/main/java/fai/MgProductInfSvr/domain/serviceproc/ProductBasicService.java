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
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getPdBindProp error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            }
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

    public int getRlPdByPropVal(int aid, int tid, int unionPriId, FaiList<Param> proIdsAndValIds, FaiList<Integer> rlPdIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRlPdByPropVal(aid, tid, unionPriId, proIdsAndValIds, rlPdIds);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRlPdByPropVal error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     * @return
     */
    public int addProductAndRel(int aid, int tid, int unionPriId, Param info, Ref<Integer> pdIdRef, Ref<Integer> rlPdIdRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addProductAndRel(aid, tid, unionPriId, info, pdIdRef, rlPdIdRef);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "addProductAndRel error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 新增商品业务关联
     * @return
     */
    public int bindProductRel(int aid, int tid, int unionPriId, Param info, Ref<Integer> rlPdIdRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.bindProductRel(aid, tid, unionPriId, info, rlPdIdRef);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "bindProductRel error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品业务关联
     * @return
     */
    public int batchBindProductRel(int aid, int tid, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductRel(aid, tid, infoList, rlPdIdsRef);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchBindProductRel error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 取消 rlPdIds 的商品业务关联
     * @return
     */
    public int batchDelPdRelBind(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchDelPdRelBind(aid, unionPriId, rlPdIds);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchDelPdRelBind error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 删除指定id列表(rlPdIds)的商品数据，同时删除所有相关业务关联数据
     * @return
     */
    public int batchDelProduct(int aid, int tid, int unionPriId, FaiList<Integer> rlPdIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchDelProduct(aid, tid, unionPriId, rlPdIds);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchDelProduct error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 根据商品业务id，获取商品业务关系数据
     * @return
     */
    public int getRelInfoByRlId(int aid, int unionPriId, int rlPdId, Param pdRelInfo) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    private int m_flow;
    private MgProductBasicCli m_cli;
}
