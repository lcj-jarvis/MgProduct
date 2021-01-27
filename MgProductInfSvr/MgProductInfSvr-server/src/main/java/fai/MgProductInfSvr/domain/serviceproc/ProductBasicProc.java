package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.comm.util.*;

/**
 * 商品基础信息服务
 */
public class ProductBasicProc {
    public ProductBasicProc(int flow) {
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
    public int bindProductRel(int aid, int tid, int unionPriId, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.bindProductRel(aid, tid, unionPriId, bindRlPdInfo, info, rlPdIdRef);
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
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductRel(aid, tid, bindRlPdInfo, infoList, rlPdIdsRef);
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
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.batchBindProductRel(aid, tid, bindRlPdInfo, infoList);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchBindProductRel error;flow=%d;aid=%d;", m_flow, aid);
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

    /**
     * 根据业务商品id集合，获取商品业务关系数据集合
     */
    public int getRelListByRlIds(int aid, int unionPriId, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRelListByRlIds(aid, unionPriId, rlPdIds, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelListByRlIds error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }
    /**
     * 根据pdIds获取业务关联数据，仅获取有限的字段，aid+unionPriId+pdId+rlPdId
     */
    public int getReducedRelsByPdIds(int aid, int unionPriId, FaiList<Integer> pdIds, FaiList<Param> list){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getReducedRelsByPdIds(aid, unionPriId, pdIds, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getReducedRelsByPdIds error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }
    /**
     * 批量新增商品数据，并添加与当前unionPriId的关联
     */
    public int batchAddProductAndRel(int aid, int tid, int unionPriId, FaiList<Param> list, FaiList<Param> idInfoList){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchAddProductAndRel(aid, tid, unionPriId, list, idInfoList);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchAddProductAndRel error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品业务关联，同时绑定多个产品数据，给悦客接入进销存中心临时使用的
     * 接入完成后，废除，该接口禁止对外开放
     */
    public int batchBindProductsRel(int aid, int tid, FaiList<Param> list){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductsRel(aid, tid, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "batchAddProductAndRel error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            }
            return rt;
        }

        return rt;
    }

    private int m_flow;
    private MgProductBasicCli m_cli;
}
