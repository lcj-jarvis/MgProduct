package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

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

    public FaiList<Param> getPdBindBiz(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getPdBindBiz(aid, unionPriId, sysType, rlPdIds, list);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getProductList error;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
        }
        return list;
    }

    public int getProductList(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getProductList(aid, unionPriId, sysType, rlPdIds, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getProductList error;flow=%d;aid=%d;uid=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
        }
        return rt;
    }

    public int getPdBindPropInfo(int aid, int tid, int unionPriId, int sysType, int rlPdId, FaiList<Param> bindPropList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        rt = m_cli.getPdBindProp(aid, tid, unionPriId, sysType, rlPdId, bindPropList);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getPdBindProp error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            }
            return rt;
        }
        return rt;
    }

    public int setPdBindPropInfo(int aid, int tid, int unionPriId, int sysType, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdBindProp(aid, tid, unionPriId, sysType, rlPdId, addList, delList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindProp error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            return rt;
        }

        return rt;
    }

    public int delPdBindProp(int aid, int unionPriId, int sysType, int rlPropId, FaiList<Integer> delPropVals) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delPdBindProp(aid, unionPriId, sysType, rlPropId, delPropVals);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindProp error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        return rt;
    }

    public int delPdBindProp(int aid, int unionPriId, int sysType, FaiList<Integer> rlPropIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delPdBindProp(aid, unionPriId, sysType, rlPropIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindProp error;flow=%d;aid=%d;uid=%d;rlPropIds=%s;", m_flow, aid, unionPriId, rlPropIds);
            return rt;
        }

        return rt;
    }

    public int getRlPdByPropVal(int aid, int tid, int unionPriId, int sysType, FaiList<Param> proIdsAndValIds, FaiList<Integer> rlPdIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRlPdByPropVal(aid, tid, unionPriId, sysType, proIdsAndValIds, rlPdIds);
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
    public int addProductAndRel(int aid, int tid, int siteId, int unionPriId, String xid, Param info, Ref<Integer> pdIdRef, Ref<Integer> rlPdIdRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addProductAndRel(aid, tid, siteId, unionPriId, xid, info, pdIdRef, rlPdIdRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "addProductAndRel error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            return rt;
        }

        return rt;
    }

    /**
     * 新增商品业务关联
     * @return
     */
    public int bindProductRel(int aid, int tid, int unionPriId, String xid, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef, Ref<Integer> pdIdRef, Ref<Boolean> existRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.bindProductRel(aid, tid, unionPriId, xid, bindRlPdInfo, info, rlPdIdRef, pdIdRef, existRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "bindProductRel error;flow=%d;aid=%d;tid=%d;unionPriId=%d;", m_flow, aid, tid, unionPriId);
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品业务关联
     * @return
     */
    public int batchBindProductRel(int aid, String xid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef, Ref<Integer> pdIdRef, Ref<FaiList<Integer>> existListRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductRel(aid, xid, tid, bindRlPdInfo, infoList, rlPdIdsRef, pdIdRef, existListRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchBindProductRel error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品业务关联
     * @return
     */
    public int batchBindProductsRel(int aid, String xid, int tid, int unionPriId, int fromUnionPriId, int sysType, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef, Ref<FaiList<Integer>> pdIdsRef, Ref<FaiList<Integer>> existListRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductsRel(aid, xid, tid, unionPriId, fromUnionPriId, sysType, infoList, rlPdIdsRef, pdIdsRef, existListRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchBindProductsRel error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }

        return rt;
    }

    /**
     * 修改商品排序
     */
    public int setPdSort(int aid, int tid, int unionPriId, int sysType, int rlPdId, int preRlPdId) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdSort(aid, tid, unionPriId, sysType, rlPdId, preRlPdId);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setSinglePd error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;preRlPdId=%s;", m_flow, aid, unionPriId, rlPdId, preRlPdId);
            return rt;
        }

        return rt;
    }

    /**
     * 修改指定商品
     */
    public int setSinglePd(int aid, String xid, int tid, int siteId, int unionPriId, int sysType, Integer rlPdId, ParamUpdater updater) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setSinglePd(aid, xid, tid, siteId, unionPriId, sysType, rlPdId, updater);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setSinglePd error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;updater=%s;", m_flow, aid, unionPriId, rlPdId, updater.toJson());
            return rt;
        }

        return rt;
    }

    /**
     * 修改指定商品
     */
    public int setProducts(int aid, int tid, int siteId, int unionPriId, int sysType, FaiList<Integer> rlPdIds, ParamUpdater updater) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setProducts(aid, tid, siteId, unionPriId, sysType, rlPdIds, updater);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDelPdRelBind error;flow=%d;aid=%d;unionPriId=%d;updater=%s;", m_flow, aid, unionPriId, updater.toJson());
            return rt;
        }

        return rt;
    }

    /**
     * 批量设置商品的状态
     * for 门店通总店批量设置上下架，若门店数据不存在，则添加
     * 可修改：分类绑定，top 字段 （现阶段接入使用的字段）
     * @return
     */
    public FaiList<Param> batchSet4YK(int aid, String xid, int ownUnionPriId, int sysType, FaiList unionPriIds, FaiList<Integer> rlPdIds, ParamUpdater updater) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get ProductBasicCli error;aid=%d;ownUid=%d;uids=%s;sysType=%s;rlPdIds=%s;updater=%s;", aid, ownUnionPriId, unionPriIds, sysType, rlPdIds, updater.toJson());
        }
        FaiList<Param> addList = new FaiList<>();
        rt = m_cli.batchSet4YK(aid, xid, ownUnionPriId, sysType, unionPriIds, rlPdIds, updater, addList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "batchSet4YK error;aid=%d;ownUid=%d;uids=%s;sysType=%s;rlPdIds=%s;updater=%s;", aid, ownUnionPriId, unionPriIds, sysType, rlPdIds, updater.toJson());
        }
        return addList;
    }

    /**
     * 克隆业务绑定关系数据
     * @return
     */
    public void cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId, boolean silentDel) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get ProductBasicCli error;flow=%d;aid=%d;fromUnionPriId=%s;toUnionPriId=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
        rt = m_cli.cloneBizBind(aid, fromUnionPriId, toUnionPriId, silentDel);
        if(rt != Errno.OK) {
            throw new MgException(rt, "cloneBizBind error;flow=%d;aid=%d;fromUnionPriId=%s;toUnionPriId=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
    }

    /**
     * 取消 rlPdIds 的商品业务关联
     * @return
     */
    public int batchDelPdRelBind(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchDelPdRelBind(aid, unionPriId, sysType, rlPdIds, softDel);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDelPdRelBind error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
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
            Log.logErr(rt, "batchBindProductRel error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        return rt;
    }

    /**
     * 删除指定id列表(rlPdIds)的商品数据，同时删除所有相关业务关联数据
     * @return
     */
    public int batchDelProduct(int aid, String xid, int tid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchDelProduct(aid, xid, tid, unionPriId, sysType, rlPdIds, softDel);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchDelProduct error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, rlPdIds);
            return rt;
        }

        return rt;
    }

    /**
     * 清空业务关联数据
     * @param softDel 目前都是使用false
     * @return
     */
    public int clearRelData(int aid, int unionPriId, boolean softDel) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;softDel=%s;", m_flow, aid, unionPriId, softDel);
            return rt;
        }
        rt = m_cli.clearRelData(aid, unionPriId, softDel);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clearRelData error;flow=%d;aid=%d;unionPriId=%d;softDel=%s;", m_flow, aid, unionPriId, softDel);
            return rt;
        }

        return rt;
    }

    /**
     * 清空数据
     * @return
     */
    public int clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        rt = m_cli.clearAcct(aid, unionPriIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "clearAcct error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }

        return rt;
    }

    /**
     * 根据商品业务id，获取商品业务关系数据
     * @return
     */
    public int getRelInfoByRlId(int aid, int unionPriId, int sysType, int rlPdId, Param pdRelInfo) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRelInfoByRlId(aid, unionPriId, sysType, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    public int getProductInfo(int aid, int unionPriId, int sysType, int rlPdId, Param info) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getProductInfo(aid, unionPriId, sysType, rlPdId, info);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    public int getInfoByPdId(int aid, int unionPriId, int pdId, Param info) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdInfoByPdId(aid, unionPriId, pdId, info);
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
    public int getRelListByRlIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        return getRelListByRlIds(aid, unionPriId, sysType, rlPdIds, false, list);
    }
    public int getRelListByRlIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean withSoftDel, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getRelListByRlIds(aid, unionPriId, sysType, rlPdIds, withSoftDel, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelListByRlIds error;flow=%d;aid=%d;unionPriId=%d;list=%s;", m_flow, aid, unionPriId, list);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 根据pdIds获取商品数据
     */
    public int getPdListByPdIds(int aid, int unionPriId, FaiList<Integer> pdIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdListByPdIds(aid, unionPriId, pdIds, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getRelListByRlIds error;flow=%d;aid=%d;unionPriId=%d;list=%s;", m_flow, aid, unionPriId, list);
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
     * 根据pdIds获取业务关联数据，仅获取有限的字段，aid+unionPriId+pdId+rlPdId
     */
    public int getPdReducedList4Adm(int aid, int unionPriId, Integer sysType, FaiList<String> names, FaiList<Integer> status, FaiList<Param> list){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdReducedList4Adm(aid, unionPriId, sysType, names, status, list);
        if(rt != Errno.OK) {
            if(rt != Errno.NOT_FOUND) {
                Log.logErr(rt, "getPdReducedList4Adm error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            }
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品数据，并添加与当前unionPriId的关联
     */
    public int batchAddProductAndRel(int aid, int tid, int siteId, int unionPriId, FaiList<Param> list, FaiList<Param> idInfoList){
        return batchAddProductAndRel(aid, null, tid, siteId, unionPriId, list, idInfoList);
    }
    public int batchAddProductAndRel(int aid, String xid, int tid, int siteId, int unionPriId, FaiList<Param> list, FaiList<Param> idInfoList){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchAddProductAndRel(aid, xid, tid, siteId, unionPriId, list, idInfoList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchAddProductAndRel error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        return rt;
    }

    /**
     * 批量新增商品业务关联，同时绑定多个产品数据
     * @Param needSyncInfo 导入方法调用时需要为 true ：用于同步商品关系，和插入分类、标签等的绑定表（场景：门店通业务要求门店的商品数据需要和总店一致，同时门店也要绑定分类等信息）
     */
    public int batchBindProductsRel(int aid, String xid, int tid, boolean needSyncInfo, FaiList<Param> list){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.batchBindProductsRel(aid, xid, tid, needSyncInfo, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "batchBindProductsRel error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }

        return rt;
    }

    public int getPdBindGroups(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdBindGroup(aid, unionPriId, sysType, rlPdIds, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getPdBindGroup error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        return rt;
    }

    public int setPdBindGroup(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdBindGroup(aid, unionPriId, sysType, rlPdId, addGroupIds, delGroupIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindGroup error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int delPdBindGroup(int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delPdBindGroup(aid, unionPriId, sysType, rlGroupIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "delPdBindGroup error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
            return rt;
        }

        return rt;
    }

    private int m_flow;
    private MgProductBasicCli m_cli;


    /********************************************商品和标签的关联开始********************************************************/
    /**
     * 获取aid，unionPriId，rlPdIds下的所有商品和标签关联的所有数据
     */
    public int getPdBindTags(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> result) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdBindTag(aid, unionPriId, sysType, rlPdIds, result);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "getPdBindTag error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int setPdBindTag(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdBindTag(aid, unionPriId, sysType, rlPdId, addRlTagIds, delRlTagIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdBindTag error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int delPdBindTag(int aid, int unionPriId, int sysType, FaiList<Integer> delRlPdIds) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductBasicCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delPdBindTag(aid, unionPriId, sysType, delRlPdIds);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            Log.logErr(rt, "delPdBindTag error;flow=%d;aid=%d;unionPriId=%d;rlPdIds=%s;", m_flow, aid, unionPriId, delRlPdIds);
            return rt;
        }
        return rt;
    }
    /********************************************商品和标签的关联结束********************************************************/


    /**
     * 备份数据
     * @param aid
     * @param unionPriIds
     * @param backupInfo
     */
    public void backupData(int aid, FaiList<Integer> unionPriIds, Param backupInfo) {
        int rt = m_cli.backupData(aid, unionPriIds, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "backupData error;flow=%d;aid=%d;uid=%s;backupInfo=%s;", m_flow, aid, unionPriIds, backupInfo);
        }
    }

    /**
     * 还原备份
     * @param aid
     * @param unionPriIds
     * @param backupInfo
     */
    public void restoreBackup(int aid, FaiList<Integer> unionPriIds, int restoreId, Param backupInfo) {
        int rt = m_cli.restoreBackupData(aid, unionPriIds, restoreId, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreBackup error;flow=%d;aid=%d;uid=%s;restoreId=%s;backupInfo=%s;", m_flow, aid, unionPriIds, restoreId, backupInfo);
        }
    }

    /**
     * 删除备份
     * @param aid
     * @param backupInfo
     */
    public void delBackup(int aid, Param backupInfo) {
        int rt = m_cli.delBackupData(aid, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreBackup error;flow=%d;aid=%d;backupInfo=%s;", m_flow, aid, backupInfo);
        }
    }

    /**
     * 克隆数据
     */
    public void cloneData(int aid, int tid, int siteId, int fromAid, FaiList<Param> cloneUnionPriIds) {
        int rt = m_cli.cloneData(aid, tid, siteId, fromAid, cloneUnionPriIds);
        if (rt != Errno.OK) {
            throw new MgException(rt, "cloneData error;flow=%d;aid=%d;uids=%s", m_flow, aid, cloneUnionPriIds);
        }
    }

    /**
     * 增量克隆数据
     */
    public void incrementalClone(int aid, int tid, int siteId, int unionPriId, int fromAid, int fromUnionPriId) {
        int rt = m_cli.incrementalClone(aid, tid, siteId, unionPriId, fromAid, fromUnionPriId);
        if (rt != Errno.OK) {
            throw new MgException(rt, "incrementalClone error;flow=%d;aid=%d;uid=%s;fromAid=%d;fromUid=%d;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }
    }

    public FaiList<Param> dataMigrate(int aid, int tid, FaiList<Param> addList) {
        return dataMigrate(aid, tid, addList, 0);
    }

    public FaiList<Param> dataMigrate(int aid, int tid, FaiList<Param> addList, int sysType) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get ProductBasicCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
        }
        FaiList<Param> returnList = new FaiList<>();
        rt = m_cli.dataMigrate(aid, tid, addList, sysType, returnList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "setPdBindTag error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
        }
        return returnList;
    }

    public FaiList<Integer> getMigratePdIds(int aid, int sysType) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get ProductBasicCli error;flow=%d;aid=%d;", m_flow, aid);
        }
        FaiList<Integer> pdIds = new FaiList<>();
        rt = m_cli.getMigratePdIds(aid, sysType, pdIds);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getMigratePdIds error;flow=%d;aid=%d;", m_flow, aid);
        }
        return pdIds;
    }

    public void restoreData(int aid, int unionPriId, String xid, FaiList<Integer> rlPdIds, int sysType, FaiList<Integer> pdIds) {
        int rt;
        if(m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get ProductBasicCli error;flow=%d;aid=%d;", m_flow, aid);
        }
        rt = m_cli.restoreData(aid, unionPriId, xid, rlPdIds, sysType, pdIds);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreData error;flow=%d;aid=%;rlPdIds=%s", m_flow, aid, rlPdIds);
        }
    }
}
