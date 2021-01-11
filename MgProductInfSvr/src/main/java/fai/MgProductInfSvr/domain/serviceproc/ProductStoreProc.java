package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.comm.util.*;

public class ProductStoreProc {
    public ProductStoreProc(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductStoreCli(flow);
        if (!m_cli.init()) {
            m_cli = null;
        }
    }

    /**
     * 刷新商品规格库存销售sku
     */
    public int refreshPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;", m_flow, aid, unionPriId, pdId, rlPdId);
            return rt;
        }
        rt = m_cli.refreshPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "refreshPdScSkuSalesStore error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;", m_flow, aid, unionPriId, pdId, rlPdId);
            return rt;
        }
        return rt;
    }

    /**
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        Log.logDbg("whalelog updaterList=%s;", updaterList);
        rt = m_cli.setPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, updaterList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "setPdSkuScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 扣减库存
     *
     * @param aid
     * @param tid
     * @param unionPriId
     * @param skuId
     * @param rlOrderId         业务订单id
     * @param count             扣减数量
     * @param reduceMode        扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     * @return
     */
    public int reducePdSkuStore(int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode, int expireTimeSeconds) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.reducePdSkuStore(aid, tid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
        if (rt != Errno.OK) {
            Log.logErr(rt, "reducePdSkuStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     *
     * @param aid
     * @param tid
     * @param unionPriId
     * @param skuId
     * @param rlOrderId  业务订单id
     * @param count      扣减数量
     */
    public int reducePdSkuHoldingStore(int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.reducePdSkuHoldingStore(aid, tid, unionPriId, skuId, rlOrderId, count);
        if (rt != Errno.OK) {
            Log.logErr(rt, "reducePdSkuHoldingStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 补偿库存
     *
     * @param aid
     * @param unionPriId
     * @param skuId
     * @param rlOrderId  业务订单id
     * @param count      补偿数量
     * @param reduceMode 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @return
     */
    public int makeUpStore(int aid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.makeUpStore(aid, unionPriId, skuId, rlOrderId, count, reduceMode);
        if (rt != Errno.OK) {
            Log.logErr(rt, "makeUpStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "setPdSkuScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 添加库存出入库记录
     */
    public int addInOutStoreRecordInfoList(int aid, int tid, int unionPriId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.addInOutStoreRecordInfoList(aid, tid, unionPriId, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addInOutStoreRecordInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }
    /**
     * 获取商品业务销售信息
     */
    public int getBizSalesSummaryInfoList(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getBizSalesSummaryInfoList(aid, tid, unionPriId, pdId, rlPdId, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addInOutStoreRecordInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 获取商品销售信息
     */
    public int getSalesSummaryInfoList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getSalesSummaryInfoList(aid, tid, unionPriId, pdIdList, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addInOutStoreRecordInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }



    private int m_flow;
    private MgProductStoreCli m_cli;


}
