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
     * 批量同步 库存销售 spu数据到sku
     */
    public int batchSynchronousStoreSalesSPU2SKU(int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> spuStoreSalesInfoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.batchSynchronousStoreSalesSPU2SKU(aid, sourceTid, sourceUnionPriId, spuStoreSalesInfoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 批量同步 出入库记录
     */
    public int batchSynchronousInOutStoreRecord(int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> spuStoreSalesInfoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.batchSynchronousInOutStoreRecord(aid, sourceTid, sourceUnionPriId, spuStoreSalesInfoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "error;flow=%d;aid=%d;", m_flow, aid);
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
     * 批量扣减库存
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     * 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     */
    public int batchReducePdSkuStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchReducePdSkuStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
        if (rt != Errno.OK) {
            Log.logErr(rt, "reducePdSkuStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 批量扣减预扣库存
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录
     * @return
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchReducePdSkuHoldingStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo);
        if (rt != Errno.OK) {
            Log.logErr(rt, "reducePdSkuHoldingStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }


    /**
     * 补偿库存
     * @param unionPriId
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     * 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @return
     */
    public int batchMakeUpStore(int aid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchMakeUpStore(aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode);
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
            Log.logErr(rt, "getPdScSkuSalesStore error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 skuId 和 unionPriIdList 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStoreBySkuIdAndUIdList(int aid, int tid, long skuId, FaiList<Integer> unionPriIdList, FaiList infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;skuId=%d;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
            return rt;
        }
        rt = m_cli.getPdScSkuSalesStoreBySkuIdAndUIdList(aid, tid, skuId, unionPriIdList, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "getPdScSkuSalesStore error;flow=%d;aid=%d;skuId=%d;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
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
     * 获取出入库存记录
     */
    public int getInOutStoreRecordInfoList(int aid, int tid, int unionPriId, boolean isSource, SearchArg searchArg, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getInOutStoreRecordInfoList(aid, tid, unionPriId, isSource, searchArg, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "addInOutStoreRecordInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 批量删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(int aid, int tid, FaiList<Integer> pdIdList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        rt = m_cli.batchDelPdAllStoreSales(aid, tid, pdIdList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "getStoreSkuSummaryInfoList error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        return rt;
    }

    /**
     * 获取指定商品所有的业务销售记录
     */
    public int getAllBizSalesSummaryInfoList(int aid, int tid, int pdId, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getAllBizSalesSummaryInfoList(aid, tid, pdId, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "getAllBizSalesSummaryInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 获取指定业务下指定商品id集的业务销售信息
     */
    public int getBizSalesSummaryInfoListByPdIdList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getBizSalesSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIdList, infoList);
        if (rt != Errno.OK) {
            Log.logErr(rt, "getBizSalesSummaryInfoListByPdIdList error;flow=%d;aid=%d;", m_flow, aid);
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
            Log.logErr(rt, "getSalesSummaryInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }
    /**
     * 获取库存SKU汇总信息
     */
    public int getStoreSkuSummaryInfoList(int aid, int tid, int unionPriId, SearchArg searchArg, FaiList<Param> list, boolean isBiz){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        if(isBiz){
            rt = m_cli.getBizStoreSkuSummaryInfoList(aid, tid, unionPriId, searchArg, list);
        }else{
            rt = m_cli.getStoreSkuSummaryInfoList(aid, tid, unionPriId, searchArg, list);
        }

        if (rt != Errno.OK) {
            Log.logErr(rt, "getStoreSkuSummaryInfoList error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }


    private int m_flow;
    private MgProductStoreCli m_cli;


}
