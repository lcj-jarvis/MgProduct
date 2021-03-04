package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.comm.util.*;

public class ProductStoreProc extends AbstractProductProc{
    public ProductStoreProc(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductStoreCli(flow);
        if (!m_cli.init()) {
            m_cli = null;
        }
    }

    /**
     * 刷新 sku 库存销售
     */
    public int refreshSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;", m_flow, aid, unionPriId, pdId, rlPdId);
            return rt;
        }
        rt = m_cli.refreshSkuStoreSales(aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;", m_flow, aid, unionPriId, pdId, rlPdId);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 修改sku库存销售信息
     */
    public int setSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setSkuStoreSales(aid, tid, unionPriId, pdId, rlPdId, updaterList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo){
        return batchReducePdSkuHoldingStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo, null);
    }
    /**
     * 批量扣减预扣库存
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录
     * @return
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo, Ref<Integer> ioStoreRecordIdRef){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchReducePdSkuHoldingStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo, ioStoreRecordIdRef);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 根据uid和pdId 获取sku库存销售信息
     */
    public int getSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList, FaiList<String> useOwnerFieldList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getSkuStoreSales(aid, tid, unionPriId, pdId, rlPdId, infoList, useOwnerFieldList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 skuId 和 unionPriIdList 获取商品规格库存销售sku
     */
    public int getStoreSalesBySkuIdList(int aid, int tid, int unionPirId, FaiList<Long> skuIdList, FaiList<Param> infoList, FaiList<String> useSourceFieldList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPirId=%d;skuIdList=%s;", m_flow, aid, unionPirId, skuIdList);
            return rt;
        }
        rt = m_cli.getStoreSalesBySkuIdList(aid, tid, unionPirId, skuIdList, infoList, useSourceFieldList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPirId=%d;skuIdList=%s;", m_flow, aid, unionPirId, skuIdList);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 skuId 和 unionPriIdList 获取sku库存销售信息
     */
    public int getStoreSalesBySkuIdAndUIdList(int aid, int tid, long skuId, FaiList<Integer> unionPriIdList, FaiList infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
            return rt;
        }
        rt = m_cli.getStoreSalesBySkuIdAndUIdList(aid, tid, skuId, unionPriIdList, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", m_flow, aid, skuId, unionPriIdList);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 pdId 获取商品规格库存销售sku
     */
    public int getSkuStoreSalesByPdId(int aid, int tid, int pdId, FaiList infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;pdId=%d;", m_flow, aid, pdId);
            return rt;
        }
        rt = m_cli.getSkuStoreSalesByPdId(aid, tid, pdId, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;pdId=%d;", m_flow, aid, pdId);
            return rt;
        }
        return rt;
    }

    /**
     * 获取预扣记录
     */
    public int getHoldingRecordList(int aid, int tid, int unionPriId, FaiList<Long> skuIdList, FaiList infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getHoldingRecordList(aid, tid, unionPriId, skuIdList, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
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
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        return rt;
    }

    /**
     * 获取spu 所以关联的业务库存销售汇总信息
     */
    public int getAllSpuBizSummaryInfoList(int aid, int tid, int pdId, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getSpuBizSummaryInfoListByPdId(aid, tid, pdId, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 pdIdList 获取指定业务下 spu业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoListByPdIdList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList, FaiList<String> useOwnerFieldList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getSpuBizSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIdList, infoList, useOwnerFieldList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    /**
     * 获取spu库存销售汇总信息
     */
    public int getSpuSummaryInfoList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getSpuSummaryInfoList(aid, tid, unionPriId, pdIdList, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }
    /**
     * 获取 SKU (业务)库存销售汇总信息
     */
    public int getSkuSummaryInfoList(int aid, int tid, int unionPriId, SearchArg searchArg, FaiList<Param> list, boolean isBiz){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        if(isBiz){
            rt = m_cli.getSkuBizSummaryInfoList(aid, tid, unionPriId, searchArg, list);
        }else{
            rt = m_cli.getSkuSummaryInfoList(aid, tid, unionPriId, searchArg, list);
        }
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }


    private int m_flow;
    private MgProductStoreCli m_cli;


}
