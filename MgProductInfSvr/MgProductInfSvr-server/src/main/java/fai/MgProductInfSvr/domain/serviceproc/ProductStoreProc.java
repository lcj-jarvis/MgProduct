package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

import java.util.Calendar;

public class ProductStoreProc extends AbstractProductProc{
    public ProductStoreProc(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductStoreCli(flow);
        if (!m_cli.init()) {
            m_cli = null;
        }
    }

    /**
     * 克隆业务绑定数据
     * for 门店通
     */
    public void cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get MgProductStoreCli error;flow=%d;aid=%d;fromUid=%d;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
        rt = m_cli.cloneBizBind(aid, fromUnionPriId, toUnionPriId);
        if (rt != Errno.OK) {
            throw new MgException(rt, "error;flow=%d;aid=%d;fromUid=%d;toUid=%s;", m_flow, aid, fromUnionPriId, toUnionPriId);
        }
    }

    /**
     * 克隆业务绑定数据
     * for 门店通
     */
    public void copyBizBind(int aid, String xid, int fromUnionPriId, FaiList<Param> list) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get MgProductStoreCli error;flow=%d;aid=%d;fromUid=%d;list=%s;", m_flow, aid, fromUnionPriId, list);
        }
        rt = m_cli.copyBizBind(aid, xid, fromUnionPriId, list);
        if (rt != Errno.OK) {
            throw new MgException(rt, "error;flow=%d;aid=%d;fromUid=%d;list=%s;", m_flow, aid, fromUnionPriId, list);
        }
    }

    /**
     * 刷新 sku 库存销售
     */
    public int refreshSkuStoreSales(int aid, int tid, int unionPriId, int sysType, String xid, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;", m_flow, aid, unionPriId, pdId, rlPdId);
            return rt;
        }
        rt = m_cli.refreshSkuStoreSales(aid, tid, unionPriId, sysType, xid, pdId, rlPdId, pdScSkuInfoList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;sourceUnionPriId=%s;", m_flow, aid, sourceUnionPriId);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;sourceUnionPriId=%s;", m_flow, aid, sourceUnionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 修改sku库存销售信息
     */
    public int setSkuStoreSales(int aid, int tid, int unionPriId, String xid, int pdId, int rlPdId, int sysType, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setSkuStoreSales(aid, tid, unionPriId, xid, pdId, rlPdId, sysType, updaterList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", m_flow, aid, unionPriId, rlPdId);
            return rt;
        }
        return rt;
    }

    public int batchSetSkuStoreSales(int aid, String xid, int tid, int ownerUnionPriId, FaiList<Integer> unionPriIds, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList) {
        return batchSetSkuStoreSales(aid, xid, tid, ownerUnionPriId, unionPriIds, pdId, rlPdId, ProductStoreValObj.SysType.PRODUCT, updaterList);
    }

    /**
     * 修改sku库存销售信息
     */
    public int batchSetSkuStoreSales(int aid, String xid, int tid, int ownerUnionPriId, FaiList<Integer> unionPriIds, int pdId, int rlPdId, int sysType, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        rt = m_cli.batchSetSkuStoreSales(aid, xid, tid, ownerUnionPriId, unionPriIds, pdId, rlPdId, sysType, updaterList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriIds=%s;rlPdId=%s;", m_flow, aid, unionPriIds, rlPdId);
            return rt;
        }
        return rt;
    }

    public int setSpuBizSummary(int aid, String xid, int unionPriId, int pdId, ParamUpdater updater) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;uid=%s;pdId=%s;update=%s;", m_flow, aid, unionPriId, pdId, updater.toJson());
            return rt;
        }
        rt = m_cli.setSpuBizSummary(aid, xid, unionPriId, pdId, updater);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;uid=%s;pdId=%s;update=%s;", m_flow, aid, unionPriId, pdId, updater.toJson());
            return rt;
        }
        return rt;
    }

    public int batchSetSpuBizSummary(int aid, String xid, FaiList<Integer> unionPriIds, FaiList<Integer> pdIds, ParamUpdater updater) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;uid=%s;pdIds=%s;update=%s;", m_flow, aid, unionPriIds, pdIds, updater.toJson());
            return rt;
        }
        rt = m_cli.batchSetSpuBizSummary(aid, xid, unionPriIds, pdIds, updater);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;uid=%s;pdIds=%s;update=%s;", m_flow, aid, unionPriIds, pdIds, updater.toJson());
            return rt;
        }
        return rt;
    }

    public int batchAddSpuBizSummary(int aid, String xid, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
            return rt;
        }
        rt = m_cli.batchAddSpuBizSummary(aid, xid, list);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;list=%s;", m_flow, aid, list);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;skuIdCountList=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;", m_flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
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
    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo, Ref<Integer> ioStoreRecordIdRef){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchReducePdSkuHoldingStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo, ioStoreRecordIdRef);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;skuIdCountList=%s;rlOrderCode=%s;outStoreRecordInfo=%s;", m_flow, aid, unionPriId, skuIdCountList, skuIdCountList, outStoreRecordInfo);
            return rt;
        }
        return rt;
    }


    /**
     * 补偿库存，不会生成入库记录
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;skuIdCountList=%s;rlOrderCode=%s;reduceMode=%s;", m_flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode);
            return rt;
        }
        return rt;
    }

    /**
     * 管理态调用 <br/>
     * 刷新 rlOrderCode 的预扣记录。<br/>
     * 根据 allHoldingRecordList 和已有的预扣尽量进行对比， <br/>
     * 如果都有，则对比数量，数量不一致，就多退少补。  <br/>
     * 如果 holdingRecordList中有 db中没有 就生成预扣记录，并进行预扣库存.  <br/>
     * 如果 holdingRecordList中没有 db中有 就删除db中的预扣记录，并进行补偿库存。 <br/>
     * @param rlOrderCode 业务订单id/code
     * @param allHoldingRecordList 当前订单的所有预扣记录 [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
     */
    public int refreshHoldingRecordOfRlOrderCode(int aid, int unionPriId, String rlOrderCode, FaiList<Param> allHoldingRecordList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.refreshHoldingRecordOfRlOrderCode(aid, unionPriId, rlOrderCode, allHoldingRecordList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;allHoldingRecordList=%s;rlOrderCode=%s;", m_flow, aid, unionPriId, allHoldingRecordList, rlOrderCode);
            return rt;
        }
        return rt;
    }

    /**
     * 退库存，会生成入库记录
     * @param unionPriId
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlRefundId 退款id
     * @return
     */
    public int batchRefundStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlRefundId, Param inStoreRecordInfo) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchRefundStore(aid, tid , unionPriId, skuIdCountList, rlRefundId, inStoreRecordInfo, null);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;skuIdCountList=%s;rlRefundId=%s;inStoreRecordInfo=%s;", m_flow, aid, unionPriId, skuIdCountList, rlRefundId, inStoreRecordInfo);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;rlPdId=%s;useOwnerFieldList=%s;", m_flow, aid, unionPriId, pdId, rlPdId, useOwnerFieldList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPirId=%d;skuIdList=%s;useSourceFieldList=%s;", m_flow, aid, unionPirId, skuIdList, useSourceFieldList);
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
     * 根据 pdIdList 和 unionPriIdList 获取sku库存销售信息
     */
    public int getStoreSalesByPdIdsAndUIdList(int aid, int tid, FaiList<Integer> pdIds, FaiList<Integer> unionPriIdList, FaiList infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", m_flow, aid, pdIds, unionPriIdList);
            return rt;
        }
        rt = m_cli.batchGetSkuStoreSalesByUidAndPdId(aid, tid, unionPriIdList, pdIds, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;skuId=%s;unionPriIdList=%s;", m_flow, aid, pdIds, unionPriIdList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;skuIdList=%s;", m_flow, aid, unionPriId, skuIdList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 重置指定操作时间之前的入库成本
     */
    public int batchResetCostPrice(int aid, int sysType, int rlPdId, Calendar optTime, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.batchResetCostPrice(aid, sysType, rlPdId, optTime, infoList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;rlPdId=%s;list=%s;", m_flow, aid, rlPdId, infoList);
            return rt;
        }
        return rt;
    }

    /**
     * 获取出入库存记录
     */
    public int getInOutStoreRecordInfoList(int aid, SearchArg searchArg, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getInOutStoreRecordInfoList(aid, searchArg, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;searchArg=%s;", m_flow, aid, getSearchArgInfo(searchArg));
            return rt;
        }
        return rt;
    }

    /**
     * 获取出入库存记录
     */
    public int getInOutStoreSumList(int aid, SearchArg searchArg, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getInOutStoreSumList(aid, searchArg, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;searchArg=%s;", m_flow, aid, getSearchArgInfo(searchArg));
            return rt;
        }
        return rt;
    }

    /**
     * 批量删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(int aid, int tid, FaiList<Integer> pdIdList, String xid, boolean softDel){
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        rt = m_cli.batchDelPdAllStoreSales(aid, tid, pdIdList, xid, softDel);
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
        rt = m_cli.getAllSpuBizSummaryInfoListByPdId(aid, tid, pdId, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        return rt;
    }
    /**
     *
     * 获取spu 所以关联的业务库存销售汇总信息
     */
    public int getAllSpuBizSummaryInfoListByPdIdList(int aid, int tid, FaiList<Integer> pdIdList, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getAllSpuBizSummaryInfoListByPdIdList(aid, tid, pdIdList, infoList);
        if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;useOwnerFieldList=%s;", m_flow, aid, unionPriId, pdIdList, useOwnerFieldList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;pdIdList=%s;", m_flow, aid, unionPriId, pdIdList);
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
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;searchArg=%s;isBiz=%s;", m_flow, aid, unionPriId, getSearchArgInfo(searchArg), isBiz);
            return rt;
        }
        return rt;
    }
    public int importStoreSales(int aid, int tid, int unionPriId, int sysType, String xid, FaiList<Param> storeSaleSkuList, Param inStoreRecordInfo) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.importStoreSales(aid, tid, unionPriId, sysType, xid, storeSaleSkuList, inStoreRecordInfo);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;storeSaleSkuList=%s;inStoreRecordInfo=%s;", m_flow, aid, unionPriId, storeSaleSkuList, inStoreRecordInfo);
            return rt;
        }
        return rt;
    }

    public int clearRelData(int aid, int unionPriId) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        rt = m_cli.clearRelData(aid, unionPriId);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        rt = m_cli.clearAcct(aid, unionPriIds);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        return rt;
    }

    public int batchAddStoreSales(int aid, int tid, int unionPriId, FaiList<Param> storeSaleSkuList) {
        int rt = Errno.ERROR;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        rt = m_cli.batchAddStoreSales(aid, tid, unionPriId, storeSaleSkuList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%s;storeSaleSkuList=%s;", m_flow, aid, unionPriId, storeSaleSkuList);
            return rt;
        }
        return rt;
    }

    public int migrate(int aid, FaiList<Param> spuList) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        rt = m_cli.migrate(aid, spuList);
        if (rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "migrate error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }

    public void migrateYKService(int aid, FaiList<Param> spuList) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
        }

        rt = m_cli.migrateYKService(aid, spuList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "migrateYKService error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public void migrateYKStoreSku(int aid, FaiList<Param> storeSkuList) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
        }

        rt = m_cli.migrateYKStoreSku(aid, storeSkuList);
        if (rt != Errno.OK) {
            throw new MgException(rt, "migrateYKService error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    public void migrateYKServiceDel(int aid, FaiList<Integer> pdIds) {
        if (pdIds == null || pdIds.isEmpty()) {
            return;
        }
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "get MgProductStoreCli error;flow=%d;aid=%d;", m_flow, aid);
        }
        rt = m_cli.migrateYKServiceDel(aid, pdIds);
        if (rt != Errno.OK) {
            throw new MgException(rt, "migrateYKService error;flow=%d;aid=%d;", m_flow, aid);
        }
    }

    private int m_flow;
    private MgProductStoreCli m_cli;
}
