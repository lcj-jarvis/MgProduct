package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.comm.util.*;


public class ProductSpecProc extends AbstractProductProc {
    public ProductSpecProc(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductSpecCli(flow);
        if(!m_cli.init()) {
            m_cli = null;
        }
    }

    /**
     * 批量添加规格模板
     */
    public int addTpScInfoList(int aid, int tid, int unionPriId, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addTpScInfoList(aid, tid, unionPriId, list);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int delTpScInfoList(int aid, int tid, int unionPriId, FaiList<Integer> rlTpScIdList){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delTpScInfoList(aid, tid, unionPriId, rlTpScIdList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 批量修改规格模板
     */
    public int setTpScInfoList(int aid, int tid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setTpScInfoList(aid, tid, unionPriId, updaterList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 获取规格模板列表
     */
    public int getTpScInfoList(int aid, int tid, int unionPriId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getTpScInfoList(aid, tid, unionPriId, infoList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 批量添加规格模板详情
     */
    public int addTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Param> list){
    int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, list);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 批量删除规格模板详情
     */
    public int delTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, tpScDtIdList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 批量修改规格模板详情
     */
    public int setTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, updaterList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 获取规格模板列表详情
     */
    public int getTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, infoList);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 导入规格模板
     */
    public int importPdScInfo(int aid, int tid, int unionPriId, int pdId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.importPdScInfo(aid, tid, unionPriId, pdId, rlTpScId, tpScDtIdList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 批量同步spu 为 sku
     */
    public int batchSynchronousSPU2SKU(int aid, int tid, int unionPriId, FaiList<Param> spuInfoList, FaiList<Param> simplePdScSkuInfoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.batchSynchronousSPU2SKU(aid, tid, unionPriId, spuInfoList, simplePdScSkuInfoList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 修改产品规格总接口
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     */
    public int unionSetPdScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList, FaiList<Param> pdScSkuInfoList ) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, pdScSkuInfoList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 获取产品规格列表
     */
    public int getPdScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> infoList, boolean onlyGetChecked) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;pdId=%s;", m_flow, aid, unionPriId, pdId);
            return rt;
        }
        rt = m_cli.getPdScInfoList(aid, tid, unionPriId, pdId, infoList, onlyGetChecked);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;pdId=%s", m_flow, aid, unionPriId, pdId);
            return rt;
        }
        return rt;
    }

    /**
     * 获取产品规格SKU列表
     */
    public int setPdSkuScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }
    /**
     * 获取产品规格列表
     */
    public int getPdSkuScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    /**
     * 根据 skuIdList 获取产品规格SKU列表
     */
    public int getPdSkuScInfoListBySkuIdList(int aid, int tid, FaiList<Long> skuIdList, FaiList<Param> infoList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        rt = m_cli.getPdSkuScInfoListBySkuIdList(aid, tid, skuIdList, infoList);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;tid=%d;", m_flow, aid, tid);
            return rt;
        }
        return rt;
    }
    /**
     * 根据 pdId 获取 pdId-skuId 集
     * @param pdIdList
     */
    public int getPdSkuIdInfoList(int aid, int tid, FaiList<Integer> pdIdList, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        rt = m_cli.getPdSkuIdInfoList(aid, tid, pdIdList, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        return rt;
    }
    /**
     * 批量删除商品所有规格
     */
    public int batchDelPdAllSc(int aid, int tid, FaiList<Integer> pdIdList, boolean softDel) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        rt = m_cli.batchDelPdAllSc(aid, tid, pdIdList, softDel);
        if(rt != Errno.OK) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;pdIdList=%s;softDel=%s;", m_flow, aid, pdIdList, softDel);
            return rt;
        }
        return rt;
    }
    /**
     * 获取 规格字符串
     */
    public int getScStrInfoList(int aid, int tid, FaiList<Integer> strIdList, FaiList<Param> list){
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get MgProductSpecCli error;flow=%d;aid=%d;strIdList=%s;", m_flow, aid, strIdList);
            return rt;
        }
        rt = m_cli.getScStrInfoList(aid, tid, strIdList, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            logErrWithPrintInvoked(rt, "error;flow=%d;aid=%d;strIdList=%s;", m_flow, aid, strIdList);
            return rt;
        }
        return rt;
    }


    private int m_flow;
    private MgProductSpecCli m_cli;
}

