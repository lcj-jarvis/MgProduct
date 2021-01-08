package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.comm.util.*;


public class SpecificationService {
    public SpecificationService(int flow) {
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addTpScInfoList(aid, tid, unionPriId, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "addTpScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delTpScInfoList(aid, tid, unionPriId, rlTpScIdList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "delTpScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setTpScInfoList(aid, tid, unionPriId, updaterList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setTpScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getTpScInfoList(aid, tid, unionPriId, infoList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getTpScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "addTpScDetailInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, tpScDtIdList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "delTpScDetailInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, updaterList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setTpScDetailInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, infoList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getTpScDetailInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.importPdScInfo(aid, tid, unionPriId, pdId, rlTpScId, tpScDtIdList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "importPdScInfo error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, pdScSkuInfoList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "unionSetPdScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdScInfoList(aid, tid, unionPriId, pdId, infoList, onlyGetChecked);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPdScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPdSkuScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
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
            Log.logErr(rt, "get SpecificationCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPdSkuScInfoList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }


    private int m_flow;
    private MgProductSpecCli m_cli;
}

