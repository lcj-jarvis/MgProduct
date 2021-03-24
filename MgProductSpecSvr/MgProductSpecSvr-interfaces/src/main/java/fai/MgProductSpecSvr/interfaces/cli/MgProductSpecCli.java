package fai.MgProductSpecSvr.interfaces.cli;

import fai.MgProductSpecSvr.interfaces.cmd.MgProductSpecCmd;
import fai.MgProductSpecSvr.interfaces.dto.*;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductSpecCli extends FaiClient {
    public MgProductSpecCli(int flow) {
        super(flow, "MgProductSpecCli");
    }
    /**
     * 初始化
     */
    public boolean init() {
        return init("MgProductSpecCli", true);
    }

    /**
     * 批量添加规格模板
     */
    public int addTpScInfoList(int aid, int tid, int unionPriId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (list == null || list.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDto.Key.TID, tid);
            m_rt = list.toBuffer(sendBody, SpecTempDto.Key.INFO_LIST, SpecTempDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempCmd.ADD_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**
     * 批量删除规格模板
     */
    public int delTpScInfoList(int aid, int tid, int unionPriId, FaiList<Integer> rlTpScIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlTpScIdList == null || rlTpScIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDto.Key.TID, tid);
            rlTpScIdList.toBuffer(sendBody, SpecTempDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempCmd.DEL_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量修改规格模板
     */
    public int setTpScInfoList(int aid, int tid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDto.Key.TID, tid);
            m_rt = updaterList.toBuffer(sendBody, SpecTempDto.Key.UPDATER_LIST, SpecTempDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempCmd.SET_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**
     * 获取规格模板列表
     */
    public int getTpScInfoList(int aid, int tid, int unionPriId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDto.Key.TID, tid);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempCmd.GET_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpecTempDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpecTempDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 批量添加规格模板详情
     */
    public int addTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (list == null || list.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDetailDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDetailDto.Key.TID, tid);
            sendBody.putInt(SpecTempDetailDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = list.toBuffer(sendBody, SpecTempDetailDto.Key.INFO_LIST, SpecTempDetailDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempDetailCmd.ADD_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量删除规格模板详情
     */
    public int delTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (tpScDtIdList == null || tpScDtIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDetailDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDetailDto.Key.TID, tid);
            sendBody.putInt(SpecTempDetailDto.Key.RL_TP_SC_ID, rlTpScId);
            tpScDtIdList.toBuffer(sendBody, SpecTempDetailDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempDetailCmd.DEL_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量修改规格模板详情
     */
    public int setTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDetailDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDetailDto.Key.TID, tid);
            sendBody.putInt(SpecTempDetailDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = updaterList.toBuffer(sendBody, SpecTempDetailDto.Key.UPDATER_LIST, SpecTempDetailDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempDetailCmd.SET_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 获取规格模板列表详情
     */
    public int getTpScDetailInfoList(int aid, int tid, int unionPriId, int rlTpScId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecTempDetailDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(SpecTempDetailDto.Key.TID, tid);
            sendBody.putInt(SpecTempDetailDto.Key.RL_TP_SC_ID, rlTpScId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecTempDetailCmd.GET_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpecTempDetailDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpecTempDetailDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 导入规格模板
     */
    public int importPdScInfo(int aid, int tid, int unionPriId, int pdId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.PD_ID, pdId);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            if(tpScDtIdList != null){
                tpScDtIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecCmd.IMPORT);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量同步spu 为 sku
     */
    public int batchSynchronousSPU2SKU(int aid, int tid, int unionPriId, FaiList<Param> spuInfoList, FaiList<Param> simplePdScSkuInfoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(spuInfoList == null || spuInfoList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "spuInfoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.UNION_PRI_ID, unionPriId);
            spuInfoList.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol(MgProductSpecCmd.ProductSpecCmd.BATCH_SYN_SPU_TO_SKU, aid, 0, sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            if(simplePdScSkuInfoList != null){
                m_rt = simplePdScSkuInfoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
                if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int unionSetPdScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList) {
        return unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, null);
    }
    /**
     * 修改产品规格总接口
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     */
    public int unionSetPdScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList, FaiList<Param> rtPdScSkuInfoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.PD_ID, pdId);
            if(addList != null){
                m_rt = addList.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.getInfoDto());
                if(m_rt != Errno.OK){
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "addList error");
                    return m_rt;
                }
            }
            if(delList != null){
                delList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }
            if(updaterList != null){
                m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.getInfoDto());
                if(m_rt != Errno.OK){
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "updaterList error");
                    return m_rt;
                }
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecCmd.UNION_SET);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            if(rtPdScSkuInfoList != null){
                m_rt = rtPdScSkuInfoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
                if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    public int batchDelPdAllSc(int aid, int tid, FaiList<Integer> pdIdList){
        return batchDelPdAllSc(aid, tid, pdIdList, false);
    }
    /**
     * 批量删除商品所有规格
     */
    public int batchDelPdAllSc(int aid, int tid, FaiList<Integer> pdIdList, boolean softDel){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || pdIdList == null || pdIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error；aid=%s;pdIdList=%s;", aid, pdIdList);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            pdIdList.toBuffer(sendBody, ProductSpecDto.Key.PD_ID_LIST);
            sendBody.putBoolean(ProductSpecDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecCmd.BATCH_DEL_PD_ALL_SC);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取产品规格列表
     */
    public int getPdScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> infoList, boolean onlyGetChecked) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.PD_ID, pdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            if(onlyGetChecked){
                sendProtocol.setCmd(MgProductSpecCmd.ProductSpecCmd.GET_CHECKED_LIST);
            }else{
                sendProtocol.setCmd(MgProductSpecCmd.ProductSpecCmd.GET_LIST);
            }
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    public int setPdSkuScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList) {
        return setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList, null);
    }
    /**
     * 批量修改产品规格SKU
     */
    public int setPdSkuScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<ParamUpdater> updaterList, FaiList<Param> rtPdScSkuInfoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(updaterList == null || updaterList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductSpecSkuDto.Key.TID, tid);
            sendBody.putInt(ProductSpecSkuDto.Key.PD_ID, pdId);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecSkuDto.Key.UPDATER_LIST, ProductSpecSkuDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecSkuCmd.SET_LIST);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            if(rtPdScSkuInfoList != null){
                m_rt = rtPdScSkuInfoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
                if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 获取产品规格SKU列表
     */
    public int getPdSkuScInfoList(int aid, int tid, int unionPriId, int pdId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductSpecSkuDto.Key.TID, tid);
            sendBody.putInt(ProductSpecSkuDto.Key.PD_ID, pdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST);

            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 根据 skuIdList 获取产品规格SKU列表
     */
    public int getPdSkuScInfoListBySkuIdList(int aid, int tid, FaiList<Long> skuIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null || skuIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdList error");
                return m_rt;
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecSkuDto.Key.TID, tid);
            skuIdList.toBuffer(sendBody, ProductSpecSkuDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST_BY_SKU_ID_LIST);

            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取 pdId-skuId 信息集
     */
    public int getPdSkuIdInfoList(int aid, int tid, FaiList<Integer> pdIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(pdIdList == null || pdIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "pdIdList error");
                return m_rt;
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecSkuDto.Key.TID, tid);
            pdIdList.toBuffer(sendBody, ProductSpecSkuDto.Key.PD_ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_SKU_ID_INFO_LIST_BY_PD_ID_LIST);

            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecSkuDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecSkuDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取 规格字符串
     */
    public int getScStrInfoList(int aid, int tid, FaiList<Integer> strIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(strIdList == null || strIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "strIdList error");
                return m_rt;
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpecStrDto.Key.TID, tid);
            strIdList.toBuffer(sendBody, SpecStrDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductSpecCmd.SpecStrCmd.GET_LIST);

            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpecStrDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpecStrDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
}
