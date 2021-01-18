package fai.MgProductStoreSvr.interfaces.cli;

import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.dto.*;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductStoreCli extends FaiClient {
    public MgProductStoreCli(int flow) {
        super(flow, "MgProductStoreCli");
    }
    /**
     * 初始化
     */
    public boolean init() {
        return init("MgProductStoreCli", true);
    }

    /**
     * 刷新商品规格库存销售sku
     */
    public int refreshPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(pdScSkuInfoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(StoreSalesSkuDto.Key.PD_ID, pdId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_PD_ID, rlPdId);
            m_rt = pdScSkuInfoList.toBuffer(sendBody, StoreSalesSkuDto.Key.INFO_LIST, StoreSalesSkuDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "pdScSkuInfoList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH);
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
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList){
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
                Log.logErr(m_rt, "updaterList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(StoreSalesSkuDto.Key.PD_ID, pdId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, StoreSalesSkuDto.Key.UPDATER_LIST, StoreSalesSkuDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.SET_LIST);
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
     * 扣减库存
     * @param aid
     * @param tid
     * @param unionPriId
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param reduceMode
     * 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     * @return
     */
    public int reducePdSkuStore(int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode, int expireTimeSeconds){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || unionPriId == 0 || skuId == 0 || rlOrderId == 0 || count == 0 || reduceMode == 0 || expireTimeSeconds < 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;reduceMode=%s;expireTimeSeconds=%s;", aid, tid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putLong(StoreSalesSkuDto.Key.SKU_ID, skuId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_ORDER_ID, rlOrderId);
            sendBody.putInt(StoreSalesSkuDto.Key.COUNT, count);
            sendBody.putInt(StoreSalesSkuDto.Key.REDUCE_MODE, reduceMode);
            sendBody.putInt(StoreSalesSkuDto.Key.EXPIRE_TIME_SECONDS, expireTimeSeconds);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.REDUCE_STORE);
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
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    @Deprecated
    public int reducePdSkuHoldingStore(int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count){
        Param outStoreRecordInfo = new Param();
        return reducePdSkuHoldingStore(aid, tid, unionPriId, skuId, rlOrderId, count, outStoreRecordInfo);
    }
    /**
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     * @param aid
     * @param tid
     * @param unionPriId
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param outStoreRecordInfo 出库记录
     */
    public int reducePdSkuHoldingStore(int aid, int tid, int unionPriId, long skuId, int rlOrderId, int count, Param outStoreRecordInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || unionPriId == 0 || skuId == 0 || rlOrderId == 0 || count == 0 || outStoreRecordInfo == null ) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;unionPriId=%s;skuId=%s;rlOrderId=%s;count=%s;outStoreRecordInfo=%s;", aid, tid, unionPriId, skuId, rlOrderId, count, outStoreRecordInfo);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putLong(StoreSalesSkuDto.Key.SKU_ID, skuId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_ORDER_ID, rlOrderId);
            sendBody.putInt(StoreSalesSkuDto.Key.COUNT, count);
            outStoreRecordInfo.toBuffer(sendBody, StoreSalesSkuDto.Key.OUT_STORE_RECORD_INFO, InOutStoreRecordDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.REDUCE_HOLDING_STORE);
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
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }
    /**
     * 补偿库存
     */
    public int makeUpStore(int aid, int unionPriId, long skuId, int rlOrderId, int count, int reduceMode){
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
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putLong(StoreSalesSkuDto.Key.SKU_ID, skuId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_ORDER_ID, rlOrderId);
            sendBody.putInt(StoreSalesSkuDto.Key.COUNT, count);
            sendBody.putInt(StoreSalesSkuDto.Key.REDUCE_MODE, reduceMode);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.MAKE_UP_STORE);
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
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }
    /**
     * 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(StoreSalesSkuDto.Key.PD_ID, pdId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_PD_ID, rlPdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, StoreSalesSkuDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != StoreSalesSkuDto.Key.INFO_LIST) {
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
     * 添加库存出入库记录
     */
    public int addInOutStoreRecordInfoList(int aid, int tid, int unionPriId, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;infoList=%s;", aid, tid, infoList);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(InOutStoreRecordDto.Key.TID, tid);
            sendBody.putInt(InOutStoreRecordDto.Key.UNION_PRI_ID, unionPriId);
            m_rt = infoList.toBuffer(sendBody, InOutStoreRecordDto.Key.INFO_LIST, InOutStoreRecordDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.InOutStoreRecordCmd.ADD_LIST);
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
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 获取指定商品所有业务销售信息
     */
    public int getAllBizSalesSummaryInfoList(int aid, int tid, int pdId, FaiList<Param> infoList){
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
            sendBody.putInt(BizSalesSummaryDto.Key.TID, tid);
            sendBody.putInt(BizSalesSummaryDto.Key.PD_ID, pdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.BizSalesSummaryCmd.GET_LIST_BY_PD_ID);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, BizSalesSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != BizSalesSummaryDto.Key.INFO_LIST) {
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
     * 获取指定业务下 指定的商品集的业务销售信息
     */
    public int getBizSalesSummaryInfoListByPdIdList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || pdIdList == null) {
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
            sendBody.putInt(BizSalesSummaryDto.Key.TID, tid);
            sendBody.putInt(BizSalesSummaryDto.Key.UNION_PRI_ID, unionPriId);
            pdIdList.toBuffer(sendBody, BizSalesSummaryDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.BizSalesSummaryCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, BizSalesSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != BizSalesSummaryDto.Key.INFO_LIST) {
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
     * 获取商品销售信息
     */
    public int getSalesSummaryInfoList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
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
            sendBody.putInt(SalesSummaryDto.Key.TID, tid);
            sendBody.putInt(SalesSummaryDto.Key.UNION_PRI_ID, unionPriId);
            pdIdList.toBuffer(sendBody, SalesSummaryDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SalesSummaryCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SalesSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SalesSummaryDto.Key.INFO_LIST) {
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
