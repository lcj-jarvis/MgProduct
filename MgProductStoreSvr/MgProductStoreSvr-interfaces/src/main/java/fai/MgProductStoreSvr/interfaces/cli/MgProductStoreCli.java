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
     * 批量同步 库存销售 spu数据到sku
     */
    public int batchSynchronousStoreSalesSPU2SKU(int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> spuStoreSalesInfoList){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, sourceTid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, sourceUnionPriId);
            m_rt = spuStoreSalesInfoList.toBuffer(sendBody, StoreSalesSkuDto.Key.INFO_LIST, StoreSalesSkuDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "pdScSkuInfoList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_SYN_SPU_TO_SKU);
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
     * 批量同步 出入库记录
     */
    public int batchSynchronousInOutStoreRecord(int aid, int sourceTid, int sourceUnionPriId, FaiList<Param> recordInfoList){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, sourceTid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, sourceUnionPriId);
            m_rt = recordInfoList.toBuffer(sendBody, InOutStoreRecordDto.Key.INFO_LIST, InOutStoreRecordDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "pdScSkuInfoList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.InOutStoreRecordCmd.BATCH_SYN_RECORD);
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
     * 批量扣减库存
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     * 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     */
    public int batchReducePdSkuStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || unionPriId == 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode) || reduceMode == 0 || expireTimeSeconds < 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;reduceMode=%s;expireTimeSeconds=%s;", aid, tid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, StoreSalesSkuDto.getInfoDto());
            sendBody.putString(StoreSalesSkuDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(StoreSalesSkuDto.Key.REDUCE_MODE, reduceMode);
            sendBody.putInt(StoreSalesSkuDto.Key.EXPIRE_TIME_SECONDS, expireTimeSeconds);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE);
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
     * 批量扣减预扣库存
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录
     * @return
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || unionPriId == 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode) || outStoreRecordInfo == null ) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;outStoreRecordInfo=%s;", aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, StoreSalesSkuDto.getInfoDto());
            sendBody.putString(StoreSalesSkuDto.Key.RL_ORDER_CODE, rlOrderCode);
            outStoreRecordInfo.toBuffer(sendBody, StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO, InOutStoreRecordDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE);
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
     * 批量补偿库存
     * @param unionPriId
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     * 扣减模式 {@link StoreSalesSkuValObj.ReduceMode}
     * @return
     */
    public int batchMakeUpStore(int aid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || unionPriId == 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlOrderCode)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;unionPriId=%s;skuIdCountList=%s;rlOrderCode=%s;", aid, unionPriId, skuIdCountList, rlOrderCode);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, StoreSalesSkuDto.getInfoDto());
            sendBody.putString(StoreSalesSkuDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(StoreSalesSkuDto.Key.REDUCE_MODE, reduceMode);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE);
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
    public int getPdScSkuSalesStore(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList, FaiList<String> useSourceFieldList){
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
            if(useSourceFieldList != null){
                useSourceFieldList.toBuffer(sendBody, StoreSalesSkuDto.Key.STR_LIST);
            }

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
     * 根据 skuId 和 unionPriIdList 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStoreBySkuIdAndUIdList(int aid, int tid, long skuId, FaiList<Integer> unionPriIdList, FaiList infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(unionPriIdList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "unionPriIdList error");
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
            sendBody.putLong(StoreSalesSkuDto.Key.SKU_ID, skuId);
            unionPriIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.UID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_AND_UID_LIST);
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
     * 获取出入库存记录
     */
    public int getInOutStoreRecordInfoList(int aid, int tid, int unionPriId, boolean isSource, SearchArg searchArg, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || searchArg == null || searchArg.isEmpty() || infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;searchArg=%s;infoList=%s;", aid, tid, searchArg, infoList);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(InOutStoreRecordDto.Key.TID, tid);
            sendBody.putInt(InOutStoreRecordDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putBoolean(InOutStoreRecordDto.Key.IS_SOURCE, isSource);
            searchArg.toBuffer(sendBody, InOutStoreRecordDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.InOutStoreRecordCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, InOutStoreRecordDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != InOutStoreRecordDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != InOutStoreRecordDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }
            return m_rt = Errno.OK;
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


    /**
     * 批量删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(int aid, int tid, FaiList<Integer> pdIdList){
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

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            pdIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_DEL_PD_ALL_STORE_SALES);
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
     * 获取 业务 库存SKU信息
     */
    public int getBizStoreSkuSummaryInfoList(int aid, int tid, int unionPriId, SearchArg searchArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSkuSummaryDto.Key.TID, tid);
            sendBody.putInt(StoreSkuSummaryDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, StoreSkuSummaryDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSkuSummaryCmd.BIZ_GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, StoreSkuSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != StoreSkuSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != StoreSkuSummaryDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 获取库存SKU汇总信息
     */
    public int getStoreSkuSummaryInfoList(int aid, int tid, int sourceUnionPriId, SearchArg searchArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSkuSummaryDto.Key.TID, tid);
            sendBody.putInt(StoreSkuSummaryDto.Key.UNION_PRI_ID, sourceUnionPriId);
            searchArg.toBuffer(sendBody, StoreSkuSummaryDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSkuSummaryCmd.GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, StoreSkuSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != StoreSkuSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != StoreSkuSummaryDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

}
