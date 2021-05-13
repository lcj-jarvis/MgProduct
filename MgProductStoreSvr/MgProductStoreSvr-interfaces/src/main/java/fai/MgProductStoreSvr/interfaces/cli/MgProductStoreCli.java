package fai.MgProductStoreSvr.interfaces.cli;

import fai.MgProductInfUtil.MgProductInternalCli;
import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.dto.*;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;

public class MgProductStoreCli extends MgProductInternalCli {
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
    public int refreshSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> pdScSkuInfoList){
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
    public int setSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList){
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
     * 修改商品规格库存销售sku
     */
    public int batchSetSkuStoreSales(int aid, int tid, FaiList<Integer> uidList, int pdId, int rlPdId, FaiList<ParamUpdater> updaterList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(Util.isEmptyList(uidList)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "uidList error");
                return m_rt;
            }
            if(Util.isEmptyList(updaterList)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            uidList.toBuffer(sendBody, StoreSalesSkuDto.Key.UID_LIST);
            sendBody.putInt(StoreSalesSkuDto.Key.PD_ID, pdId);
            sendBody.putInt(StoreSalesSkuDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, StoreSalesSkuDto.Key.UPDATER_LIST, StoreSalesSkuDto.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList error;");
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_SET_LIST);
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
     * @param skuIdCountList [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
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
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, SkuCountChangeDto.getInfoDto());
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

    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo){
        return batchReducePdSkuHoldingStore(aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo, null);
    }
    /**
     * 批量扣减预扣库存
     * 预扣模式 {@link StoreSalesSkuValObj.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录
     * @return
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo, Ref<Integer> ioStoreRecordIdRef){
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
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, SkuCountChangeDto.getInfoDto());
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
            if(ioStoreRecordIdRef != null){
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv body null");
                    return m_rt;
                }
                // recv info
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, ioStoreRecordIdRef);
                if(m_rt != Errno.OK || keyRef.value != StoreSalesSkuDto.Key.IN_OUT_STORE_REC_ID){
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 批量补偿库存，不需要生成入库记录
     * @param unionPriId
     * @param skuIdCountList [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
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
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, SkuCountChangeDto.getInfoDto());
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
     * 管理态调用 <br/>
     * 刷新 rlOrderCode 的预扣记录。<br/>
     * 根据 allHoldingRecordList 和已有的预扣尽量进行对比， <br/>
     * 如果都有，则对比数量，数量不一致，就多退少补。  <br/>
     * 如果 holdingRecordList中有 db中没有 就生成预扣记录，并进行预扣库存.  <br/>
     * 如果 holdingRecordList中没有 db中有 就删除db中的预扣记录，并进行补偿库存。 <br/>
     * @param rlOrderCode 业务订单id/code
     * @param allHoldingRecordList 当前订单的所有预扣记录 [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
     */
    public int refreshHoldingRecordOfRlOrderCode(int aid, int unionPriId, String rlOrderCode, FaiList<Param> allHoldingRecordList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || unionPriId == 0 || allHoldingRecordList == null || allHoldingRecordList.isEmpty() || Str.isEmpty(rlOrderCode)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;unionPriId=%s;allHoldingRecordList=%s;rlOrderCode=%s;", aid, unionPriId, allHoldingRecordList, rlOrderCode);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            allHoldingRecordList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, SkuCountChangeDto.getInfoDto());
            sendBody.putString(StoreSalesSkuDto.Key.RL_ORDER_CODE, rlOrderCode);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE);
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
     * 批量退库存，需要生成入库记录
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlRefundId 退款id
     * @return
     */
    public int batchRefundStore(int aid, int tid, int unionPriId, FaiList<Param> skuIdCountList, String rlRefundId, Param inStoreRecordInfo, Ref<Integer> ioStoreRecordIdRef){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || tid == 0 || unionPriId == 0 || skuIdCountList == null || skuIdCountList.isEmpty() || Str.isEmpty(rlRefundId) || inStoreRecordInfo == null ) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;tid=%s;unionPriId=%s;skuIdCountList=%s;rlRefundId=%s;inStoreRecordInfo=%s;", aid, tid, unionPriId, skuIdCountList, rlRefundId, inStoreRecordInfo);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            skuIdCountList.toBuffer(sendBody, StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST, SkuCountChangeDto.getInfoDto());
            sendBody.putString(StoreSalesSkuDto.Key.RL_REFUND_ID, rlRefundId);
            inStoreRecordInfo.toBuffer(sendBody, StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO, InOutStoreRecordDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REFUND_STORE);
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
            if(ioStoreRecordIdRef != null){
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv body null");
                    return m_rt;
                }
                // recv info
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, ioStoreRecordIdRef);
                if(m_rt != Errno.OK || keyRef.value != StoreSalesSkuDto.Key.IN_OUT_STORE_REC_ID){
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }


    /**
     * 根据uid和pdId 获取商品规格库存销售sku
     */
    public int getSkuStoreSales(int aid, int tid, int unionPriId, int pdId, int rlPdId, FaiList<Param> infoList, FaiList<String> useSourceFieldList){
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
    public int getStoreSalesBySkuIdAndUIdList(int aid, int tid, long skuId, FaiList<Integer> unionPriIdList, FaiList<Param> infoList){
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
     * 根据 unionPriIdList 和 pdIdList 获取商品规格库存销售sku
     */
    public int batchGetSkuStoreSalesByUidAndPdId(int aid, int tid, FaiList<Integer> unionPriIdList, FaiList<Integer> pdIdList, FaiList<Param> infoList){
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
            if(pdIdList == null){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            unionPriIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.UID_LIST);
            pdIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_GET_BY_UID_AND_PD_ID);
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
     * 根据 unionPriIdList 和 skuIdList 获取商品规格库存销售sku
     */
    public int batchGetSkuStoreSalesByUidAndSkuId(int aid, int tid, FaiList<Integer> unionPriIdList, FaiList<Long> skuIdList, FaiList<Param> infoList){
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
            if(skuIdList == null){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            unionPriIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.UID_LIST);
            skuIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_GET_BY_UID_AND_SKU_ID);
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
    public int getStoreSalesBySkuIdList(int aid, int tid, int unionPirId, FaiList<Long> skuIdList, FaiList<Param> infoList, FaiList<String> useSourceFieldList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null){
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
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPirId);
            skuIdList.toBuffer(sendBody, StoreSalesSkuDto.Key.ID_LIST);
            if(useSourceFieldList != null){
                useSourceFieldList.toBuffer(sendBody, StoreSalesSkuDto.Key.STR_LIST);
            }
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_LIST);
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
     * 根据 pdId 获取商品规格库存销售sku
     */
    public int getSkuStoreSalesByPdId(int aid, int tid, int pdId, FaiList<Param> infoList){
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
            sendBody.putInt(StoreSalesSkuDto.Key.PD_ID, pdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_PD_ID);
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
     * 获取预扣记录
     */
    public int getHoldingRecordList(int aid, int tid, int unionPriId, FaiList<Long> skuIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdList error");
            }
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(HoldingRecordDto.Key.TID, tid);
            sendBody.putInt(HoldingRecordDto.Key.UNION_PRI_ID, unionPriId);
            skuIdList.toBuffer(sendBody, HoldingRecordDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.HoldingRecordCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, HoldingRecordDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != HoldingRecordDto.Key.INFO_LIST) {
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
    public int getInOutStoreRecordInfoList(int aid, SearchArg searchArg, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || searchArg == null || searchArg.isEmpty() || infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;searchArg=%s;infoList=%s;", aid, searchArg, infoList);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            searchArg.toBuffer(sendBody, InOutStoreRecordDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.InOutStoreRecordCmd.NEW_GET_LIST);
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

    public int getInOutStoreSumList(int aid, SearchArg searchArg, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || searchArg == null || searchArg.isEmpty() || infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%s;searchArg=%s;infoList=%s;", aid, searchArg, infoList);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            searchArg.toBuffer(sendBody, InOutStoreRecordDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.InOutStoreRecordCmd.GET_SUM_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, InOutStoreRecordDto.getSumInfoDto());
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
     * 根据pdId 获取所关联的 spu 业务库存销售汇总信息
     */
    public int getAllSpuBizSummaryInfoListByPdId(int aid, int tid, int pdId, FaiList<Param> infoList){
       return getSpuBizSummaryInfoListByPdIdAndUidList(aid, tid, null, pdId, infoList);
    }

    /**
     * 根据pdId和unionPriIdList获取匹配上的数据
     */
    public int getSpuBizSummaryInfoListByPdIdAndUidList(int aid, int tid, FaiList<Integer> uidList, int pdId, FaiList<Param> infoList){
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
            if(uidList != null && uidList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "uidList error");
                return m_rt;
            }
            if(uidList == null){
                uidList = new FaiList<Integer>();
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuBizSummaryDto.Key.PD_ID, pdId);
            uidList.toBuffer(sendBody, SpuBizSummaryDto.Key.UID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuBizSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
    public int getAllSpuBizSummaryInfoListByPdIdList(int aid, int tid, FaiList<Integer> pdIdList, FaiList<Param> infoList){
        return getSpuBizSummaryInfoListByPdIdListAndUidList(aid, tid, null, pdIdList, infoList);
    }
    /**
     * 根据pdIdList 获取所关联的 spu 业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoListByPdIdListAndUidList(int aid, int tid, FaiList<Integer> uidList, FaiList<Integer> pdIdList, FaiList<Param> infoList){
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
            if(uidList != null && uidList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "uidList error");
                return m_rt;
            }
            if(uidList == null){
                uidList = new FaiList<Integer>();
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            pdIdList.toBuffer(sendBody, SpuBizSummaryDto.Key.ID_LIST);
            uidList.toBuffer(sendBody, SpuBizSummaryDto.Key.UID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuBizSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO_LIST) {
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
     * 根据 pdIdList 获取指定业务下 spu业务库存销售汇总信息
     */
    public int getSpuBizSummaryInfoListByPdIdList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList, FaiList<String> useSourceFieldList){
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
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuBizSummaryDto.Key.UNION_PRI_ID, unionPriId);
            pdIdList.toBuffer(sendBody, SpuBizSummaryDto.Key.ID_LIST);
            if(useSourceFieldList != null){
                useSourceFieldList.toBuffer(sendBody, StoreSalesSkuDto.Key.STR_LIST);
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuBizSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO_LIST) {
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
     * 获取 spu 业务销售汇总的数据状态
     */
    public int getSpuBizSummaryDataStatus(int aid, int tid, int unionPriId, Param dataStatusInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(dataStatusInfo == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "dataStatusInfo error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuBizSummaryDto.Key.UNION_PRI_ID, unionPriId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_DATA_STATUS);
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
            m_rt = dataStatusInfo.fromBuffer(recvBody, keyRef, DataStatus.Dto.getDataStatusDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO) {
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
     * 获取 spu 业务销售汇总 的全部数据的部分字段
     */
    public int getSpuBizSummaryAllData(int aid, int tid, int unionPriId, FaiList<Param> infoList){
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
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuBizSummaryDto.Key.UNION_PRI_ID, unionPriId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_ALL_DATA_PART_FIELD);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuBizSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO_LIST) {
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
     * 直接从db搜索 spu 业务销售汇总 ，返回部分字段
     */
    public int searchSpuBizSummaryFromDb(int aid, int tid, int unionPriId, SearchArg searchArg, FaiList<Param> infoList){
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
            if(infoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(SpuBizSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuBizSummaryDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, SpuBizSummaryDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuBizSummaryCmd.SEARCH_PART_FIELD);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuBizSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuBizSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != SpuBizSummaryDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取spu库存销售汇总信息
     */
    public int getSpuSummaryInfoList(int aid, int tid, int unionPriId, FaiList<Integer> pdIdList, FaiList<Param> infoList){
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
            sendBody.putInt(SpuSummaryDto.Key.TID, tid);
            sendBody.putInt(SpuSummaryDto.Key.UNION_PRI_ID, unionPriId);
            pdIdList.toBuffer(sendBody, SpuSummaryDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SpuSummaryCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, SpuSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SpuSummaryDto.Key.INFO_LIST) {
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
     * 获取 SKU业务库存销售汇总信息
     */
    public int getSkuBizSummaryInfoList(int aid, int tid, int unionPriId, SearchArg searchArg, FaiList<Param> list){
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
            sendBody.putInt(SkuSummaryDto.Key.TID, tid);
            sendBody.putInt(SkuSummaryDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, SkuSummaryDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SkuSummaryCmd.BIZ_GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, SkuSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SkuSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != SkuSummaryDto.Key.TOTAL_SIZE){
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
     * 获取SKU库存销售汇总信息
     */
    public int getSkuSummaryInfoList(int aid, int tid, int sourceUnionPriId, SearchArg searchArg, FaiList<Param> list){
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
            sendBody.putInt(SkuSummaryDto.Key.TID, tid);
            sendBody.putInt(SkuSummaryDto.Key.UNION_PRI_ID, sourceUnionPriId);
            searchArg.toBuffer(sendBody, SkuSummaryDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.SkuSummaryCmd.GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, SkuSummaryDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != SkuSummaryDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != SkuSummaryDto.Key.TOTAL_SIZE){
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

    public int batchDelPdAllStoreSales(int aid, int tid, FaiList<Integer> pdIdList){
        return batchDelPdAllStoreSales(aid, tid, pdIdList, false);
    }
    /**
     * 批量删除商品所有库存销售相关信息
     */
    public int batchDelPdAllStoreSales(int aid, int tid, FaiList<Integer> pdIdList, boolean softDel){
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
            sendBody.putBoolean(StoreSalesSkuDto.Key.SOFT_DEL, softDel);

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
     * 导入数据
     */
    public int importStoreSales(int aid, int tid, int unionPriId, FaiList<Param> storeSaleSkuList, Param inStoreRecordInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(Util.isEmptyList(storeSaleSkuList)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "storeSaleSkuList error");
                return m_rt;
            }
            if(Str.isEmpty(inStoreRecordInfo)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "inStoreRecordInfo error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(StoreSalesSkuDto.Key.TID, tid);
            sendBody.putInt(StoreSalesSkuDto.Key.UNION_PRI_ID, unionPriId);
            storeSaleSkuList.toBuffer(sendBody, StoreSalesSkuDto.Key.INFO_LIST, StoreSalesSkuDto.getInfoDto());
            inStoreRecordInfo.toBuffer(sendBody, StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO, InOutStoreRecordDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductStoreCmd.StoreSalesSkuCmd.IMPORT);
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
}
