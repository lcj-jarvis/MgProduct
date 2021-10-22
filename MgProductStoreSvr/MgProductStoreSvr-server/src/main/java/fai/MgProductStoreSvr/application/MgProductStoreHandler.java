package fai.MgProductStoreSvr.application;

import fai.MgProductStoreSvr.application.mq.ReportConsumeListener;
import fai.MgProductStoreSvr.application.service.*;
import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.*;
import fai.comm.fseata.client.core.rpc.annotation.SagaTransaction;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.NKDef;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.mq.api.Consumer;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.util.*;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;
import java.util.Calendar;

public class MgProductStoreHandler extends MiddleGroundHandler {
    public MgProductStoreHandler(FaiServer server) {
        super(server);
        ServerConfig serverConfig = server.getConfig();
        initMq();
    }

    private void initMq() {
        Producer producer = MqFactory.createProducer(MqConfig.SpuBizReport.PRODUCER);
        int rt = producer.start();
        if (rt != Errno.OK) {
            Log.logErr(rt, "producer start err;");
            throw new RuntimeException("producer start err");
        }

        Consumer consumer = MqFactory.createConsumer(MqConfig.SpuBizReport.CONSUMER);
        consumer.subscribe(MqConfig.Report.TOPIC, "*", new ReportConsumeListener(m_summaryService));
        rt = consumer.start();
        if (rt != Errno.OK) {
            Log.logErr(rt, "consumer start err;");
            throw new RuntimeException("consumer start err");
        }

    }

    private void shutDownMq() {
        MqFactory.getProducer(MqConfig.SpuBizReport.PRODUCER).shutdown();
        MqFactory.getConsumer(MqConfig.SpuBizReport.CONSUMER).shutdown();
    }


    @Override
    public void destroy() {
        super.destroy();
        shutDownMq();
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.REFRESH_ROLLBACK)
    private int refreshSkuStoreSales(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyInteger(value = StoreSalesSkuDto.Key.SYS_TYPE, useDefault = true) final int sysType,
                                     @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) final String xid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                     @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.INFO_LIST)
                                             FaiList<Param> pdScSkuInfoList) throws IOException {
        return m_storeSalesSkuService.refreshSkuStoreSales(session, flow, aid, tid, unionPriId, sysType, xid, pdId, rlPdId, pdScSkuInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH_ROLLBACK)
    private int refreshSkuStoreSalesRollback(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                             @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_storeSalesSkuService.refreshSkuStoreSalesRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.CLONE_BIZ_BIND)
    private int cloneBizBind(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(StoreSalesSkuDto.Key.FROM_UNION_PRI_ID) final int fromUnionPriId,
                              @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int toUnionPriId) throws IOException {
        return m_storeSalesSkuService.cloneBizBind(session, flow, aid, fromUnionPriId, toUnionPriId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.COPY_BIZ_BIND)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.COPY_BIZ_BIND_ROLLBACK)
    private int copyBizBind(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) final String xid,
                             @ArgBodyInteger(StoreSalesSkuDto.Key.FROM_UNION_PRI_ID) final int fromUnionPriId,
                             @ArgList(keyMatch = StoreSalesSkuDto.Key.INFO_LIST, classDef = StoreSalesSkuDto.class,
                             methodDef = "getCopyDto") FaiList<Param> copyBindList) throws IOException {
        return m_storeSalesSkuService.copyBizBind(session, flow, aid, xid, fromUnionPriId, copyBindList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.COPY_BIZ_BIND_ROLLBACK)
    private int copyBizBindRollback(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                         @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_storeSalesSkuService.copyBizBindRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REPORT)
    private int reportSummary(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgList(keyMatch = StoreSalesSkuDto.Key.PD_ID, useDefault = true) FaiList<Integer> pdIds,
                                     @ArgList(keyMatch = StoreSalesSkuDto.Key.SKU_ID, useDefault = true) FaiList<Long> skuIds,
                                     @ArgBodyBoolean(StoreSalesSkuDto.Key.REPORT_COUNT) boolean reportCount,
                                     @ArgBodyBoolean(StoreSalesSkuDto.Key.REPORT_PRICE) boolean reportPrice) throws IOException {
        return m_storeSalesSkuService.reportSummary(session, flow, aid, pdIds, skuIds, reportCount, reportPrice);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_SYN_SPU_TO_SKU)
    private int batchSynchronousStoreSalesSPU2SKU(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int sourceTid,
                                                  @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int sourceUnionPriId,
                                                  @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.INFO_LIST)
                                                          FaiList<Param> spuStoreSalesInfoList) throws IOException {
        return m_storeSalesSkuService.batchSynchronousStoreSalesSPU2SKU(session, flow, aid, sourceTid, sourceUnionPriId, spuStoreSalesInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.BATCH_SYN_RECORD)
    private int batchSynchronousInOutStoreRecord(final FaiSession session,
                                                 @ArgFlow final int flow,
                                                 @ArgAid final int aid,
                                                 @ArgBodyInteger(InOutStoreRecordDto.Key.TID) final int sourceTid,
                                                 @ArgBodyInteger(InOutStoreRecordDto.Key.UNION_PRI_ID) final int sourceUnionPriId,
                                                 @ArgList(classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto", keyMatch = InOutStoreRecordDto.Key.INFO_LIST)
                                                         FaiList<Param> infoList) throws IOException {
        return m_recordService.batchSynchronousInOutStoreRecord(session, flow, aid, sourceTid, sourceUnionPriId, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.SET_LIST)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.SET_SKU_STORE_SALES_ROLLBACK)
    private int setSkuStoreSales(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) final String xid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                 @ArgBodyInteger(value = StoreSalesSkuDto.Key.SYS_TYPE, useDefault = true) final int sysType,
                                 @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.UPDATER_LIST)
                                         FaiList<ParamUpdater> updaterList) throws IOException {
        return m_storeSalesSkuService.setSkuStoreSales(session, flow, aid, tid, unionPriId, xid, pdId, rlPdId, sysType, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.SET_SKU_STORE_SALES_ROLLBACK)
    private int setSkuStoreSalesRollback(final FaiSession session,
                                                @ArgFlow final int flow,
                                                @ArgAid final int aid,
                                                @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                                @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_storeSalesSkuService.setSkuStoreSalesRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_SET_LIST)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.SET_SKU_STORE_SALES_ROLLBACK)
    private int batchSetSkuStoreSales(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) final String xid,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int ownerUnionPriId,
                                      @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) final FaiList<Integer> unionPriIdList,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                      @ArgBodyInteger(value = StoreSalesSkuDto.Key.SYS_TYPE, useDefault = true) final int sysType,
                                      @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.UPDATER_LIST)
                                              FaiList<ParamUpdater> updaterList) throws IOException {
        return m_storeSalesSkuService.batchSetSkuStoreSales(session, flow, aid, tid, ownerUnionPriId, unionPriIdList, xid, pdId, rlPdId, sysType, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE)
    private int batchReducePdSkuStore(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgList(classDef = SkuCountChangeDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                              FaiList<Param> skuIdCountList,
                                      @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode,
                                      @ArgBodyInteger(StoreSalesSkuDto.Key.EXPIRE_TIME_SECONDS) final int expireTimeSeconds) throws IOException {
        return m_storeSalesSkuService.batchReducePdSkuStore(session, flow, aid, tid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE)
    private int batchReducePdSkuHoldingStore(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                             @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                             @ArgList(classDef = SkuCountChangeDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                                     FaiList<Param> skuIdCountList,
                                             @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                                             @ArgParam(classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO)
                                                     Param outStoreRecordInfo) throws IOException {
        return m_storeSalesSkuService.batchReducePdSkuHoldingStore(session, flow, aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE)
    private int batchMakeUpStore(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgList(classDef = SkuCountChangeDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                         FaiList<Param> skuIdCountList,
                                 @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode) throws IOException {
        return m_storeSalesSkuService.batchMakeUpStore(session, flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE)
    private int refreshHoldingRecordOfRlOrderCode(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                                  @ArgList(classDef = SkuCountChangeDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                                          FaiList<Param> holdingRecordList,
                                                  @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode) throws IOException {
        return m_storeSalesSkuService.refreshHoldingRecordOfRlOrderCode(session, flow, aid, unionPriId, rlOrderCode, holdingRecordList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REFUND_STORE)
    private int batchRefundStore(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                         FaiList<Param> skuIdCountList,
                                 @ArgBodyString(StoreSalesSkuDto.Key.RL_REFUND_ID) final String rlRefundId,
                                 @ArgParam(classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO)
                                         Param inStoreRecordInfo) throws IOException {
        return m_storeSalesSkuService.batchRefundStore(session, flow, aid, tid, unionPriId, skuIdCountList, rlRefundId, inStoreRecordInfo);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST)
    private int getSkuStoreSales(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                 @ArgList(keyMatch = StoreSalesSkuDto.Key.STR_LIST, useDefault = true)
                                         FaiList<String> useSourceFieldList) throws IOException {
        return m_storeSalesSkuService.getSkuStoreSales(session, flow, aid, tid, unionPriId, pdId, rlPdId, useSourceFieldList);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_LIST)
    private int getSkuStoreSalesBySkuIdList(final FaiSession session,
                                            @ArgFlow final int flow,
                                            @ArgAid final int aid,
                                            @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                            @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                            @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST)
                                                    FaiList<Long> skuIdList,
                                            @ArgList(keyMatch = StoreSalesSkuDto.Key.STR_LIST, useDefault = true)
                                                    FaiList<String> useSourceFieldList) throws IOException {
        return m_storeSalesSkuService.getSkuStoreSalesBySkuIdList(session, flow, aid, tid, unionPriId, skuIdList, useSourceFieldList);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_AND_UID_LIST)
    private int getStoreSalesBySkuIdAndUIdList(final FaiSession session,
                                               @ArgFlow final int flow,
                                               @ArgAid final int aid,
                                               @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                               @ArgBodyLong(StoreSalesSkuDto.Key.SKU_ID) final long skuId,
                                               @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) FaiList<Integer> unionPriIdList) throws IOException {
        return m_storeSalesSkuService.getStoreSalesBySkuIdAndUIdList(session, flow, aid, skuId, unionPriIdList);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_GET_BY_UID_AND_PD_ID)
    private int batchGetSkuStoreSalesByUidAndPdId(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                                  @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) FaiList<Integer> unionPriIdList,
                                                  @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST) FaiList<Integer> pdIdList) throws IOException {
        return m_storeSalesSkuService.batchGetSkuStoreSalesByUidAndPdId(session, flow, aid, unionPriIdList, pdIdList);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_GET_BY_UID_AND_SKU_ID)
    private int batchGetSkuStoreSalesByUidAndSkuId(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                                  @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) FaiList<Integer> unionPriIdList,
                                                  @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST) FaiList<Long> skuIdList) throws IOException {
        return m_storeSalesSkuService.batchGetSkuStoreSalesByUidAndSkuId(session, flow, aid, unionPriIdList, skuIdList);
    }

    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_PD_ID)
    private int getSkuStoreSalesByPdId(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                       @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId) throws IOException {
        return m_storeSalesSkuService.getSkuStoreSalesByPdId(session, flow, aid, pdId);
    }

    @Cmd(MgProductStoreCmd.HoldingRecordCmd.GET_LIST)
    private int getHoldingRecord(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(HoldingRecordDto.Key.TID) final int tid,
                                 @ArgBodyInteger(HoldingRecordDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgList(keyMatch = HoldingRecordDto.Key.ID_LIST) final FaiList<Long> skuIdList) throws IOException {
        return m_storeSalesSkuService.getHoldingRecordList(session, flow, aid, tid, unionPriId, skuIdList);
    }


    @WrittenCmd
    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.ADD_LIST)
    private int addInOutStoreRecordInfoList(final FaiSession session,
                                            @ArgFlow final int flow,
                                            @ArgAid final int aid,
                                            @ArgBodyInteger(InOutStoreRecordDto.Key.TID) final int tid,
                                            @ArgBodyInteger(InOutStoreRecordDto.Key.UNION_PRI_ID) final int unionPriId,
                                            @ArgList(classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto", keyMatch = InOutStoreRecordDto.Key.INFO_LIST)
                                                    FaiList<Param> infoList) throws IOException {
        return m_recordService.addInOutStoreRecordInfoList(session, flow, aid, tid, unionPriId, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.BATCH_RESET_PRICE)
    private int batchResetCostPrice(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(value = InOutStoreRecordDto.Key.SYS_TYPE, useDefault = true) final int sysType,
                                    @ArgBodyInteger(InOutStoreRecordDto.Key.RL_PD_ID) final int rlPdId,
                                    @ArgBodyCalendar(InOutStoreRecordDto.Key.OPT_TIME)Calendar optTime,
                                    @ArgList(classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto", keyMatch = InOutStoreRecordDto.Key.INFO_LIST)
                                                FaiList<Param> infoList) throws IOException {
        return m_recordService.batchResetCostPrice(session, flow, aid, sysType, rlPdId, optTime, infoList);
    }

    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.GET_LIST)
    private int getInOutStoreRecordInfoList(final FaiSession session,
                                            @ArgFlow final int flow,
                                            @ArgAid final int aid,
                                            @ArgBodyInteger(InOutStoreRecordDto.Key.TID) final int tid,
                                            @ArgBodyInteger(InOutStoreRecordDto.Key.UNION_PRI_ID) final int unionPriId,
                                            @ArgBodyBoolean(InOutStoreRecordDto.Key.IS_SOURCE) final boolean isSource,
                                            @ArgSearchArg(InOutStoreRecordDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_recordService.getInOutStoreRecordInfoList(session, flow, aid, tid, unionPriId, isSource, searchArg);
    }
    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.NEW_GET_LIST)
    private int newGetInOutStoreRecordInfoList(final FaiSession session,
                                            @ArgFlow final int flow,
                                            @ArgAid final int aid,
                                            @ArgSearchArg(InOutStoreRecordDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_recordService.newGetInOutStoreRecordInfoList(session, flow, aid, searchArg);
    }

    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.GET_SUM_LIST)
    private int getInOutStoreSumList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgSearchArg(InOutStoreRecordDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_recordService.getInOutStoreSumList(session, flow, aid, searchArg);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID)
    private int getSpuBizSummaryInfoListByPdId(final FaiSession session,
                                               @ArgFlow final int flow,
                                               @ArgAid final int aid,
                                               @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                               @ArgBodyInteger(SpuBizSummaryDto.Key.PD_ID) final int pdId,
                                               @ArgList(keyMatch = SpuBizSummaryDto.Key.UID_LIST, useDefault = true) FaiList<Integer> uidList) throws IOException {
        return m_summaryService.getSpuBizSummaryInfoListByPdId(session, flow, aid, tid, pdId, uidList);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID_LIST)
    private int getSpuBizSummaryInfoListByPdIdList(final FaiSession session,
                                                   @ArgFlow final int flow,
                                                   @ArgAid final int aid,
                                                   @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                                   @ArgList(keyMatch = SpuBizSummaryDto.Key.ID_LIST) final FaiList<Integer> pdIdList,
                                                   @ArgList(keyMatch = SpuBizSummaryDto.Key.UID_LIST, useDefault = true) FaiList<Integer> uidList) throws IOException {
        return m_summaryService.getSpuBizSummaryInfoListByPdIdList(session, flow, aid, tid, pdIdList, uidList);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST)
    private int getSpuBizSummaryInfoList(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                         @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                         @ArgList(keyMatch = SpuSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList,
                                         @ArgList(keyMatch = StoreSalesSkuDto.Key.STR_LIST, useDefault = true)
                                                 FaiList<String> useSourceFieldList) throws IOException {
        return m_summaryService.getSpuBizSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList, useSourceFieldList);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_DATA_STATUS)
    private int getSpuBizSummaryDataStatus(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                           @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId) throws IOException {
        return m_summaryService.getSpuBizSummaryDataStatus(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_ALL_DATA_PART_FIELD)
    private int getSpuBizSummaryAllData(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                        @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId) throws IOException {
        return m_summaryService.getSpuBizSummaryAllData(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.SEARCH_PART_FIELD)
    private int searchSpuBizSummaryFromDb(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                          @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                          @ArgSearchArg(value = SpuBizSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_summaryService.searchSpuBizSummaryFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductStoreCmd.SpuSummaryCmd.GET_LIST)
    private int getSpuSummaryInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SpuSummaryDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SpuSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgList(keyMatch = SpuSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList) throws IOException {
        return m_summaryService.getSpuSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList);
    }


    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_DEL_PD_ALL_STORE_SALES)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.BATCH_DEL_PD_ALL_STORE_SALES_ROLLBACK)
    private int batchDelPdAllStoreSales(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                        @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST)
                                                FaiList<Integer> pdIdList,
                                        @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) String xid) throws IOException {
        return m_storeService.batchDelPdAllStoreSales(session, flow, aid, tid, pdIdList, xid);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_DEL_PD_ALL_STORE_SALES_ROLLBACK)
    private int batchDelPdAllStoreSalesRollback(final FaiSession session,
                                                @ArgFlow final int flow,
                                                @ArgAid final int aid,
                                                @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                                @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_storeService.batchDelPdAllStoreSalesRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.IMPORT)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.StoreSalesSkuCmd.IMPORT_STORE_SALES_ROLLBACK)
    private int importStoreSales(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgBodyInteger(value = StoreSalesSkuDto.Key.SYS_TYPE, useDefault = true) final int sysType,
                                 @ArgBodyXid(value = StoreSalesSkuDto.Key.XID, useDefault = true) String xid,
                                 @ArgList(keyMatch = StoreSalesSkuDto.Key.INFO_LIST,
                                         classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto") FaiList<Param> storeSaleSkuList,
                                 @ArgParam(keyMatch = StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO,
                                         classDef = InOutStoreRecordDto.class, methodDef = "getInfoDto") Param inStoreRecordInfo) throws IOException {
        return m_storeService.importStoreSales(session, flow, aid, tid, unionPriId, sysType, xid, storeSaleSkuList, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.IMPORT_STORE_SALES_ROLLBACK)
    private int importStoreSalesRollback(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                         @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_storeService.importStoreSalesRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.CLEAR_REL_DATA)
    private int clearRelData(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId) throws IOException {
        return m_storeService.clearRelData(session, flow, aid, unionPriId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.CLEAR_ACCT)
    private int clearAcct(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgList(keyMatch = StoreSalesSkuDto.Key.UNION_PRI_IDS) final FaiList<Integer> unionPriIds) throws IOException {
        return m_storeService.clearAcct(session, flow, aid, unionPriIds);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_ADD)
    private int batchAddStoreSales(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgList(keyMatch = StoreSalesSkuDto.Key.INFO_LIST,
                                         classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto") FaiList<Param> storeSaleSkuList) throws IOException {
        return m_storeSalesSkuService.batchAddStoreSales(session, flow, aid, tid, unionPriId, storeSaleSkuList);
    }

    @Cmd(MgProductStoreCmd.SkuSummaryCmd.BIZ_GET_LIST)
    private int getSkuBizSummaryInfoList(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyInteger(SkuSummaryDto.Key.TID) final int tid,
                                         @ArgBodyInteger(SkuSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                         @ArgSearchArg(SkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_summaryService.getSkuBizSummaryInfoList(session, flow, aid, tid, unionPriId, searchArg);
    }

    @Cmd(MgProductStoreCmd.SkuSummaryCmd.GET_LIST)
    private int getSkuSummaryInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SkuSummaryDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SkuSummaryDto.Key.UNION_PRI_ID) final int sourceUnionPriId,
                                      @ArgSearchArg(SkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_summaryService.getSkuSummaryInfoList(session, flow, aid, tid, sourceUnionPriId, searchArg);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.SET)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.SpuBizSummaryCmd.SET_ROLLBACK)
    private int setSpuBizSummary(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyXid(value = SpuBizSummaryDto.Key.XID, useDefault = true) String xid,
                                      @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) int unionPriId,
                                      @ArgBodyInteger(SpuBizSummaryDto.Key.PD_ID) int pdId,
                                      @ArgParamUpdater(keyMatch = SpuBizSummaryDto.Key.UPDATER, methodDef = "getInfoDto",
                                      classDef = SpuBizSummaryDto.class) ParamUpdater updater) throws IOException {
        return m_summaryService.setSpuBizSummary(session, flow, aid, xid, unionPriId, pdId, updater);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.SET_ROLLBACK)
    private int setSpuBizSummaryRollback(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                         @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_summaryService.setSpuBizSummaryRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.BATCH_SET)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.SpuBizSummaryCmd.BATCH_SET_ROLLBACK)
    private int batchSetSpuBizSummary(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyXid(value = SpuBizSummaryDto.Key.XID, useDefault = true) String xid,
                                 @ArgList(keyMatch = SpuBizSummaryDto.Key.UID_LIST) FaiList<Integer> unionPriIds,
                                 @ArgList(keyMatch = SpuBizSummaryDto.Key.PD_ID) FaiList<Integer> pdIds,
                                 @ArgParamUpdater(keyMatch = SpuBizSummaryDto.Key.UPDATER, methodDef = "getInfoDto",
                                         classDef = SpuBizSummaryDto.class) ParamUpdater updater) throws IOException {
        return m_summaryService.batchSetSpuBizSummary(session, flow, aid, xid, unionPriIds, pdIds, updater);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.BATCH_SET_ROLLBACK)
    private int batchSetSpuBizSummaryRollback(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                         @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_summaryService.batchSetSpuBizSummaryRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.BATCH_ADD)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductStoreCmd.SpuBizSummaryCmd.BATCH_ADD_ROLLBACK)
    private int batchAddSpuBizSummary(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyXid(value = SpuBizSummaryDto.Key.XID, useDefault = true) String xid,
                                 @ArgList(keyMatch = SpuBizSummaryDto.Key.INFO_LIST, methodDef = "getInfoDto",
                                         classDef = SpuBizSummaryDto.class) FaiList<Param> list) throws IOException {
        return m_summaryService.batchAddSpuBizSummary(session, flow, aid, xid, list);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.BATCH_ADD_ROLLBACK)
    private int batchAddSpuBizSummaryRollback(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                         @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_summaryService.batchAddSpuBizSummaryRollback(session, flow, aid, xid, branchId);
    }

    @Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
    @WrittenCmd
    private int clearCache(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid) throws IOException {
        return m_storeService.clearAllCache(session, flow, aid);
    }

    @Cmd(MgProductStoreCmd.Cmd.MIGRATE)
    @WrittenCmd
    private int migrate(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgList(keyMatch = SpuBizSummaryDto.Key.INFO_LIST, methodDef = "getInfoDto",
                           classDef = SpuBizSummaryDto.class) FaiList<Param> spuList) throws IOException {
        return migrateService.migrate(session, flow, aid, spuList);
    }

    private static final String CLI_NAME = "MgProductStoreCli";
    private StoreService m_storeService = ServiceProxy.create(new StoreService());
    private StoreSalesSkuService m_storeSalesSkuService = ServiceProxy.create(new StoreSalesSkuService());
    private RecordService m_recordService = new RecordService();
    private SummaryService m_summaryService = new SummaryService();

    private MigrateService migrateService = new MigrateService();
}
