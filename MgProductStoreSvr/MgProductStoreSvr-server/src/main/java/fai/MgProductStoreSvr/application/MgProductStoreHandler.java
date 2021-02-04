package fai.MgProductStoreSvr.application;

import fai.MgProductStoreSvr.application.mq.SpuBizReportConsumeListener;
import fai.MgProductStoreSvr.application.mq.SkuSummaryReportConsumeListener;
import fai.MgProductStoreSvr.application.service.StoreService;
import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.*;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.mq.api.Consumer;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.util.*;
import fai.middleground.svrutil.service.MiddleGroundHandler;

import java.io.IOException;

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
        consumer.subscribe(MqConfig.SpuBizReport.TOPIC, MqConfig.SpuBizReport.TAG, new SpuBizReportConsumeListener(m_storeService));
        consumer.subscribe(MqConfig.SkuReport.TOPIC, MqConfig.SkuReport.TAG, new SkuSummaryReportConsumeListener(m_storeService));
        rt = consumer.start();
        if (rt != Errno.OK) {
            Log.logErr(rt, "consumer start err;");
            throw new RuntimeException("consumer start err");
        }

    }
    private void shutDownMq(){
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
    private int refreshSkuStoreSales(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                     @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.INFO_LIST)
                                           FaiList<Param> pdScSkuInfoList) throws IOException {
        return  m_storeService.refreshSkuStoreSales(session, flow, aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
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
        return  m_storeService.batchSynchronousStoreSalesSPU2SKU(session, flow, aid, sourceTid, sourceUnionPriId, spuStoreSalesInfoList);
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
        return  m_storeService.batchSynchronousInOutStoreRecord(session, flow, aid, sourceTid, sourceUnionPriId, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.SET_LIST)
    private int setSkuStoreSales(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                 @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.UPDATER_LIST)
                                                 FaiList<ParamUpdater> updaterList) throws IOException {
        return  m_storeService.setSkuStoreSales(session, flow, aid, tid, unionPriId, pdId, rlPdId, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE)
    private int batchReducePdSkuStore(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                          FaiList<Param> skuIdCountList,
                                 @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.EXPIRE_TIME_SECONDS) final int expireTimeSeconds) throws IOException {
        return  m_storeService.batchReducePdSkuStore(session, flow, aid, tid, unionPriId, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE)
    private int batchReducePdSkuHoldingStore(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                        @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                        @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                                 FaiList<Param> skuIdCountList,
                                        @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                                        @ArgParam(classDef = InOutStoreRecordDto.class, methodDef ="getInfoDto", keyMatch=StoreSalesSkuDto.Key.IN_OUT_STORE_RECORD_INFO)
                                                Param outStoreRecordInfo) throws IOException {
        return  m_storeService.batchReducePdSkuHoldingStore(session, flow, aid, tid, unionPriId, skuIdCountList, rlOrderCode, outStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE)
    private int batchMakeUpStore(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                            @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.SKU_ID_COUNT_LIST)
                                     FaiList<Param> skuIdCountList,
                            @ArgBodyString(StoreSalesSkuDto.Key.RL_ORDER_CODE) final String rlOrderCode,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode) throws IOException {
        return  m_storeService.batchMakeUpStore(session, flow, aid, unionPriId, skuIdCountList, rlOrderCode, reduceMode);
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
        return  m_storeService.getSkuStoreSales(session, flow, aid, tid, unionPriId, pdId, rlPdId, useSourceFieldList);
    }
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_AND_UID_LIST)
    private int getStoreSalesBySkuIdAndUIdList(final FaiSession session,
                                               @ArgFlow final int flow,
                                               @ArgAid final int aid,
                                               @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                               @ArgBodyLong(StoreSalesSkuDto.Key.SKU_ID) final long skuId,
                                               @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) FaiList<Integer> unionPriIdList) throws IOException {
        return  m_storeService.getStoreSalesBySkuIdAndUIdList(session, flow, aid, skuId, unionPriIdList);
    }
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.GET_LIST_BY_PD_ID)
    private int getSkuStoreSalesByPdId(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                       @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_storeService.getSkuStoreSalesByPdId(session, flow, aid, pdId);
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
        return  m_storeService.addInOutStoreRecordInfoList(session, flow, aid, tid, unionPriId, infoList);
    }

    @Cmd(MgProductStoreCmd.InOutStoreRecordCmd.GET_LIST)
    private int getInOutStoreRecordInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(InOutStoreRecordDto.Key.TID) final int tid,
                                     @ArgBodyInteger(InOutStoreRecordDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyBoolean(InOutStoreRecordDto.Key.IS_SOURCE) final boolean isSource,
                                     @ArgSearchArg(InOutStoreRecordDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return  m_storeService.getInOutStoreRecordInfoList(session, flow, aid, tid, unionPriId, isSource, searchArg);
    }



    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID)
    private int getSpuBizSummaryInfoListByPdId(final FaiSession session,
                                               @ArgFlow final int flow,
                                               @ArgAid final int aid,
                                               @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                               @ArgBodyInteger(SpuBizSummaryDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_storeService.getSpuBizSummaryInfoListByPdId(session, flow, aid, tid, pdId);
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
        return  m_storeService.getSpuBizSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList, useSourceFieldList);
    }

    @Cmd(MgProductStoreCmd.SpuSummaryCmd.GET_LIST)
    private int getSpuSummaryInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SpuSummaryDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SpuSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgList(keyMatch = SpuSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList) throws IOException {
        return  m_storeService.getSpuSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList);
    }


    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_DEL_PD_ALL_STORE_SALES)
    private int batchDelPdAllStoreSales(final FaiSession session,
                                            @ArgFlow final int flow,
                                            @ArgAid final int aid,
                                            @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                            @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST)
                                                    FaiList<Integer> pdIdList) throws IOException {
        return  m_storeService.batchDelPdAllStoreSales(session, flow, aid, tid, pdIdList);
    }

    @Cmd(MgProductStoreCmd.SkuSummaryCmd.BIZ_GET_LIST)
    private int getSkuBizSummaryInfoList(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyInteger(SkuSummaryDto.Key.TID) final int tid,
                                         @ArgBodyInteger(SkuSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                         @ArgSearchArg(SkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return  m_storeService.getSkuBizSummaryInfoList(session, flow, aid, tid, unionPriId, searchArg);
    }

    @Cmd(MgProductStoreCmd.SkuSummaryCmd.GET_LIST)
    private int getSkuSummaryInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SkuSummaryDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SkuSummaryDto.Key.UNION_PRI_ID) final int sourceUnionPriId,
                                      @ArgSearchArg(SkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return  m_storeService.getSkuSummaryInfoList(session, flow, aid, tid, sourceUnionPriId, searchArg);
    }

    private StoreService m_storeService = new StoreService();
}
