package fai.MgProductStoreSvr.application;

import fai.MgProductStoreSvr.application.mq.BizSalesReportConsumeListener;
import fai.MgProductStoreSvr.application.mq.StoreSkuReportConsumeListener;
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
        Producer producer = MqFactory.createProducer(MqConfig.BizSalesReport.PRODUCER);
        int rt = producer.start();
        if (rt != Errno.OK) {
            Log.logErr(rt, "producer start err;");
            throw new RuntimeException("producer start err");
        }

        Consumer consumer = MqFactory.createConsumer(MqConfig.BizSalesReport.CONSUMER);
        consumer.subscribe(MqConfig.BizSalesReport.TOPIC, MqConfig.BizSalesReport.TAG, new BizSalesReportConsumeListener(m_storeService));
        consumer.subscribe(MqConfig.StoreSkuReport.TOPIC, MqConfig.StoreSkuReport.TAG, new StoreSkuReportConsumeListener(m_storeService));
        rt = consumer.start();
        if (rt != Errno.OK) {
            Log.logErr(rt, "consumer start err;");
            throw new RuntimeException("consumer start err");
        }

    }
    private void shutDownMq(){
        MqFactory.getProducer(MqConfig.BizSalesReport.PRODUCER).shutdown();
        MqFactory.getConsumer(MqConfig.BizSalesReport.CONSUMER).shutdown();
    }


    @Override
    public void destroy() {
        super.destroy();
        shutDownMq();
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REFRESH)
    private int refreshPdScSkuSalesStore(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                   @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                   @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                   @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                   @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.INFO_LIST)
                                           FaiList<Param> pdScSkuInfoList) throws IOException {
        return  m_storeService.refreshPdScSkuSalesStore(session, flow, aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
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
    private int setPdScSkuSalesStore(final FaiSession session,
                                         @ArgFlow final int flow,
                                         @ArgAid final int aid,
                                         @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                         @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                         @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                         @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId,
                                         @ArgList(classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto", keyMatch = StoreSalesSkuDto.Key.UPDATER_LIST)
                                                 FaiList<ParamUpdater> updaterList) throws IOException {
        return  m_storeService.setPdScSkuSalesStore(session, flow, aid, tid, unionPriId, pdId, rlPdId, updaterList);
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
    private int getPdScSkuSalesStore(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.PD_ID) final int pdId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.RL_PD_ID) final int rlPdId) throws IOException {
        return  m_storeService.getPdScSkuSalesStore(session, flow, aid, tid, unionPriId, pdId, rlPdId);
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


    @Cmd(MgProductStoreCmd.BizSalesSummaryCmd.GET_LIST_BY_PD_ID)
    private int getBizSalesSummaryInfoListByPdId(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.TID) final int tid,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_storeService.getBizSalesSummaryInfoListByPdId(session, flow, aid, tid, pdId);
    }

    @Cmd(MgProductStoreCmd.BizSalesSummaryCmd.GET_LIST)
    private int getBizSalesSummaryInfoList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(BizSalesSummaryDto.Key.TID) final int tid,
                                           @ArgBodyInteger(BizSalesSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                           @ArgList(keyMatch = SalesSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList) throws IOException {
        return  m_storeService.getBizSalesSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList);
    }

    @Cmd(MgProductStoreCmd.SalesSummaryCmd.GET_LIST)
    private int getSalesSummaryInfoList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(SalesSummaryDto.Key.TID) final int tid,
                                           @ArgBodyInteger(SalesSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                           @ArgList(keyMatch = SalesSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList) throws IOException {
        return  m_storeService.getSalesSummaryInfoList(session, flow, aid, tid, unionPriId, pdIdList);
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

    @Cmd(MgProductStoreCmd.StoreSkuSummaryCmd.BIZ_GET_LIST)
    private int getBizStoreSkuSummaryInfoList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(StoreSkuSummaryDto.Key.TID) final int tid,
                                           @ArgBodyInteger(StoreSkuSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                           @ArgSearchArg(StoreSkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return  m_storeService.getBizStoreSkuSummaryInfoList(session, flow, aid, tid, unionPriId, searchArg);
    }

    @Cmd(MgProductStoreCmd.StoreSkuSummaryCmd.GET_LIST)
    private int getStoreSkuSummaryInfoList(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(StoreSkuSummaryDto.Key.TID) final int tid,
                                        @ArgBodyInteger(StoreSkuSummaryDto.Key.UNION_PRI_ID) final int sourceUnionPriId,
                                        @ArgSearchArg(StoreSkuSummaryDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return  m_storeService.getStoreSkuSummaryInfoList(session, flow, aid, tid, sourceUnionPriId, searchArg);
    }

    private StoreService m_storeService = new StoreService();
}
