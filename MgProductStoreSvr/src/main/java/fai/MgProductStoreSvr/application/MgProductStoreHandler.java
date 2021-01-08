package fai.MgProductStoreSvr.application;

import fai.MgProductStoreSvr.application.mq.BizSalesReportConsumeListener;
import fai.MgProductStoreSvr.application.service.StoreService;
import fai.MgProductStoreSvr.domain.repository.DaoCtrl;
import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.MgProductStoreSvr.interfaces.dto.BizSalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.InOutStoreRecordDto;
import fai.MgProductStoreSvr.interfaces.dto.SalesSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.ServerHandlerContext;
import fai.comm.jnetkit.server.fai.FaiHandler;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.mq.api.Consumer;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.util.*;

import java.io.IOException;

public class MgProductStoreHandler extends FaiHandler {
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
    public boolean fallback(int flow, final FaiSession session, final Exception cause) {
        int aid = -1;
        int cmd = -1;
        try {
            aid = session.getAid();
            cmd = session.getCmd();
            // 业务方可以覆盖 fallback 方法来定制异常回调，不输出日志
            session.write(Errno.ERROR);
        } catch (IOException e) {
            // 远程客户端发送失败了，直接关闭连接了
            return true;
        } finally {
            Log.logErr(cause, "FaiHandler task error;flow=%d;aid=%d;cmd=%d", flow, aid, cmd);
        }
        return false;
    }
    @Override
    public void channelRead(final ServerHandlerContext context,
                            final Object message) throws Exception {
        try {
            super.channelRead(context, message);
        }finally {
            DaoCtrl.closeDao4End();
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        DaoCtrl.destroy();
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
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REDUCE_STORE)
    private int reducePdSkuStore(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyLong(StoreSalesSkuDto.Key.SKU_ID) final long skuId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.RL_ORDER_ID) final int rlOrderId,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.COUNT) final int count,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode,
                                     @ArgBodyInteger(StoreSalesSkuDto.Key.EXPIRE_TIME_SECONDS) final int expireTimeSeconds) throws IOException {
        return  m_storeService.reducePdSkuStore(session, flow, aid, tid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
    }

    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.REDUCE_HOLDING_STORE)
    private int reducePdSkuHoldingStore(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                 @ArgBodyLong(StoreSalesSkuDto.Key.SKU_ID) final long skuId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.RL_ORDER_ID) final int rlOrderId,
                                 @ArgBodyInteger(StoreSalesSkuDto.Key.COUNT) final int count) throws IOException {
        return  m_storeService.reducePdSkuHoldingStore(session, flow, aid, tid, unionPriId, skuId, rlOrderId, count);
    }
    @WrittenCmd
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.MAKE_UP_STORE)
    private int makeUpStore(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                            @ArgBodyLong(StoreSalesSkuDto.Key.SKU_ID) final long skuId,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.RL_ORDER_ID) final int rlOrderId,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.COUNT) final int count,
                            @ArgBodyInteger(StoreSalesSkuDto.Key.REDUCE_MODE) final int reduceMode) throws IOException {
        return  m_storeService.makeUpStore(session, flow, aid, unionPriId, skuId, rlOrderId, count, reduceMode);
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


    @Cmd(MgProductStoreCmd.BizSalesSummaryCmd.GET_LIST)
    private int getBizSalesSummaryInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.TID) final int tid,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.PD_ID) final int pdId,
                                     @ArgBodyInteger(BizSalesSummaryDto.Key.RL_PD_ID) final int rlPdId) throws IOException {
        return  m_storeService.getBizSalesSummaryInfoList(session, flow, aid, tid, unionPriId, pdId, rlPdId);
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


    private StoreService m_storeService = new StoreService();
}
