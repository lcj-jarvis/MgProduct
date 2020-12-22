package fai.MgProductSpecSvr.application;

import fai.MgProductSpecSvr.application.service.ProductSpecService;
import fai.MgProductSpecSvr.application.service.SpecTempService;
import fai.MgProductSpecSvr.domain.repository.DaoCtrl;
import fai.MgProductSpecSvr.interfaces.cmd.MgProductSpecCmd;
import fai.MgProductSpecSvr.interfaces.dto.*;
import fai.comm.jnetkit.server.ServerHandlerContext;
import fai.comm.jnetkit.server.fai.FaiHandler;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.*;

import java.io.IOException;

public class MgProductSpecHandler extends FaiHandler {
    public MgProductSpecHandler(FaiServer server) {
        super(server);
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

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempCmd.ADD_LIST)
    private int addTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid,
                                @ArgList(classDef = SpecTempDto.class, methodDef = "getInfoDto",
                            keyMatch = SpecTempDto.Key.INFO_LIST) FaiList<Param> recvInfoList) throws IOException {
        return  m_specTempService.addTpScInfoList(session, flow, aid, unionPriId, tid, recvInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempCmd.SET_LIST)
    private int setTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid,
                                @ArgList(classDef = SpecTempDto.class, methodDef = "getInfoDto",
                                        keyMatch = SpecTempDto.Key.UPDATER_LIST) FaiList<ParamUpdater> recvInfoList) throws IOException {
        return  m_specTempService.setTpScInfoList(session, flow, aid, unionPriId, tid, recvInfoList);
    }
    @Cmd(MgProductSpecCmd.SpecTempCmd.DEL_LIST)
    private int delTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid,
                                @ArgList(keyMatch = SpecTempDto.Key.ID_LIST) FaiList<Integer> tpScIdList) throws IOException {
        return  m_specTempService.delTpScInfoList(session, flow, aid, unionPriId, tpScIdList);
    }

    @Cmd(MgProductSpecCmd.SpecTempCmd.GET_LIST)
    private int getTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid) throws IOException {
        return  m_specTempService.getTpScInfoList(session, flow, aid, unionPriId);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempDetailCmd.ADD_LIST)
    private int addTpScDetailInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDetailDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDetailDto.Key.TID) final int tid,
                                @ArgBodyInteger(SpecTempDetailDto.Key.RL_TP_SC_ID) final int rlTpScId,
                                @ArgList(classDef = SpecTempDetailDto.class, methodDef = "getInfoDto",
                                        keyMatch = SpecTempDetailDto.Key.INFO_LIST) FaiList<Param> recvInfoList) throws IOException {
        return  m_specTempService.addTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, recvInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempDetailCmd.SET_LIST)
    private int setTpScDetailInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid,
                                @ArgBodyInteger(SpecTempDetailDto.Key.RL_TP_SC_ID) final int rlTpScId,
                                @ArgList(classDef = SpecTempDetailDto.class, methodDef = "getInfoDto",
                                        keyMatch = SpecTempDetailDto.Key.UPDATER_LIST) FaiList<ParamUpdater> recvInfoList) throws IOException {
        return  m_specTempService.setTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, recvInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempDetailCmd.DEL_LIST)
    private int delTpScDetailInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.RL_TP_SC_ID) final int rlTpScId,
                                      @ArgList(classDef = SpecTempDetailDto.class, methodDef = "getInfoDto",
                                              keyMatch = SpecTempDetailDto.Key.ID_LIST) FaiList<Integer> tpScDtIdList) throws IOException {
        return  m_specTempService.delTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, tpScDtIdList);
    }

    @Cmd(MgProductSpecCmd.SpecTempDetailCmd.GET_LIST)
    private int getTpScDetailInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDetailDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDetailDto.Key.TID) final int tid,
                                @ArgBodyInteger(SpecTempDetailDto.Key.RL_TP_SC_ID) final int rlTpScId) throws IOException {
        return  m_specTempService.getTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId);
    }
    ///////////////////////////////////////////////////////////////////////////////
    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.IMPORT)
    private int importPdScInfo(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                      @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId,
                                      @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) final int rlTpScId,
                                      @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true) FaiList<Integer> tpScDtIdList) throws IOException {
        Param tpScInfo = m_specTempService.getTpScWithDetail(flow, aid, unionPriId, rlTpScId, tpScDtIdList);
        return m_productSpecService.importPdScInfo(session, flow, aid, pdId, tpScInfo);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.UNION_SET)
    private int unionSetPdScInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                     @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                     @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId,
                                     @ArgList(classDef = ProductSpecDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecDto.Key.INFO_LIST, useDefault = true)
                                             FaiList<Param> addPdScInfoList,
                                     @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true)
                                             FaiList<Integer> delPdScIdList,
                                     @ArgList(classDef = ProductSpecDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecDto.Key.UPDATER_LIST, useDefault = true)
                                             FaiList<ParamUpdater> updaterList) throws IOException {

        return  m_productSpecService.unionSetPdScInfoList(session, flow, aid, unionPriId, pdId, addPdScInfoList, delPdScIdList, updaterList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecCmd.GET_LIST)
    private int getPdScInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                      @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_productSpecService.getPdScInfoList(session, flow, aid, unionPriId, pdId);
    }
    @Cmd(MgProductSpecCmd.ProductSpecCmd.GET_CHECKED_LIST)
    private int getPdCheckedScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_productSpecService.getPdCheckedScInfoList(session, flow, aid, unionPriId, pdId);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.SET_LIST)
    private int setPdSkuScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                @ArgBodyInteger(ProductSpecSkuDto.Key.PD_ID) final int pdId,
                                @ArgList(classDef = ProductSpecSkuDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecSkuDto.Key.UPDATER_LIST)
                                               FaiList<ParamUpdater> updaterList) throws IOException {
        return  m_productSpecService.setPdSkuScInfoList(session, flow, aid, unionPriId, pdId, updaterList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST)
    private int getPdSkuScInfoList(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.PD_ID) final int pdId) throws IOException {
        return  m_productSpecService.getPdSkuScInfoList(session, flow, aid, unionPriId, pdId);
    }

    private SpecTempService m_specTempService = new SpecTempService();

    private ProductSpecService m_productSpecService = new ProductSpecService();
}
