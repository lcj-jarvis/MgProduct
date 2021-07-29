package fai.MgProductSpecSvr.application;

import fai.MgProductSpecSvr.application.service.ProductSpecService;
import fai.MgProductSpecSvr.application.service.SpecStrService;
import fai.MgProductSpecSvr.application.service.SpecTempService;
import fai.MgProductSpecSvr.interfaces.cmd.MgProductSpecCmd;
import fai.MgProductSpecSvr.interfaces.dto.*;
import fai.comm.fseata.client.core.rpc.annotation.SagaTransaction;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.NKDef;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;
import fai.middleground.svrutil.service.MiddleGroundHandler;

import java.io.IOException;

public class MgProductSpecHandler extends MiddleGroundHandler {
    public MgProductSpecHandler(FaiServer server) {
        super(server);
        System.out.println(m_specTempService);
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
        return m_specTempService.addTpScInfoList(session, flow, aid, unionPriId, tid, recvInfoList);
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
        return m_specTempService.setTpScInfoList(session, flow, aid, unionPriId, tid, recvInfoList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.SpecTempCmd.DEL_LIST)
    private int delTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid,
                                @ArgList(keyMatch = SpecTempDto.Key.ID_LIST) FaiList<Integer> tpScIdList) throws IOException {
        return m_specTempService.delTpScInfoList(session, flow, aid, unionPriId, tpScIdList);
    }

    @Cmd(MgProductSpecCmd.SpecTempCmd.GET_LIST)
    private int getTpScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(SpecTempDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(SpecTempDto.Key.TID) final int tid) throws IOException {
        return m_specTempService.getTpScInfoList(session, flow, aid, unionPriId);
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
        return m_specTempService.addTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, recvInfoList);
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
        return m_specTempService.setTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, recvInfoList);
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
        return m_specTempService.delTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId, tpScDtIdList);
    }

    @Cmd(MgProductSpecCmd.SpecTempDetailCmd.GET_LIST)
    private int getTpScDetailInfoList(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.UNION_PRI_ID) final int unionPriId,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.TID) final int tid,
                                      @ArgBodyInteger(SpecTempDetailDto.Key.RL_TP_SC_ID) final int rlTpScId) throws IOException {
        return m_specTempService.getTpScDetailInfoList(session, flow, aid, unionPriId, rlTpScId);
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
        return m_productSpecService.importPdScInfo(session, flow, aid, tid, unionPriId, pdId, tpScInfo);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.BATCH_SYN_SPU_TO_SKU)
    private int batchSynchronousSPU2SKU(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                        @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                        @ArgList(classDef = ProductSpecDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecDto.Key.INFO_LIST)
                                                FaiList<Param> spuInfoList) throws IOException {
        return m_productSpecService.batchSynchronousSPU2SKU(session, flow, aid, tid, unionPriId, spuInfoList);
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

        return m_productSpecService.unionSetPdScInfoList(session, flow, aid, tid, unionPriId, pdId, addPdScInfoList, delPdScIdList, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.BATCH_DEL_PD_ALL_SC)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductSpecCmd.ProductSpecCmd.BATCH_DEL_PD_ALL_SC_ROLLBACK)
    private int batchDelPdAllSc(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                @ArgList(keyMatch = ProductSpecDto.Key.PD_ID_LIST)
                                        FaiList<Integer> pdIdList,
                                @ArgBodyXid(value = CommonDto.Key.XID, useDefault = true) String xid,
                                @ArgBodyBoolean(value = ProductSpecDto.Key.SOFT_DEL, useDefault = true) final boolean softDel) throws IOException {
        return m_productSpecService.batchDelPdAllSc(session, flow, aid, tid, pdIdList, xid, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.BATCH_DEL_PD_ALL_SC_ROLLBACK)
    private int batchDelPdAllScRollback(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                        @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return m_productSpecService.batchDelPdAllScRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.IMPORT_PD_SC_WITH_SKU)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductSpecCmd.ProductSpecCmd.IMPORT_PD_SC_WITH_SKU_ROLLBACK)
    private int importPdScWithSku(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyXid(value = CommonDto.Key.XID, useDefault = true) String xid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                  @ArgList(classDef = ProductSpecDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecDto.Key.INFO_LIST)
                                          FaiList<Param> specList,
                                  @ArgList(classDef = ProductSpecSkuDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecDto.Key.SKU_INFO_LIST/*特意不同*/)
                                          FaiList<Param> specSkuList) throws IOException {
        return m_productSpecService.importPdScWithSku(session, flow, aid, xid, tid, unionPriId, specList, specSkuList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecCmd.IMPORT_PD_SC_WITH_SKU_ROLLBACK)
    private int importPdScWithSkuRollback(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                          @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException{
        return m_productSpecService.importPdScWithSkuRollback(session, flow, aid, xid, branchId);
    }

    @Cmd(MgProductSpecCmd.ProductSpecCmd.GET_LIST)
    private int getPdScInfoList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId) throws IOException {
        return m_productSpecService.getPdScInfoList(session, flow, aid, unionPriId, pdId);
    }

    @Cmd(MgProductSpecCmd.ProductSpecCmd.GET_CHECKED_LIST)
    private int getPdCheckedScInfoList(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                       @ArgBodyInteger(ProductSpecDto.Key.TID) final int tid,
                                       @ArgBodyInteger(ProductSpecDto.Key.PD_ID) final int pdId) throws IOException {
        return m_productSpecService.getPdCheckedScInfoList(session, flow, aid, unionPriId, pdId);
    }

    @Cmd(MgProductSpecCmd.ProductSpecCmd.GET_LIST_4ADM)
    private int getPdScInfoList4Adm(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductSpecDto.Key.UNION_PRI_ID) final int unionPriId,
                                    @ArgList(keyMatch = ProductSpecDto.Key.PD_ID_LIST) FaiList<Integer> pdIds,
                                    @ArgBodyBoolean(value = ProductSpecDto.Key.ONLY_CHECKED,
                                    useDefault = true, defaultValue = false) boolean onlyChecked) throws IOException {
        return m_productSpecService.getPdScInfoList4Adm(session, flow, aid, unionPriId, pdIds, onlyChecked);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.SET_LIST)
    private int setPdSkuScInfoList(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.PD_ID) final int pdId,
                                   @ArgList(classDef = ProductSpecSkuDto.class, methodDef = "getInfoDto", keyMatch = ProductSpecSkuDto.Key.UPDATER_LIST)
                                           FaiList<ParamUpdater> updaterList) throws IOException {
        return m_productSpecService.setPdSkuScInfoList(session, flow, aid, tid, unionPriId, pdId, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.BATCH_GEN_SPU)
    private int batchGenSkuRepresentSpuInfo(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                @ArgList(keyMatch = ProductSpecSkuDto.Key.PD_ID_LIST)
                                        FaiList<Integer> pdIdList) throws IOException {
        return m_productSpecService.batchGenSkuRepresentSpuInfo(session, flow, aid, tid, unionPriId, pdIdList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST)
    private int getPdSkuScInfoList(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPriId,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.PD_ID) final int pdId,
                                   @ArgBodyBoolean(value = ProductSpecSkuDto.Key.WITH_SPU_INFO, useDefault = true) final boolean withSpuInfo) throws IOException {
        return m_productSpecService.getPdSkuScInfoList(session, flow, aid, unionPriId, pdId, withSpuInfo);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST_BY_SKU_ID_LIST)
    private int getPdSkuScInfoListBySkuIdList(final FaiSession session,
                                              @ArgFlow final int flow,
                                              @ArgAid final int aid,
                                              @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                              @ArgList(keyMatch = ProductSpecSkuDto.Key.ID_LIST) final FaiList<Long> skuIdList) throws IOException {
        return m_productSpecService.getPdSkuScInfoListBySkuIdList(session, flow, aid, skuIdList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_SKU_ID_INFO_LIST_BY_PD_ID_LIST)
    private int getPdSkuIdInfoList(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                   @ArgList(keyMatch = ProductSpecSkuDto.Key.PD_ID_LIST) final FaiList<Integer> pdIdList,
                                   @ArgBodyBoolean(value = ProductSpecSkuDto.Key.WITH_SPU_INFO, useDefault = true) final boolean withSpuInfo) throws IOException {
        return m_productSpecService.getPdSkuIdInfoList(session, flow, aid, pdIdList, withSpuInfo);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_LIST_4ADM)
    private int getPdSkuInfoList4Adm(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgList(keyMatch = ProductSpecSkuDto.Key.PD_ID_LIST) final FaiList<Integer> pdIdList,
                                   @ArgBodyBoolean(value = ProductSpecSkuDto.Key.WITH_SPU_INFO, useDefault = true) final boolean withSpuInfo) throws IOException {
        return m_productSpecService.getPdSkuInfoList4Adm(session, flow, aid, pdIdList, withSpuInfo);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_ONLY_SPU_INFO_LIST)
    private int getOnlySpuInfoList(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                   @ArgList(keyMatch = ProductSpecSkuDto.Key.PD_ID_LIST) final FaiList<Integer> pdIdList) throws IOException {
        return m_productSpecService.getOnlySpuInfoList(session, flow, aid, pdIdList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.GET_SKU_CODE_LIST)
    private int getExistsSkuCodeList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                    @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPirId,
                                    @ArgList(keyMatch = ProductSpecSkuDto.Key.SKU_CODE_LIST) final FaiList<String> skuCodeList) throws IOException {
        return m_productSpecService.getExistsSkuCodeList(session, flow, aid, unionPirId, skuCodeList);
    }

    @Cmd(MgProductSpecCmd.ProductSpecSkuCmd.SEARCH_SKU_ID_INFO_LIST_BY_SKU_CODE)
    private int searchPdSkuIdInfoListBySkuCode(final FaiSession session,
                                               @ArgFlow final int flow,
                                               @ArgAid final int aid,
                                               @ArgBodyInteger(ProductSpecSkuDto.Key.TID) final int tid,
                                               @ArgBodyInteger(ProductSpecSkuDto.Key.UNION_PRI_ID) final int unionPirId,
                                               @ArgBodyString(ProductSpecSkuDto.Key.SKU_CODE) final String skuCode,
                                               @ArgParam(keyMatch = ProductSpecSkuDto.Key.CONDITION, classDef = ConditionDto.class, methodDef = "getInfoDto") Param condition) throws IOException {
        return m_productSpecService.searchPdSkuIdInfoListBySkuCode(session, flow, aid, unionPirId, skuCode, condition);
    }

    @Cmd(MgProductSpecCmd.SkuCodeCmd.GET_DATA_STATUS)
    private int getSkuCodeDataStatus(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) final int unionPirId) throws IOException {
        return m_productSpecService.getSkuCodeDataStatus(session, flow, aid, unionPirId);
    }

    @Cmd(MgProductSpecCmd.SkuCodeCmd.GET_ALL_DATA)
    private int getSkuCodeAllData(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) final int unionPirId) throws IOException {
        return m_productSpecService.getSkuCodeAllData(session, flow, aid, unionPirId);
    }

    @Cmd(MgProductSpecCmd.SkuCodeCmd.SEARCH_FROM_DB)
    private int searchSkuCodeFromDb(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) final int unionPirId,
                                   @ArgSearchArg(ProductSpecSkuCodeDao.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return m_productSpecService.searchSkuCodeFromDb(session, flow, aid, unionPirId, searchArg);
    }

    @Cmd(MgProductSpecCmd.SpecStrCmd.GET_LIST)
    private int getScStrInfoList(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(SpecStrDto.Key.TID) final int tid,
                                 @ArgList(keyMatch = SpecStrDto.Key.ID_LIST) final FaiList<Integer> strIdList) throws IOException {
        return m_specStrService.getScStrInfoList(session, flow, aid, tid, strIdList);
    }

    @Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
    @WrittenCmd
    private int clearCache(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid) throws IOException {
        return m_productSpecService.clearAllCache(session, flow, aid);
    }

    @Cmd(MgProductSpecCmd.CommCmd.CLEAR_ACCT)
    @WrittenCmd
    private int clearAcct(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgList(keyMatch = ProductSpecSkuDto.Key.UNION_PRI_IDS) final FaiList<Integer> unionPriIds) throws IOException {
        return m_productSpecService.clearAcct(session, flow, aid, unionPriIds);
    }

    private SpecTempService m_specTempService = new SpecTempService();

    private ProductSpecService m_productSpecService = new ProductSpecService();

    private SpecStrService m_specStrService = new SpecStrService();

    private static final String CLI_NAME = "MgProductSpecCli";
}
