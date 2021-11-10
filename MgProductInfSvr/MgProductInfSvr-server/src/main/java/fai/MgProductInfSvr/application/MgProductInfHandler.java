package fai.MgProductInfSvr.application;

import fai.MgProductInfSvr.application.service.*;
import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.*;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.fseata.client.core.exception.TransactionException;
import fai.comm.jnetkit.server.fai.FaiHandler;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.netkit.NKDef;
import fai.comm.util.*;
import fai.comm.middleground.app.CloneDef;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;
import java.util.Calendar;

public class MgProductInfHandler extends FaiHandler {
    public MgProductInfHandler(FaiServer server) {
        super(server);
    }

    @Cmd(MgProductInfCmd.MgProductSearchCmd.SEARCH_LIST)
    public int searchList(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(MgProductSearchDto.Key.TID) int tid,
                           @ArgBodyInteger(MgProductSearchDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(MgProductSearchDto.Key.LGID) int lgId,
                           @ArgBodyInteger(MgProductSearchDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING) String esSearchParamString,
                           @ArgBodyString(MgProductSearchDto.Key.DB_SEARCH_PARAM_STRING) String searchParamString,
                           @ArgBodyString(MgProductSearchDto.Key.PAGE_INFO_STRING) String pageInfoString) throws IOException {
        return searchService.searchList(session, flow, aid, tid, siteId, lgId, keepPriId1, esSearchParamString, searchParamString, pageInfoString);
    }

    @Cmd(MgProductInfCmd.MgProductSearchCmd.SEARCH_PD)
    public int searchProduct(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING) String esSearchParamString,
                                  @ArgBodyString(MgProductSearchDto.Key.DB_SEARCH_PARAM_STRING) String dbSearchParamString,
                                  @ArgBodyString(MgProductSearchDto.Key.PAGE_INFO_STRING) String pageInfoString,
                                  @ArgParam(keyMatch = MgProductDto.Key.COMBINED, classDef = MgProductDto.class, methodDef = "getCombinedInfoDto") Param combined) throws IOException {
        return searchService.searchProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, esSearchParamString, dbSearchParamString, pageInfoString, combined);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_INFO_4ES)
    public int getPdInfo4Es(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(MgProductDto.Key.UNIONPRI_ID) int unionPriId,
                            @ArgBodyInteger(MgProductDto.Key.PD_ID) int pdId) throws IOException {
        return mgProductInfService.getPdInfo4ES(session, flow, aid, unionPriId, pdId);
    }

    @Cmd(MgProductInfCmd.PropCmd.GET_LIST)
    public int getPropList(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                           @ArgSearchArg(ProductPropDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return propService.getPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, searchArg);
    }

    @Cmd(MgProductInfCmd.PropCmd.GET_VAL_LIST)
    public int getPropValList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                              @ArgList(keyMatch = ProductPropDto.Key.RL_PROP_IDS)FaiList<Integer> rlPropIds) throws IOException {
        return propService.getPropValList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.BATCH_ADD)
    public int batchAddPropList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                @ArgList(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                                keyMatch = ProductPropDto.Key.PROP_LIST) FaiList<Param> list) throws IOException {
        return propService.addPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, list);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.BATCH_DEL)
    public int batchDelPropList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(value = ProductPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                @ArgList(keyMatch = ProductPropDto.Key.RL_PROP_IDS)FaiList<Integer> idList) throws IOException {
        return propService.delPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, libId, idList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.BATCH_SET)
    public int batchSetPropList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                @ArgList(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                                keyMatch = ProductPropDto.Key.UPDATERLIST)FaiList<ParamUpdater> updaterList) throws IOException {
        return propService.setPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.UNION_SET_PROP_LIST)
    public int unionSetPropList(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                @ArgList(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                                        keyMatch = ProductPropDto.Key.ADD_LIST) FaiList<Param> addList,
                                @ArgList(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                                        keyMatch = ProductPropDto.Key.UPDATER_LIST)FaiList<ParamUpdater> updaterList,
                                @ArgList(keyMatch = ProductPropDto.Key.DEL_LIST)FaiList<Integer> delList
                                ) throws IOException {
        return propService.unionSetPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, addList, updaterList, delList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.ADD_WITH_VAL)
    public int addPropInfoWithVal(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                  @ArgParam(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                                  keyMatch = ProductPropDto.Key.PROP_INFO)Param propInfo,
                                  @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                                  keyMatch = ProductPropDto.Key.VAL_LIST) FaiList<Param> propValList) throws IOException {
        return propService.addPropInfoWithVal(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, propInfo, propValList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.SET_WITH_VAL)
    public int setPropAndVal(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                             @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                             @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                             @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                             @ArgBodyInteger(value = ProductPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                             @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                             @ArgBodyInteger(ProductPropDto.Key.RL_PROP_ID) int rlPropId,
                             @ArgParamUpdater(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                             keyMatch = ProductPropDto.Key.UPDATER)ParamUpdater propUpdater,
                             @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                             keyMatch = ProductPropDto.Key.VAL_LIST) FaiList<Param> addValList,
                             @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                             keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> setValList,
                             @ArgList(keyMatch = ProductPropDto.Key.VAL_IDS) FaiList<Integer> delValIds) throws IOException {
        return propService.setPropAndVal(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, libId, rlPropId, propUpdater, addValList, setValList, delValIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.PropCmd.BATCH_SET_VAL)
    public int setPropValList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductPropDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductPropDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductPropDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                              @ArgBodyInteger(ProductPropDto.Key.RL_PROP_IDS) int rlPropId,
                              @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                                      keyMatch = ProductPropDto.Key.VAL_LIST) FaiList<Param> addValList,
                              @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                                      keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> setValList,
                              @ArgList(keyMatch = ProductPropDto.Key.VAL_IDS) FaiList<Integer> delValIds) throws IOException {
        return propService.setPropValList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, libId, rlPropId, addValList, setValList, delValIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PROP_LIST)
    public int getBindPropInfo(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                               @ArgBodyInteger(ProductBasicDto.Key.RL_LIB_ID) int rlLibId) throws IOException {
        return basicService.getBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, rlLibId);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PROP_LIST)
    public int setBindPropInfo(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                               @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                               keyMatch = ProductBasicDto.Key.PROP_BIND) FaiList<Param> addPropList,
                               @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                               keyMatch = ProductBasicDto.Key.DEL_PROP_BIND) FaiList<Param> delPropList) throws IOException {
        return basicService.setProductBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, addPropList, delPropList);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_RLPDIDS_BY_PROP)
    public int getRlPdByPropVal(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                                keyMatch = ProductBasicDto.Key.BIND_PROP_INFO) FaiList<Param> proIdsAndValIds) throws IOException {
        return basicService.getRlPdByPropVal(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, proIdsAndValIds);
    }



    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempCmd.ADD_LIST)
    public int addTpScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(classDef = ProductSpecDto.SpecTemp.class, methodDef = "getInfoDto",
                                       keyMatch = ProductSpecDto.Key.INFO_LIST) FaiList<Param> list) throws IOException {
        return specService.addTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, list);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempCmd.DEL_LIST)
    public int delTpScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlTpScIdList) throws IOException {
        return specService.delTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScIdList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempCmd.SET_LIST)
    public int setTpScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(classDef = ProductSpecDto.SpecTemp.class, methodDef = "getInfoDto",
                                       keyMatch = ProductSpecDto.Key.UPDATER_LIST)  FaiList<ParamUpdater> updaterList) throws IOException {
        return specService.setTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, updaterList);
    }

    @Cmd(MgProductInfCmd.SpecTempCmd.GET_LIST)
    public int getTpScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1) throws IOException {
        return specService.getTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempDetailCmd.ADD_LIST)
    public int addTpScDetailInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                     @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                     @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                     @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                     @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId,
                                     @ArgList(classDef = ProductSpecDto.SpecTempDetail.class, methodDef = "getInfoDto",
                                             keyMatch = ProductSpecDto.Key.INFO_LIST) FaiList<Param> list) throws IOException {
        return specService.addTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, list);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempDetailCmd.DEL_LIST)
    public int delTpScDetailInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                     @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                     @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                     @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                     @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId,
                                     @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> tpScDtIdList) throws IOException {
        return specService.delTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, tpScDtIdList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.SpecTempDetailCmd.SET_LIST)
    public int setTpScDetailInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                     @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                     @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                     @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                     @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId,
                                     @ArgList(classDef = ProductSpecDto.SpecTempDetail.class, methodDef = "getInfoDto",
                                             keyMatch = ProductSpecDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return specService.setTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, updaterList);
    }

    @Cmd(MgProductInfCmd.SpecTempDetailCmd.GET_LIST)
    public int getTpScDetailInfoList(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                     @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                     @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                     @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                     @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId) throws IOException {
        return specService.getTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.ProductSpecCmd.IMPORT)
    public int importPdScInfo(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                              @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId,
                              @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true) FaiList<Integer> tpScDtIdList) throws IOException {
        return specService.importPdScInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, rlTpScId, tpScDtIdList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.ProductSpecCmd.UNION_SET)
    public int unionSetPdScInfoList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgBodyString(value = ProductSpecDto.Key.XID, useDefault = true) String xid,
                                    @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                    @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                    @ArgList(classDef = ProductSpecDto.Spec.class, methodDef = "getInfoDto",
                                            keyMatch = ProductSpecDto.Key.INFO_LIST, useDefault = true) FaiList<Param> addList,
                                    @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true) FaiList<Integer> delList,
                                    @ArgList(classDef = ProductSpecDto.Spec.class, methodDef = "getInfoDto",
                                            keyMatch = ProductSpecDto.Key.UPDATER_LIST, useDefault = true) FaiList<ParamUpdater> updaterList) throws IOException, TransactionException {
        if (!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow);
        }
        return specService.unionSetPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, xid, addList, delList, updaterList);
    }

    @Cmd(MgProductInfCmd.ProductSpecCmd.GET_LIST)
    public int getPdScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                               @ArgBodyBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED) boolean onlyGetChecked
    ) throws IOException {
        return specService.getPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, onlyGetChecked);
    }

    @Cmd(MgProductInfCmd.ProductSpecCmd.GET_LIST_4ADM)
    public int getPdScInfoList4Adm(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgList(keyMatch = ProductSpecDto.Key.RL_PD_ID) FaiList<Integer> rlPdIds,
                               @ArgBodyBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED) boolean onlyGetChecked) throws IOException {
        return specService.getPdScInfoList4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds, onlyGetChecked);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.SET_LIST)
    public int setPdSkuScInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                  @ArgList(classDef = ProductSpecDto.SpecSku.class, methodDef = "getInfoDto",
                                          keyMatch = ProductSpecDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return specService.setPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, updaterList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_LIST)
    public int getPdSkuScInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                  @ArgBodyBoolean(value = ProductSpecDto.Key.WITH_SPU_INFO, useDefault = true) boolean withSpuInfo) throws IOException {
        return specService.getPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, withSpuInfo);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_LIST_BY_SKU_ID_LIST)
    public int getPdSkuScInfoListBySkuIdList(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                             @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                             @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                             @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                             @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Long> skuIdList) throws IOException {
        return specService.getPdSkuScInfoListBySkuIdList(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_ONLY_SPU_INFO_LIST)
    public int getOnlySpuPdSkuScInfoList(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                             @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                             @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                             @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                             @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                             @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return specService.getOnlySpuPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIdList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_ID_LIST)
    public int getPdSkuIdInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = ProductSpecDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlPdIdList,
                                  @ArgBodyBoolean(value = ProductSpecDto.Key.WITH_SPU_INFO, useDefault = true) boolean withSpuInfo) throws IOException {
        return specService.getPdSkuIdInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIdList, withSpuInfo);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_LIST)
    public int getProductList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getProductList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_REDUCE_BY_NAME)
    public int getPdReducedList4Adm(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true, defaultValue = -1) int sysType,
                              @ArgList(keyMatch = ProductBasicDto.Key.NAME) FaiList<String> names,
                              @ArgList(keyMatch = ProductBasicDto.Key.STATUS, useDefault = true) FaiList<Integer> status) throws IOException {
        return basicService.getPdReducedList4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, names, status);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_BIND_BIZS)
    public int getPdBindBizs(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getPdBindBiz(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_CODE_LIST)
    public int getExistsSkuCodeList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgList(keyMatch = ProductSpecDto.Key.SKU_CODE_LIST) FaiList<String> skuNumList) throws IOException {
        return specService.getExistsSkuCodeList(session, flow, aid, tid, siteId, lgId, keepPriId1, skuNumList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.SEARCH_SKU_ID_INFO_LIST_BY_SKU_CODE)
    public int searchPdSkuIdInfoListBySkuCode(final FaiSession session,
                                              @ArgFlow final int flow,
                                              @ArgAid final int aid,
                                              @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                              @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                              @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                              @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                              @ArgBodyString(ProductSpecDto.Key.SKU_CODE) String skuNumKeyWord,
                                              @ArgParam(keyMatch = ProductSpecDto.Key.CONDITION,
                                                      classDef = ProductSpecDto.Condition.class, methodDef = "getInfoDto") Param condition) throws IOException {
        return specService.searchPdSkuIdInfoListBySkuCode(session, flow, aid, tid, siteId, lgId, keepPriId1, skuNumKeyWord, condition);
    }

    @Cmd(MgProductInfCmd.BasicCmd.ADD_PD_AND_REL)
    public int addProductAndRel(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                keyMatch = ProductBasicDto.Key.PD_INFO) Param info) throws IOException {
        return basicService.addProductAndRel(session, flow, aid, tid, siteId, lgId, keepPriId1, info);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.ADD_PD_INFO)
    public int addProductInfo(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                   @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                   @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                   @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                   @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                                   @ArgParam(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                           keyMatch = ProductBasicDto.Key.UNION_INFO) Param addInfo,
                                   @ArgParam(keyMatch = MgProductDto.Key.IN_OUT_STORE_RECORD_INFO,
                                           classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto") Param inStoreRecordInfo) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return basicService.addProductInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, xid, addInfo, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.ADD_PD_BIND)
    public int bindProductRel(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyString(value = MgProductDto.Key.XID, useDefault = true) String xid,
                                @ArgParam(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                        keyMatch = MgProductDto.Key.INFO) Param addInfo,
                                @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                        keyMatch = MgProductDto.Key.BIND_PD_INFO) Param bindPdInfo,
                                @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                        keyMatch = ProductBasicDto.Key.IN_OUT_RECOED) Param inStoreRecordInfo) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return basicService.bindProductRel(session, flow, aid, xid, tid, siteId, lgId, keepPriId1, addInfo, bindPdInfo, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_ADD_PD_BIND)
    public int batchBindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                              @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                      keyMatch = ProductBasicDto.Key.PD_BIND_INFO) Param bindRlPdInfo,
                              @ArgList(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductBasicDto.Key.PD_LIST) FaiList<Param> infoList,
                              @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                      keyMatch = ProductBasicDto.Key.IN_OUT_RECOED, useDefault = true) Param inStoreRecordInfo) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return basicService.batchBindProductRel(session, flow, aid, tid, bindRlPdInfo, infoList, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_BIND_PDS_REL)
    public int batchBindProductsRel(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                                   @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                            keyMatch = ProductBasicDto.Key.PRIMARY_KEY) Param primaryKey,
                                   @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                            keyMatch = ProductBasicDto.Key.FROM_PRIMARY_KEY) Param fromPrimaryKey,
                                   @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                   @ArgList(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                           keyMatch = ProductBasicDto.Key.PD_LIST) FaiList<Param> infoList,
                                   @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                           keyMatch = ProductBasicDto.Key.IN_OUT_RECOED, useDefault = true) Param inStoreRecordInfo) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return basicService.batchBindProductsRel(session, flow, aid, primaryKey, fromPrimaryKey, sysType, infoList, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PD_SORT)
    public int setPdSort(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgBodyInteger( ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                           @ArgBodyInteger( ProductBasicDto.Key.PRE_RL_PD_ID) int preRlPdId) throws IOException {
        return basicService.setPdSort(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, preRlPdId);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_SINGLE_PD)
    public int setSinglePd(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgBodyInteger( ProductBasicDto.Key.RL_PD_ID) Integer rlPdId,
                           @ArgParamUpdater(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException {
        return basicService.setSinglePd(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, updater);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PD_INFO)
    public int setProductInfo(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                              @ArgBodyInteger( ProductBasicDto.Key.RL_PD_ID) Integer rlPdId,
                              @ArgParam(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductBasicDto.Key.UPDATER) Param combinedUpdater) throws IOException, TransactionException {
        if (!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow);
        }
        return basicService.setProductInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, xid, rlPdId, combinedUpdater);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PDS)
    public int setProducts(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                           @ArgParamUpdater(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException {
        return basicService.setProducts(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds, updater);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PD_BIND)
    public int batchDelPdRelBind(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                 @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                 @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                 @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                 @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                 @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                 @ArgBodyBoolean(value = ProductBasicDto.Key.SOFT_DEL,
                                 useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return basicService.batchDelPdRelBind(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PDS)
    public int batchDelProduct(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                               @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                               @ArgBodyBoolean(value = ProductBasicDto.Key.SOFT_DEL,
                               useDefault = true, defaultValue = false) boolean softDel) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return basicService.batchDelProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, xid, rlPdIds, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PD_BIND_GROUP)
    public int setPdBindGroup(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                              @ArgList(keyMatch = ProductBasicDto.Key.BIND_GROUP_IDS) FaiList<Integer> addRlGroupIds,
                              @ArgList(keyMatch = ProductBasicDto.Key.DEL_BIND_GROUP_IDS) FaiList<Integer> delRlGroupIds) throws IOException {
        return basicService.setPdBindGroup(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, addRlGroupIds, delRlGroupIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_BIND_GROUPS)
    public int getPdBindGroups(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getPdBindGroupList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.SET_LIST)
    public int setSkuStoreSales(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                            keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return storeService.setSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_SET_LIST)
    public int batchSetSkuStoreSales(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                        keyMatch = ProductStoreDto.Key.PRIMARY_KEYS) FaiList<Param> primaryKeys,
                                @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                        keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return storeService.batchSetSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, primaryKeys, rlPdId, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_ADD_LIST)
    public int batchAddSkuStoreSales(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                     @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                     @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                     @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                     @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                     @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                             keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> addList) throws IOException {
        return storeService.batchAddSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, addList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE)
    public int batchReducePdSkuStore(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgList(classDef = ProductStoreDto.SkuCountChange.class, methodDef = "getInfoDto",
                                        keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                                @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                                @ArgBodyInteger(ProductStoreDto.Key.REDUCE_MODE) int reduceMode,
                                @ArgBodyInteger(ProductStoreDto.Key.EXPIRE_TIME_SECONDS) int expireTimeSeconds) throws IOException {
        return storeService.batchReducePdSkuStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, reduceMode, expireTimeSeconds);
    }


    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE)
    public int batchReducePdSkuHoldingStore(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                       @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                       @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                       @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                       @ArgList(classDef = ProductStoreDto.SkuCountChange.class, methodDef = "getInfoDto",
                                                    keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                                       @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                                       @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                               keyMatch = ProductStoreDto.Key.IN_OUT_STORE_RECORD) Param outStoreRecordInfo) throws IOException {
        return storeService.batchReducePdSkuHoldingStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, outStoreRecordInfo);
    }


    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE)
    public int batchMakeUpStore(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgList(classDef = ProductStoreDto.SkuCountChange.class, methodDef = "getInfoDto",
                                             keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                                @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                                @ArgBodyInteger(ProductStoreDto.Key.REDUCE_MODE) int reduceMode) throws IOException {
        return storeService.batchMakeUpStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, reduceMode);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE)
    public int refreshHoldingRecordOfRlOrderCode(final FaiSession session,
                                                 @ArgFlow final int flow,
                                                 @ArgAid final int aid,
                                                 @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                                 @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                                 @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                                 @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                                 @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                                                 @ArgList(classDef = ProductStoreDto.SkuCountChange.class, methodDef = "getInfoDto",
                                                         keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList) throws IOException {
        return storeService.refreshHoldingRecordOfRlOrderCode(session, flow, aid, tid, siteId, lgId, keepPriId1, rlOrderCode, skuIdCountList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_REFUND_STORE)
    public int batchRefundStore(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgList(classDef = ProductStoreDto.SkuCountChange.class, methodDef = "getInfoDto",
                                        keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                                @ArgBodyString(ProductStoreDto.Key.RL_REFUND_ID) String rlRefundId,
                                @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                        keyMatch = ProductStoreDto.Key.IN_OUT_STORE_RECORD) Param inStoreRecordInfo) throws IOException {
        return storeService.batchRefundStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlRefundId, inStoreRecordInfo);
    }


    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST)
    public int getSkuStoreSalesList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                    @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                    @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getSkuStoreSalesList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, useOwnerFieldList);
    }
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_LIST)
    public int getSkuStoreSalesBySkuIdList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Long> skuIdList,
                                    @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getSkuStoreSalesBySkuIdList(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdList, useOwnerFieldList);
    }

    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID)
    public int getSkuStoreSalesBySkuId(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                       @ArgBodyLong(ProductStoreDto.Key.SKU_ID) long skuId,
                                       @ArgList(keyMatch = ProductStoreDto.Key.INFO_LIST,
                                            classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto") FaiList<Param> bizInfoList) throws IOException {
        return storeService.getSkuStoreSalesBySkuId(session, flow, aid, tid, skuId, bizInfoList);
    }
    @Cmd(MgProductInfCmd.HoldingRecordCmd.GET_LIST)
    public int getHoldingRecordList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Long> skuIdList) throws IOException {
        return storeService.getHoldingRecordList(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdList);
    }


    @WrittenCmd
    @Cmd(MgProductInfCmd.InOutStoreRecordCmd.ADD_LIST)
    public int addInOutStoreRecordInfoList(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgList(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                   keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return storeService.addInOutStoreRecordInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.InOutStoreRecordCmd.BATCH_RESET_PRICE)
    public int batchResetCostPrice(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                   @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                   @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                   @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                   @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                   @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                   @ArgBodyCalendar(ProductStoreDto.Key.OPT_TIME) Calendar optTime,
                                   @ArgList(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                           keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return storeService.batchResetCostPrice(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, optTime, infoList);
    }

    @Cmd(MgProductInfCmd.InOutStoreRecordCmd.GET_LIST)
    public int searchInOutStoreRecordInfoList(final FaiSession session,
                                              @ArgFlow final int flow,
                                              @ArgAid final int aid,
                                              @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                              @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                              @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                              @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                              @ArgBodyBoolean(value = ProductStoreDto.Key.IS_BIZ, useDefault = true) boolean isBiz,
                                              @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg,
                                              @ArgList(classDef = ProductStoreDto.PrimaryKey.class, methodDef = "getInfoDto",
                                                      keyMatch = ProductStoreDto.Key.PRI_IDS, useDefault = true)FaiList<Param> primaryKeys) throws IOException {
        return storeService.searchInOutStoreRecordInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, isBiz, searchArg, primaryKeys);
    }

    @Cmd(MgProductInfCmd.InOutStoreRecordCmd.GET_SUM_LIST)
    public int searchInOutStoreSumList(final FaiSession session,
                                              @ArgFlow final int flow,
                                              @ArgAid final int aid,
                                              @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                              @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                              @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                              @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                              @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg,
                                              @ArgList(classDef = ProductStoreDto.PrimaryKey.class, methodDef = "getInfoDto",
                                                      keyMatch = ProductStoreDto.Key.PRI_IDS, useDefault = true)FaiList<Param> primaryKeys) throws IOException {
        return storeService.searchInOutStoreSumList(session, flow, aid, tid, siteId, lgId, keepPriId1, searchArg, primaryKeys);
    }

    @Cmd(MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID)
    public int getAllBizSpuSummaryInfoList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                           @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                           @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return storeService.getAllBizSpuSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId);
    }

    @Cmd(MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID_LIST)
    public int getAllBizSpuSummaryInfoListByPdIdList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                           @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                           @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return storeService.getAllBizSpuSummaryInfoListByPdIdList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIdList);
    }

    @Cmd(MgProductInfCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID_LIST)
    public int getSpuBizStoreSalesSummaryInfoList(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                        @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                        @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                        @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                        @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                        @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList,
                                        @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getSpuBizStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIdList, useOwnerFieldList);
    }

    @Cmd(MgProductInfCmd.SpuSummaryCmd.GET_LIST)
    public int getSpuStoreSalesSummaryInfoList(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                          @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                          @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                          @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                          @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                          @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return storeService.getSpuStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIdList);
    }
    @Cmd(MgProductInfCmd.SkuSummaryCmd.GET_LIST)
    public int searchSkuStoreSalesSummaryInfoList(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                                  @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                                  @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                                  @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                                  @ArgBodyInteger(value = ProductStoreDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                                  @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg,
                                                  @ArgBodyBoolean(value = ProductStoreDto.Key.IS_BIZ, useDefault = true) boolean isBiz) throws IOException {
        return storeService.searchSkuStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, searchArg, isBiz);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.TempCmd.SYN_SPU_TO_SKU)
    public int synSPU2SKU(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyInteger(ProductTempDto.Key.TID) int tid,
                                          @ArgBodyInteger(ProductTempDto.Key.SITE_ID) int siteId,
                                          @ArgBodyInteger(ProductTempDto.Key.LGID) int lgId,
                                          @ArgBodyInteger(ProductTempDto.Key.KEEP_PRIID1) int keepPriId1,
                                          @ArgBodyInteger(value = ProductTempDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                          @ArgList(keyMatch = ProductTempDto.Key.INFO_LIST,
                                                  classDef = ProductTempDto.Info.class, methodDef = "getInfoDto") FaiList<Param> spuInfoList) throws IOException {
        int rt = specService.batchSynchronousSPU2SKU(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, spuInfoList);
        if(rt != Errno.OK){
            return rt;
        }
        rt = storeService.batchSynchronousSPU2SKU(session, flow, aid, tid, siteId, lgId, keepPriId1, spuInfoList);
        return rt;
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.TempCmd.SYN_IN_OUT_STORE_RECORD)
    public int synInOutStoreRecord(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid final int aid,
                          @ArgBodyInteger(ProductTempDto.Key.TID) int tid,
                          @ArgBodyInteger(ProductTempDto.Key.SITE_ID) int siteId,
                          @ArgBodyInteger(ProductTempDto.Key.LGID) int lgId,
                          @ArgBodyInteger(ProductTempDto.Key.KEEP_PRIID1) int keepPriId1,
                          @ArgBodyInteger(value = ProductTempDto.Key.SYS_TYPE, useDefault = true) int sysType,
                          @ArgList(keyMatch = ProductTempDto.Key.INFO_LIST,
                                  classDef = ProductTempDto.StoreRecord.class, methodDef = "getInfoDto") FaiList<Param> recordInfoList) throws IOException {
        return storeService.batchSynchronousInOutStoreRecord(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, recordInfoList);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_FULL_INFO)
    public int getProductFullInfo(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = MgProductDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgBodyInteger(MgProductDto.Key.ID) int rlPdId) throws IOException {
        return mgProductInfService.getProductFullInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_FULL_LIST_4ADM)
    public int getProductList4Adm(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = MgProductDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgList(keyMatch = MgProductDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                  @ArgParam(keyMatch = MgProductDto.Key.COMBINED,
                                  classDef = MgProductDto.class, methodDef = "getCombinedInfoDto") Param combined) throws IOException {
        return mgProductInfService.getProductList4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds, combined);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_SUM_LIST_4ADM)
    public int getProductSummary4Adm(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(value = MgProductDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgList(keyMatch = MgProductDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                  @ArgParam(keyMatch = MgProductDto.Key.COMBINED,
                                          classDef = MgProductDto.class, methodDef = "getCombinedInfoDto") Param combined) throws IOException {
        return mgProductInfService.getProductSummary4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds, combined);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.IMPORT_PRODUCT)
    public int importProduct(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                             @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                             @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                             @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                             @ArgBodyInteger(value = MgProductDto.Key.SYS_TYPE, useDefault = true) int sysType,
                             @ArgBodyString(value = MgProductDto.Key.XID, useDefault = true) String xid,
                             @ArgList(keyMatch = MgProductDto.Key.INFO_LIST,
                                     classDef = MgProductDto.class, methodDef = "getInfoDto") FaiList<Param> productList,
                             @ArgParam(keyMatch = MgProductDto.Key.IN_OUT_STORE_RECORD_INFO,
                                     classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto") Param inStoreRecordInfo,
                             @ArgBodyBoolean(value = MgProductDto.Key.USE_BASIC, useDefault = true) boolean useMgProductBasicInfo) throws IOException, TransactionException {
        if (!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow);
        }
        return mgProductInfService.importProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, xid, productList, inStoreRecordInfo, useMgProductBasicInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.CLEAR_REL_DATA)
    public int clearRelData(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                            @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                            @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1) throws IOException {
        return mgProductInfService.clearRelData(session, flow, aid, tid, siteId, lgId, keepPriId1);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.CLEAR_ACCT)
    public int clearAcct(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                            @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                            @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                 keyMatch = MgProductDto.Key.PRIMARY_KEYS) FaiList<Param> primaryKeys) throws IOException {
        return mgProductInfService.clearAcct(session, flow, aid, tid, siteId, lgId, keepPriId1, primaryKeys);
    }

    @WrittenCmd
    @Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
    public int clearCache(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid final int aid) throws IOException {
        return mgProductInfService.clearCache(session, flow, aid);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.CLONE_DATA)
    public int cloneData(final FaiSession session,
                         @ArgFlow final int flow,
                         @ArgAid final int aid,
                         @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                 keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                         @ArgBodyInteger(MgProductDto.Key.FROM_AID) int fromAid,
                         @ArgList(classDef = CloneDef.Dto.class, methodDef = "getExternalDto",
                                 keyMatch = MgProductDto.Key.PRIMARY_KEYS) FaiList<Param> clonePrimaryKeys,
                         @ArgParam(classDef = MgProductDto.class, methodDef = "getOptionDto",
                                 keyMatch = MgProductDto.Key.OPTION, useDefault = true) Param cloneOption) throws IOException {
        return mgProductInfService.cloneData(session, flow, aid, primaryKey, fromAid, clonePrimaryKeys, cloneOption);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.INC_CLONE)
    public int incrementalClone(final FaiSession session,
                         @ArgFlow final int flow,
                         @ArgAid final int aid,
                         @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                 keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                         @ArgBodyInteger(MgProductDto.Key.FROM_AID) int fromAid,
                         @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                 keyMatch = MgProductDto.Key.FROM_PRIMARY_KEY) Param fromPrimaryKey,
                         @ArgParam(classDef = MgProductDto.class, methodDef = "getOptionDto",
                                 keyMatch = MgProductDto.Key.OPTION, useDefault = true) Param cloneOption) throws IOException {
        return mgProductInfService.incrementalClone(session, flow, aid, primaryKey, fromAid, fromPrimaryKey, cloneOption);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.CLONE_BIZ_BIND)
    public int cloneBizBind(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                        keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                                @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                        keyMatch = MgProductDto.Key.FROM_PRIMARY_KEY) Param fromPrimaryKey) throws IOException {
        return mgProductInfService.cloneBizBind(session, flow, aid, primaryKey, fromPrimaryKey);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_SET_4YK)
    public int batchSet4YK(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                            @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                    keyMatch = ProductBasicDto.Key.PRIMARY_KEY) Param ownPrimaryKey,
                            @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                            @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                            @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                    keyMatch = ProductBasicDto.Key.PRIMARY_KEYS) FaiList<Param> toPrimaryKeys,
                           @ArgParamUpdater(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return mgProductInfService.batchSet4YK(session, flow, aid, ownPrimaryKey, sysType, rlPdIds, toPrimaryKeys, updater);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_SET_BIZ)
    public int batchSetBizBind(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyString(value = ProductBasicDto.Key.XID, useDefault = true) String xid,
                           @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                   keyMatch = ProductBasicDto.Key.PRIMARY_KEY) Param ownPrimaryKey,
                           @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                           @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                   keyMatch = ProductBasicDto.Key.PRIMARY_KEYS) FaiList<Param> toPrimaryKeys,
                           @ArgParam(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) Param combinedUpdater) throws IOException, TransactionException {
        if(!Str.isEmpty(xid)) {
            RootContext.bind(xid, flow); // 方便后面使用GlobalTransactionContext.getCurrentOrCreate
        }
        return mgProductInfService.batchSetBizBind(session, flow, aid, ownPrimaryKey, sysType, rlPdId, toPrimaryKeys, combinedUpdater);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.BACKUP)
    public int backupData(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                        keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                                @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                  keyMatch = MgProductDto.Key.PRIMARY_KEYS) FaiList<Param> backupPrimaryKeys,
                                @ArgBodyInteger(MgProductDto.Key.RL_BACKUPID) int rlBackupId) throws IOException {
        return mgProductInfService.backupData(session, flow, aid, primaryKey, backupPrimaryKeys, rlBackupId);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.RESTORE)
    public int restoreBackupData(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid final int aid,
                          @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                  keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                          @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                  keyMatch = MgProductDto.Key.PRIMARY_KEYS) FaiList<Param> backupPrimaryKeys,
                          @ArgBodyInteger(MgProductDto.Key.RL_RESTOREID) int restoreId,
                          @ArgBodyInteger(MgProductDto.Key.RL_BACKUPID) int rlBackupId,
                          @ArgParam(classDef = MgProductDto.class, methodDef = "getOptionDto",
                                  keyMatch = MgProductDto.Key.OPTION, useDefault = true) Param restoreOption) throws IOException {
        return mgProductInfService.restoreBackupData(session, flow, aid, primaryKey, backupPrimaryKeys, restoreId, rlBackupId, restoreOption);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.DEL_BACKUP)
    public int delBackupData(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgParam(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                         keyMatch = MgProductDto.Key.PRIMARY_KEY) Param primaryKey,
                                 @ArgBodyInteger(MgProductDto.Key.RL_BACKUPID) int rlBackupId) throws IOException {
        return mgProductInfService.delBackup(session, flow, aid, primaryKey, rlBackupId);
    }

    /*** 商品分类 start ***/
    @Cmd(MgProductInfCmd.GroupCmd.GET_GROUP_LIST)
    public int getPdGroupList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgid,
                              @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgSearchArg(value = ProductGroupDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return groupService.getPdGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, searchArg);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.ADD_GROUP)
    public int addProductGroup(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgid,
                               @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(ProductGroupDto.Key.SYS_TYPE) int sysType,
                               @ArgParam(keyMatch = ProductGroupDto.Key.INFO, methodDef = "getPdGroupDto",
                               classDef = ProductGroupDto.class) Param addInfo) throws IOException {
        return groupService.addProductGroup(session, flow, aid, tid, siteId, lgid, keepPriId1, sysType, addInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.DEL_GROUP_LIST)
    public int delPdGroupList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgid,
                              @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgList(keyMatch = ProductGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> rlGroupIds,
                              @ArgBodyInteger(ProductGroupDto.Key.SYS_TYPE) int sysType,
                              @ArgBodyBoolean(ProductGroupDto.Key.SOFT_DEL) boolean softDel) throws IOException {
        return groupService.delPdGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, rlGroupIds, sysType, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.UNION_SET_GROUP_LIST)
    public int unionSetGroupList(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                                 @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                                 @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgId,
                                 @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                                 @ArgBodyBoolean(ProductGroupDto.Key.SOFT_DEL) boolean softDel,
                                 @ArgList(keyMatch = ProductGroupDto.Key.INFO, methodDef = "getPdGroupDto",
                                         classDef = ProductGroupDto.class) FaiList<Param> addList,
                                 @ArgList(keyMatch = ProductGroupDto.Key.UPDATERLIST, methodDef = "getPdGroupDto",
                                         classDef = ProductGroupDto.class) FaiList<ParamUpdater> updaterList,
                                 @ArgList(keyMatch = ProductGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> delList,
                                 @ArgBodyInteger(ProductGroupDto.Key.SYS_TYPE) int sysType) throws IOException{
        return groupService.unionSetGroupList(session, flow, aid, tid, siteId, lgId, keepPriId1, addList, updaterList, delList, sysType, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.SET_GROUP_LIST)
    public int setPdGroupList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgid,
                              @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(ProductGroupDto.Key.SYS_TYPE) int sysType,
                              @ArgList(keyMatch = ProductGroupDto.Key.UPDATERLIST, methodDef = "getPdGroupDto",
                              classDef = ProductGroupDto.class) FaiList<ParamUpdater> updaterList) throws IOException {
        return groupService.setPdGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, sysType, updaterList);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.SET_ALL_GROUP_LIST)
    public int setAllGroupList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(keyMatch = ProductGroupDto.Key.UPDATERLIST, methodDef = "getPdGroupTreeDto",
                                       classDef = ProductGroupDto.class) FaiList<Param> treeDataList,
                               @ArgBodyInteger(ProductGroupDto.Key.SYS_TYPE) int sysType,
                               @ArgBodyInteger(ProductGroupDto.Key.GROUP_LEVEL) int groupLevel,
                               @ArgBodyBoolean(ProductGroupDto.Key.SOFT_DEL) boolean softDel) throws IOException {
        return groupService.setAllGroupList(session, flow, aid, tid, siteId, lgId, keepPriId1, treeDataList, sysType, groupLevel, softDel);
    }

    /*** 商品分类 end ***/

    /**商品库 start*/
    @WrittenCmd
    @Cmd(MgProductInfCmd.LibCmd.ADD_LIB)
    public int addProductLib(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                             @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                             @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                             @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                             @ArgParam(keyMatch = ProductLibDto.Key.INFO, methodDef = "getPdLibDto",
                                     classDef = ProductLibDto.class) Param addInfo) throws IOException {
        return libService.addProductLib(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.LibCmd.DEL_LIB_LIST)
    public int delPdLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgList(keyMatch = ProductLibDto.Key.RL_LIB_IDS) FaiList<Integer> rlLibIds) throws IOException {
        return libService.delPdLibList(session, flow, aid, tid, siteId, lgid, keepPriId1, rlLibIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.LibCmd.SET_LIB_LIST)
    public int setPdLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgList(keyMatch = ProductLibDto.Key.UPDATERLIST, methodDef = "getPdLibDto",
                                    classDef = ProductLibDto.class) FaiList<ParamUpdater> updaterList) throws IOException {
        return libService.setPdLibList(session, flow, aid, tid, siteId, lgid, keepPriId1, updaterList);
    }

    @Cmd(MgProductInfCmd.LibCmd.GET_LIB_LIST)
    public int getPdLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgSearchArg(value = ProductLibDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return libService.getPdLibList(session, flow, aid, tid, siteId, lgid, keepPriId1, searchArg);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.LibCmd.UNION_SET_LIB_LIST)
    public int unionSetLibList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                               @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(classDef = ProductLibDto.class, methodDef = "getPdLibDto",
                                       keyMatch = ProductLibDto.Key.INFO_LIST) FaiList<Param> addInfoList,
                               @ArgList(classDef = ProductLibDto.class, methodDef = "getPdLibDto",
                                       keyMatch = ProductLibDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
                               @ArgList(keyMatch = ProductLibDto.Key.RL_LIB_IDS) FaiList<Integer> delRlLibIds) throws IOException{
        return libService.unionSetLibList(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfoList, updaterList, delRlLibIds);
    }

    @Cmd(MgProductInfCmd.LibCmd.GET_REL_LIB_LIST)
    public int getPdRelLibList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductLibDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductLibDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductLibDto.Key.LGID) int lgid,
                               @ArgBodyInteger(ProductLibDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgSearchArg(value = ProductLibDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return libService.getPdRelLibList(session, flow, aid, tid, siteId, lgid, keepPriId1, searchArg);
    }
    /**商品库 end*/

    /**商品标签 start*/
    @WrittenCmd
    @Cmd(MgProductInfCmd.TagCmd.ADD_TAG)
    public int addProductTag(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                             @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                             @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                             @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                             @ArgParam(keyMatch = ProductTagDto.Key.INFO, methodDef = "getPdTagDto",
                                     classDef = ProductTagDto.class) Param addInfo) throws IOException {
        return tagService.addProductTag(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.TagCmd.DEL_TAG_LIST)
    public int delPdTagList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgList(keyMatch = ProductTagDto.Key.RL_TAG_IDS) FaiList<Integer> rlTagIds) throws IOException {
        return tagService.delPdTagList(session, flow, aid, tid, siteId, lgid, keepPriId1, rlTagIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.TagCmd.SET_TAG_LIST)
    public int setPdTagList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgList(keyMatch = ProductTagDto.Key.UPDATERLIST, methodDef = "getPdTagDto",
                                    classDef = ProductTagDto.class) FaiList<ParamUpdater> updaterList) throws IOException {
        return tagService.setPdTagList(session, flow, aid, tid, siteId, lgid, keepPriId1, updaterList);
    }

    @Cmd(MgProductInfCmd.TagCmd.GET_TAG_LIST)
    public int getPdTagList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                            @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgSearchArg(value = ProductTagDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return tagService.getPdTagList(session, flow, aid, tid, siteId, lgid, keepPriId1, searchArg);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.TagCmd.UNION_SET_TAG_LIST)
    public int unionSetTagList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                               @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(classDef = ProductTagDto.class, methodDef = "getPdTagDto",
                                       keyMatch = ProductTagDto.Key.INFO_LIST) FaiList<Param> addInfoList,
                               @ArgList(classDef = ProductTagDto.class, methodDef = "getPdTagDto",
                                       keyMatch = ProductTagDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
                               @ArgList(keyMatch = ProductTagDto.Key.RL_TAG_IDS) FaiList<Integer> delRlTagIds) throws IOException{
        return tagService.unionSetTagList(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfoList, updaterList, delRlTagIds);
    }

    @Cmd(MgProductInfCmd.TagCmd.GET_REL_TAG_LIST)
    public int getPdRlTagList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductTagDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductTagDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductTagDto.Key.LGID) int lgid,
                               @ArgBodyInteger(ProductTagDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgSearchArg(value = ProductTagDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return tagService.getPdRlTagList(session, flow, aid, tid, siteId, lgid, keepPriId1, searchArg);
    }
    /**商品标签 end*/

    /**商品和标签的关联 begin*/
    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_BIND_TAGS)
    public int getPdBindTags(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                             @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                             @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                             @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                             @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                             @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getPdBindTagList(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.SET_PD_BIND_TAG)
    public int setPdBindTag(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                              @ArgList(keyMatch = ProductBasicDto.Key.BIND_TAG_IDS) FaiList<Integer> addRlTagIds,
                              @ArgList(keyMatch = ProductBasicDto.Key.DEL_BIND_TAG_IDS) FaiList<Integer> delRlTagIds) throws IOException {
        return basicService.setPdBindTag(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, rlPdId, addRlTagIds, delRlTagIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.DEL_PD_TAG_LIST)
    public int delPdBindTag(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                            @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                            @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                            @ArgBodyInteger(value = ProductBasicDto.Key.SYS_TYPE, useDefault = true) int sysType,
                            @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> delRlPdIds) throws IOException {
        return basicService.delPdBindTag(session, flow, aid, tid, siteId, lgId, keepPriId1, sysType, delRlPdIds);
    }
    /**商品和标签的关联 end*/

    @WrittenCmd
    @Cmd(MgProductInfCmd.Cmd.MIGRATE)
    public int migrateData(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid) throws IOException {
        return dataMigrateService.migrateYK(session, flow, aid);
    }

    //MgProductInfService mgProductInfService = new MgProductInfService();
    //ProductBasicService basicService = new ProductBasicService();
    MgProductInfService mgProductInfService = ServiceProxy.create(new MgProductInfService());
    ProductBasicService basicService = ServiceProxy.create(new ProductBasicService());
    DataMigrateService dataMigrateService = ServiceProxy.create(new DataMigrateService());

    ProductSearchService searchService = ServiceProxy.create(new ProductSearchService());
    ProductPropService propService = new ProductPropService();
    ProductSpecService specService = ServiceProxy.create(new ProductSpecService());
    ProductStoreService storeService = ServiceProxy.create(new ProductStoreService());
    ProductGroupService groupService = ServiceProxy.create(new ProductGroupService());
    ProductLibService libService = ServiceProxy.create(new ProductLibService());
    ProductTagService tagService = ServiceProxy.create(new ProductTagService());

}
