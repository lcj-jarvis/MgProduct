package fai.MgProductInfSvr.application;

import fai.MgProductInfSvr.application.service.*;
import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.*;
import fai.comm.jnetkit.server.fai.FaiHandler;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.netkit.NKDef;
import fai.comm.util.*;
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
                           @ArgBodyString(MgProductSearchDto.Key.SEARCH_PARAM_STRING) String searchParamString) throws IOException {
        return searchService.searchList(session, flow, aid, tid, siteId, lgId, keepPriId1, searchParamString);
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
                                @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                                @ArgList(keyMatch = ProductPropDto.Key.RL_PROP_IDS)FaiList<Integer> idList) throws IOException {
        return propService.delPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, idList);
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
                             @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                             @ArgBodyInteger(ProductPropDto.Key.RL_PROP_ID) int rlPropId,
                             @ArgParamUpdater(classDef = ProductPropDto.class, methodDef = "getPropInfoDto",
                             keyMatch = ProductPropDto.Key.UPDATER)ParamUpdater propUpdater,
                             @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                             keyMatch = ProductPropDto.Key.VAL_LIST) FaiList<Param> addValList,
                             @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                             keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> setValList,
                             @ArgList(keyMatch = ProductPropDto.Key.VAL_IDS) FaiList<Integer> delValIds) throws IOException {
        return propService.setPropAndVal(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropId, propUpdater, addValList, setValList, delValIds);
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
                              @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
                              @ArgBodyInteger(ProductPropDto.Key.RL_PROP_IDS) int rlPropId,
                              @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                                      keyMatch = ProductPropDto.Key.VAL_LIST) FaiList<Param> addValList,
                              @ArgList(classDef = ProductPropDto.class, methodDef = "getPropValInfoDto",
                                      keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> setValList,
                              @ArgList(keyMatch = ProductPropDto.Key.VAL_IDS) FaiList<Integer> delValIds) throws IOException {
        return propService.setPropValList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropId, addValList, setValList, delValIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PROP_LIST)
    public int getBindPropInfo(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                               @ArgBodyInteger(ProductBasicDto.Key.RL_LIB_ID) int rlLibId) throws IOException {
        return basicService.getBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, rlLibId);
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
                               @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                               @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                               keyMatch = ProductBasicDto.Key.PROP_BIND) FaiList<Param> addPropList,
                               @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                               keyMatch = ProductBasicDto.Key.DEL_PROP_BIND) FaiList<Param> delPropList) throws IOException {
        return basicService.setProductBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, addPropList, delPropList);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_RLPDIDS_BY_PROP)
    public int getRlPdByPropVal(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgList(classDef = ProductBasicDto.class, methodDef = "getBindPropValDto",
                                keyMatch = ProductBasicDto.Key.BIND_PROP_INFO) FaiList<Param> proIdsAndValIds) throws IOException {
        return basicService.getRlPdByPropVal(session, flow, aid, tid, siteId, lgId, keepPriId1, proIdsAndValIds);
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
                              @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                              @ArgBodyInteger(ProductSpecDto.Key.RL_TP_SC_ID) int rlTpScId,
                              @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true) FaiList<Integer> tpScDtIdList) throws IOException {
        return specService.importPdScInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, rlTpScId, tpScDtIdList);
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
                                    @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                    @ArgList(classDef = ProductSpecDto.Spec.class, methodDef = "getInfoDto",
                                            keyMatch = ProductSpecDto.Key.INFO_LIST, useDefault = true) FaiList<Param> addList,
                                    @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST, useDefault = true) FaiList<Integer> delList,
                                    @ArgList(classDef = ProductSpecDto.Spec.class, methodDef = "getInfoDto",
                                            keyMatch = ProductSpecDto.Key.UPDATER_LIST, useDefault = true) FaiList<ParamUpdater> updaterList) throws IOException {
        return specService.unionSetPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, addList, delList, updaterList);
    }

    @Cmd(MgProductInfCmd.ProductSpecCmd.GET_LIST)
    public int getPdScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                               @ArgBodyBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED) boolean onlyGetChecked
    ) throws IOException {
        return specService.getPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, onlyGetChecked);
    }

    @Cmd(MgProductInfCmd.ProductSpecCmd.GET_LIST_4ADM)
    public int getPdScInfoList4Adm(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(keyMatch = ProductSpecDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                               @ArgBodyBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED) boolean onlyGetChecked) throws IOException {
        return specService.getPdScInfoList4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, onlyGetChecked);
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
                                  @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                  @ArgList(classDef = ProductSpecDto.SpecSku.class, methodDef = "getInfoDto",
                                          keyMatch = ProductSpecDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return specService.setPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updaterList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_LIST)
    public int getPdSkuScInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId,
                                  @ArgBodyBoolean(value = ProductSpecDto.Key.WITH_SPU_INFO, useDefault = true) boolean withSpuInfo) throws IOException {
        return specService.getPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, withSpuInfo);
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
                                             @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return specService.getOnlySpuPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_ID_LIST)
    public int getPdSkuIdInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlPdIdList,
                                  @ArgBodyBoolean(value = ProductSpecDto.Key.WITH_SPU_INFO, useDefault = true) boolean withSpuInfo) throws IOException {
        return specService.getPdSkuIdInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList, withSpuInfo);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_LIST)
    public int getProductList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                              @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                              @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                              @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getProductList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
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
                                   @ArgParam(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                           keyMatch = ProductBasicDto.Key.UNION_INFO) Param addInfo,
                                   @ArgParam(keyMatch = MgProductDto.Key.IN_OUT_STORE_RECORD_INFO,
                                           classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto") Param inStoreRecordInfo) throws IOException {
        return basicService.addProductInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, addInfo, inStoreRecordInfo);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.ADD_PD_BIND)
    public int bindProductRel(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                        keyMatch = ProductBasicDto.Key.PD_BIND_INFO) Param bindRlPdInfo,
                                @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                        keyMatch = ProductBasicDto.Key.PD_REL_INFO) Param info) throws IOException {
        return basicService.bindProductRel(session, flow, aid, tid, siteId, lgId, keepPriId1, bindRlPdInfo, info);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.BasicCmd.BATCH_ADD_PD_BIND)
    public int batchBindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                              @ArgParam(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                      keyMatch = ProductBasicDto.Key.PD_BIND_INFO) Param bindRlPdInfo,
                              @ArgList(classDef = ProductBasicDto.class, methodDef = "getProductRelDto",
                                      keyMatch = ProductBasicDto.Key.PD_REL_INFO_LIST) FaiList<Param> infoList) throws IOException {
        return basicService.batchBindProductRel(session, flow, aid, tid, bindRlPdInfo, infoList);
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
                           @ArgBodyInteger( ProductBasicDto.Key.RL_PD_ID) Integer rlPdId,
                           @ArgParamUpdater(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException {
        return basicService.setSinglePd(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updater);
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
                              @ArgBodyInteger( ProductBasicDto.Key.RL_PD_ID) Integer rlPdId,
                              @ArgParamUpdater(classDef = MgProductDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException {
        return basicService.setProductInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updater);
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
                           @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                           @ArgParamUpdater(classDef = ProductBasicDto.class, methodDef = "getProductDto",
                                   keyMatch = ProductBasicDto.Key.UPDATER) ParamUpdater updater) throws IOException {
        return basicService.setProducts(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, updater);
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
                                 @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                 @ArgBodyBoolean(value = ProductBasicDto.Key.SOFT_DEL,
                                 useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return basicService.batchDelPdRelBind(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, softDel);
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
                               @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                               @ArgBodyBoolean(value = ProductBasicDto.Key.SOFT_DEL,
                               useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return basicService.batchDelProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, softDel);
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
                              @ArgBodyInteger(ProductBasicDto.Key.RL_PD_ID) int rlPdId,
                              @ArgList(keyMatch = ProductBasicDto.Key.BIND_GROUP_IDS) FaiList<Integer> addRlGroupIds,
                              @ArgList(keyMatch = ProductBasicDto.Key.DEL_BIND_GROUP_IDS) FaiList<Integer> delRlGroupIds) throws IOException {
        return basicService.setPdBindGroup(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, addRlGroupIds, delRlGroupIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.GET_PD_BIND_GROUPS)
    public int getPdBindGroups(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.getPdBindGroupList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
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
                                @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                            keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return storeService.setSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updaterList);
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
                                @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                @ArgList(classDef = MgProductDto.class, methodDef = "getPrimaryKeyDto",
                                        keyMatch = ProductStoreDto.Key.PRIMARY_KEYS) FaiList<Param> primaryKeys,
                                @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                        keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return storeService.batchSetSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, primaryKeys, rlPdId, updaterList);
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
                                     @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                             keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> addList) throws IOException {
        return storeService.batchAddSkuStoreSales(session, flow, aid, tid, siteId, lgId, keepPriId1, addList);
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
                                    @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                    @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getSkuStoreSalesList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, useOwnerFieldList);
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
                           @ArgList(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                   keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return storeService.addInOutStoreRecordInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, infoList);
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
                                   @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                   @ArgBodyCalendar(ProductStoreDto.Key.OPT_TIME) Calendar optTime,
                                   @ArgList(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                           keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return storeService.batchResetCostPrice(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, optTime, infoList);
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
                                           @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return storeService.getAllBizSpuSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
    }

    @Cmd(MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID_LIST)
    public int getAllBizSpuSummaryInfoListByPdIdList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                           @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return storeService.getAllBizSpuSummaryInfoListByPdIdList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
    }

    @Cmd(MgProductInfCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID_LIST)
    public int getSpuBizStoreSalesSummaryInfoList(final FaiSession session,
                                        @ArgFlow final int flow,
                                        @ArgAid final int aid,
                                        @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                        @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                        @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                        @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                        @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList,
                                        @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getSpuBizStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList, useOwnerFieldList);
    }

    @Cmd(MgProductInfCmd.SpuSummaryCmd.GET_LIST)
    public int getSpuStoreSalesSummaryInfoList(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                          @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                          @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                          @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                          @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return storeService.getSpuStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
    }
    @Cmd(MgProductInfCmd.SkuSummaryCmd.GET_LIST)
    public int searchSkuStoreSalesSummaryInfoList(final FaiSession session,
                                                  @ArgFlow final int flow,
                                                  @ArgAid final int aid,
                                                  @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                                  @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                                  @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                                  @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                                  @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg,
                                                  @ArgBodyBoolean(value = ProductStoreDto.Key.IS_BIZ, useDefault = true) boolean isBiz) throws IOException {
        return storeService.searchSkuStoreSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, searchArg, isBiz);
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
                                          @ArgList(keyMatch = ProductTempDto.Key.INFO_LIST,
                                                  classDef = ProductTempDto.Info.class, methodDef = "getInfoDto") FaiList<Param> spuInfoList) throws IOException {
        int rt = specService.batchSynchronousSPU2SKU(session, flow, aid, tid, siteId, lgId, keepPriId1, spuInfoList);
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
                          @ArgList(keyMatch = ProductTempDto.Key.INFO_LIST,
                                  classDef = ProductTempDto.StoreRecord.class, methodDef = "getInfoDto") FaiList<Param> recordInfoList) throws IOException {
        return storeService.batchSynchronousInOutStoreRecord(session, flow, aid, tid, siteId, lgId, keepPriId1, recordInfoList);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_FULL_INFO)
    public int getProductFullInfo(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(MgProductDto.Key.ID) int rlPdId) throws IOException {
        return mgProductInfService.getProductFullInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_FULL_LIST_4ADM)
    public int getProductList4Adm(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgList(keyMatch = MgProductDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                  @ArgParam(keyMatch = MgProductDto.Key.COMBINED,
                                  classDef = MgProductDto.class, methodDef = "getCombinedInfoDto") Param combined) throws IOException {
        return mgProductInfService.getProductList4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, combined);
    }

    @Cmd(MgProductInfCmd.Cmd.GET_SUM_LIST_4ADM)
    public int getProductSummary4Adm(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(MgProductDto.Key.TID) int tid,
                                  @ArgBodyInteger(MgProductDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(MgProductDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(MgProductDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgList(keyMatch = MgProductDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                  @ArgParam(keyMatch = MgProductDto.Key.COMBINED,
                                          classDef = MgProductDto.class, methodDef = "getCombinedInfoDto") Param combined) throws IOException {
        return mgProductInfService.getProductSummary4Adm(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds, combined);
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
                             @ArgList(keyMatch = MgProductDto.Key.INFO_LIST,
                                     classDef = MgProductDto.class, methodDef = "getInfoDto") FaiList<Param> productList,
                             @ArgParam(keyMatch = MgProductDto.Key.IN_OUT_STORE_RECORD_INFO,
                                     classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto") Param inStoreRecordInfo,
                             @ArgBodyBoolean(value = MgProductDto.Key.USE_BASIC, useDefault = true) boolean useMgProductBasicInfo) throws IOException {
        return mgProductInfService.importProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, productList, inStoreRecordInfo, useMgProductBasicInfo);
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
    @Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
    public int clearCache(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid final int aid) throws IOException {
        return mgProductInfService.clearCache(session, flow, aid);
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
                               @ArgParam(keyMatch = ProductGroupDto.Key.INFO, methodDef = "getPdGroupDto",
                               classDef = ProductGroupDto.class) Param addInfo) throws IOException {
        return groupService.addProductGroup(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfo);
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
                              @ArgList(keyMatch = ProductGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> rlGroupIds) throws IOException {
        return groupService.delPdGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, rlGroupIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.GroupCmd.UNION_SET_GROUP_LIST)
    public int unionSetGroupList(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductGroupDto.Key.TID) int tid,
                                 @ArgBodyInteger(ProductGroupDto.Key.SITE_ID) int siteId,
                                 @ArgBodyInteger(ProductGroupDto.Key.LGID) int lgid,
                                 @ArgBodyInteger(ProductGroupDto.Key.KEEP_PRIID1) int keepPriId1,
                                 @ArgParam(keyMatch = ProductGroupDto.Key.INFO, methodDef = "getPdGroupDto",
                                         classDef = ProductGroupDto.class) Param addInfo,
                                 @ArgList(keyMatch = ProductGroupDto.Key.UPDATERLIST, methodDef = "getPdGroupDto",
                                         classDef = ProductGroupDto.class) FaiList<ParamUpdater> updaterList,
                                 @ArgList(keyMatch = ProductGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> delList) throws IOException{
        return groupService.unionSetGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, addInfo, updaterList, delList);
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
                              @ArgList(keyMatch = ProductGroupDto.Key.UPDATERLIST, methodDef = "getPdGroupDto",
                              classDef = ProductGroupDto.class) FaiList<ParamUpdater> updaterList) throws IOException {
        return groupService.setPdGroupList(session, flow, aid, tid, siteId, lgid, keepPriId1, updaterList);
    }

    /*** 商品分类 end ***/

    MgProductInfService mgProductInfService = new MgProductInfService();

    ProductSearchService searchService = new ProductSearchService();
    ProductBasicService basicService = new ProductBasicService();
    ProductPropService propService = new ProductPropService();
    ProductSpecService specService = new ProductSpecService();
    ProductStoreService storeService = new ProductStoreService();
    ProductGroupService groupService = ServiceProxy.create(new ProductGroupService());
}
