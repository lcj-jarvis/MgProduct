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
import fai.comm.util.*;

import java.io.IOException;

public class MgProductInfHandler extends FaiHandler {
    public MgProductInfHandler(FaiServer server) {
        super(server);
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
                                  @ArgBodyInteger(ProductSpecDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return specService.getPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
    }

    @Cmd(MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_ID_LIST)
    public int getPdSkuIdInfoList(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgList(keyMatch = ProductSpecDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return specService.getPdSkuIdInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
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

    @Cmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PD_BIND)
    public int batchDelPdRelBind(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                                 @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                                 @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                                 @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                                 @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.batchDelPdRelBind(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
    }

    @Cmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PDS)
    public int batchDelProduct(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBasicDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductBasicDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductBasicDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductBasicDto.Key.KEEP_PRIID1) int keepPriId1,
                               @ArgList(keyMatch = ProductBasicDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return basicService.batchDelProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.SET_LIST)
    public int setPdScSkuSalesStore(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                    @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                            keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return storeService.setPdScSkuSalesStore(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updaterList);
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
                                @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
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
                                       @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                                    keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                                       @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                                       @ArgParam(classDef = ProductStoreDto.InOutStoreRecord.class, methodDef = "getInfoDto",
                                               keyMatch = ProductStoreDto.Key.IN_OUT_STORE_RECODR) Param outStoreRecordInfo) throws IOException {
        return storeService.batchReducePdSkuHoldingStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, outStoreRecordInfo);
    }


    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE)
    public int batchMakeUpPdSkuStore(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                           @ArgList(classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto",
                                             keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> skuIdCountList,
                           @ArgBodyString(ProductStoreDto.Key.RL_ORDER_CODE) String rlOrderCode,
                           @ArgBodyInteger(ProductStoreDto.Key.REDUCE_MODE) int reduceMode) throws IOException {
        return storeService.batchMakeUpStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, reduceMode);
    }


    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST)
    public int getPdScSkuSalesStore(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId,
                                  @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getPdScSkuSalesStore(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, useOwnerFieldList);
    }

    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID)
    public int getPdScSkuSalesStoreBySkuId(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyLong(ProductStoreDto.Key.SKU_ID) long skuId,
                                    @ArgList(keyMatch = ProductStoreDto.Key.INFO_LIST,
                                            classDef = ProductStoreDto.StoreSalesSku.class, methodDef = "getInfoDto") FaiList<Param> bizInfoList) throws IOException {
        return storeService.getPdScSkuSalesStoreBySkuId(session, flow, aid, tid, skuId, bizInfoList);
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
    @Cmd(MgProductInfCmd.InOutStoreRecordCmd.GET_LIST)
    public int getInOutStoreRecordInfoList(final FaiSession session,
                                           @ArgFlow final int flow,
                                           @ArgAid final int aid,
                                           @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                           @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                           @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                           @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                           @ArgBodyBoolean(value = ProductStoreDto.Key.IS_BIZ, useDefault = true) boolean isBiz,
                                           @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return storeService.getInOutStoreRecordInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, isBiz, searchArg);
    }

    @Cmd(MgProductInfCmd.BizSalesSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID)
    public int getAllBizSalesSummaryInfoList(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                             @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                             @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                             @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                             @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return storeService.getAllBizSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
    }

    @Cmd(MgProductInfCmd.BizSalesSummaryCmd.GET_LIST_BY_PD_ID_LIST)
    public int getPdBizSalesSummaryInfoList(final FaiSession session,
                                             @ArgFlow final int flow,
                                             @ArgAid final int aid,
                                             @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                             @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                             @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                             @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                             @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList,
                                            @ArgList(keyMatch = ProductStoreDto.Key.STR_LIST, useDefault = true) FaiList<String> useOwnerFieldList) throws IOException {
        return storeService.getPdBizSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList, useOwnerFieldList);
    }

    @Cmd(MgProductInfCmd.SalesSummaryCmd.GET_LIST)
    public int getSalesSummaryInfoList(final FaiSession session,
                                          @ArgFlow final int flow,
                                          @ArgAid final int aid,
                                          @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                          @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                          @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                          @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                          @ArgList(keyMatch = ProductStoreDto.Key.ID_LIST) FaiList<Integer> rlPdIdList) throws IOException {
        return storeService.getSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
    }
    @Cmd(MgProductInfCmd.StoreSkuSummaryCmd.GET_LIST)
    public int getStoreSkuSummaryInfoList(final FaiSession session,
                                       @ArgFlow final int flow,
                                       @ArgAid final int aid,
                                       @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                       @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                       @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                       @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                       @ArgSearchArg(ProductStoreDto.Key.SEARCH_ARG) SearchArg searchArg,
                                       @ArgBodyBoolean(value = ProductStoreDto.Key.IS_BIZ, useDefault = true) boolean isBiz) throws IOException {
        return storeService.getStoreSkuSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, searchArg, isBiz);
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

    MgProductInfService mgProductInfService = new MgProductInfService();

    ProductBasicService basicService = new ProductBasicService();
    ProductPropService propService = new ProductPropService();
    ProductSpecService specService = new ProductSpecService();
    ProductStoreService storeService = new ProductStoreService();
}
