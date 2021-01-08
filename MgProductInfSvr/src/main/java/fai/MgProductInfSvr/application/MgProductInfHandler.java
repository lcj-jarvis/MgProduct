package fai.MgProductInfSvr.application;

import fai.MgProductInfSvr.application.service.MgProductInfService;
import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.*;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDetailDto;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDto;
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
        return service.getPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, searchArg);
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
        return service.getPropValList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropIds);
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
        return service.addPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, list);
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
        return service.delPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, idList);
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
        return service.setPropList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, updaterList);
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
        return service.addPropInfoWithVal(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, propInfo, propValList);
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
        return service.setPropAndVal(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropId, propUpdater, addValList, setValList, delValIds);
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
        return service.setPropValList(session, flow, aid, tid, siteId, lgId, keepPriId1, libId, rlPropId, addValList, setValList, delValIds);
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
        return service.getBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, rlLibId);
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
        return service.setProductBindPropInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, addPropList, delPropList);
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
        return service.getRlPdByPropVal(session, flow, aid, tid, siteId, lgId, keepPriId1, proIdsAndValIds);
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
        return service.addTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, list);
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
        return service.delTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScIdList);
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
        return service.setTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, updaterList);
    }

    @Cmd(MgProductInfCmd.SpecTempCmd.GET_LIST)
    public int getTpScInfoList(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductSpecDto.Key.TID) int tid,
                               @ArgBodyInteger(ProductSpecDto.Key.SITE_ID) int siteId,
                               @ArgBodyInteger(ProductSpecDto.Key.LGID) int lgId,
                               @ArgBodyInteger(ProductSpecDto.Key.KEEP_PRIID1) int keepPriId1) throws IOException {
        return service.getTpScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1);
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
        return service.addTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, list);
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
        return service.delTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, tpScDtIdList);
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
        return service.setTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId, updaterList);
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
        return service.getTpScDetailInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlTpScId);
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
        return service.importPdScInfo(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, rlTpScId, tpScDtIdList);
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
        return service.unionSetPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, addList, delList, updaterList);
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
        return service.getPdScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, onlyGetChecked);
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
        return service.setPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updaterList);
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
        return service.getPdSkuScInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
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
        return service.addProductAndRel(session, flow, aid, tid, siteId, lgId, keepPriId1, info);
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
        return service.bindProductRel(session, flow, aid, tid, siteId, lgId, keepPriId1, bindRlPdInfo, info);
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
        return service.batchBindProductRel(session, flow, aid, tid, bindRlPdInfo, infoList);
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
        return service.batchDelPdRelBind(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
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
        return service.batchDelProduct(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIds);
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
                                    @ArgList(classDef = ProductStoreDto.class, methodDef = "getStoreSalesSkuDto",
                                            keyMatch = ProductStoreDto.Key.UPDATER_LIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return service.setPdScSkuSalesStore(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId, updaterList);
    }
    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.REDUCE_STORE)
    public int reducePdSkuStore(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgBodyLong(ProductStoreDto.Key.SKU_ID) long skuId,
                                    @ArgBodyInteger(ProductStoreDto.Key.RL_ORDER_ID) int rlOrderId,
                                @ArgBodyInteger(ProductStoreDto.Key.COUNT) int count,
                                @ArgBodyInteger(ProductStoreDto.Key.REDUCE_MODE) int reduceMode,
                                @ArgBodyInteger(ProductStoreDto.Key.EXPIRE_TIME_SECONDS) int expireTimeSeconds) throws IOException {
        return service.reducePdSkuStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.REDUCE_HOLDING_STORE)
    public int reducePdSkuHoldingStore(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyLong(ProductStoreDto.Key.SKU_ID) long skuId,
                                @ArgBodyInteger(ProductStoreDto.Key.RL_ORDER_ID) int rlOrderId,
                                @ArgBodyInteger(ProductStoreDto.Key.COUNT) int count) throws IOException {
        return service.reducePdSkuHoldingStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuId, rlOrderId, count);
    }

    @WrittenCmd
    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.MAKE_UP_STORE)
    public int makeUpStore(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                @ArgBodyLong(ProductStoreDto.Key.SKU_ID) long skuId,
                                @ArgBodyInteger(ProductStoreDto.Key.RL_ORDER_ID) int rlOrderId,
                                @ArgBodyInteger(ProductStoreDto.Key.COUNT) int count,
                                @ArgBodyInteger(ProductStoreDto.Key.REDUCE_MODE) int reduceMode) throws IOException {
        return service.makeUpStore(session, flow, aid, tid, siteId, lgId, keepPriId1, skuId, rlOrderId, count, reduceMode);
    }

    @Cmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST)
    public int getPdScSkuSalesStore(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid final int aid,
                                  @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                  @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                  @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                  @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                  @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return service.getPdScSkuSalesStore(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
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
                           @ArgList(classDef = ProductStoreDto.class, methodDef = "getInOutStoreRecordDto",
                                   keyMatch = ProductStoreDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return service.addInOutStoreRecordInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, infoList);
    }

    @Cmd(MgProductInfCmd.BizSalesSummaryCmd.GET_LIST)
    public int getBizSalesSummaryInfoList(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductStoreDto.Key.TID) int tid,
                                    @ArgBodyInteger(ProductStoreDto.Key.SITE_ID) int siteId,
                                    @ArgBodyInteger(ProductStoreDto.Key.LGID) int lgId,
                                    @ArgBodyInteger(ProductStoreDto.Key.KEEP_PRIID1) int keepPriId1,
                                    @ArgBodyInteger(ProductStoreDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return service.getBizSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdId);
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
        return service.getSalesSummaryInfoList(session, flow, aid, tid, siteId, lgId, keepPriId1, rlPdIdList);
    }

    MgProductInfService service = new MgProductInfService();
}
