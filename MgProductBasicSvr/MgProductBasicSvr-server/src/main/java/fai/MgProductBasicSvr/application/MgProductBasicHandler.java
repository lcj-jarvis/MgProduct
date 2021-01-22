package fai.MgProductBasicSvr.application;

import fai.MgProductBasicSvr.application.service.ProductBasicService;
import fai.MgProductBasicSvr.application.service.ProductPropBindService;
import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.middleground.svrutil.service.MiddleGroundHandler;

import java.io.IOException;

public class MgProductBasicHandler extends MiddleGroundHandler {
    public MgProductBasicHandler(FaiServer server) {
        super(server);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_LIST)
    public int getPdBindProp(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductBindPropDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return propBindservice.getPdBindProp(session, flow, aid, unionPriId, tid, rlPdId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindPropCmd.BATCH_SET)
    public int setPdBindProp(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductBindPropDto.Key.RL_PD_ID) int rlPdId,
                            @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                            keyMatch = ProductBindPropDto.Key.PROP_BIND) FaiList<Param> addList,
                            @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                            keyMatch = ProductBindPropDto.Key.DEL_PROP_BIND) FaiList<Param> delList) throws IOException {
        return propBindservice.setPdBindProp(session, flow, aid, unionPriId, tid, rlPdId, addList, delList);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_LIST_BY_PROP)
    public int getRlPdByPropVal(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                             @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                             @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                             keyMatch = ProductBindPropDto.Key.INFO_LIST) FaiList<Param> proIdsAndValIds) throws IOException {
        return propBindservice.getRlPdByPropVal(session, flow, aid, unionPriId, tid, proIdsAndValIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REL)
    public int getRelInfoByRlId(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyInteger(ProductRelDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return service.getRelInfoByRlId(session, flow, aid, unionPriId, rlPdId);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REL_LIST)
    public int getRelListByRlIds(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return service.getRelListByRlIds(session, flow, aid, unionPriId, rlPdIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REDUCED_REL_LIST)
    public int getReducedRelsByPdIds(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgList(keyMatch = ProductRelDto.Key.PD_IDS) FaiList<Integer> pdIds) throws IOException {
        return service.getReducedRelsByPdIds(session, flow, aid, unionPriId, pdIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL)
    public int addProductAndRel(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgParam(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                keyMatch = ProductRelDto.Key.INFO) Param info) throws IOException {
        return service.addProductAndRel(session, flow, aid, tid, unionPriId, info);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_REL_BIND)
    public int bindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param bindRlPdInfo,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param info) throws IOException {
        return service.bindProductRel(session, flow, aid, tid, unionPriId, bindRlPdInfo, info);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND)
    public int batchBindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param bindRlPdInfo,
                              @ArgList(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                      keyMatch = ProductRelDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return service.batchBindProductRel(session, flow, aid, tid, bindRlPdInfo, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.DEL_PDS)
    public int delProductList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return service.delProductList(session, flow, aid, tid, unionPriId, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.DEL_REL_BIND)
    public int batchDelPdRelBind(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return service.batchDelPdRelBind(session, flow, aid, unionPriId, rlPdIds);
    }

    private ProductBasicService service = new ProductBasicService();
    private ProductPropBindService propBindservice = new ProductPropBindService();
}
