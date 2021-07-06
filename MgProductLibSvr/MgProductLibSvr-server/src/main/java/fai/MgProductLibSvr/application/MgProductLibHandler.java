package fai.MgProductLibSvr.application;

import fai.MgProductLibSvr.application.service.ProductLibService;
import fai.MgProductLibSvr.interfaces.cmd.MgProductLibCmd;
import fai.MgProductLibSvr.interfaces.dto.ProductLibRelDto;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:18
 */
public class MgProductLibHandler extends MiddleGroundHandler {

    public MgProductLibHandler(FaiServer server) {
        super(server);
    }

    ProductLibService libService = ServiceProxy.create(new ProductLibService());

    @WrittenCmd
    @Cmd(MgProductLibCmd.LibCmd.ADD)
    public int addProductLib(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid int aid,
                               @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                               @ArgBodyInteger(ProductLibRelDto.Key.TID) int tid,
                               @ArgParam(classDef = ProductLibRelDto.class, methodDef = "getAllInfoDto",
                                       keyMatch = ProductLibRelDto.Key.INFO) Param info) throws IOException {
        return libService.addProductLib(session, flow, aid, unionPriId, tid, info);
    }

    @Cmd(MgProductLibCmd.LibCmd.BATCH_DEL)
    public int delLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid int aid,
                            @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgList(keyMatch = ProductLibRelDto.Key.RL_LIB_IDS) FaiList<Integer> rlLibIds) throws IOException {
        return libService.delLibList(session, flow, aid, unionPriId, rlLibIds);
    }

    @Cmd(MgProductLibCmd.LibCmd.BATCH_SET)
    public int setLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid int aid,
                            @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgList(classDef = ProductLibRelDto.class, methodDef = "getAllInfoDto",
                                    keyMatch = ProductLibRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return libService.setLibList(session, flow, aid, unionPriId, updaterList);
    }

    @Cmd(MgProductLibCmd.LibCmd.GET_LIST)
    public int getLibList(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid int aid,
                            @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgSearchArg(ProductLibRelDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return libService.getLibList(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductLibCmd.LibCmd.GET_ALL_REL)
    public int getAllLibRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return libService.getAllLibRel(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductLibCmd.LibCmd.SEARCH_REL)
    public int getLibRelFromDb(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid int aid,
                                    @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                                    @ArgSearchArg(ProductLibRelDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return libService.getLibRelFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductLibCmd.LibCmd.GET_REL_DATA_STATUS)
    public int getLibRelDataStatus(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid int aid,
                                 @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return libService.getLibRelDataStatus(session, flow, aid, unionPriId);
    }


    @WrittenCmd
    @Cmd(MgProductLibCmd.LibCmd.UNION_SET_LIB_LIST)
    public int unionSetGroupList(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid int aid,
                                 @ArgBodyInteger(ProductLibRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgBodyInteger(ProductLibRelDto.Key.TID) int tid,
                                 @ArgList(classDef = ProductLibRelDto.class, methodDef = "getAllInfoDto",
                                         keyMatch = ProductLibRelDto.Key.INFO_LIST) FaiList<Param> addInfoList,
                                 @ArgList(classDef = ProductLibRelDto.class, methodDef = "getAllInfoDto",
                                         keyMatch = ProductLibRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
                                 @ArgList(keyMatch = ProductLibRelDto.Key.RL_LIB_IDS) FaiList<Integer> delRlLibIds) throws IOException {
        return  libService.unionSetLibList(session, flow, aid, unionPriId, tid, addInfoList, updaterList, delRlLibIds);
    }
}
