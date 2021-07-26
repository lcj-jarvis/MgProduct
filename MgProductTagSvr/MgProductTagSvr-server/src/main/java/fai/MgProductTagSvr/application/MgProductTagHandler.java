package fai.MgProductTagSvr.application;

import fai.MgProductTagSvr.application.service.ProductTagService;
import fai.MgProductTagSvr.interfaces.cmd.MgProductTagCmd;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:51
 */
public class MgProductTagHandler extends MiddleGroundHandler {

    public MgProductTagHandler(FaiServer server) {
        super(server);
    }

    ProductTagService tagService = ServiceProxy.create(new ProductTagService());

    @WrittenCmd
    @Cmd(MgProductTagCmd.TagCmd.ADD)
    public int addProductTag(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid int aid,
                             @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                             @ArgBodyInteger(ProductTagRelDto.Key.TID) int tid,
                             @ArgParam(classDef = ProductTagRelDto.class, methodDef = "getAllInfoDto",
                                     keyMatch = ProductTagRelDto.Key.INFO) Param info) throws IOException {
        return tagService.addProductTag(session, flow, aid, unionPriId, tid, info);
    }

    @Cmd(MgProductTagCmd.TagCmd.BATCH_DEL)
    public int delTagList(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid int aid,
                          @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                          @ArgList(keyMatch = ProductTagRelDto.Key.RL_TAG_IDS) FaiList<Integer> rlTagIds) throws IOException {
        return tagService.delTagList(session, flow, aid, unionPriId, rlTagIds);
    }

    @Cmd(MgProductTagCmd.TagCmd.BATCH_SET)
    public int setTagList(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid int aid,
                          @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                          @ArgList(classDef = ProductTagRelDto.class, methodDef = "getAllInfoDto",
                                  keyMatch = ProductTagRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList) throws IOException {
        return tagService.setTagList(session, flow, aid, unionPriId, updaterList);
    }

    @Cmd(MgProductTagCmd.TagCmd.GET_LIST)
    public int getTagList(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid int aid,
                          @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                          @ArgSearchArg(ProductTagRelDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return tagService.getTagList(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductTagCmd.TagCmd.GET_ALL_REL)
    public int getAllTagRel(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid int aid,
                            @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return tagService.getAllTagRel(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductTagCmd.TagCmd.SEARCH_REL)
    public int getTagRelFromDb(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid int aid,
                               @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                               @ArgSearchArg(ProductTagRelDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
        return tagService.getTagRelFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductTagCmd.TagCmd.GET_REL_DATA_STATUS)
    public int getTagRelDataStatus(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid int aid,
                                   @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return tagService.getTagRelDataStatus(session, flow, aid, unionPriId);
    }


    @WrittenCmd
    @Cmd(MgProductTagCmd.TagCmd.UNION_SET_TAG_LIST)
    public int unionSetTagList(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid int aid,
                                 @ArgBodyInteger(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgBodyInteger(ProductTagRelDto.Key.TID) int tid,
                                 @ArgList(classDef = ProductTagRelDto.class, methodDef = "getAllInfoDto",
                                         keyMatch = ProductTagRelDto.Key.INFO_LIST) FaiList<Param> addInfoList,
                                 @ArgList(classDef = ProductTagRelDto.class, methodDef = "getAllInfoDto",
                                         keyMatch = ProductTagRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
                                 @ArgList(keyMatch = ProductTagRelDto.Key.RL_TAG_IDS) FaiList<Integer> delRlTagIds) throws IOException {
        return  tagService.unionSetTagList(session, flow, aid, unionPriId, tid, addInfoList, updaterList, delRlTagIds);
    }

    @WrittenCmd
    @Cmd(MgProductTagCmd.TagCmd.CLONE)
    public int cloneData(final FaiSession session,
                         @ArgFlow final int flow,
                         @ArgAid int aid,
                         @ArgBodyBoolean(ProductTagRelDto.Key.FROM_AID) int fromAid,
                         @ArgList(classDef = CloneDef.Dto.class, methodDef = "getInternalDto",
                                 keyMatch = ProductTagRelDto.Key.CLONE_UNION_PRI_IDS) FaiList<Param> cloneUnionPriIds) throws IOException {
        return tagService.cloneData(session, flow, aid, fromAid, cloneUnionPriIds);
    }

    @WrittenCmd
    @Cmd(MgProductTagCmd.TagCmd.INCR_CLONE)
    public int incrementalClone(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid int aid,
                                @ArgBodyBoolean(ProductTagRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyBoolean(ProductTagRelDto.Key.FROM_AID) int fromAid,
                                @ArgBodyBoolean(ProductTagRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId) throws IOException {
        return tagService.incrementalClone(session, flow, aid, unionPriId, fromAid, fromUnionPriId);
    }

}
