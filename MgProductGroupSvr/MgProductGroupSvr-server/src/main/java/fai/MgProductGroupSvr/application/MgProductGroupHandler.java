package fai.MgProductGroupSvr.application;

import fai.MgProductGroupSvr.application.service.ProductGroupService;
import fai.MgProductGroupSvr.interfaces.cmd.MgProductGroupCmd;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.netkit.NKDef;
import fai.comm.util.*;
import fai.mgproduct.comm.CloneDef;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

public class MgProductGroupHandler extends MiddleGroundHandler {

	public MgProductGroupHandler(FaiServer server) {
		super(server);
	}
	
	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.ADD)
	public int addProductGroup(final FaiSession session,
							   @ArgFlow final int flow,
							   @ArgAid int aid,
							   @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							   @ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
							   @ArgParam(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
							   keyMatch = ProductGroupRelDto.Key.INFO) Param info) throws IOException {
		return groupService.addGroupInfo(session, flow, aid, unionPriId, tid, info);
	}
	
	@Cmd(MgProductGroupCmd.GroupCmd.GET_LIST)
	public int getGroupList(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							@ArgSearchArg(ProductGroupRelDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
		return groupService.getGroupList(session, flow, aid, unionPriId, searchArg);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.GET_REL_DATA_STATUS)
	public int getGroupRelStatus(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
		return groupService.getGroupRelDataStatus(session, flow, aid, unionPriId);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.SEARCH_REL)
	public int searchGroupRelFromDB(final FaiSession session,
									@ArgFlow final int flow,
									@ArgAid int aid,
									@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int uninoPriId,
									@ArgSearchArg(ProductGroupRelDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
		return groupService.searchGroupRelFromDb(session, flow, aid, uninoPriId, searchArg);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.GET_ALL_REL)
	public int getAllGroupRel(final FaiSession session,
									@ArgFlow final int flow,
									@ArgAid int aid,
									@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int uninoPriId) throws IOException {
		return groupService.getAllGroupRel(session, flow, aid, uninoPriId);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.BATCH_SET)
	public int setGroupList(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							@ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
									keyMatch = ProductGroupRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList) throws IOException {
		return groupService.setGroupList(session, flow, aid, unionPriId, updaterList);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.BATCH_DEL)
	public int delGroupList(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							@ArgList(keyMatch = ProductGroupRelDto.Key.RL_GROUP_IDS) FaiList<Integer> rlGroupIds) throws IOException {
		return groupService.delGroupList(session, flow, aid, unionPriId, rlGroupIds);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.UNION_SET_GROUP_LIST)
	public int unionSetGroupList(final FaiSession session,
								 @ArgFlow final int flow,
								 @ArgAid int aid,
								 @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
								 @ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
								 @ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
										 keyMatch = ProductGroupRelDto.Key.INFO) FaiList<Param> addList,
								 @ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
										 keyMatch = ProductGroupRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
								 @ArgList(keyMatch = ProductGroupRelDto.Key.RL_GROUP_IDS) FaiList<Integer> delList) throws IOException {
		return groupService.unionSetGroupList(session, flow, aid, unionPriId, tid, addList, updaterList, delList);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.CLONE)
	public int cloneData(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyBoolean(ProductGroupRelDto.Key.FROM_AID) int fromAid,
							@ArgList(classDef = CloneDef.Dto.class, methodDef = "getDto",
									keyMatch = ProductGroupRelDto.Key.CLONE_UNION_PRI_IDS) FaiList<Param> cloneUnionPriIds) throws IOException {
		return groupService.cloneData(session, flow, aid, fromAid, cloneUnionPriIds);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.INCR_CLONE)
	public int incrementalClone(final FaiSession session,
						 @ArgFlow final int flow,
						 @ArgAid int aid,
						 @ArgBodyBoolean(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
						 @ArgBodyBoolean(ProductGroupRelDto.Key.FROM_AID) int fromAid,
						 @ArgBodyBoolean(ProductGroupRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId) throws IOException {
		return groupService.incrementalClone(session, flow, aid, unionPriId, fromAid, fromUnionPriId);
	}

	@Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
	public int clearCache(final FaiSession session,
						  @ArgFlow final int flow,
						  @ArgAid int aid) throws IOException {
		return groupService.clearCache(session, flow, aid);
	}
	
	ProductGroupService groupService = ServiceProxy.create(new ProductGroupService());
}
