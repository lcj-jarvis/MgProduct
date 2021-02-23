package fai.MgProductGroupSvr.application;

import fai.MgProductGroupSvr.application.service.ProductGroupService;
import fai.MgProductGroupSvr.interfaces.cmd.MgProductGroupCmd;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupDto;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
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
	
	ProductGroupService groupService = (ProductGroupService) (new ServiceProxy(new ProductGroupService()).create());
}
