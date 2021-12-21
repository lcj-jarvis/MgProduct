package fai.MgProductGroupSvr.application;

import fai.MgBackupSvr.interfaces.dto.MgBackupDto;
import fai.MgProductGroupSvr.application.service.ProductGroupService;
import fai.MgProductGroupSvr.interfaces.cmd.MgProductGroupCmd;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.netkit.NKDef;
import fai.comm.util.*;
import fai.comm.middleground.app.CloneDef;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

public class MgProductGroupHandler extends MiddleGroundHandler {

	public MgProductGroupHandler(FaiServer server, RedisCacheManager cache) {
		super(server);
		groupService.initBackupStatus(cache);
	}
	
	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.ADD)
	public int addProductGroup(final FaiSession session,
							   @ArgFlow final int flow,
							   @ArgAid int aid,
							   @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							   @ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
							   @ArgBodyInteger(ProductGroupRelDto.Key.SYS_TYPE) int sysType,
							   @ArgParam(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
							   keyMatch = ProductGroupRelDto.Key.INFO) Param info) throws IOException {
		return groupService.addGroupInfo(session, flow, aid, unionPriId, tid, sysType, info);
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
							@ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							@ArgBodyInteger(ProductGroupRelDto.Key.SYS_TYPE) int sysType,
							@ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
									keyMatch = ProductGroupRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList) throws IOException {
		return groupService.setGroupList(session, flow, aid, tid, unionPriId, sysType, updaterList);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.SET_ALL_GROUP_LIST)
	public int setAllGroupList(final FaiSession session,
							   @ArgFlow final int flow,
							   @ArgAid int aid,
							   @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							   @ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
							   @ArgList(classDef = ProductGroupRelDto.class, methodDef = "getTreeInfoDto",
									   keyMatch = ProductGroupRelDto.Key.UPDATERLIST) FaiList<Param> treeDataList,
							   @ArgBodyInteger(ProductGroupRelDto.Key.SYS_TYPE) int sysType,
							   @ArgBodyInteger(ProductGroupRelDto.Key.GROUP_LEVEL) int groupLevel,
							   @ArgBodyBoolean(ProductGroupRelDto.Key.SOFT_DEL) boolean softDel) throws IOException {
		return groupService.setAllGroupList(session, flow, aid, unionPriId, tid, treeDataList, sysType, groupLevel, softDel);
	}

	@Cmd(MgProductGroupCmd.GroupCmd.BATCH_DEL)
	public int delGroupList(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
							@ArgList(keyMatch = ProductGroupRelDto.Key.RL_GROUP_IDS) FaiList<Integer> rlGroupIds,
							@ArgBodyInteger(ProductGroupRelDto.Key.SYS_TYPE) int sysType,
							@ArgBodyBoolean(ProductGroupRelDto.Key.SOFT_DEL) boolean softDel) throws IOException {
		return groupService.delGroupList(session, flow, aid, unionPriId, rlGroupIds, sysType, softDel);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.UNION_SET_GROUP_LIST)
	public int unionSetGroupList(final FaiSession session,
								 @ArgFlow final int flow,
								 @ArgAid int aid,
								 @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
								 @ArgBodyInteger(ProductGroupRelDto.Key.TID) int tid,
								 @ArgBodyBoolean(ProductGroupRelDto.Key.SOFT_DEL) boolean softDel,
								 @ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
										 keyMatch = ProductGroupRelDto.Key.INFO) FaiList<Param> addList,
								 @ArgList(classDef = ProductGroupRelDto.class, methodDef = "getAllInfoDto",
										 keyMatch = ProductGroupRelDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
								 @ArgList(keyMatch = ProductGroupRelDto.Key.RL_GROUP_IDS) FaiList<Integer> delList,
								 @ArgBodyInteger(ProductGroupRelDto.Key.SYS_TYPE) int sysType) throws IOException {
		return groupService.unionSetGroupList(session, flow, aid, unionPriId, tid, addList, updaterList, delList, sysType, softDel);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.CLONE)
	public int cloneData(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid int aid,
							@ArgBodyInteger(ProductGroupRelDto.Key.FROM_AID) int fromAid,
							@ArgList(classDef = CloneDef.Dto.class, methodDef = "getInternalDto",
									keyMatch = ProductGroupRelDto.Key.CLONE_UNION_PRI_IDS) FaiList<Param> cloneUnionPriIds) throws IOException {
		return groupService.cloneData(session, flow, aid, fromAid, cloneUnionPriIds);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.INCR_CLONE)
	public int incrementalClone(final FaiSession session,
						 @ArgFlow final int flow,
						 @ArgAid int aid,
						 @ArgBodyInteger(ProductGroupRelDto.Key.UNION_PRI_ID) int unionPriId,
						 @ArgBodyInteger(ProductGroupRelDto.Key.FROM_AID) int fromAid,
						 @ArgBodyInteger(ProductGroupRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId) throws IOException {
		return groupService.incrementalClone(session, flow, aid, unionPriId, fromAid, fromUnionPriId);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.BACKUP)
	public int backupData(final FaiSession session,
						  @ArgFlow final int flow,
						  @ArgAid int aid,
						  @ArgList(keyMatch = ProductGroupRelDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
						  @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
						  keyMatch = ProductGroupRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return groupService.backupData(session, flow, aid, unionPriIds, backupInfo);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.RESTORE)
	public int restoreBackupData(final FaiSession session,
						  @ArgFlow final int flow,
						  @ArgAid int aid,
						  @ArgList(keyMatch = ProductGroupRelDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
						  @ArgBodyInteger(ProductGroupRelDto.Key.RESTORE_ID) int restoreId,
						  @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
								  keyMatch = ProductGroupRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return groupService.restoreBackupData(session, flow, aid, unionPriIds, restoreId, backupInfo);
	}

	@WrittenCmd
	@Cmd(MgProductGroupCmd.GroupCmd.DEL_BACKUP)
	public int delBackupData(final FaiSession session,
								 @ArgFlow final int flow,
								 @ArgAid int aid,
								 @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
										 keyMatch = ProductGroupRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return groupService.delBackupData(session, flow, aid, backupInfo);
	}

	@Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
	public int clearCache(final FaiSession session,
						  @ArgFlow final int flow,
						  @ArgAid int aid) throws IOException {
		return groupService.clearCache(session, flow, aid);
	}
	
	ProductGroupService groupService = ServiceProxy.create(new ProductGroupService());
}
