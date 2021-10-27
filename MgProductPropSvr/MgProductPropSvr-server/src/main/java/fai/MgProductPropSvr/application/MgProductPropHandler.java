package fai.MgProductPropSvr.application;

import fai.MgBackupSvr.interfaces.dto.MgBackupDto;
import fai.MgProductPropSvr.application.service.ProductPropService;
import fai.MgProductPropSvr.interfaces.cmd.MgProductPropCmd;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.middleground.app.CloneDef;
import fai.comm.netkit.NKDef;
import fai.comm.util.*;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

public class MgProductPropHandler extends MiddleGroundHandler {

	public MgProductPropHandler(FaiServer server, RedisCacheManager cache) {
		super(server);
		service.initBackupStatus(cache);
	}

	@Cmd(MgProductPropCmd.PropCmd.GET_LIST)
	public int getPropList(final FaiSession session,
						   @ArgFlow final int flow,
						   @ArgAid final int aid,
						   @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
						   @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
						   @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
						   @ArgSearchArg(value = ProductPropDto.Key.SEARCH_ARG, useDefault = true)SearchArg searchArg) throws IOException {
		return service.getPropInfoList(session, flow, aid, unionPriId, tid, libId, searchArg);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.BATCH_ADD)
	public int addPropList(final FaiSession session,
						   @ArgFlow final int flow,
						   @ArgAid final int aid,
						   @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
						   @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
						   @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
						   @ArgList(classDef = ProductPropDto.class, methodDef = "getInfoDto",
						   keyMatch = ProductPropDto.Key.INFO_LIST) FaiList<Param> recvInfoList) throws IOException {
		return service.addPropInfoList(session, flow, aid, unionPriId, tid, libId, recvInfoList);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.ADD_WITH_VAL)
	public int addPropInfoWithVal(final FaiSession session,
								  @ArgFlow final int flow,
								  @ArgAid final int aid,
								  @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
								  @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
								  @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
								  @ArgParam(classDef = ProductPropDto.class, methodDef = "getInfoDto",
										  keyMatch = ProductPropDto.Key.INFO) Param recvInfo,
								  @ArgList(classDef = ProductPropValDto.class, methodDef = "getInfoDto",
								  			keyMatch = ProductPropValDto.Key.INFO_LIST) FaiList<Param> valList) throws IOException {
		return service.addPropInfoWithVal(session, flow, aid, unionPriId, tid, libId, recvInfo, valList);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.BATCH_SET)
	public int setPropList(final FaiSession session,
						   @ArgFlow final int flow,
						   @ArgAid final int aid,
						   @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
						   @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
						   @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
						   @ArgList(classDef = ProductPropDto.class, methodDef = "getInfoDto",
						   keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList) throws IOException {
		return service.setPropInfoList(session, flow, aid, unionPriId, tid, libId, updaterList);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.BATCH_DEL)
	public int delPropList(final FaiSession session,
						   @ArgFlow final int flow,
						   @ArgAid final int aid,
						   @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
						   @ArgBodyInteger(ProductPropDto.Key.TID) int tid,
						   @ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
						   @ArgList(keyMatch = ProductPropDto.Key.RL_PROP_IDS) FaiList<Integer> rlPropIdList) throws IOException {
		return service.delPropInfoList(session, flow, aid, unionPriId, tid, libId, rlPropIdList);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.CLEAR_REL_DATA)
	public int clearRelData(final FaiSession session,
						   @ArgFlow final int flow,
						   @ArgAid final int aid,
						   @ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
		return service.clearRelData(session, flow, aid, unionPriId);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.CLEAR_ACCT)
	public int clearAcct(final FaiSession session,
							@ArgFlow final int flow,
							@ArgAid final int aid,
							@ArgList(keyMatch = ProductPropDto.Key.UNION_PRI_IDS) FaiList<Integer> unionPriIds) throws IOException {
		return service.clearAcct(session, flow, aid, unionPriIds);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.PropCmd.UNION_SET)
	public int unionSetPropList(final FaiSession session,
								@ArgFlow final int flow,
								@ArgAid final int aid,
								@ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
								@ArgBodyInteger(ProductPropDto.Key.TID) int tid,
								@ArgBodyInteger(ProductPropDto.Key.LIB_ID) int libId,
								@ArgList(classDef = ProductPropDto.class, methodDef = "getInfoDto",
										keyMatch = ProductPropDto.Key.INFO_LIST) FaiList<Param> addList,
								@ArgList(classDef = ProductPropDto.class, methodDef = "getInfoDto",
										keyMatch = ProductPropDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
								@ArgList(keyMatch = ProductPropDto.Key.RL_PROP_IDS) FaiList<Integer> delList) throws IOException{
		return service.unionSetPropList(session, flow, aid, unionPriId, tid, libId, addList, updaterList, delList);
	}

	@Cmd(MgProductPropCmd.PropValCmd.GET_LIST)
	public int getPropValList(final FaiSession session,
							  @ArgFlow final int flow,
							  @ArgAid final int aid,
							  @ArgBodyInteger(ProductPropValDto.Key.UNION_PRI_ID) int unionPriId,
							  @ArgBodyInteger(ProductPropValDto.Key.TID) int tid,
							  @ArgBodyInteger(ProductPropValDto.Key.LIB_ID) int libId,
							  @ArgList(keyMatch = ProductPropValDto.Key.RL_PROP_IDS) FaiList<Integer> rlPropIdList) throws IOException {
		return service.getPropValList(session, flow, aid, unionPriId, tid, libId, rlPropIdList);
	}

	@Cmd(MgProductPropCmd.PropValCmd.BATCH_SET)
	public int setPropValList(final FaiSession session,
							  @ArgFlow final int flow,
							  @ArgAid final int aid,
							  @ArgBodyInteger(ProductPropValDto.Key.UNION_PRI_ID) int unionPriId,
							  @ArgBodyInteger(ProductPropValDto.Key.TID) int tid,
							  @ArgBodyInteger(ProductPropValDto.Key.LIB_ID) int libId,
							  @ArgBodyInteger(ProductPropValDto.Key.RL_PROP_ID) int rlPropId,
							  @ArgList(classDef = ProductPropValDto.class, methodDef = "getInfoDto",
							  keyMatch = ProductPropValDto.Key.UPDATERLIST) FaiList<ParamUpdater> updaterList,
							  @ArgList(keyMatch = ProductPropValDto.Key.VAL_IDS) FaiList<Integer> delValIds,
							  @ArgList(classDef = ProductPropValDto.class, methodDef = "getInfoDto",
							  keyMatch = ProductPropValDto.Key.INFO_LIST) FaiList<Param> addInfoList) throws IOException {
		return service.setPropValList(session, flow, aid, unionPriId, tid, libId, rlPropId, updaterList, delValIds, addInfoList);
	}

	@Cmd(MgProductPropCmd.PropValCmd.GET_DATA_STATUS)
	public int getPropValDataStatus(final FaiSession session,
									@ArgFlow int flow,
									@ArgAid final int aid) throws IOException {
		return service.getPropValDataStatus(session, flow, aid);
	}

	@Cmd(MgProductPropCmd.PropValCmd.GET_ALL_DATA)
	public int getAllPropValData(final FaiSession session,
								 @ArgFlow int flow,
								 @ArgAid final int aid) throws IOException {
		return service.getAllPropValData(session, flow, aid);
	}

	@Cmd(MgProductPropCmd.PropValCmd.SEARCH_FROM_DB)
	public int searchPropValFromDb(final FaiSession session,
								 @ArgFlow int flow,
								 @ArgAid final int aid,
								 @ArgSearchArg(ProductPropValDto.Key.SEARCH_ARG) SearchArg searchArg) throws IOException {
		return service.searchPropValFromDb(session, flow, aid, searchArg);
	}

	@Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
	public int clearCache(final FaiSession session,
								   @ArgFlow int flow,
								   @ArgAid final int aid) throws IOException {
		return service.clearCache(session, flow, aid);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.Cmd.BACKUP)
	public int backupData(final FaiSession session,
						  @ArgFlow final int flow,
						  @ArgAid int aid,
						  @ArgList(keyMatch = ProductPropDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
						  @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
								  keyMatch = ProductPropDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return service.backupData(session, flow, aid, unionPriIds, backupInfo);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.Cmd.RESTORE)
	public int restoreBackupData(final FaiSession session,
								 @ArgFlow final int flow,
								 @ArgAid int aid,
								 @ArgList(keyMatch = ProductPropDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
								 @ArgBodyInteger(ProductPropDto.Key.RESTORE_ID) int restoreId,
								 @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
										 keyMatch = ProductPropDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return service.restoreBackupData(session, flow, aid, unionPriIds, restoreId, backupInfo);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.Cmd.DEL_BACKUP)
	public int delBackupData(final FaiSession session,
							 @ArgFlow final int flow,
							 @ArgAid int aid,
							 @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
									 keyMatch = ProductPropDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
		return service.delBackupData(session, flow, aid, backupInfo);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.Cmd.CLONE)
	public int cloneData(final FaiSession session,
						 @ArgFlow final int flow,
						 @ArgAid int aid,
						 @ArgBodyInteger(ProductPropDto.Key.FROM_AID) int fromAid,
						 @ArgList(classDef = CloneDef.Dto.class, methodDef = "getInternalDto",
								 keyMatch = ProductPropDto.Key.CLONE_UNION_PRI_IDS) FaiList<Param> cloneUnionPriIds) throws IOException {
		return service.cloneData(session, flow, aid, fromAid, cloneUnionPriIds);
	}

	@WrittenCmd
	@Cmd(MgProductPropCmd.Cmd.INCR_CLONE)
	public int incrementalClone(final FaiSession session,
								@ArgFlow final int flow,
								@ArgAid int aid,
								@ArgBodyInteger(ProductPropDto.Key.UNION_PRI_ID) int unionPriId,
								@ArgBodyInteger(ProductPropDto.Key.FROM_AID) int fromAid,
								@ArgBodyInteger(ProductPropDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId) throws IOException {
		return service.incrementalClone(session, flow, aid, unionPriId, fromAid, fromUnionPriId);
	}

	private ProductPropService service = ServiceProxy.create(new ProductPropService());
}
