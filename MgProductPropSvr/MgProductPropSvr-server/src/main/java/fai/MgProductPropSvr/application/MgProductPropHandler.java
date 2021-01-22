package fai.MgProductPropSvr.application;

import fai.MgProductPropSvr.application.service.ProductPropService;
import fai.MgProductPropSvr.interfaces.cmd.MgProductPropCmd;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.util.*;
import fai.middleground.svrutil.service.MiddleGroundHandler;

import java.io.IOException;

public class MgProductPropHandler extends MiddleGroundHandler {

	public MgProductPropHandler(FaiServer server) {
		super(server);
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
							  keyMatch = ProductPropValDto.Key.INFO_LIST) FaiList<Param> addInfoList) {
		return service.setPropValList(session, flow, aid, unionPriId, tid, libId, rlPropId, updaterList, delValIds, addInfoList);
	}

	private ProductPropService service = new ProductPropService();
}
