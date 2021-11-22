package fai.MgProductBasicSvr.application;

import fai.MgBackupSvr.interfaces.dto.MgBackupDto;
import fai.MgProductBasicSvr.application.service.*;
import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.GfwUtil;
import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.fseata.client.core.rpc.annotation.SagaTransaction;
import fai.comm.fseata.client.core.rpc.def.CommDef;
import fai.comm.jnetkit.server.ServerHandlerContext;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.WrittenCmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.middleground.app.CloneDef;
import fai.comm.netkit.NKDef;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;
import fai.middleground.svrutil.service.MiddleGroundHandler;
import fai.middleground.svrutil.service.ServiceProxy;

import java.io.IOException;

public class MgProductBasicHandler extends MiddleGroundHandler {
    public MgProductBasicHandler(FaiServer server, RedisCacheManager cache) {
        super(server);
        service.initBackupStatus(cache);
    }

    @Override
    public void channelRead(final ServerHandlerContext context,
                            final Object message) throws Exception {
        try {
            super.channelRead(context, message);
        }finally {
            afterRequest();
        }
    }

    /**
     * 请求结束后执行，不管请求是否报错，都会执行
     */
    private void afterRequest() {
        ESUtil.clear();
        GfwUtil.clear();
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_LIST)
    public int getPdBindProp(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgBodyInteger(value = ProductBindPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                            @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductBindPropDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return propBindService.getPdBindProp(session, flow, aid, unionPriId, sysType, tid, rlPdId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindPropCmd.BATCH_SET)
    public int setPdBindProp(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                            @ArgBodyInteger(value = ProductBindPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                            @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                            @ArgBodyInteger(ProductBindPropDto.Key.RL_PD_ID) int rlPdId,
                            @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                            keyMatch = ProductBindPropDto.Key.PROP_BIND) FaiList<Param> addList,
                            @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                            keyMatch = ProductBindPropDto.Key.DEL_PROP_BIND) FaiList<Param> delList) throws IOException {
        return propBindService.setPdBindProp(session, flow, aid, unionPriId, sysType, tid, rlPdId, addList, delList);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_LIST_BY_PROP)
    public int getRlPdByPropVal(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                             @ArgBodyInteger(value = ProductBindPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                             @ArgBodyInteger(ProductBindPropDto.Key.TID) int tid,
                             @ArgList(classDef = ProductBindPropDto.class, methodDef = "getInfoDto",
                             keyMatch = ProductBindPropDto.Key.INFO_LIST) FaiList<Param> proIdsAndValIds) throws IOException {
        return propBindService.getRlPdByPropVal(session, flow, aid, unionPriId, sysType, tid, proIdsAndValIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindPropCmd.DEL_BY_PROP_IDS)
    public int delPdBindPropByProps(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                                   @ArgBodyInteger(value = ProductBindPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                   @ArgList(keyMatch = ProductBindPropDto.Key.RL_PROP_IDS) FaiList<Integer> rlPropIds) throws IOException {
        return propBindService.delPdBindPropByPropId(session, flow, aid, unionPriId, sysType, rlPropIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindPropCmd.DEL_BY_VAL_IDS)
    public int delPdBindPropByVals(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                             @ArgBodyInteger(value = ProductBindPropDto.Key.SYS_TYPE, useDefault = true) int sysType,
                             @ArgBodyInteger(ProductBindPropDto.Key.RL_PROP_ID) int rlPropId,
                             @ArgList(keyMatch = ProductBindPropDto.Key.PROP_VAL_IDS) FaiList<Integer> delPropValIds) throws IOException {
        return propBindService.delPdBindPropByValId(session, flow, aid, unionPriId, sysType, rlPropId, delPropValIds);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_DATA_STATUS)
    public int getBindPropDataStatus(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return propBindService.getBindPropDataStatus(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.GET_ALL_DATA)
    public int getAllBindProp(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return propBindService.getAllBindProp(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindPropCmd.SEARCH_FROM_DB)
    public int searchBindPropFromDb(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId,
                                    @ArgSearchArg(ProductBindPropDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return propBindService.searchBindPropFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REL)
    public int getRelInfoByRlId(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgBodyInteger(ProductRelDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return service.getRelInfoByRlId(session, flow, aid, unionPriId, sysType, rlPdId);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REL_LIST)
    public int getRelListByRlIds(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                @ArgBodyBoolean(value = ProductRelDto.Key.SOFT_DEL, useDefault = true) boolean withSoftDel) throws IOException {
        return service.getRelListByRlIds(session, flow, aid, unionPriId, sysType, rlPdIds, withSoftDel);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REDUCED_REL_LIST)
    public int getReducedRelsByPdIds(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgList(keyMatch = ProductRelDto.Key.PD_IDS) FaiList<Integer> pdIds) throws IOException {
        return service.getReducedRelsByPdIds(session, flow, aid, unionPriId, pdIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_REDUCED_LIST_BY_NAME)
    public int getPdReducedList4Adm(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                     @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true, defaultValue = -1) int sysType,
                                     @ArgList(keyMatch = ProductRelDto.Key.NAME) FaiList<String> names,
                                     @ArgList(keyMatch = ProductRelDto.Key.STATUS, useDefault = true) FaiList<Integer> status) throws IOException {
        return service.getPdReducedList4Adm(session, flow, aid, unionPriId, sysType, names, status);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL_ROLLBACK)
    public int addProductAndRel(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                                @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                                @ArgBodyInteger(value = ProductRelDto.Key.SITE_ID, useDefault = true, defaultValue = -1) int siteId,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgParam(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                keyMatch = ProductRelDto.Key.INFO) Param info) throws IOException {
        return service.addProductAndRel(session, flow, aid, xid, tid, siteId, unionPriId, info);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL_ROLLBACK)
    public int addProductAndRelRollback(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                      @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.addProductAndRelRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_PD_AND_REL)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL_ROLLBACK)
    public int batchAddProductAndRel(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                               @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                               @ArgBodyInteger(value = ProductRelDto.Key.SITE_ID, useDefault = true, defaultValue = -1) int siteId,
                               @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                               @ArgList(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                       keyMatch = ProductRelDto.Key.INFO) FaiList<Param> list) throws IOException {
        return service.batchAddProductAndRel(session, flow, aid, xid, tid, siteId, unionPriId, list);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_REL_BIND)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.ADD_REL_BIND_ROLLBACK)
    public int bindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param bindRlPdInfo,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param info) throws IOException {
        return service.bindProductRel(session, flow, aid, xid, tid, unionPriId, bindRlPdInfo, info);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.ADD_REL_BIND_ROLLBACK)
    public int bindProductRelRollback(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                              @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.bindProductRelRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_BIND_PDS_REL)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND_ROLLBACK)
    public int batchBindProductRel(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                                   @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                                   @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int toUnionPriId,
                                   @ArgBodyInteger(ProductRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId,
                                   @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                   @ArgList(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                           keyMatch = ProductRelDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return service.batchBindProductRel(session, flow, xid, aid, tid, toUnionPriId, fromUnionPriId, sysType, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND_ROLLBACK)
    public int batchBindProductRel(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgParam(classDef = ProductRelDto.class, methodDef = "getInfoDto",
                                      keyMatch = ProductRelDto.Key.INFO) Param bindRlPdInfo,
                              @ArgList(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                      keyMatch = ProductRelDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return service.batchBindProductRel(session, flow, xid, aid, tid, bindRlPdInfo, infoList);
    }
    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND_ROLLBACK)
    public int batchBindProductRelRollback(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                      @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.batchBindProductRelRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_PDS_REL_BIND)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND_ROLLBACK)
    public int batchBindProductsRel(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                                   @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                                   @ArgBodyBoolean(ProductRelDto.Key.NEED_SYNC_INFO) boolean needSyncInfo,
                                   @ArgList(classDef = ProductRelDto.class, methodDef = "getTmpBindDto",
                                           keyMatch = ProductRelDto.Key.INFO_LIST) FaiList<Param> infoList) throws IOException {
        return service.batchBindProductsRel(session, flow, aid, xid, tid, needSyncInfo, infoList);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.DEL_PDS)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.DEL_PDS_ROLLBACK)
    public int delProductList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                              @ArgBodyInteger(ProductRelDto.Key.TID) int tid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                              @ArgBodyBoolean(value = ProductRelDto.Key.SOFT_DEL,
                              useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return service.delProductList(session, flow, aid, xid, tid, unionPriId, sysType, rlPdIds, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.DEL_PDS_ROLLBACK)
    public int delProductListRollback(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                      @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.delProductListRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.CLEAR_REL_DATA)
    public int clearRelData(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyBoolean(value = ProductRelDto.Key.SOFT_DEL,
                                      useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return service.clearRelData(session, flow, aid, unionPriId, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.CLEAR_ACCT)
    public int clearRelData(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgList(keyMatch = ProductRelDto.Key.UNION_PRI_IDS) FaiList<Integer> unionPriIds) throws IOException {
        return service.clearAcct(session, flow, aid, unionPriIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_SET_4YK)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.BATCH_SET_4YK_ROLLBACK)
    public int batchSet4YK(final FaiSession session,
                            @ArgFlow final int flow,
                            @ArgAid final int aid,
                            @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                            @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int ownUnionPriId,
                            @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                            @ArgList(keyMatch = ProductRelDto.Key.UNION_PRI_IDS) FaiList<Integer> unionPriIds,
                            @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                            @ArgParamUpdater(keyMatch = ProductRelDto.Key.UPDATER, classDef = ProductRelDto.class,
                            methodDef = "getRelAndPdDto") ParamUpdater updater) throws IOException {
        return service.batchSet4YK(session, flow, aid, xid, ownUnionPriId, sysType, unionPriIds, rlPdIds, updater);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.BATCH_SET_4YK_ROLLBACK)
    public int batchSet4YKRollback(final FaiSession session,
                                   @ArgFlow final int flow,
                                   @ArgAid final int aid,
                                   @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                   @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.batchSet4YKRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.CLONE_BIZ_BIND)
    public int cloneBizBind(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int toUnionPriId,
                                 @ArgBodyBoolean(value = ProductRelDto.Key.SOFT_DEL, useDefault = true) boolean silentDel) throws IOException {
        return service.cloneBizBind(session, flow, aid, fromUnionPriId, toUnionPriId, silentDel);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.DEL_REL_BIND)
    public int batchDelPdRelBind(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                 @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                                 @ArgBodyBoolean(value = ProductRelDto.Key.SOFT_DEL,
                                 useDefault = true, defaultValue = false) boolean softDel) throws IOException {
        return service.batchDelPdRelBind(session, flow, aid, unionPriId, sysType, rlPdIds, softDel);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.SET_SORT)
    public int setPdSort(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(value = ProductRelDto.Key.TID, useDefault = true) int tid,
                           @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                           @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgBodyInteger(ProductRelDto.Key.RL_PD_ID) Integer rlPdId,
                           @ArgBodyInteger(ProductRelDto.Key.PRE_RL_PD_ID) Integer preRlPdId) throws IOException {
        return service.setPdSort(session, flow, aid, tid, unionPriId, sysType, rlPdId, preRlPdId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.SET_SINGLE_PD)
    @SagaTransaction(clientName = CLI_NAME, rollbackCmd = MgProductBasicCmd.BasicCmd.SET_SINGLE_PD_ROLLBACK)
    public int setSinglePd(final FaiSession session,
                                 @ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyXid(value = ProductRelDto.Key.XID, useDefault = true) String xid,
                                 @ArgBodyInteger(value = ProductRelDto.Key.TID, useDefault = true) int tid,
                                 @ArgBodyInteger(value = ProductRelDto.Key.SITE_ID, useDefault = true, defaultValue = -1) int siteId,
                                 @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                 @ArgBodyInteger(ProductRelDto.Key.RL_PD_ID) Integer rlPdId,
                                 @ArgParamUpdater(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                 keyMatch = ProductRelDto.Key.UPDATER) ParamUpdater recvUpdater) throws IOException {
        return service.setSingle(session, flow, aid, xid, tid, siteId, unionPriId, sysType, rlPdId, recvUpdater);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.SET_SINGLE_PD_ROLLBACK)
    public int setSinglePdRollback(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyString(CommDef.Protocol.Key.XID) String xid,
                                      @ArgBodyLong(CommDef.Protocol.Key.BRANCH_ID) Long branchId) throws IOException {
        return service.setSinglePdRollback(session, flow, aid, xid, branchId);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BasicCmd.SET_PDS)
    public int setProducts(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(value = ProductRelDto.Key.TID, useDefault = true) int tid,
                           @ArgBodyInteger(value = ProductRelDto.Key.SITE_ID, useDefault = true, defaultValue = -1) int siteId,
                           @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                           @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                           @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds,
                           @ArgParamUpdater(classDef = ProductRelDto.class, methodDef = "getRelAndPdDto",
                                   keyMatch = ProductRelDto.Key.UPDATER) ParamUpdater recvUpdater) throws IOException {
        return service.setProducts(session, flow, aid, tid, siteId, unionPriId, sysType, rlPdIds, recvUpdater);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_BY_PDID)
    public int getProductByPdId(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(ProductRelDto.Key.PD_ID) int pdId) throws IOException {
        return service.getInfoByPdId(session, flow, aid, unionPriId, pdId);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_LIST_BY_PDID)
    public int getPdListByPdIds(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid int aid,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgList(keyMatch = ProductRelDto.Key.PD_IDS) FaiList<Integer> pdIds) throws IOException {
        return service.getPdListByPdIds(session, flow, aid, unionPriId, pdIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_PD_INFO)
    public int getProductInfo(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductRelDto.Key.RL_PD_ID) int rlPdId) throws IOException {
        return service.getProductInfo(session, flow, aid, unionPriId, sysType, rlPdId);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_PD_LIST)
    public int getProductList(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return service.getProductList(session, flow, aid, unionPriId, sysType, rlPdIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_BIND_BIZ)
    public int getPdBindBiz(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductRelDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductRelDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return service.getPdBindBizInfo(session, flow, aid, unionPriId, sysType, rlPdIds);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.PD_DATA_STATUS)
    public int getPdDataStatus(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid) throws IOException {
        return service.getProductDataStatus(session, flow, aid);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_FROM_DB)
    public int searchPdFromDb(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgSearchArg(ProductDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return service.searchProductFromDb(session, flow, aid, searchArg);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD)
    public int getAllProduct(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid) throws IOException {
        return service.getAllProduct(session, flow, aid);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.PD_REL_DATA_STATUS)
    public int getPdRelDataStatus(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return service.getProductRelDataStatus(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_REL_FROM_DB)
    public int searchPdRelFromDb(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgSearchArg(ProductRelDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return service.searchProductRelFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @Cmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD_REL)
    public int getAllProductRel(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid final int aid,
                             @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return service.getAllProductRel(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_LIST)
    public int getPdsBindGroup(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid int aid,
                                @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyInteger(value = ProductBindGroupDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                @ArgList(keyMatch = ProductBindGroupDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return groupBindService.getPdsBindGroup(session, flow, aid, unionPriId, sysType, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindGroupCmd.BATCH_SET)
    public int setPdBindGroup(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid int aid,
                               @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId,
                               @ArgBodyInteger(value = ProductBindGroupDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgBodyInteger(ProductBindGroupDto.Key.RL_PD_ID) int rlPdId,
                               @ArgList(keyMatch = ProductBindGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> addGroupIds,
                               @ArgList(keyMatch = ProductBindGroupDto.Key.DEL_RL_GROUP_IDS) FaiList<Integer> delGroupIds) throws IOException {
        return groupBindService.setPdBindGroup(session, flow, aid, unionPriId, sysType, rlPdId, addGroupIds, delGroupIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindGroupCmd.DEL)
    public int delPdBindGroup(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductBindGroupDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductBindGroupDto.Key.DEL_RL_GROUP_IDS) FaiList<Integer> delGroupIds) throws IOException {
        return groupBindService.delBindGroupList(session, flow, aid, unionPriId, sysType, delGroupIds);
    }

    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_PD_BY_GROUP)
    public int getRlPdByRlGroupId(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid int aid,
                                  @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId,
                                  @ArgBodyInteger(value = ProductBindGroupDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgList(keyMatch = ProductBindGroupDto.Key.RL_GROUP_IDS) FaiList<Integer> rlGroupIds) throws IOException {
        return groupBindService.getRlPdByRlGroupId(session, flow, aid, unionPriId, sysType, rlGroupIds);
    }

    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_DATA_STATUS)
    public int getBindGroupDataStatus(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return groupBindService.getBindGroupDataStatus(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_ALL_DATA)
    public int getAllBindGroup(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid final int aid,
                              @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return groupBindService.getAllBindGroup(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindGroupCmd.SEARCH_FROM_DB)
    public int searchBindGroupFromDb(final FaiSession session,
                                    @ArgFlow final int flow,
                                    @ArgAid final int aid,
                                    @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId,
                                    @ArgSearchArg(ProductBindGroupDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return groupBindService.searchBindGroupFromDb(session, flow, aid, unionPriId, searchArg);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.BACKUP)
    public int backupData(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid int aid,
                          @ArgList(keyMatch = ProductRelDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
                          @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
                                  keyMatch = ProductRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
        return service.backupData(session, flow, aid, unionPriIds, backupInfo);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.RESTORE)
    public int restoreBackupData(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid int aid,
                          @ArgList(keyMatch = ProductRelDto.Key.UNION_PRI_ID) FaiList<Integer> unionPriIds,
                          @ArgBodyInteger(ProductRelDto.Key.RESTORE_ID) int restoreId,
                          @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
                                  keyMatch = ProductRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
        return service.restoreBackupData(session, flow, aid, unionPriIds, restoreId, backupInfo);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.DEL_BACKUP)
    public int delBackupData(final FaiSession session,
                             @ArgFlow final int flow,
                             @ArgAid int aid,
                             @ArgParam(classDef = MgBackupDto.class, methodDef = "getInfoDto",
                                     keyMatch = ProductRelDto.Key.BACKUP_INFO) Param backupInfo) throws IOException {
        return service.delBackupData(session, flow, aid, backupInfo);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.CLONE)
    public int cloneData(final FaiSession session,
                         @ArgFlow final int flow,
                         @ArgAid int aid,
                         @ArgBodyInteger(ProductRelDto.Key.TID) int toTid,
                         @ArgBodyInteger(ProductRelDto.Key.SITE_ID) int toSiteId,
                         @ArgBodyInteger(ProductRelDto.Key.FROM_AID) int fromAid,
                         @ArgList(classDef = CloneDef.Dto.class, methodDef = "getInternalDto",
                                 keyMatch = ProductRelDto.Key.CLONE_UNION_PRI_IDS) FaiList<Param> cloneUnionPriIds) throws IOException {
        return service.cloneData(session, flow, aid, toTid, toSiteId, fromAid, cloneUnionPriIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.INCR_CLONE)
    public int incrementalClone(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid int aid,
                                @ArgBodyInteger(ProductRelDto.Key.TID) int toTid,
                                @ArgBodyInteger(ProductRelDto.Key.SITE_ID) int toSiteId,
                                @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId,
                                @ArgBodyInteger(ProductRelDto.Key.FROM_AID) int fromAid,
                                @ArgBodyInteger(ProductRelDto.Key.FROM_UNION_PRI_ID) int fromUnionPriId) throws IOException {
        return service.incrementalClone(session, flow, aid, toTid, toSiteId, unionPriId, fromAid, fromUnionPriId);
    }

    @Cmd(NKDef.Protocol.Cmd.CLEAR_CACHE)
    public int clearCache(final FaiSession session,
                          @ArgFlow final int flow,
                          @ArgAid final int aid) throws IOException {
        return service.clearCache(session, flow, aid);
    }

    /**==========================================操作商品与标签关联开始===========================================================*/
    @Cmd(MgProductBasicCmd.BindTagCmd.GET_LIST)
    public int getPdBindTag(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid int aid,
                               @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId,
                               @ArgBodyInteger(value = ProductBindTagDto.Key.SYS_TYPE, useDefault = true) int sysType,
                               @ArgList(keyMatch = ProductBindTagDto.Key.RL_PD_IDS) FaiList<Integer> rlPdIds) throws IOException {
        return tagBindService.getPdBindTag(session, flow, aid, unionPriId, sysType, rlPdIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindTagCmd.BATCH_SET)
    public int setPdBindTag(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductBindTagDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgBodyInteger(ProductBindTagDto.Key.RL_PD_ID) int rlPdId,
                              @ArgList(keyMatch = ProductBindTagDto.Key.RL_TAG_IDS) FaiList<Integer> addRlTagIds,
                              @ArgList(keyMatch = ProductBindTagDto.Key.DEL_RL_TAG_IDS) FaiList<Integer> delRlTagIds) throws IOException {
        return tagBindService.setPdBindTag(session, flow, aid, unionPriId, sysType, rlPdId, addRlTagIds, delRlTagIds);
    }

    @WrittenCmd
    @Cmd(MgProductBasicCmd.BindTagCmd.DEL)
    public int delPdBindTag(final FaiSession session,
                              @ArgFlow final int flow,
                              @ArgAid int aid,
                              @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId,
                              @ArgBodyInteger(value = ProductBindTagDto.Key.SYS_TYPE, useDefault = true) int sysType,
                              @ArgList(keyMatch = ProductBindTagDto.Key.RL_PD_IDS) FaiList<Integer> delRlPdIds) throws IOException {
        return tagBindService.delBindTagList(session, flow, aid, unionPriId, sysType, delRlPdIds);
    }

    @Cmd(MgProductBasicCmd.BindTagCmd.GET_PD_BY_TAG)
    public int getRlPdIdsByRlTagIds(final FaiSession session,
                                  @ArgFlow final int flow,
                                  @ArgAid int aid,
                                  @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId,
                                  @ArgBodyInteger(value = ProductBindTagDto.Key.SYS_TYPE, useDefault = true) int sysType,
                                  @ArgList(keyMatch = ProductBindTagDto.Key.RL_TAG_IDS) FaiList<Integer> rlTagIds) throws IOException {
        return tagBindService.getRlPdIdsByRlTagIds(session, flow, aid, unionPriId, sysType, rlTagIds);
    }

    @Cmd(MgProductBasicCmd.BindTagCmd.GET_DATA_STATUS)
    public int getBindTagDataStatus(final FaiSession session,
                                      @ArgFlow final int flow,
                                      @ArgAid final int aid,
                                      @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return tagBindService.getBindTagDataStatus(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindTagCmd.GET_ALL_DATA)
    public int getAllBindTag(final FaiSession session,
                               @ArgFlow final int flow,
                               @ArgAid final int aid,
                               @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId) throws IOException {
        return tagBindService.getAllPdBindTag(session, flow, aid, unionPriId);
    }

    @Cmd(MgProductBasicCmd.BindTagCmd.SEARCH_FROM_DB)
    public int getBindTagFromDb(final FaiSession session,
                                     @ArgFlow final int flow,
                                     @ArgAid final int aid,
                                     @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId,
                                     @ArgSearchArg(ProductBindTagDto.Key.SEARCH_ARG)SearchArg searchArg) throws IOException {
        return tagBindService.getBindTagFromDb(session, flow, aid, unionPriId, searchArg);
    }
    /**==========================================操作商品与标签关联结束===========================================================*/

    @WrittenCmd
    @Cmd(MgProductBasicCmd.Cmd.MIGRATE)
    public int dataMigrate(final FaiSession session,
                                @ArgFlow final int flow,
                                @ArgAid final int aid,
                                @ArgBodyInteger(ProductDto.Key.TID) int tid,
                                @ArgList(keyMatch = ProductDto.Key.INFO_LIST, methodDef = "getInfoDto",
                                classDef = MigrateDef.Dto.class) FaiList<Param> list,
                                @ArgBodyInteger(value = ProductDto.Key.SYS_TYPE, useDefault = true) int sysType) throws IOException {
        return dataMigrateService.dataMigrate(session, flow, aid, tid, list, sysType);
    }

    @Cmd(MgProductBasicCmd.Cmd.MIGRATE_GET)
    public int getMigratePdIds(final FaiSession session,
                           @ArgFlow final int flow,
                           @ArgAid final int aid,
                           @ArgBodyInteger(value = ProductDto.Key.SYS_TYPE, useDefault = true) int sysType) throws IOException {
        return dataMigrateService.getMigratePdIds(session, flow, aid, sysType);
    }

    private ProductBasicService service = ServiceProxy.create(new ProductBasicService());
    private ProductBindGroupService groupBindService = ServiceProxy.create(new ProductBindGroupService());
    private ProductBindPropService propBindService = ServiceProxy.create(new ProductBindPropService());
    private ProductBindTagService tagBindService = ServiceProxy.create(new ProductBindTagService());

    private DataMigrateService dataMigrateService = ServiceProxy.create(new DataMigrateService());

    private final static String CLI_NAME = "MgProductBasicCli";
}
