package fai.MgProductBasicSvr.application;

import fai.MgProductBasicSvr.application.service.ProductBasicService;
import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.jnetkit.server.fai.FaiHandler;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.ArgAid;
import fai.comm.jnetkit.server.fai.annotation.args.ArgBodyInteger;
import fai.comm.jnetkit.server.fai.annotation.args.ArgFlow;
import fai.comm.jnetkit.server.fai.annotation.args.ArgList;
import fai.comm.util.FaiList;
import fai.comm.util.Param;

import java.io.IOException;

public class MgProductBasicHandler extends FaiHandler {
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
        return service.getPdBindProp(session, flow, aid, unionPriId, tid, rlPdId);
    }

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
                            keyMatch = ProductBindPropDto.Key.DEL_PROP_BIND) FaiList<Param> delList) {
        return service.setPdBindProp(session, flow, aid, unionPriId, tid, rlPdId, addList, delList);
    }

    private ProductBasicService service = new ProductBasicService();
}
