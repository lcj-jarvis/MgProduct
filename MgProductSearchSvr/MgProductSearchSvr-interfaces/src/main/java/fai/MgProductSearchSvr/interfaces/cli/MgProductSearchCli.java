package fai.MgProductSearchSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.cmd.MgProductSearchCmd;
import fai.comm.jnetkit.server.fai.RemoteStandResult;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.ArgAid;
import fai.comm.jnetkit.server.fai.annotation.args.ArgBodyInteger;
import fai.comm.jnetkit.server.fai.annotation.args.ArgBodyString;
import fai.comm.jnetkit.server.fai.annotation.args.ArgFlow;
import fai.comm.rpc.client.BaseRpcClient;
import fai.comm.rpc.client.annotation.RecvDecoder;
import fai.comm.util.Param;

/**
 * @author Lu
 * @date 2021-10-11 9:40
 */
public interface MgProductSearchCli extends BaseRpcClient {

    /**
     * 搜索服务接口
     * @param flow 流水号
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @param tid
     * @param productCount 商品数量
     * @param esSearchParamString es的搜索条件
     * @param dbSearchParamString db的搜索条件
     * @return 返回包含搜索结果的Param，Param的key参考MgProductSearchResult.Info
     */
    @Cmd(MgProductSearchCmd.SearchCmd.SEARCH_LIST)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = MgProductSearchDto.Key.RESULT_INFO, keyClass = Param.class,
        classDef = MgProductSearchDto.class, methodDef = "getProductSearchDto"))
    RemoteStandResult searchList(@ArgFlow final int flow,
                                 @ArgAid final int aid,
                                 @ArgBodyInteger(MgProductSearchDto.Key.UNION_PRI_ID) int unionPriId,
                                 @ArgBodyInteger(MgProductSearchDto.Key.TID) int tid,
                                 @ArgBodyInteger(MgProductSearchDto.Key.PRODUCT_COUNT) int productCount,
                                 @ArgBodyString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING) String esSearchParamString,
                                 @ArgBodyString(MgProductSearchDto.Key.DB_SEARCH_PARAM_STRING) String dbSearchParamString,
                                 @ArgBodyString(MgProductSearchDto.Key.PAGE_INFO_STRING) String pageInfoString);
}
