package fai.MgProductStoreSvr.interfaces.cli.async;

import fai.MgProductStoreSvr.interfaces.cmd.MgProductStoreCmd;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.SpuSummaryDto;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.*;
import fai.comm.rpc.client.BaseRpcClient;
import fai.comm.rpc.client.DefaultFuture;
import fai.comm.rpc.client.annotation.Async;
import fai.comm.rpc.client.annotation.RecvDecoder;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.SearchArg;
import fai.mgproduct.comm.DataStatus;

/**
 * @author Lu
 * @date 2021-09-23 10:16
 *
 * 异步cli只能接口的形式存在，后面实体类Cli里的方法都要改成注解和抽象方法的形式迁到这里来
 */
public interface MgProductStoreCli extends BaseRpcClient {

    /**
     * 获取 spu 业务销售汇总的数据状态
     * @param flow 流水号
     * @param aid aid 企业id
     * @param tid
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture获取结果
     */
    @Async
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = SpuBizSummaryDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getSpuBizSummaryDataStatus(@ArgFlow final int flow, @ArgAid int aid, @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid, @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId);

    /**
     * 直接从db搜索 spu 业务销售汇总 ，返回部分字段
     * @param flow 流水号
     * @param aid 企业id
     * @param tid
     * @param unionPriId 联合主键
     * @param searchArg 查询条件
     * @return 通过DefaultFuture获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.SEARCH_PART_FIELD)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = SpuBizSummaryDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = SpuBizSummaryDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = SpuBizSummaryDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchSpuBizSummaryFromDb(@ArgFlow final int flow, @ArgAid final int aid, @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid, @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId, @ArgSearchArg(value = SpuBizSummaryDto.Key.SEARCH_ARG) SearchArg searchArg);

    /**
     * 获取 spu 业务销售汇总 的全部数据的部分字段
     * @param flow 流水号
     * @param aid 企业id
     * @param tid
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_ALL_DATA_PART_FIELD)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = SpuBizSummaryDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = SpuBizSummaryDto.class, methodDef = "getInfoDto")})
    DefaultFuture getSpuBizSummaryAllData(@ArgFlow final int flow, @ArgAid final int aid, @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid, @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId);

    /**
     * 根据 unionPriIdList 和 pdIdList 获取商品规格库存销售sku
     * @param flow flow 流水号
     * @param aid 企业id
     * @param tid
     * @param unionPriIdList 联合主键List
     * @param pdIdList 商品id的集合
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductStoreCmd.StoreSalesSkuCmd.BATCH_GET_BY_UID_AND_PD_ID)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = StoreSalesSkuDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = StoreSalesSkuDto.class, methodDef = "getInfoDto")})
    DefaultFuture batchGetSkuStoreSalesByUidAndPdId(@ArgFlow final int flow, @ArgAid final int aid, @ArgBodyInteger(StoreSalesSkuDto.Key.TID) final int tid, @ArgList(keyMatch = StoreSalesSkuDto.Key.UID_LIST) FaiList<Integer> unionPriIdList, @ArgList(keyMatch = StoreSalesSkuDto.Key.ID_LIST) FaiList<Integer> pdIdList);

    /**
     * 根据 pdIdList 获取指定业务下 spu业务库存销售汇总信息
     * @param flow
     * @param aid
     * @param tid
     * @param unionPriId
     * @param pdIdList
     * @param useSourceFieldList
     * @return
     */
    @Async
    @Cmd(MgProductStoreCmd.SpuBizSummaryCmd.GET_LIST)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = SpuBizSummaryDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = SpuBizSummaryDto.class, methodDef = "getInfoDto")})
    DefaultFuture getSpuBizSummaryInfoList(@ArgFlow final int flow,
                                                     @ArgAid final int aid,
                                                     @ArgBodyInteger(SpuBizSummaryDto.Key.TID) final int tid,
                                                     @ArgBodyInteger(SpuBizSummaryDto.Key.UNION_PRI_ID) final int unionPriId,
                                                     @ArgList(keyMatch = SpuSummaryDto.Key.ID_LIST) FaiList<Integer> pdIdList,
                                                     @ArgList(keyMatch = StoreSalesSkuDto.Key.STR_LIST, useDefault = true) FaiList<String> useSourceFieldList);
}
