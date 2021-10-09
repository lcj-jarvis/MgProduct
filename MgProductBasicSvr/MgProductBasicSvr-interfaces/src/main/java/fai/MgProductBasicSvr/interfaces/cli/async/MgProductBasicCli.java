package fai.MgProductBasicSvr.interfaces.cli.async;

import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.*;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.ArgAid;
import fai.comm.jnetkit.server.fai.annotation.args.ArgBodyInteger;
import fai.comm.jnetkit.server.fai.annotation.args.ArgSearchArg;
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
 * @date 2021-09-23 10:11
 *
 * 异步cli只能接口的形式存在，后面实体类Cli里的方法都要改成注解和抽象方法的形式迁到这里来
 */
public interface MgProductBasicCli extends BaseRpcClient {

    /**
     * 异步获取商品数据状态
     * @param aid 企业id
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.PD_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getPdDaStatus(@ArgAid int aid);

    /**
     * 获取商品关联数据状态
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.PD_REL_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductRelDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getPdRelDataStatus(@ArgAid int aid, @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId);


    /**
     * 获取商品参数关联数据状态
     * @param aid  企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductBasicCmd.BindPropCmd.GET_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductBindPropDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getBindPropDataStatus(@ArgAid int aid, @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId);


    /**
     * 获取商品分类关联数据状态
     * @param aid  企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductBindGroupDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getBindGroupDataStatus(@ArgAid int aid, @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId);

    /**
     * 获取商品标签关联数据状态
     * @param aid  企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductBasicCmd.BindTagCmd.GET_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductBindTagDto.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getBindTagDataStatus(@ArgAid int aid, @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId);

    /**
     * 从db查询商品数据
     * @param aid 企业id
     * @param searchArg 搜索条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchPdFromDb(@ArgAid final int aid, @ArgSearchArg(ProductDto.Key.SEARCH_ARG) SearchArg searchArg);

    /**
     * 从db获取aid下所有的商品数据
     * 只获取部分字段：ProductEntity.MANAGE_FIELDS
     * 目前提供给搜索服务使用
     */

    /**
     * 从db获取aid下所有的商品数据
     * 只获取部分字段：ProductEntity.MANAGE_FIELDS
     * 目前提供给搜索服务使用
     * @param aid 企业id
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductDto.class, methodDef = "getInfoDto")})
    DefaultFuture getAllPdData(@ArgAid final int aid);

    /**
     * 从db查询商品关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @param searchArg 查询条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_REL_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductRelDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductRelDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductRelDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchPdRelFromDb(@ArgAid final int aid, @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId, @ArgSearchArg(ProductRelDto.Key.SEARCH_ARG)SearchArg searchArg);


    /**
     * 获取aid + unionPriId 下所有商品关联数据
     * @param aid  企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD_REL)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductRelDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductRelDto.class, methodDef = "getInfoDto")})
    DefaultFuture getAllPdRelData(@ArgAid final int aid, @ArgBodyInteger(ProductRelDto.Key.UNION_PRI_ID) int unionPriId);

    /**
     * 从db查询商品参数关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @param searchArg 查询条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindPropCmd.SEARCH_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindPropDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindPropDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductBindPropDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchBindPropFromDb(@ArgAid final int aid, @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId, @ArgSearchArg(ProductBindPropDto.Key.SEARCH_ARG)SearchArg searchArg);

    /**
     * 获取aid + unionPriId 下所有商品参数关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindPropCmd.GET_ALL_DATA)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindPropDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindPropDto.class, methodDef = "getInfoDto")})
    DefaultFuture getAllBindPropData(@ArgAid final int aid, @ArgBodyInteger(ProductBindPropDto.Key.UNION_PRI_ID) int unionPriId);

    /**
     * 从db查询商品分类关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @param searchArg  查询条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindGroupCmd.SEARCH_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindGroupDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindGroupDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductBindGroupDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchBindGroupFromDb(@ArgAid final int aid, @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId, @ArgSearchArg(ProductBindGroupDto.Key.SEARCH_ARG)SearchArg searchArg);

    /**
     * 获取aid + unionPriId 下所有商品分类关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindGroupCmd.GET_ALL_DATA)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindGroupDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindGroupDto.class, methodDef = "getInfoDto")})
    DefaultFuture getAllBindGroupData(@ArgAid final int aid, @ArgBodyInteger(ProductBindGroupDto.Key.UNION_PRI_ID) int unionPriId);

    /**
     *  从db查询商品标签关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @param searchArg 搜索条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindTagCmd.SEARCH_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindTagDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindTagDto.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductBindTagDto.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchBindTagFromDb(@ArgAid final int aid, @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId, @ArgSearchArg(ProductBindTagDto.Key.SEARCH_ARG)SearchArg searchArg);

    /**
     * 获取aid + unionPriId 下所有商品标签关联数据
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductBasicCmd.BindTagCmd.GET_ALL_DATA)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductBindTagDto.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductBindTagDto.class, methodDef = "getInfoDto")})
    DefaultFuture getAllPdBindTagData(@ArgAid final int aid, @ArgBodyInteger(ProductBindTagDto.Key.UNION_PRI_ID) int unionPriId);
}
