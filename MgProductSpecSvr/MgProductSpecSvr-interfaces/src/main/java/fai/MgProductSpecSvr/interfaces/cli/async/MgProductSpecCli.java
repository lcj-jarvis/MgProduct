package fai.MgProductSpecSvr.interfaces.cli.async;

import fai.MgProductSpecSvr.interfaces.cmd.MgProductSpecCmd;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuCodeDao;
import fai.comm.jnetkit.server.fai.annotation.Cmd;
import fai.comm.jnetkit.server.fai.annotation.args.ArgAid;
import fai.comm.jnetkit.server.fai.annotation.args.ArgBodyInteger;
import fai.comm.jnetkit.server.fai.annotation.args.ArgFlow;
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
 * @date 2021-09-23 10:13
 *
 * 异步cli只能接口的形式存在，后面实体类Cli里的方法都要改成注解和抽象方法的形式迁到这里来
 */
public interface MgProductSpecCli extends BaseRpcClient {

    /**
     * 获取 skuCode数据状态
     * @param aid 企业id
     * @param unionPriId 联合主键
     * @return 通过DefaultFuture回调获取结果
     */
    @Async
    @Cmd(MgProductSpecCmd.SkuCodeCmd.GET_DATA_STATUS)
    @RecvDecoder(value = @RecvDecoder.Decoder(key = ProductSpecSkuCodeDao.Key.DATA_STATUS, keyClass = Param.class,
        classDef = DataStatus.Dto.class, methodDef = "getDataStatusDto"))
    DefaultFuture getSkuCodeDataStatus(@ArgAid int aid, @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) int unionPriId);

    /**
     * 直接从db搜索 返回部分字段
     * @param aid 企业id
     * @param unionPirId 联合主键
     * @param searchArg 查询条件
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductSpecCmd.SkuCodeCmd.SEARCH_FROM_DB)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductSpecSkuCodeDao.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductSpecSkuCodeDao.class, methodDef = "getInfoDto"),
        @RecvDecoder.Decoder(key = ProductSpecSkuCodeDao.Key.TOTAL_SIZE, keyClass = Integer.class, required = false)})
    DefaultFuture searchSkuCodeFromDb(@ArgAid final int aid, @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) final int unionPirId, @ArgSearchArg(ProductSpecSkuCodeDao.Key.SEARCH_ARG) SearchArg searchArg);

    /**
     * 获取aid + unionPirId下全部数据（部分字段）
     * @param aid 企业id
     * @param unionPirId 联合主键
     * @return 通过DefaultFuture回调获取保存结果的FaiList
     */
    @Async
    @Cmd(MgProductSpecCmd.SkuCodeCmd.GET_ALL_DATA)
    @RecvDecoder(value = {@RecvDecoder.Decoder(key = ProductSpecSkuCodeDao.Key.INFO_LIST, keyClass = FaiList.class, classDef = ProductSpecSkuCodeDao.class, methodDef = "getInfoDto")})
    DefaultFuture getSkuCodeAllData(@ArgAid final int aid, @ArgBodyInteger(ProductSpecSkuCodeDao.Key.UNION_PRI_ID) final int unionPirId);
}



