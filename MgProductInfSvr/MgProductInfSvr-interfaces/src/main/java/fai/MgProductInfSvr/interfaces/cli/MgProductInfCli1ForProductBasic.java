package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.*;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicValObj;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchArg;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductInfCli1ForProductBasic extends MgProductParentInfCli {
    public MgProductInfCli1ForProductBasic(int flow) {
        super(flow);
    }

    @Deprecated
    public int getProductFullInfo(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, Param combinedInfo){
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .build();
        return getProductFullInfo(mgProductArg, combinedInfo);
    }

    @Deprecated
    public int getProductList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdIds(rlPdIds)
                .build();
        return getProductList(mgProductArg, list);
    }

    @Deprecated
    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo){
        return importProduct(aid, tid, siteId, lgId, keepPriId1, productList, inStoreRecordInfo, null, null);
    }

    @Deprecated
    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, FaiList<Param> errProductList){
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setImportProductList(productList)
                .setInOutStoreRecordInfo(inStoreRecordInfo)
                .build();
        return importProduct(mgProductArg, errProductList, null);
    }

    @Deprecated
    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, FaiList<Param> errProductList, Ref<FaiList<Integer>> rlPdIdsRef){
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setImportProductList(productList)
                .setInOutStoreRecordInfo(inStoreRecordInfo)
                .build();
        return importProduct(mgProductArg, errProductList, rlPdIdsRef);
    }

    @Deprecated
    public int addProductAndRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param info, Ref<Integer> rlPdIdRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setAddInfo(info)
                .build();
        return addProductAndRel(mgProductArg, rlPdIdRef);
    }

    @Deprecated
    public int bindProductRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setBindRlPdInfo(bindRlPdInfo)
                .setAddInfo(info)
                .build();
        return bindProductRel(mgProductArg, rlPdIdRef);
    }

    @Deprecated
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, 0, 0, 0)
                .setBindRlPdInfo(bindRlPdInfo)
                .setAddList(infoList)
                .build();
        return batchBindProductRel(mgProductArg, rlPdIdsRef);
    }

    @Deprecated
    public int setSinglePd(int aid, int tid, int siteId, int lgId, int keepPriId1, Integer rlPdId, ParamUpdater updater) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .setUpdater(updater)
                .build();
        return setSinglePd(mgProductArg);
    }

    @Deprecated
    public int setProducts(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, ParamUpdater updater) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdIds(rlPdIds)
                .setUpdater(updater)
                .build();
        return setProducts(mgProductArg);
    }

    @Deprecated
    public int batchDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelPdRelBind(aid, tid, siteId, lgId, keepPriId1, rlPdIds, false);
    }

    @Deprecated
    public int batchSoftDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelPdRelBind(aid, tid, siteId, lgId, keepPriId1, rlPdIds, true);
    }

    @Deprecated
    public int batchDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, boolean softDel) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdIds(rlPdIds)
                .setSoftDel(softDel)
                .build();
        return batchDelPdRelBind(mgProductArg);
    }

    @Deprecated
    public int batchDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelProduct(aid, tid, siteId, lgId, keepPriId1, rlPdIds, false);
    }

    @Deprecated
    public int batchSoftDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelProduct(aid, tid, siteId, lgId, keepPriId1, rlPdIds, true);
    }

    @Deprecated
    public int batchDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, boolean softDel) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdIds(rlPdIds)
                .setSoftDel(softDel)
                .build();
        return batchDelProduct(mgProductArg);
    }

    /**==============================================   商品信息接口开始   ==============================================*/
    /**
     * 商品中台搜索，根据 mgProductSearch（fai.MgProductInfSvr.interfaces.utils.MgProductSearch）, 在 商品中台内 搜索商品
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setMgProductSearchArg(mgProductSearchArg)  //搜索条件。不设置就默认搜索MgProductRel的数据
     *                 .build();
     * @param searchResultDef 接收搜索结果，对应到的Param参考 {@link fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult.Info} 实体
     * @return {@link Errno}
     */
    public int mgProductSearch(MgProductArg mgProductArg, Ref<Param> searchResultDef){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            MgProductSearchArg mgProductSearchArg = mgProductArg.getMgProductSearchArg();
            // 搜索条件为空
            if (mgProductSearchArg == null) {
                mgProductSearchArg = new MgProductSearchArg();
                // 搜索条件为空。默认搜索MgProductRel的数据.mgProductSearchArg.isEmpty()
                Log.logStd("mgProductSearchArg == null;flow=%d", m_flow);
            }
            if(searchResultDef == null){
                searchResultDef = new Ref<Param>();
            }

            // 获取es的查询条件
            Param esSearchParam = mgProductSearchArg.getEsSearchParam();
            // 获取db的查询条件
            Param dbSearchParam = mgProductSearchArg.getDbSearchParam();
            // 分页相关
            Param pageInfo = mgProductSearchArg.getPageParam();

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductSearchDto.Key.TID, tid),
                new Pair(MgProductSearchDto.Key.SITE_ID, siteId),
                new Pair(MgProductSearchDto.Key.LGID, lgId),
                new Pair(MgProductSearchDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING, esSearchParam.toJson());
            sendBody.putString(MgProductSearchDto.Key.DB_SEARCH_PARAM_STRING, dbSearchParam.toJson());
            sendBody.putString(MgProductSearchDto.Key.PAGE_INFO_STRING, pageInfo.toJson());
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.MgProductSearchCmd.SEARCH_LIST, sendBody, true);
            if (m_rt != Errno.OK && m_rt != Errno.NOT_FOUND) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            Param searchResult = new Param();
            searchResult.fromBuffer(recvBody, keyRef, MgProductSearchDto.getProductSearchDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductSearchDto.Key.RESULT_INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            searchResultDef.value = searchResult;

            // 设置搜索结果总数
            mgProductSearchArg.setTotal(searchResult.getInt(MgProductSearchResult.Info.TOTAL, 0));

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取商品及其组合的信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *          .setMgProductSearchArg(mgProductSearchArg)  // 搜索条件（选填）。不设置就默认搜索MgProductRel表的数据
     *          .setCombined(combined) // 设置需要获取商品哪些相关信息（选填）。不设置的话默认获取商品的(基础)信息
     *          .build();
     *
     *        combined: 设置需要获取商品哪些相关信息。Param的key参考{@link MgProductEntity.Info}
     *        默认获取商品的(基础)信息，可以根据需要进行以下获取进行设置获取更多的信息。
     *        combined.setBoolean(MgProductEntity.Info.SPEC, true); 获取商品规格
     *        combined.setBoolean(MgProductEntity.Info.SPEC_SKU, true); 获取商品规格sku
     *        combined.setBoolean(MgProductEntity.Info.STORE_SALES, true);  获取库存销售数据
     *        combined.setBoolean(MgProductEntity.Info.SPU_SALES, true); 获取库存销售数据(spu汇总)
     * @param resultList 接收搜索的结果集（经过分页后的）。resultList里的Param保存的内容结构如下
     *            Param key                               Param value
     *       MgProductEntity.Info.BASIC               商品（基础）信息的Param  详见基础服务（商品Entity）
     *       MgProductEntity.Info.SPEC                商品规格信息的FaiList<Param>  详见规格服务（商品规格Entity）
     *       MgProductEntity.Info.SPEC_SKU            商品规格sku信息的FaiList<Param> 详见规格服务（商品规格SKU Entity）
     *       MgProductEntity.Info.STORE_SALES         销售库存相关的FaiList<Param> 详见库存服务（商品规格库存销售 SKU Entity）
     *       MgProductEntity.Info.SPU_SALES           spu销售库存相关的Param 详见库存服务（spu业务库存销售汇总 Entity）
     * @return {@link Errno}
     */
    public int searchProduct(MgProductArg mgProductArg, FaiList<Param> resultList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            MgProductSearchArg mgProductSearchArg = mgProductArg.getMgProductSearchArg();

            // 搜索条件为空
            if (mgProductSearchArg == null) {
                mgProductSearchArg = new MgProductSearchArg();
                // 搜索条件为空。默认搜索MgProductRel的数据.mgProductSearchArg.isEmpty()
                Log.logStd("mgProductSearchArg == null;flow=%d", m_flow);
            }
            Param combined = mgProductArg.getCombined();
            if (combined == null) {
                combined = new Param();
            }
            if (resultList == null) {
                resultList = new FaiList<Param>();
            }
            resultList.clear();
            // 获取es的查询条件
            Param esSearchParam = mgProductSearchArg.getEsSearchParam();
            // 获取db的查询条件
            Param dbSearchParam = mgProductSearchArg.getDbSearchParam();
            // 分页相关
            Param pageInfo = mgProductSearchArg.getPageParam();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductSearchDto.Key.TID, tid),
                new Pair(MgProductSearchDto.Key.SITE_ID, siteId),
                new Pair(MgProductSearchDto.Key.LGID, lgId),
                new Pair(MgProductSearchDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING, esSearchParam.toJson());
            sendBody.putString(MgProductSearchDto.Key.DB_SEARCH_PARAM_STRING, dbSearchParam.toJson());
            sendBody.putString(MgProductSearchDto.Key.PAGE_INFO_STRING, pageInfo.toJson());
            combined.toBuffer(sendBody, MgProductDto.Key.COMBINED, MgProductDto.getCombinedInfoDto());
            int aid = mgProductArg.getAid();
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.MgProductSearchCmd.SEARCH_PD, sendBody, true);

            if (m_rt != Errno.OK && m_rt != Errno.NOT_FOUND) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = resultList.fromBuffer(recvBody, keyRef, MgProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            // recv total
            Ref<Integer> totalRef = new Ref<Integer>();
            m_rt = recvBody.getInt(keyRef, totalRef);
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.TOTAL) {
                Log.logErr(m_rt, "recv total codec err");
                return m_rt;
            }
            // 设置搜索结果总条数
            mgProductSearchArg.setTotal(totalRef.value);

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 获取的商品全部组合信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setSysType(sysType) // 选填 系统类型，默认为0
     *                 .setRlPdId(rlPdId) // 必填 商品业务id
     *                 .build();
     * @param combinedInfo 返回商品中台各个服务组合的数据 {@link MgProductEntity.Info}
     * @return {@link Errno}
     */
    public int getProductFullInfo(MgProductArg mgProductArg, Param combinedInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (combinedInfo == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "combinedInfo error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(MgProductDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(MgProductDto.Key.ID,  mgProductArg.getRlPdId());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.GET_FULL_INFO, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = combinedInfo.fromBuffer(recvBody, keyRef, MgProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取多个商品在多个UnionPriId下的数据 详细文档 https://train.faisco.biz/fmn2
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setPrimaryList(primaryKeys) // 必填 查询的主键维度
     *                 .setRlPdIds(rlPdIds) // 必填 商品业务id
     *                 .setSysType(sysType) // 必填 系统类型，默认为0
     *                 .setCombined(combined) // 选填 需要查询的内容
     *                 .build();
     * @param list 返回商品中台数据
     * @return {@link Errno}
     */
    public int getProductListByUidsAndRlPdIdsFromDb(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if (primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;primaryKeys is null or empty;");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty;");
                return m_rt;
            }
            Param combined = mgProductArg.getCombined();
            if (combined == null) {
                combined = new Param();
                combined.setBoolean(MgProductEntity.Info.SPEC, true);
                combined.setBoolean(MgProductEntity.Info.SPEC_SKU, true);
                combined.setBoolean(MgProductEntity.Info.STORE_SALES, true);
                combined.setBoolean(MgProductEntity.Info.SPU_SALES, true);
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIds.toBuffer(sendBody, MgProductDto.Key.RL_PD_IDS);
            sendBody.putInt(MgProductDto.Key.SYS_TYPE, mgProductArg.getSysType());
            primaryKeys.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            combined.toBuffer(sendBody, MgProductDto.Key.COMBINED, MgProductDto.getCombinedInfoDto());

            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.GET_PRI_INFO, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<>();
            m_rt = list.fromBuffer(recvBody, keyRef, MgProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            stat.end(m_rt != Errno.OK && m_rt != Errno.NOT_FOUND, m_rt);
        }
    }

    /**
     * 根据rlPdIds获取商品数据（商品表+商品业表）
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds) // 必填 商品业务id集合
     *                 .setSysType(sysType)
     *                 .build();
     * sysType: 类型 商品/服务/... {@link ProductBasicValObj.ProductValObj.SysType} 选填，默认为0
     * @param list 接收结果的集合
     * @return {@link Errno}
     */
    public int getProductList(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PD_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBasicDto.getProductDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.PD_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 根据rlPdIds获取商品数据（商品表+商品业表）
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds) // 必填 商品业务id集合
     *                 .setSysType(sysType)
     *                 .build();
     * sysType: 类型 商品/服务/... {@link ProductBasicValObj.ProductValObj.SysType} 选填，默认为0
     * @param list 接收结果的集合
     * @return {@link Errno}
     */
    public int getPdReducedList4Adm(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Integer sysType = mgProductArg.getSysTypeWithoutDefault();
            FaiList<String> names = mgProductArg.getNames();
            if(names == null || names.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr("arg error;names is empty");
                return m_rt;
            }
            FaiList<Integer> status = mgProductArg.getStatus();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            if(sysType != null) {
                sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, sysType);
            }
            names.toBuffer(sendBody, ProductBasicDto.Key.NAME);
            if(status != null) {
                status.toBuffer(sendBody, ProductBasicDto.Key.STATUS);
            }
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PD_REDUCE_BY_NAME, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBasicDto.getProductDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.PD_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 根据rlPdIds获取商品在哪些业务下绑定了
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds) // 必填 商品业务id集合
     *                 .setSysType(sysType) // 选填 系统类型，默认为0
     *                 .build();
     * @param list 接收结果的集合
     * @return {@link Errno}
     */
    public int getPdBindBizs(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PD_BIND_BIZS, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBasicDto.getBindBizDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.PD_REL_INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 导入商品数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setImportProductList(productList) 商品中台各个服务组合的数据 {@link MgProductEntity.Info}
     *                 .setInOutStoreRecordInfo(inStoreRecordInfo) {@link ProductStoreEntity.InOutStoreRecordInfo}  非必要
     *                 .setUseMgProductBasicInfo(true) 是否使用商品中台基础信息 不设置默认为 false
     *                 .build();
     * @param errProductList 返回导入出错的数据，并且每个Param有对应的错误码 {@link MgProductEntity.Info}
     * @param rlPdIdsRef 返回导入的商品id，导入失败的id会是 -1
     * @return {@link Errno}
     */
    public int importProduct(MgProductArg mgProductArg, FaiList<Param> errProductList, Ref<FaiList<Integer>> rlPdIdsRef){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> productList = mgProductArg.getImportProductList();
            if(productList == null || productList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "productList error");
                return m_rt;
            }
            Param inStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if(inStoreRecordInfo == null){
                inStoreRecordInfo = new Param();
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(MgProductDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putString(MgProductDto.Key.XID, mgProductArg.getXid());
            m_rt = productList.toBuffer(sendBody, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
            if(m_rt != Errno.OK){
                Log.logErr(m_rt, "productList.toBuffer error;productList=%s;", productList);
                return m_rt;
            }
            m_rt = inStoreRecordInfo.toBuffer(sendBody, MgProductDto.Key.IN_OUT_STORE_RECORD_INFO, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                Log.logErr(m_rt, "inStoreRecordInfo.toBuffer error;inStoreRecordInfo=%s;", inStoreRecordInfo);
                return m_rt;
            }
            boolean useMgProductBasicInfo = mgProductArg.getUseMgProductBasicInfo();
            sendBody.putBoolean(MgProductDto.Key.USE_BASIC, useMgProductBasicInfo);
            // send
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.Cmd.IMPORT_PRODUCT);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }
            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            // 特殊判断逻辑
            int realRt = recvProtocol.getResult();
            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            if(errProductList != null){
                m_rt = errProductList.fromBuffer(recvBody, keyRef, MgProductDto.getInfoDto());
                if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO_LIST) {
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            if (rlPdIdsRef != null) {
                FaiList<Integer> rlPdIds = new FaiList<Integer>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;
            }
            // 特殊判断逻辑
            m_rt = realRt;
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 新增商品数据，并添加业务关联
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setAddInfo(info)  // 必填 添加商品信息
     *                 .build();
     * @param rlPdIdRef 接收返回的商品业务id
     * @return {@link Errno}
     */
    public int addProductAndRel(MgProductArg mgProductArg, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param info = mgProductArg.getAddInfo();
            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            info.toBuffer(sendBody, ProductBasicDto.Key.PD_INFO, ProductBasicDto.getProductDto());
            // send and recv
            boolean rlPdIdRefNotNull = (rlPdIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.ADD_PD_AND_REL, sendBody, false, rlPdIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPdIdRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_ID) {
                    Log.logErr(m_rt, "recv rlPdIdRef codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 新增商品数据、同时添加规格、库存数据以及参数和分类绑定
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *              .setAddInfo(addInfo)                          // 必填
     *              .setInOutStoreRecordInfo(inOutStoreRecordInfo)  // 选填
     *              .build();
     * addInfo 添加信息 {@link MgProductDto#getInfoDto()}
     *        只需要其中的 MgProductEntity.Info.BASIC、MgProductEntity.Info.SPEC、MgProductEntity.Info.SPEC_SKU、MgProductEntity.Info.STORE_SALES
     * inOutStoreRecordInfo 出入库信息 {@link ProductStoreEntity.InOutStoreRecordInfo}
     * @param rlPdIdRef 接收添加商品后的 rlPdId 不需要的时候请传null
     * @return {@link Errno}
     */
    public int addProductInfo(MgProductArg mgProductArg, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param addInfo = mgProductArg.getAddInfo();
            if (addInfo == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addInfo is empty");
                return m_rt;
            }
            Param inOutStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if (inOutStoreRecordInfo == null) {
                inOutStoreRecordInfo = new Param();
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            String xid = mgProductArg.getXid();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            m_rt = addInfo.toBuffer(sendBody, ProductBasicDto.Key.UNION_INFO, MgProductDto.getInfoDto());
            if(m_rt != Errno.OK){
                Log.logErr(m_rt, "addInfo.toBuffer error;addInfo=%s;", addInfo);
                return m_rt;
            }
            m_rt = inOutStoreRecordInfo.toBuffer(sendBody, MgProductDto.Key.IN_OUT_STORE_RECORD_INFO, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                Log.logErr(m_rt, "inStoreRecordInfo.toBuffer error;inStoreRecordInfo=%s;", inOutStoreRecordInfo);
                return m_rt;
            }
            // send and recv
            boolean rlPdIdRefNotNull = (rlPdIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.ADD_PD_INFO, sendBody, false, rlPdIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPdIdRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_ID) {
                    Log.logErr(m_rt, "recv rlPdIdRef codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 新增商品业务关联
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setBindRlPdInfo(bindRlPdInfo) // 必填 绑定源信息 包含 绑定方 uid 和 绑定方 rlPdId
     *                 .setAddInfo(info)            // 必填 业务信息
     *                 .build();
     * @param rlPdIdRef 接收返回商品业务id
     * @return {@link Errno}
     */
    public int bindProductRel(MgProductArg mgProductArg, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param addInfo = mgProductArg.getAddInfo();
            if (Str.isEmpty(addInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }
            Param bindRlPdInfo = mgProductArg.getBindRlPdInfo();
            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }
            Param inStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if(inStoreRecordInfo == null) {
                inStoreRecordInfo = new Param();
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            String xid = mgProductArg.getXid();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(MgProductDto.Key.XID, xid);
            addInfo.toBuffer(sendBody, MgProductDto.Key.INFO, MgProductDto.getInfoDto());
            bindRlPdInfo.toBuffer(sendBody, MgProductDto.Key.BIND_PD_INFO, ProductBasicDto.getProductRelDto());
            inStoreRecordInfo.toBuffer(sendBody, ProductBasicDto.Key.IN_OUT_RECOED, ProductStoreDto.InOutStoreRecord.getInfoDto());
            // send and recv
            boolean rlPdIdRefNotNull = (rlPdIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.ADD_PD_BIND, sendBody, false, rlPdIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPdIdRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_ID) {
                    Log.logErr(m_rt, "recv sid codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量新增商品业务关联
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, 0, 0, 0)
     *                 .setBindRlPdInfo(bindRlPdInfo) // 必填 绑定源信息 包含 绑定方 uid 和 绑定方 rlPdId
     *                 .setAddList(infoList)    // 必填 业务信息列表
     *                 .build();
     * @param rlPdIdsRef 接收返回的商品业务id集合
     * @return {@link Errno}
     */
    public int batchBindProductRel(MgProductArg mgProductArg, Ref<FaiList<Integer>> rlPdIdsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> infoList = mgProductArg.getAddList();
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;infoList is empty");
                return m_rt;
            }
            Param bindRlPdInfo = mgProductArg.getBindRlPdInfo();
            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }
            Param inStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if(inStoreRecordInfo == null) {
                inStoreRecordInfo = new Param();
            }

            String xid = mgProductArg.getXid();

            // packaging send data
            FaiBuffer sendBody = new FaiBuffer(true);
            int tid = mgProductArg.getTid();
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            bindRlPdInfo.toBuffer(sendBody, ProductBasicDto.Key.PD_BIND_INFO, ProductBasicDto.getProductRelDto());
            infoList.toBuffer(sendBody, ProductBasicDto.Key.PD_LIST, MgProductDto.getInfoDto());
            inStoreRecordInfo.toBuffer(sendBody, ProductBasicDto.Key.IN_OUT_RECOED, ProductStoreDto.InOutStoreRecord.getInfoDto());

            // send and recv
            boolean rlPdIdRefsNotNull = (rlPdIdsRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_ADD_PD_BIND, sendBody, false, rlPdIdRefsNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPdIdRefsNotNull) {
                FaiList<Integer> rlPdIds = new FaiList<Integer>();
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int batchBindProductsRel(MgProductArg mgProductArg, Ref<FaiList<Integer>> rlPdIdsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> infoList = mgProductArg.getAddList();
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;infoList is empty");
                return m_rt;
            }
            Param fromPrimaryKey = mgProductArg.getFromPrimaryKey();
            if (Str.isEmpty(fromPrimaryKey)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;fromPrimaryKey is empty");
                return m_rt;
            }
            Param inStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if(inStoreRecordInfo == null) {
                inStoreRecordInfo = new Param();
            }
            String xid = mgProductArg.getXid();

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);

            // packaging send data
            FaiBuffer sendBody = new FaiBuffer(true);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            primaryKey.toBuffer(sendBody, ProductBasicDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            fromPrimaryKey.toBuffer(sendBody, ProductBasicDto.Key.FROM_PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            infoList.toBuffer(sendBody, ProductBasicDto.Key.PD_LIST, MgProductDto.getInfoDto());
            inStoreRecordInfo.toBuffer(sendBody, ProductBasicDto.Key.IN_OUT_RECOED, ProductStoreDto.InOutStoreRecord.getInfoDto());

            // send and recv
            boolean rlPdIdRefsNotNull = (rlPdIdsRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_BIND_PDS_REL, sendBody, false, rlPdIdRefsNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPdIdRefsNotNull) {
                FaiList<Integer> rlPdIds = new FaiList<Integer>();
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int setPdSort(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int sysType = mgProductArg.getSysType();

            int preRlPdId = mgProductArg.getPreRlPdId();

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            int rlPdId = mgProductArg.getRlPdId();
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            sendBody.putInt(ProductBasicDto.Key.PRE_RL_PD_ID, preRlPdId);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PD_SORT, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 根据商品 id 和 updater，修改 商品 基础信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setSysType(sysType)    // 选填，默认为0
     *                 .setRlPdId(rlPdId)    // 必填
     *                 .setUpdater(updater)  // 必填
     *                 .build();
     * @return {@link Errno}
     */
    public int setSinglePd(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            ParamUpdater updater = mgProductArg.getUpdater();
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int sysType = mgProductArg.getSysType();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            int rlPdId = mgProductArg.getRlPdId();
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            updater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, ProductBasicDto.getProductDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_SINGLE_PD, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 修改商品信息 包括 规格和库存服务
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setCombinedUpdater(combinedUpdater)   // 必填
     *                 .setRlPdId(rlPdId)             // 必填
     *                 .setXid(xid)                   // 选填
     *                 .build();
     * updater说明： updater详见 {@link MgProductDto#getInfoDto}
     *          只需要其中的 MgProductEntity.Info.BASIC、MgProductEntity.Info.SPEC_SKU、MgProductEntity.Info.STORE_SALES
     *          其他描述详见接口文档
     * @return {@link Errno}
     */
    public int setProductInfo(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param combinedUpdater = mgProductArg.getCombinedUpdater();
            if (Str.isEmpty(combinedUpdater)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr("arg error;updater is empty");
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int rlPdId = mgProductArg.getRlPdId();
            int sysType = mgProductArg.getSysType();
            String xid = mgProductArg.getXid();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, sysType);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            combinedUpdater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, MgProductDto.getInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PD_INFO, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量修改商品，每个商品的改动都是一样的
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .setUpdater(updater)  // 必填
     *                 .build();
     * @return {@link Errno}
     */
    public int setProducts(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            ParamUpdater updater = mgProductArg.getUpdater();
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            updater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, ProductBasicDto.getProductDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PDS, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * for yk 批量修改多门店数据
     * 支持修改上下架状态、绑定分类、top 字段 （现阶段接入使用的字段）
     * @param mgProductArg
     * @return
     */
    public int batchSet4YK(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;primaryKeys is empty;aid=%d;", aid);
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty;aid=%d;", aid);
                return m_rt;
            }
            ParamUpdater updater = mgProductArg.getUpdater();
            if(updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty;aid=%d;", aid);
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);

            String xid = mgProductArg.getXid();

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            primaryKey.toBuffer(sendBody, ProductBasicDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            primaryKeys.toBuffer(sendBody, ProductBasicDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            updater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, ProductBasicDto.getProductDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_SET_4YK, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量修改多门店数据
     * 支持修改基础信息业务关系、sku库存销售、spu库存销售
     * @param mgProductArg
     * @return
     */
    public int batchSetBizBind(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;primaryKeys is empty;aid=%d;", aid);
                return m_rt;
            }
            int rlPdId = mgProductArg.getRlPdId();
            if(rlPdId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdId is empty;aid=%d;", aid);
                return m_rt;
            }
            Param combinedUpdater = mgProductArg.getCombinedUpdater();
            if(Str.isEmpty(combinedUpdater)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty;aid=%d;", aid);
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            String xid = mgProductArg.getXid();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductBasicDto.Key.XID, xid);
            }
            primaryKey.toBuffer(sendBody, ProductBasicDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            primaryKeys.toBuffer(sendBody, ProductBasicDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            combinedUpdater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, MgProductDto.getInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_SET_BIZ, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 指定业务下，取消 rlPdIds 的商品业务关联
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .setSoftDel(softDel)  // 选填 默认为false
     *                 .setSysType(sysType)   // 系统类型
     *                 .build();
     *         return batchDelPdRelBind(mgProductArg);
     * @return {@link Errno}
     */
    public int batchDelPdRelBind(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            boolean softDel = mgProductArg.getSoftDel();
            sendBody.putBoolean(ProductBasicDto.Key.SOFT_DEL, softDel);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_DEL_PD_BIND, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 删除 rlPdIds 的商品数据及所有商品业务关联数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .setSoftDel(softDel)  // 选填
     *                 .setSysType(sysStyle)  // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int batchDelProduct(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            String xid = mgProductArg.getXid();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putString(ProductBasicDto.Key.XID, xid);
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            boolean softDel = mgProductArg.getSoftDel();
            sendBody.putBoolean(ProductBasicDto.Key.SOFT_DEL, softDel);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.BATCH_DEL_PDS, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**----------------------------------------------   商品信息接口结束   ----------------------------------------------*/

    /**  优化start **/
    /**
     * 获取的商品指定组合信息
     */
    public int getProductList4Adm(MgProductArg mgProductArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is empty");
                return m_rt;
            }
            Param combined = mgProductArg.getCombined();
            if(combined == null) {
                combined = new Param();
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(MgProductDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlPdIds.toBuffer(sendBody, MgProductDto.Key.RL_PD_IDS);
            combined.toBuffer(sendBody, MgProductDto.Key.COMBINED, MgProductDto.getCombinedInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.GET_FULL_LIST_4ADM, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, MgProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取的商品指定组合汇总信息(例：门店通总店维度获取数据)
     */
    public int getProductSummaryList4Adm(MgProductArg mgProductArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is empty");
                return m_rt;
            }
            Param combined = mgProductArg.getCombined();
            if(combined == null) {
                combined = new Param();
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(MgProductDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlPdIds.toBuffer(sendBody, MgProductDto.Key.RL_PD_IDS);
            combined.toBuffer(sendBody, MgProductDto.Key.COMBINED, MgProductDto.getCombinedInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.GET_SUM_LIST_4ADM, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, MgProductDto.getSummaryInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
}
