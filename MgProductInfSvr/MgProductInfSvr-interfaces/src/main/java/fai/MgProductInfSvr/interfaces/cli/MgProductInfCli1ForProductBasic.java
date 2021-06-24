package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearch;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductInfCli1ForProductBasic extends MgProductParentInfCli {
    public MgProductInfCli1ForProductBasic(int flow) {
        super(flow);
    }

    @Deprecated
    public int mgProductSearch(int aid, int tid, int siteId, int lgId, int keepPriId1, MgProductSearch mgProductSearch, Param searchResult){
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setMgProductSearch(mgProductSearch)
                .build();
        return mgProductSearch(mgProductArg, searchResult);
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
        return importProduct(aid, tid, siteId, lgId, keepPriId1, productList, inStoreRecordInfo, null);
    }

    @Deprecated
    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, FaiList<Param> errProductList){
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setImportProductList(productList)
                .setInOutStoreRecordInfo(inStoreRecordInfo)
                .build();
        return importProduct(mgProductArg, errProductList);
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
                .setPdRelInfo(info)
                .build();
        return bindProductRel(mgProductArg, rlPdIdRef);
    }

    @Deprecated
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, 0, 0, 0)
                .setBindRlPdInfo(bindRlPdInfo)
                .setPdRelInfoList(infoList)
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
     *                 .setMgProductSearch(mgProductSearch) // 必填 搜索条件
     *                 .build();
     * @param searchResult 搜索结果，对应 MgProductSearchResult 实体
     * @return {@link Errno}
     */
    public int mgProductSearch(MgProductArg mgProductArg, Param searchResult){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if(searchResult == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchResult == null error");
                return Errno.ARGS_ERROR;
            }
            searchResult.clear();
            // 如果没有筛选条件，返回空数据，防止误调用
            MgProductSearch mgProductSearch = mgProductArg.getMgProductSearch();
            if(mgProductSearch == null || mgProductSearch.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "mgProductSearch == null error");
                return Errno.ARGS_ERROR;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductSearchDto.Key.TID, tid), new Pair(MgProductSearchDto.Key.SITE_ID, siteId), new Pair(MgProductSearchDto.Key.LGID, lgId), new Pair(MgProductSearchDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(MgProductSearchDto.Key.SEARCH_PARAM_STRING, mgProductSearch.getSearchParam().toJson());
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.MgProductSearchCmd.SEARCH_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            searchResult.fromBuffer(recvBody, keyRef, MgProductSearchDto.getProductSearchDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductSearchDto.Key.RESULT_INFO) {
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
     * 获取的商品全部组合信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
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
            int rlPdId = mgProductArg.getRlPdId();
            sendBody.putInt(MgProductDto.Key.ID, rlPdId);
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
     * 根据rlPdIds获取商品数据（商品表+商品业表）
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds) // 必填 商品业务id集合
     *                 .build();
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
     * 导入商品数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setImportProductList(productList) 商品中台各个服务组合的数据 {@link MgProductEntity.Info}
     *                 .setInOutStoreRecordInfo(inStoreRecordInfo) {@link ProductStoreEntity.InOutStoreRecordInfo}  非必要
     *                 .build();
     * @param errProductList 返回导入出错的数据，并且每个Param有对应的错误码 {@link MgProductEntity.Info}
     * @return {@link Errno}
     */
    public int importProduct(MgProductArg mgProductArg, FaiList<Param> errProductList){
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
     *              .setCombined(combined)                          // 必填
     *              .setInOutStoreRecordInfo(inOutStoreRecordInfo)  // 必填
     *              .build();
     * combined 添加信息 {@link MgProductDto#getInfoDto()}
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
            Param addInfo = mgProductArg.getCombined();
            if (addInfo == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addInfo is empty");
                return m_rt;
            }
            Param inOutStoreRecordInfo = mgProductArg.getInOutStoreRecordInfo();
            if (inOutStoreRecordInfo == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;inOutStoreRecordInfo is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
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
     *                 .setPdRelInfo(info)            // 必填 业务信息
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
            Param info = mgProductArg.getPdRelInfo();
            if (Str.isEmpty(info)) {
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
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            bindRlPdInfo.toBuffer(sendBody, ProductBasicDto.Key.PD_BIND_INFO, ProductBasicDto.getProductRelDto());
            info.toBuffer(sendBody, ProductBasicDto.Key.PD_REL_INFO, ProductBasicDto.getProductRelDto());
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
     *                 .setPdRelInfoList(infoList)    // 必填 业务信息列表
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
            FaiList<Param> infoList = mgProductArg.getPdRelInfoList();
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
            // packaging send data
            FaiBuffer sendBody = new FaiBuffer(true);
            int tid = mgProductArg.getTid();
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            bindRlPdInfo.toBuffer(sendBody, ProductBasicDto.Key.PD_BIND_INFO, ProductBasicDto.getProductRelDto());
            infoList.toBuffer(sendBody, ProductBasicDto.Key.PD_REL_INFO_LIST, ProductBasicDto.getProductRelDto());

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

    /**
     * 根据商品 id 和 updater，修改 商品 基础信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            int rlPdId = mgProductArg.getRlPdId();
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
     *                 .setCombinedUpdater(updater)   // 必填
     *                 .setRlPdId(rlPdId)             // 必填
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
            ParamUpdater updater = mgProductArg.getCombinedUpdater();
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr("arg error;updater is empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int rlPdId = mgProductArg.getRlPdId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            updater.toBuffer(sendBody, ProductBasicDto.Key.UPDATER, MgProductDto.getInfoDto());
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
     * 指定业务下，取消 rlPdIds 的商品业务关联
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .setSoftDel(softDel)  // 选填 默认为false
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
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
