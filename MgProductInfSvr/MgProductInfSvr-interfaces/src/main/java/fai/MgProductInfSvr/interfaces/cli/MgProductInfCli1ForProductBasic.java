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

    /**==============================================   商品信息接口开始   ==============================================*/
    /**
     * 商品中台搜索，根据 mgProductSearch（fai.MgProductInfSvr.interfaces.utils.MgProductSearch）, 在 商品中台内 搜索商品
     * @param tid 调用搜索的业务，
     * @param siteId 调用搜索的siteId
     * @param lgId 调用搜索的lgId
     * @param keepPriId1 调用搜索的keepPriId1
     * @param mgProductSearch 搜索条件
     * @param searchResult 搜索结果，对应 MgProductSearchResult 实体
     *
     */
    public int mgProductSearch(int aid, int tid, int siteId, int lgId, int keepPriId1, MgProductSearch mgProductSearch, Param searchResult){
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
            if(mgProductSearch == null || mgProductSearch.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "mgProductSearch == null error");
                return Errno.ARGS_ERROR;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductSearchDto.Key.TID, tid), new Pair(MgProductSearchDto.Key.SITE_ID, siteId), new Pair(MgProductSearchDto.Key.LGID, lgId), new Pair(MgProductSearchDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(MgProductSearchDto.Key.SEARCH_PARAM_STRING, mgProductSearch.getSearchParam().toJson());
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
     * @param tid 创建商品的tid
     * @param siteId 创建商品的siteId
     * @param lgId 创建商品的lgId
     * @param keepPriId1 创建商品的keepPriId1
     * @param rlPdId 业务商品id
     * @param combinedInfo 返回商品中台各个服务组合的数据 {@link MgProductEntity.Info}
     */
    public int getProductFullInfo(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, Param combinedInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
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
     *
     * @param rlPdIds 商品业务id集合
     * @param list    接收结果的集合
     * @return
     */
    public int getProductList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
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


    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo){
        return importProduct(aid, tid, siteId, lgId, keepPriId1, productList, inStoreRecordInfo, null);
    }
    /**
     * 导入商品数据
     * @param tid 创建商品的tid
     * @param siteId 创建商品的siteId
     * @param lgId 创建商品的lgId
     * @param keepPriId1 创建商品的keepPriId1
     * @param productList 商品中台各个服务组合的数据 {@link MgProductEntity.Info}
     * @param inStoreRecordInfo 入库记录 {@link ProductStoreEntity.InOutStoreRecordInfo}  非必要
     * @param errProductList 返回导入出错的数据，并且每个Param有对应的错误码 {@link MgProductEntity.Info}
     * @return
     */
    public int importProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, FaiList<Param> errProductList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(productList == null || productList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "productList error");
                return m_rt;
            }
            if(inStoreRecordInfo == null){
                inStoreRecordInfo = new Param();
            }
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
     */
    public int addProductAndRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param info, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }
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
     * 新增商品业务关联
     */
    public int bindProductRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }
            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }
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
     */
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;infoList is empty");
                return m_rt;
            }
            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = new FaiBuffer(true);
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

    //  根据商品 id 和 updater，修改 商品 基础信息
    public int setSinglePd(int aid, int tid, int siteId, int lgId, int keepPriId1, Integer rlPdId, ParamUpdater updater) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
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
     * 批量修改商品，每个商品的改动都是一样的
     */
    public int setProducts(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, ParamUpdater updater) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;updater is empty");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
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
     */
    public int batchDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelPdRelBind(aid, tid, siteId, lgId, keepPriId1, rlPdIds, false);
    }
    /**
     * 指定业务下，软删除 rlPdIds 的商品业务关联
     */
    public int batchSoftDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelPdRelBind(aid, tid, siteId, lgId, keepPriId1, rlPdIds, true);
    }
    private int batchDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, boolean softDel) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
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
     */
    public int batchDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelProduct(aid, tid, siteId, lgId, keepPriId1, rlPdIds, false);
    }
    /**
     * 软删除 rlPdIds 的商品数据及所有商品业务关联数据
     */
    public int batchSoftDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
        return batchDelProduct(aid, tid, siteId, lgId, keepPriId1, rlPdIds, true);
    }
    private int batchDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, boolean softDel) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
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
