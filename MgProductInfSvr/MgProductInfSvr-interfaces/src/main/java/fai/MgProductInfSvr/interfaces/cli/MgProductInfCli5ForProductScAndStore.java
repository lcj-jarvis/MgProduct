package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.dto.ProductTempDto;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.MgProductInfSvr.interfaces.entity.ProductTempEntity;
import fai.MgProductInfSvr.interfaces.utils.InOutStoreSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;

import java.util.Calendar;

public class MgProductInfCli5ForProductScAndStore extends MgProductInfCli4ForProductTpSc{
    public MgProductInfCli5ForProductScAndStore(int flow) {
        super(flow);
    }

    /**==============================================   商品商品规格接口 + 库存接口开始   ==============================================*/
    /**
     * 获取产品规格列表
     *
     * @param rlPdId         商品业务id {@link ProductSpecEntity.SpecInfo#RL_PD_ID}
     * @param infoList       Param 见 {@link ProductSpecEntity.SpecInfo} <br/>
     * @param onlyGetChecked 是否只获取有勾选的商品规格
     * @return {@link Errno}
     */
    public int getPdScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList, boolean onlyGetChecked) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            sendBody.putBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED, onlyGetChecked);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.Spec.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    public int getPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList){
        return getPdSkuScInfoList(aid, tid, siteId, lgId, keepPriId1, rlPdId, false, infoList, null);
    }
    /**
     * 获取产品规格SKU列表
     * @param rlPdId 商品业务id {@link ProductSpecEntity.SpecSkuInfo#RL_PD_ID}
     * @param withSpuInfo 是否同时获取spu的相关数据，例如商品条码
     * @param rtInfoList Param 见 {@link ProductSpecEntity.SpecSkuInfo} <br/>
     * @param rtSpuInfo spu信息
     * @return {@link Errno}
     */
    public int getPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, boolean withSpuInfo, FaiList<Param> rtInfoList, Param rtSpuInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rtInfoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rtInfoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            sendBody.putBoolean(ProductSpecDto.Key.WITH_SPU_INFO, withSpuInfo);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = rtInfoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(rtSpuInfo != null){
                keyRef = new Ref<Integer>();
                m_rt = rtSpuInfo.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
                if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.SPU_INFO) {
                    Log.logErr(m_rt, "recv codec err");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 根据 skuIdList 获取产品规格SKU列表
     * @param skuIdList skuId集合 {@link ProductSpecEntity.SpecSkuInfo#SKU_ID}
     * @param infoList Param 见 {@link ProductSpecEntity.SpecSkuInfo} <br/>
     * @return {@link Errno}
     */
    public int getPdSkuIdInfoListBySkuIdList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Long> skuIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            skuIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.GET_LIST_BY_SKU_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
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
     * 获取只有表示为spu的商品sku数据。 <br/>
     * 例如：商品条码信息
     * @param rlPdIdList 商品业务id集
     * @param infoList 只有表示为spu的数据集 Param 见 {@link ProductSpecEntity.SpecSkuInfo}
     */
    public int getOnlySpuPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(rlPdIdList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.GET_ONLY_SPU_INFO_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    public int getPdSkuIdInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList) {
        return getPdSkuIdInfoList(aid, tid, siteId, lgId, keepPriId1, rlPdIdList, false, infoList);
    }
    /**
     * 根据业务商品id获取skuId集
     * @param rlPdIdList  {@link ProductSpecEntity.SpecSkuInfo#RL_PD_ID}
     * @param withSpuInfo 是否同时获取spu的相关数据，例如商品条码
     * @param infoList Param 中只有 {@link ProductSpecEntity.SpecSkuInfo#RL_PD_ID} 和 {@link ProductSpecEntity.SpecSkuInfo#SKU_ID}
     * @return {@link Errno}
     */
    public int getPdSkuIdInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, boolean withSpuInfo, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(rlPdIdList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            sendBody.putBoolean(ProductSpecDto.Key.WITH_SPU_INFO, withSpuInfo);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
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
     * 根据 skuCode 模糊搜索匹配上的skuIdInfo
     * @param tid 创建商品的 tid
     * @param siteId 创建商品的 siteId
     * @param lgId 创建商品的 lgId
     * @param keepPriId1 创建商品的 keepPriId1
     * @param skuCodeKeyWord 搜索关键词
     * @param condition 条件(搜索条件/返回结果形式) {@link ProductSpecEntity.Condition}
     * @param skuIdInfoList 匹配上的结果集
     */
    public int searchPdSkuIdInfoListBySkuCode(int aid, int tid, int siteId, int lgId, int keepPriId1, String skuCodeKeyWord, Param condition, FaiList<Param> skuIdInfoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(Str.isEmpty(skuCodeKeyWord)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuCodeKeyWord error");
                return m_rt;
            }
            if (skuIdInfoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdInfoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(ProductSpecDto.Key.SKU_CODE, skuCodeKeyWord);
            condition.toBuffer(sendBody, ProductSpecDto.Key.CONDITION, ProductSpecDto.Condition.getInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.SEARCH_SKU_ID_INFO_LIST_BY_SKU_CODE, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = skuIdInfoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
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
     * 获取已经存在的 skuCodeList
     * @param tid 创建商品的 tid
     * @param siteId 创建商品的 siteId
     * @param lgId 创建商品的 lgId
     * @param keepPriId1 创建商品的 keepPriId1
     * @param skuCodeList 需要判断的 skuCode 集合
     * @param existsSkuCodeListRef 返回存在的 skuCode 集合
     */
    public int getExistsSkuCodeList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<String> skuCodeList, Ref<FaiList<String>> existsSkuCodeListRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuCodeList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuCodeList error");
                return m_rt;
            }
            if (existsSkuCodeListRef == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "existsSkuCodeListRef error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            skuCodeList.toBuffer(sendBody, ProductSpecDto.Key.SKU_CODE_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.GET_SKU_CODE_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            Ref<String> valRef = new Ref<String>();
            m_rt = recvBody.getString(keyRef, valRef);
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.SKU_CODE_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            existsSkuCodeListRef.value = FaiList.parseStringList(valRef.value);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 获取 sku 库存销售信息
     * @param rlPdId 商品业务id
     * @param useOwnerFieldList 使用 创建商品的业务数据 <br/>
     *      例如：悦客价格是由总店控制，门店只能使用总店的价格，这时查询门店的的信息时，选择使用总店的价格进行覆盖
     * @param infoList Param 见  {@link ProductStoreEntity.StoreSalesSkuInfo}
     * @return {@link Errno}
     */
    public int getSkuStoreSalesList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<String> useOwnerFieldList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            if(useOwnerFieldList != null){
                m_rt = useOwnerFieldList.toBuffer(sendBody, ProductStoreDto.Key.STR_LIST);
                if(m_rt != Errno.OK){
                    return m_rt;
                }
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.StoreSalesSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 根据skuIdList 获取 sku 库存销售信息
     * @param skuIdList skuId 集合
     * @param useOwnerFieldList 使用 创建商品的业务数据 <br/>
     *      例如：悦客价格是由总店控制，门店只能使用总店的价格，这时查询门店的的信息时，选择使用总店的价格进行覆盖
     * @param infoList Param 见  {@link ProductStoreEntity.StoreSalesSkuInfo}
     * @return {@link Errno}
     */
    public int getSkuStoreSalesBySkuIdList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Long> skuIdList, FaiList<String> useOwnerFieldList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null || skuIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            skuIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            if(useOwnerFieldList != null){
                m_rt = useOwnerFieldList.toBuffer(sendBody, ProductStoreDto.Key.STR_LIST);
                if(m_rt != Errno.OK){
                    return m_rt;
                }
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.StoreSalesSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 获取 sku 关联业务的库存销售信息  <br/>
     *  场景： <br/>
     *      悦客查看某个规格的库存分布。
     * @param skuId skuId
     * @param bizInfoList 所关联的业务集 Param 只需要 tid，siteId, lgId, keepPriId1 {@link ProductStoreEntity.StoreSalesSkuInfo}
     * @param infoList {@link ProductStoreEntity.StoreSalesSkuInfo}
     * @return {@link Errno}
     */
    public int getSkuStoreSalesBySkuId(int aid, int tid, long skuId, FaiList<Param> bizInfoList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(bizInfoList == null || bizInfoList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "bizInfoList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putLong(ProductStoreDto.Key.SKU_ID, skuId);
            bizInfoList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.GET_LIST_BY_SKU_ID, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.StoreSalesSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 获取预扣记录
     * @param skuIdList skuId 集合
     * @param infoList  Param 见 {@link ProductStoreEntity.HoldingRecordInfo}
     * @return {@link Errno}
     */
    public int getHoldingRecordList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Long> skuIdList, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdList == null || skuIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            skuIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.HoldingRecordCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.HoldingRecord.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 根据rlPdIdList 获取 spu 所有关联的业务的库存销售信息汇总 <br/>
     * 适用场景： <br/>
     *    例如：积分商品  绑定了指定的部分门店， 每个积分商品绑定 门店不同，数量不同，我们在获取列表时候，就需要获取到各个门店的  库存spu信息 出来
     * @param tid 创建商品的 tid
     * @param siteId 创建商品的 siteId
     * @param lgId 创建商品的 siteId
     * @param keepPriId1 创建商品的 keepPriId1
     * @param rlPdIdList 商品业务id 集
     * @param infoList Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int getAllSpuBizStoreSalesSummaryInfoListByPdIdList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(rlPdIdList == null || rlPdIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SpuBizSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 获取 spu 指定业务的库存销售信息汇总 <br/>
     * 适用场景： <br/>
     *    例如：悦客门店查看商品 门店维度的信息汇总
     * @param rlPdIdList 商品业务id 集
     * @param useOwnerFieldList 使用 创建商品的业务数据
     *   例如：悦客价格是由总店控制，门店只能使用总店的价格，这时查询门店的的信息时，选择使用总店的价格进行覆盖
     * @param infoList Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int getSpuBizStoreSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<String> useOwnerFieldList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(rlPdIdList == null || rlPdIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            if(m_rt != Errno.OK){
                return m_rt;
            }
            if(useOwnerFieldList != null){
                m_rt = useOwnerFieldList.toBuffer(sendBody, ProductStoreDto.Key.STR_LIST);
                if(m_rt != Errno.OK){
                    return m_rt;
                }
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpuBizSummaryCmd.GET_LIST_BY_PD_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SpuBizSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 获取 spu 库存销售信息汇总 <br/>
     * 适用场景： <br/>
     *    例如：悦客总店查看商品信息汇总
     * @param rlPdIdList rlPdIdList 商品业务id 集
     * @param infoList Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int getSpuStoreSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpuSummaryCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SpuSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 查询 sku 库存汇总
     * 例如：悦客 所有门店 sku维度 的汇总信息
     * @param tid 创建商品的tid
     * @param siteId 创建商品siteId
     * @param lgId 创建商品的lgId
     * @param keepPriId1 创建商品的keepPriId1
     */
    public int searchSkuStoreSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, SearchArg searchArg, FaiList<Param> list){
        return searchSkuStoreSalesSummaryInfoList(aid, tid, siteId, lgId, keepPriId1, false, searchArg, list);
    }
    /**
     * 查询 sku 库存汇总
     * @param tid 创建商品的tid | 相关联的tid
     * @param siteId 创建商品siteId | 相关联的siteId
     * @param lgId 创建商品的lgId | 相关联的lgId
     * @param keepPriId1 创建商品的keepPriId1 | 相关联的keepPriId1
     * @param isBiz 是否是 查询 业务（主键）+sku 维度
     * @param searchArg 查询条件
     * 分页限制：100  <br/>
     *      {@link ProductStoreEntity.SkuSummaryInfo#COUNT}  可查询、排序  <br/>
     *      {@link ProductStoreEntity.SkuSummaryInfo#REMAIN_COUNT}  可查询、排序  <br/>
     *      {@link ProductStoreEntity.SkuSummaryInfo#HOLDING_COUNT}  可查询、排序  <br/>
     * @param list Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int searchSkuStoreSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, boolean isBiz, SearchArg searchArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = searchArg.toBuffer(sendBody, ProductStoreDto.Key.SEARCH_ARG);
            if(m_rt != Errno.OK){
                return m_rt;
            }
            sendBody.putBoolean(ProductStoreDto.Key.IS_BIZ, isBiz);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SkuSummaryCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductStoreDto.SkuSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != ProductStoreDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 获取 spu 所有关联的业务的库存销售信息汇总 <br/>
     * 适用场景： <br/>
     *    例如：悦客总店查看某个商品时，想进一步查看这个商品在门店的维度下的数据
     * @param rlPdId 商品业务id
     * @param infoList Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int getAllSpuBizStoreSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SpuBizSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
     * 查询出入库记录
     * @param tid 创建商品的tid | 相关联的tid
     * @param siteId 创建商品siteId | 相关联的siteId
     * @param lgId 创建商品的lgId | 相关联的lgId
     * @param keepPriId1 创建商品的keepPriId1 | 相关联的keepPriId1
     * @param isBiz 是否是 查询 业务（主键）+sku 维度 <br/>
     *              例：<br/>
     *              isBiz：false 悦客-查询所有门店的数据 <br/>
     *              isBiz：true 悦客-查询指定门店的数据 <br/>
     * @param searchArg 查询条件
     * 分页限制：100 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#RL_PD_ID}  可查询条件 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OPT_TYPE}  可查询条件 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#C_TYPE}  可查询条件 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#S_TYPE}  可查询条件 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OPT_TIME}  可查询条件 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SYS_CREATE_TIME}  可查询条件 <br/>
     * 默认按创建时间降序
     * @param list 出入库记录集合 Param 见 {@link ProductStoreEntity.InOutStoreRecordInfo} <br/>
     * @return {@link Errno}
     */
    public int searchInOutStoreRecordInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, boolean isBiz, SearchArg searchArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putBoolean(ProductStoreDto.Key.IS_BIZ, isBiz);
            searchArg.toBuffer(sendBody, ProductStoreDto.Key.SEARCH_ARG);

            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.InOutStoreRecordCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != ProductStoreDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 添加库存出入库记录
     * @param siteId 业务商品所属权的 siteId (如:悦客总店的 siteId)
     * @param lgId 业务商品所属权的 lgId (如:悦客总店的 lgId)
     * @param keepPriId1 业务商品所属权的 keepPriId1 (如:悦客总店的 keepPriId1)
     * @param infoList 出入库记录集合 Param 见 {@link ProductStoreEntity.InOutStoreRecordInfo} <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SITE_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#LGID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#KEEP_PRI_ID1} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SKU_ID} 或者 {@link ProductStoreEntity.InOutStoreRecordInfo#IN_PD_SC_STR_NAME_LIST} 必填其一  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#RL_PD_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OWNER_RL_PD_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OPT_TYPE} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#CHANGE_COUNT} 必填  <br/>
     * @return {@link Errno}
     */
    public int addInOutStoreRecordInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = infoList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.InOutStoreRecordCmd.ADD_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }


    /**
     * 批量同步spu 为 sku
     * @param ownerTid 创建商品的 tid
     * @param ownerSiteId 创建商品的 siteId
     * @param ownerLgId 创建商品的 lgId
     * @param ownerKeepPriId1 创建商品的 keepPriId1
     * @param spuInfoList Param见 {@link ProductTempEntity.ProductInfo}
     */
    public int synSPU2SKU(int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> spuInfoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(spuInfoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "spuInfoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTempDto.Key.TID, ownerTid), new Pair(ProductTempDto.Key.SITE_ID, ownerSiteId), new Pair(ProductTempDto.Key.LGID, ownerLgId), new Pair(ProductTempDto.Key.KEEP_PRIID1, ownerKeepPriId1));
            m_rt = spuInfoList.toBuffer(sendBody, ProductTempDto.Key.INFO_LIST, ProductTempDto.Info.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TempCmd.SYN_SPU_TO_SKU, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 批量同步 出入库记录
     * @param ownerTid 创建商品的 tid
     * @param ownerSiteId 创建商品的 siteId
     * @param ownerLgId 创建商品的 lgId
     * @param ownerKeepPriId1 创建商品的 keepPriId1
     * @param recordInfoList 出入库记录集 Param见 {@link ProductTempEntity.StoreRecordInfo}
     */
    @Deprecated
    public int synInOutStoreRecord(int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> recordInfoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(recordInfoList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "recordInfoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTempDto.Key.TID, ownerTid), new Pair(ProductTempDto.Key.SITE_ID, ownerSiteId), new Pair(ProductTempDto.Key.LGID, ownerLgId), new Pair(ProductTempDto.Key.KEEP_PRIID1, ownerKeepPriId1));
            m_rt = recordInfoList.toBuffer(sendBody, ProductTempDto.Key.INFO_LIST, ProductTempDto.StoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TempCmd.SYN_IN_OUT_STORE_RECORD, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }



    /**
     * 修改产品规格总接口 <br/>
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     *
     * @param rlPdId      商品业务id {@link ProductSpecEntity.SpecInfo#RL_PD_ID}
     * @param addList     Param 见 {@link ProductSpecEntity.SpecInfo} <br/>
     *                    {@link ProductSpecEntity.SpecInfo#NAME} 必填 <br/>
     *                    {@link ProductSpecEntity.SpecInfo#RL_PD_ID} 必填 <br/>
     *                    {@link ProductSpecEntity.SpecInfo#IN_PD_SC_VAL_LIST} 必填 <br/>
     * @param delList     集合中元素为 商品规格id {@link ProductSpecEntity.SpecInfo#PD_SC_ID}
     * @param updaterList Param 见 {@link ProductSpecEntity.SpecInfo}
     *                    {@link ProductSpecEntity.SpecInfo#PD_SC_ID} 必填 <br/>
     * @return {@link Errno}
     */
    public int unionSetPdScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (addList == null && delList == null && updaterList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args 2 error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            if (addList != null) {
                if (addList.isEmpty()) {
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "addList isEmpty");
                    return m_rt;
                }
                m_rt = addList.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.Spec.getInfoDto());
                if (m_rt != Errno.OK) {
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "addList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                    return m_rt;
                }
            }
            if (delList != null) {
                if (delList.isEmpty()) {
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "delList isEmpty");
                    return m_rt;
                }
                delList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }
            if (updaterList != null) {
                if (updaterList.isEmpty()) {
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "updaterList isEmpty");
                    return m_rt;
                }
                m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.Spec.getInfoDto());
                if (m_rt != Errno.OK) {
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                    return m_rt;
                }
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecCmd.UNION_SET, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 修改商品规格SKU。 <br/>
     * 为了支持商品条码，允许一个ParamUpdater可以不提供skuId or inPdScStrNameList，但是spu（{@link ProductSpecEntity.SpecSkuInfo#SPU}}）必须为true
     *
     * @param rlPdId      商品业务id {@link ProductSpecEntity.SpecSkuInfo#RL_PD_ID}
     * @param updaterList Param 见 {@link ProductSpecEntity.SpecSkuInfo} <br/>
     *                    {@link ProductSpecEntity.SpecSkuInfo#SKU_ID} 或者 {@link ProductSpecEntity.SpecSkuInfo#IN_PD_SC_STR_NAME_LIST} 两个必须要有一个 <br/>
     * @return {@link Errno}
     */
    public int setPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecSkuCmd.SET_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    /**
     * 修改 sku 库存销售信息
     * @param rlPdId 商品业务id {@link ProductStoreEntity.StoreSalesSkuInfo#RL_PD_ID}
     * @param updaterList Param 见 {@link ProductStoreEntity.StoreSalesSkuInfo}
     *       {@link ProductStoreEntity.StoreSalesSkuInfo#SKU_ID} 或者 {@link ProductStoreEntity.StoreSalesSkuInfo#IN_PD_SC_STR_NAME_LIST} 两个必须要有一个 <br/>
     * @return {@link Errno}
     */
    public int setSkuStoreSales(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, ProductStoreDto.Key.UPDATER_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.SET_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    /**
     * 批量扣减库存
     * @param skuIdCountList    [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 15, count:2}] count > 0
     * @param rlOrderCode       业务订单id/code
     * @param reduceMode        扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @param expireTimeSeconds 扣减模式 - 预扣 下 步骤1 -> 步骤2 过程超时时间，单位s；这个值基本比订单超时时间值大
     * @return {@link Errno} 和 {@link MgProductErrno}
     */
    public int batchReducePdSkuStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdCountList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SkuCountChange.getInfoDto());
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(ProductStoreDto.Key.REDUCE_MODE, reduceMode);
            sendBody.putInt(ProductStoreDto.Key.EXPIRE_TIME_SECONDS, expireTimeSeconds);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int batchReducePdSkuHoldingStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo){
        return batchReducePdSkuHoldingStore(aid, tid, siteId, lgId, keepPriId1, skuIdCountList, rlOrderCode, outStoreRecordInfo, null);
    }
    /**
     * 批量扣除锁住的库存
     * 预扣模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 15, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录 见 {@link ProductStoreEntity.InOutStoreRecordInfo}
     * @return {@link Errno} 和 {@link MgProductErrno}
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo, Ref<Integer> inOutStoreRecordIdRef){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg skuIdCountList error");
                return m_rt;
            }
            if(Str.isEmpty(outStoreRecordInfo)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg outStoreRecordInfo error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SkuCountChange.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            m_rt = outStoreRecordInfo.toBuffer(sendBody, ProductStoreDto.Key.IN_OUT_STORE_RECORD, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            // send and recv
            boolean inOutStoreRecordIdRefNotNull = (inOutStoreRecordIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE, sendBody, false, inOutStoreRecordIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if(inOutStoreRecordIdRefNotNull){
                // recv info
                Ref<Integer> keyRef = new Ref<Integer>();
                recvBody.getInt(keyRef, inOutStoreRecordIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.IN_OUT_STORE_RECORD_ID) {
                    Log.logErr(m_rt, "recv codec err");
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
     * 批量补偿库存
     * @param skuIdCountList [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 15, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     *      扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @return {@link Errno} 和 {@link MgProductErrno}
     */
    public int batchMakeUpPdSkuStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg skuIdCountList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SkuCountChange.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(ProductStoreDto.Key.REDUCE_MODE, reduceMode);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 管理态调用 <br/>
     * 刷新 rlOrderCode 的预扣记录。<br/>
     * 根据 allHoldingRecordList 和已有的预扣尽量进行对比， <br/>
     * 如果都有，则对比数量，数量不一致，就多退少补。  <br/>
     * 如果 holdingRecordList中有 db中没有 就生成预扣记录，并进行预扣库存.  <br/>
     * 如果 holdingRecordList中没有 db中有 就删除db中的预扣记录，并进行补偿库存。 <br/>
     * @param rlOrderCode 业务订单id/code
     * @param allHoldingRecordList 当前订单的所有预扣记录 [{ skuId: 122, itemId: 11, count:12},{ skuId: 142, itemId: 21, count:2}] count > 0
     */
    public int refreshHoldingRecordOfRlOrderCode(int aid, int tid, int siteId, int lgId, int keepPriId1, String rlOrderCode, FaiList<Param> allHoldingRecordList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(allHoldingRecordList == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg allHoldingRecordList error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            m_rt = allHoldingRecordList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SkuCountChange.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.REFRESH_HOLDING_RECORD_OF_RL_ORDER_CODE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    /**
     * 退库存，会生成入库记录
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlRefundId 退款id
     * @param inStoreRecordInfo 入库记录 见 {@link ProductStoreEntity.InOutStoreRecordInfo}
     * @return {@link Errno} 和 {@link MgProductErrno}
     */
    public int batchRefundPdSkuStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlRefundId, Param inStoreRecordInfo){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg skuIdCountList error");
                return m_rt;
            }
            if(Str.isEmpty(inStoreRecordInfo)){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg inStoreRecordInfo error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.SkuCountChange.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            sendBody.putString(ProductStoreDto.Key.RL_REFUND_ID, rlRefundId);
            m_rt = inStoreRecordInfo.toBuffer(sendBody, ProductStoreDto.Key.IN_OUT_STORE_RECORD, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_REFUND_STORE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**----------------------------------------------   商品商品规格接口 + 库存接口结束   ----------------------------------------------*/


    /**----------------------------------------------   优化start   ----------------------------------------------**/
    /**
     * 替换原有的 addInOutStoreRecordInfoList 方法
     * 添加库存出入库记录
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setAddList(addList) // 必填，要添加的出入库记录详见addList说明
     *                 .build();
     * addList说明： 出入库记录集合 Param 见 {@link ProductStoreEntity.InOutStoreRecordInfo} <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SITE_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#LGID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#KEEP_PRI_ID1} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SKU_ID} 或者 {@link ProductStoreEntity.InOutStoreRecordInfo#IN_PD_SC_STR_NAME_LIST} 必填其一  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#RL_PD_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OWNER_RL_PD_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#OPT_TYPE} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#CHANGE_COUNT} 必填  <br/>
     * @return {@link Errno}
     */
    public int addInOutStoreRecordInfoList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid  = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> infoList = mgProductArg.getAddList();
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = infoList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }
            // send and recv
            sendAndRecv(aid, MgProductInfCmd.InOutStoreRecordCmd.ADD_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 查询出入库记录
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setIsBiz(true) // 选填，默认为false，详见isBiz说明
     *                 .build();
     * isBiz说明: 是否是 查询 业务（主键）+sku 维度 <br/>
     *              例：<br/>
     *              isBiz：false 悦客-查询所有门店的数据 <br/>
     *              isBiz：true 悦客-查询指定门店的数据 <br/>
     * @param search 查询条件 使用详见 https://train.faisco.biz/bP7CIQh 中 InOutStoreSearch说明
     * @param list 出入库记录集合 Param
     *             详情数据实体见 {@link ProductStoreEntity.InOutStoreRecordInfo} <br/>
     *             汇总数据实体见 {@link ProductStoreEntity.InOutStoreSumInfo} <br/>
     * @return {@link Errno}
     */
    public int searchInOutStoreRecordList(MgProductArg mgProductArg, InOutStoreSearch search, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(search == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "search is null");
                return m_rt;
            }
            SearchArg searchArg = search.getSearchArg();
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "get searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<Param> unionPriIds = search.getPrimaryKeys();
            if(unionPriIds == null) {
                unionPriIds = new FaiList<Param>();
                int searchTid = search.getTid();
                int searchSiteId = search.getSiteId();
                int searchLgid = search.getLgId();
                int searchKeep = search.getKeepPriId1();
                Param info = new Param()
                        .setInt(ProductStoreEntity.StoreSalesSkuInfo.TID, searchTid)
                        .setInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, searchSiteId)
                        .setInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, searchLgid)
                        .setInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, searchKeep);
                unionPriIds.add(info);
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid),
                    new Pair(ProductStoreDto.Key.SITE_ID, siteId),
                    new Pair(ProductStoreDto.Key.LGID, lgId),
                    new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));

            int cmd = 0;
            InOutStoreSearch.SearchType searchType = search.getSearchType();
            if(InOutStoreSearch.SearchType.InOutStoreRecord.equals(searchType)) {
                cmd = MgProductInfCmd.InOutStoreRecordCmd.GET_LIST;
                sendBody.putBoolean(ProductStoreDto.Key.IS_BIZ, mgProductArg.getIsBiz());
            }else {
                cmd = MgProductInfCmd.InOutStoreRecordCmd.GET_SUM_LIST;
            }

            searchArg.toBuffer(sendBody, ProductStoreDto.Key.SEARCH_ARG);
            unionPriIds.toBuffer(sendBody, ProductStoreDto.Key.PRI_IDS, ProductStoreDto.PrimaryKey.getInfoDto());


            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, cmd, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(search.totalSize != null){
                recvBody.getInt(keyRef, search.totalSize);
                if(keyRef.value != ProductStoreDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 重置指定商品 在 指定操作时间之前的入库成本
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdId(rlPdId) // 必填，要重置成本的商品业务id
     *                 .setOptTime(optTime) // 必填，指定操作时间，操作时间小于该时间的才进行重置成本
     *                 .setSkuList(infoList) // 必填，要重置的数据 详见infoList说明
     *                 .build();
     * infoList说明： 指定要要重置的数据集合 Param 见 {@link ProductStoreEntity.InOutStoreRecordInfo} <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#TID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SITE_ID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#LGID} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#KEEP_PRI_ID1} 必填  <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#SKU_ID} 必填 <br/>
     *      {@link ProductStoreEntity.InOutStoreRecordInfo#PRICE} 必填 <br/>
     * @return
     */
    public int batchResetCostPrice(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid  = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> infoList = mgProductArg.getSkuList();
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            int rlPdId = mgProductArg.getRlPdId();
            if(rlPdId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdId error");
                return m_rt;
            }
            Calendar optTime = mgProductArg.getOptTime();
            if(optTime == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "optTime error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            sendBody.putCalendar(ProductStoreDto.Key.OPT_TIME, optTime);
            m_rt = infoList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }
            // send and recv
            sendAndRecv(aid, MgProductInfCmd.InOutStoreRecordCmd.BATCH_RESET_PRICE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 修改 sku 库存销售信息
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPirId1)
     *                 .setPrimaryList(primaryList) // 要修改数据的主键id集合
     *                 .setRlPdId(rlPdId) //商品业务id {@link ProductStoreEntity.StoreSalesSkuInfo#RL_PD_ID}
     *                 .setUpdaterList(updaterList) //{@link ProductStoreEntity.StoreSalesSkuInfo#SKU_ID} 或者 {@link ProductStoreEntity.StoreSalesSkuInfo#IN_PD_SC_STR_NAME_LIST} 两个必须要有一个 <br/>
     *                 .build();
     * @return
     */
    public int batchSetSkuStoreSales(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid= mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if (primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int rlPdId = mgProductArg.getRlPdId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            m_rt = primaryKeys.toBuffer(sendBody, ProductStoreDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }
            m_rt = updaterList.toBuffer(sendBody, ProductStoreDto.Key.UPDATER_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_SET_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 新增 sku 库存销售信息
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPirId1)
     *                 .setSkuStoreSales(skuStoreSales) //{@link ProductStoreEntity.StoreSalesSkuInfo}
     *                 .build();
     * @return
     */
    public int batchAddSkuStoreSales(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid= mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> skuStoreSales = mgProductArg.getSkuStoreSales();
            if (skuStoreSales == null || skuStoreSales.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuStoreSales error");
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = skuStoreSales.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.StoreSalesSkuCmd.BATCH_ADD_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 根据rlPdIdList 获取 spu 所有关联的业务的库存销售信息汇总 <br/>
     * 适用场景： <br/>
     *    例如：积分商品  绑定了指定的部分门店， 每个积分商品绑定 门店不同，数量不同，我们在获取列表时候，就需要获取到各个门店的  库存spu信息 出来
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId)
     *                 .setRlPdIds(rlPdIds) // 商品业务id 集合
     *                 .build();
     * @param infoList Param 见 {@link ProductStoreEntity.SpuBizSummaryInfo}
     * @return {@link Errno}
     */
    public int getAllSpuBizStoreSalesSummaryListByPdIdList(MgProductArg mgProductArg, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> rlPdIdList = mgProductArg.getRlPdIds();
            if(rlPdIdList == null || rlPdIdList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIdList error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductStoreDto.Key.TID, tid), new Pair(ProductStoreDto.Key.SITE_ID, siteId), new Pair(ProductStoreDto.Key.LGID, lgId), new Pair(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpuBizSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SpuBizSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
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
