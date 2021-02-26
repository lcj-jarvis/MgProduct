package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductSearchSvr.domain.entity.MgProductSearchResultEntity;
import fai.MgProductSearchSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MgProductSearchService {

    public void initMgProductSearchService(RedisCacheManager cache){
        m_cache = cache;
    }

    @SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamString=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);
        long beginTime = System.currentTimeMillis();
        try{
            Param searchParam = Param.parseParam(searchParamString);
            if(Str.isEmpty(searchParam)){
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; searchParam is empty err", flow, aid, unionPriId, tid);
            }

            MgProductSearch mgProductSearch = new MgProductSearch();
            mgProductSearch.initProductSearch(searchParam);    // 初始化 ProductSearch
            Log.logDbg("md5=%s;searchParam=%s;", MD5Util.MD5Encode(searchParamString, "utf-8"), searchParam.toJson());

            // 判断本地的缓存的时间是否发生了变化


            Param mockDataStatusInfo = new Param();
            mockDataStatusInfo.setLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME, 1614096000000L);  // 2021-02-24 00:00:00
            mockDataStatusInfo.setLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME, 1614182400000L);  // 2021-02-25 15:34:37
            mockDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 1000);                           // 1000 的数据量
            Ref<Long> maxChangeTime = new Ref<Long>(0L);  //  2021-02-24 11:58:07, 用于判断结果的缓存是否失效


            // 搜索结果的缓存
            Param resultCacheInfo = m_cache.getParam(getResultCacheKey(aid, unionPriId, mgProductSearch), MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            long resultCacheTime = 0;
            if(resultCacheInfo != null){
                resultCacheTime = resultCacheInfo.getLong(MgProductSearchResultEntity.Info.CACHE_TIME);
                Log.logDbg("key=%s;resultCacheInfo=%s;", getResultCacheKey(aid, unionPriId, mgProductSearch), resultCacheInfo.toJson());
            }


            /* 后面需要搞为异步获取数据 */
            // 1、在 "商品基础表" mgProduct_xxxx 搜索
            ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
            Ref<Boolean> mgProductBasicDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> productBasicAllSize = new Ref<Integer>(-1);
            Ref<Param> remoteProductBasicDataStatusInfo = new Ref<Param>();
            Ref<Param> productBasicDataStatusCacheInfo = new Ref<Param>();
            if(!productBasicSearchMatcher.isEmpty()){
                checkDataStatus(aid, unionPriId, flow, unionPriId, productBasicSearchMatcher, maxChangeTime, mgProductBasicDataIsChange, productBasicAllSize, mockDataStatusInfo, remoteProductBasicDataStatusInfo, productBasicDataStatusCacheInfo);
            }

            // 2、在 "商品业务关系表" mgProductRel_xxxx 搜索
            ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
            Ref<Param> remoteProductRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productRelDataStatusCacheInfo = new Ref<Param>();
            Ref<Boolean> mgProductRelDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> mgProductRelAllSize = new Ref<Integer>(-1);
            if(!productRelSearchMatcher.isEmpty()){
                checkDataStatus(aid, unionPriId, flow, unionPriId, productRelSearchMatcher, maxChangeTime, mgProductRelDataIsChange, mgProductRelAllSize, mockDataStatusInfo, remoteProductRelDataStatusInfo, productRelDataStatusCacheInfo);
            }

            // 3、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
            Ref<Param> remoteProductBindPropDataStatusInfo = new Ref<Param>();
            Ref<Param> productBindPropDataStatusCacheInfo = new Ref<Param>();
            Ref<Boolean> productBindPropDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> productBindPropAllSize = new Ref<Integer>(-1);
            ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
            if(!productBindPropDataSearchMatcher.isEmpty()){
                // 首先判断本地缓存的数据和状态
                checkDataStatus(aid, unionPriId, flow, unionPriId, productBindPropDataSearchMatcher, maxChangeTime, productBindPropDataIsChange, productBindPropAllSize, mockDataStatusInfo, remoteProductBindPropDataStatusInfo, productBindPropDataStatusCacheInfo);
            }

            // 4、在 "分类业务关系表" mgProductGroupRel_xxxx 搜索
            Ref<Param> remoteProductGroupRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productGroupRelDataStatusCacheInfo = new Ref<Param>();
            Ref<Boolean> mgProductGroupRelDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> mgProductGroupRelAllSize = new Ref<Integer>(-1);
            ParamMatcher mgProductGroupRelSearchMatcher = mgProductSearch.getProductGroupRelSearchMatcher(null);
            if(!mgProductGroupRelSearchMatcher.isEmpty()){
                // 首先判断本地缓存的数据和状态
                checkDataStatus(aid, unionPriId, flow, unionPriId, mgProductGroupRelSearchMatcher, maxChangeTime, mgProductGroupRelDataIsChange, mgProductGroupRelAllSize, mockDataStatusInfo, remoteProductGroupRelDataStatusInfo, productGroupRelDataStatusCacheInfo);
            }

            // 5、在 "标签业务关系表" mgProductLableRel_xxxx 搜索
            Ref<Boolean> mgProductLableRelDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> mgProductLableRelAllSize = new Ref<Integer>(-1);
            Ref<Param> remoteProductLableRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productLableRelDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher mgProductLableRelSearchMatcher = mgProductSearch.getProductLableRelSearchMatcher(null);
            if(!mgProductLableRelSearchMatcher.isEmpty()){
                // 首先判断本地缓存的数据和状态
                checkDataStatus(aid, unionPriId, flow, unionPriId, mgProductGroupRelSearchMatcher, maxChangeTime, mgProductLableRelDataIsChange, mgProductLableRelAllSize, mockDataStatusInfo, remoteProductLableRelDataStatusInfo, productLableRelDataStatusCacheInfo);
            }

            // 6、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
            Ref<Boolean> mgSpuBizSummaryDataIsChange = new Ref<Boolean>(false);
            Ref<Integer> mgSpuBizSummaryAllSize = new Ref<Integer>(-1);
            Ref<Param> remoteSpuBizSummaryDataStatusInfo = new Ref<Param>();
            Ref<Param> productSpuBizSummaryDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
            if(!mgSpuBizSummarySearchMatcher.isEmpty()){
                // 首先判断本地缓存的数据和状态
                checkDataStatus(aid, unionPriId, flow, unionPriId, mgSpuBizSummarySearchMatcher, maxChangeTime, mgSpuBizSummaryDataIsChange, mgSpuBizSummaryAllSize, mockDataStatusInfo, remoteSpuBizSummaryDataStatusInfo, productSpuBizSummaryDataStatusCacheInfo);
            }


            // 判断缓存的时间，是否需要进行重新搜索缓存
            if(resultCacheTime == 0 || resultCacheTime < maxChangeTime.value){
                resultCacheInfo = new Param();
                resultCacheInfo.setLong(MgProductSearchResultEntity.Info.CACHE_TIME, maxChangeTime.value);
                FaiList<Integer> idList = new FaiList<Integer>();
                idList.add(1);
                resultCacheInfo.setList(MgProductSearchResultEntity.Info.ID_LIST, idList);
                resultCacheInfo.setInt(MgProductSearchResultEntity.Info.TOTAL, 30000);
            }


            Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;", aid, unionPriId, tid, productCount, flow);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            session.write(sendBuf);
        }finally {
            long endTime = System.currentTimeMillis();
            Log.logDbg("flow=%d;rt=%d;aid=%d;unionPriId=%d;tid=%d;useTimeMillis=%s;", flow, rt, aid, unionPriId, tid, (endTime - beginTime));
        }
        return rt;
    }

    private void checkDataStatus(int aid, int unionPriId, int flow, int tid, ParamMatcher searchMatcher, Ref<Long> maxChangeTime, Ref<Boolean> mgDataIsChange, Ref<Integer> dataAllSize, Param mockDataStatusInfo, Ref<Param> remoteDataStatusInfo, Ref<Param> localDataStatusCacheInfo){
        // 首先判断本地缓存的数据和状态
        localDataStatusCacheInfo.value = localDataStatusCache.get(getDataStatusCacheKey(aid, unionPriId, MgProductSearch.SearchTableNameEnum.MG_PRODUCT));
        remoteDataStatusInfo.value = mockDataStatusInfo;    // 待获取数据
        Log.logDbg("key=%s;productBasicSearchMatcher=%s;remoteDataStatusInfo=%s;", getDataStatusCacheKey(aid, unionPriId, MgProductSearch.SearchTableNameEnum.MG_PRODUCT), searchMatcher.getSql(), remoteDataStatusInfo.value.toJson());
        if(localDataStatusCacheInfo.value != null && remoteDataStatusInfo.value != null){
            if(localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME) < remoteDataStatusInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME) || localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME) < remoteDataStatusInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
                mgDataIsChange.value = true;
            }
        }else if (localDataStatusCacheInfo.value == null && remoteDataStatusInfo.value != null){
            localDataStatusCacheInfo.value = remoteDataStatusInfo.value; // 赋值到新的 cache
            mgDataIsChange.value = true;
        }else if (localDataStatusCacheInfo.value == null && remoteDataStatusInfo.value == null){
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d; dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid);
        }
        if(maxChangeTime.value < localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME)){
            maxChangeTime.value = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        }
        if(maxChangeTime.value < localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
            maxChangeTime.value = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        }
        dataAllSize.value = localDataStatusCacheInfo.value.getInt(DataStatusCacheInfo.DATA_COUNT);
    }


    private ParamMatcher getBasicMatcher(int aid, int unionPriId){
        ParamMatcher paramMatcher = new ParamMatcher();
        paramMatcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        paramMatcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        return paramMatcher;
    }

    //  ConcurrentHashMap<Integer, ParamCache1>  eg: <unionPriId, ParamCache1>
    private ConcurrentHashMap<Integer, ParamCache1> mgProductCache = new ConcurrentHashMap<Integer, ParamCache1>();

    // 数据的更新时间和总条数的缓存
    private ParamCache1 localDataStatusCache = new ParamCache1();
    static String getDataStatusCacheKey(int aid, int unionPriId, MgProductSearch.SearchTableNameEnum searchTableNameEnum){
        String key = aid + "-" + unionPriId + "-" + searchTableNameEnum.searchTableName;
        return key;
    }
    static final class DataStatusCacheInfo {
        static final String MANAGE_DATA_UPDATE_TIME = "mdut";
        static final String VISTOR_DATA_UPDATE_TIME = "vdut";
        static final String DATA_COUNT = "dc";
    }

    // 搜索结果集的缓存
    private RedisCacheManager m_cache;
    static String getResultCacheKey(int aid, int unionPriId, MgProductSearch mgProductSearch){
        // 根据搜索词的 md5
        String key = aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(mgProductSearch.toString(), "utf-8");
        return key;
    }
}
