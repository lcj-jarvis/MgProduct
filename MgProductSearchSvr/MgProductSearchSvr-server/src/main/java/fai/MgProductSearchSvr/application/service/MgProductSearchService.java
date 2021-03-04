package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductGroupSvr.interfaces.entity.ProductGroupRelEntity;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.entity.MgProductSearchResultEntity;
import fai.MgProductSearchSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.config.FaiConfig;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.Collections;
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


            FaiList<Param> sorterSizeInfoList = new FaiList<Param>();  // 根据搜索的table的数据大小排序，从小到大排序
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
            Ref<Param> remoteProductBasicDataStatusInfo = new Ref<Param>();
            Ref<Param> productBasicDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
            if(!productBasicSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT, productBasicSearchMatcher, maxChangeTime, remoteProductBasicDataStatusInfo, productBasicDataStatusCacheInfo, sorterSizeInfoList);
            }

            // 2、在 "商品业务关系表" mgProductRel_xxxx 搜索
            Ref<Param> remoteProductRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productRelDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
            if(!productRelSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL, productRelSearchMatcher, maxChangeTime, remoteProductRelDataStatusInfo, productRelDataStatusCacheInfo, sorterSizeInfoList);
            }

            // 3、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
            Ref<Param> remoteProductBindPropDataStatusInfo = new Ref<Param>();
            Ref<Param> productBindPropDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
            if(!productBindPropDataSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP, productBindPropDataSearchMatcher, maxChangeTime, remoteProductBindPropDataStatusInfo, productBindPropDataStatusCacheInfo, sorterSizeInfoList);
            }

            // 4、在 "分类业务关系表" mgProductGroupRel_xxxx 搜索
            Ref<Param> remoteProductGroupRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productGroupRelDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher mgProductGroupRelSearchMatcher = mgProductSearch.getProductGroupRelSearchMatcher(null);
            if(!mgProductGroupRelSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_GROUP_REL, mgProductGroupRelSearchMatcher, maxChangeTime, remoteProductGroupRelDataStatusInfo, productGroupRelDataStatusCacheInfo, sorterSizeInfoList);
            }

            // 5、在 "标签业务关系表" mgProductLableRel_xxxx 搜索
            Ref<Param> remoteProductLableRelDataStatusInfo = new Ref<Param>();
            Ref<Param> productLableRelDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher mgProductLableRelSearchMatcher = mgProductSearch.getProductLableRelSearchMatcher(null);
            if(!mgProductLableRelSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL, mgProductLableRelSearchMatcher, maxChangeTime, remoteProductLableRelDataStatusInfo, productLableRelDataStatusCacheInfo, sorterSizeInfoList);
            }

            // 6、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
            Ref<Param> remoteSpuBizSummaryDataStatusInfo = new Ref<Param>();
            Ref<Param> productSpuBizSummaryDataStatusCacheInfo = new Ref<Param>();
            ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
            if(!mgSpuBizSummarySearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY, mgSpuBizSummarySearchMatcher, maxChangeTime, remoteSpuBizSummaryDataStatusInfo, productSpuBizSummaryDataStatusCacheInfo, sorterSizeInfoList);
            }
            // 如果搜索条件的内容为空，直接抛异常
            if(sorterSizeInfoList.isEmpty()){
                throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList.isEmpty err", flow, aid, unionPriId, tid);
            }

            // 判断缓存的时间，是否需要进行重新搜索缓存
            if(resultCacheTime == 0 || resultCacheTime < maxChangeTime.value){
                resultCacheInfo = new Param();
                resultCacheInfo.setLong(MgProductSearchResultEntity.Info.CACHE_TIME, maxChangeTime.value);
                FaiList<Integer> idList = new FaiList<Integer>();
                idList.add(1);
                resultCacheInfo.setList(MgProductSearchResultEntity.Info.ID_LIST, idList);
                resultCacheInfo.setInt(MgProductSearchResultEntity.Info.TOTAL, 30000);


                // 初始化需要搜索的数据，从本地和远端判断
                for(Param sorterSizeInfo : sorterSizeInfoList){
                    getNeedSearchDataList(flow, aid, unionPriId, sorterSizeInfo);
                }

                // 根据搜索的表的数据由小到大排序
                ParamComparator compForSorterSize = new ParamComparator(SorterSizeInfo.DATA_COUNT, false);
                Collections.sort(sorterSizeInfoList, compForSorterSize);
                for(Param sorterSizeInfo : sorterSizeInfoList){
                    Log.logDbg("sorterSizeInfo=%s;", sorterSizeInfo);
                }

            }


            Log.logDbg("flow=%s;aid=%d;unionPriId=%d;tid=%d;productCount=%d;", flow, aid, unionPriId, tid, productCount);
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

    private Param getDataStatusInfoFromEachSvr(String tableName){

        Param remoteDataStatusInfo = new Param();
        remoteDataStatusInfo.setLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME, 1614096000000L);  // 2021-02-24 00:00:00
        remoteDataStatusInfo.setLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME, 1614182400000L);  // 2021-02-25 15:34:37
        remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 1000);                           // 1000 的数据量

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 1000);
            return remoteDataStatusInfo;
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 1000);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 5000);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_GROUP_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 300);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 3000);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatusCacheInfo.DATA_COUNT, 1000);
            return remoteDataStatusInfo;
        }
        return remoteDataStatusInfo;
    }


    private ParamListCache1 getInfoCache(int unionPriId) {
        ParamListCache1 cache = mgProductCache.get(unionPriId);
        if (cache != null) {
            return cache;
        }
        synchronized (mgProductCache) {
            // double check
            cache = mgProductCache.get(unionPriId);
            if (cache != null) {
                return cache;
            }
            cache = new ParamListCache1();
            mgProductCache.put(unionPriId, cache);
            //m_cacheRecycle.addParamCache("mgpd-" + unionPriId, cache);
            return cache;
        }
    }


    private void getNeedSearchDataList(int flow, int aid, int unionPriId, Param sorterSizeInfo){
        String tableName = sorterSizeInfo.getString(SorterSizeInfo.DATA_TABLE);
        int dataCount = sorterSizeInfo.getInt(SorterSizeInfo.DATA_COUNT);
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        boolean needLoadFromDb = dataCount > dataLoadFromDbThreshold;
        Log.logDbg("needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;", needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold);
        boolean needGetDataFromRemote = sorterSizeInfo.getBoolean(SorterSizeInfo.NEED_GET_DATA_FROM_REMOTE);
        if(!needGetDataFromRemote){
            ParamListCache1 cache1 = getInfoCache(unionPriId);
            if(!cache1.containsKey(getMgProductCacheKey(aid, tableName))){
                sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, new FaiList<Param>());
                Log.logStd("cache1 == null; flow=%s;aid=%d;unionPriId=%d;sorterSizeInfo=%s;", flow, aid, unionPriId, sorterSizeInfo);
            }else{
                sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, cache1.get(getMgProductCacheKey(aid, tableName)));
            }
            return;
        }
        int mockAid = 6370736;
        int mockUnionPriId = 100;
        FaiList<Param> mockList = new FaiList<Param>();
        FaiList<Param> searchDataList= null;   // 需要真正获取的数据
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }

            // 从远端获取数据, 待完善
            Param info = new Param();
            info.setInt(ProductEntity.Info.AID, mockAid);
            info.setInt(ProductEntity.Info.PD_ID, 1);
            info.setInt(ProductEntity.Info.PD_TYPE, 0);
            info.setString(ProductEntity.Info.NAME, "测试商品");
            mockList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductEntity.Info.PD_ID, 5);
            info2.setString(ProductEntity.Info.PD_ID, "测试商品2");
            mockList.add(info2);
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }
            // 从远端获取数据, 待完善
            Param info = new Param();
            info.setInt(ProductRelEntity.Info.AID, mockAid);    //  aid
            info.setInt(ProductRelEntity.Info.UNION_PRI_ID, mockUnionPriId);  // 联合主键id
            info.setInt(ProductRelEntity.Info.RL_PD_ID, 1);    //  业务商品id
            info.setInt(ProductRelEntity.Info.PD_ID, 1);       //  商品id
            info.setInt(ProductRelEntity.Info.RL_LIB_ID, 1);   // 库 id
            info.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 00:04:12");   // 录入时间
            info.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.UP);   // 上架状态
            mockList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductRelEntity.Info.RL_PD_ID, 5);    //  业务商品id
            info2.setInt(ProductRelEntity.Info.PD_ID, 5);       //  商品id
            info2.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 05:04:12");   // 录入时间
            mockList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }
            // 从远端获取数据, 待完善
            Param info = new Param();
            info.setInt(ProductBindPropEntity.Info.AID, mockAid);   //  aid
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, mockUnionPriId);  //  业务商品id
            info.setInt(ProductBindPropEntity.Info.PD_ID, 1);  //  商品id
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, 1);  //  业务商品id
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, 1);  //  业务商品参数id
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, 1);  //  商品参数值id
            mockList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductBindPropEntity.Info.PD_ID, 5);  //  商品id
            info2.setInt(ProductBindPropEntity.Info.RL_PD_ID, 5);  //  业务商品id
            info2.setInt(ProductBindPropEntity.Info.RL_PROP_ID, 2);  //  业务商品参数id
            info2.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, 2);  //  商品参数值id
            mockList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_GROUP_REL.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }
            // 从远端获取数据, 待完善
            Param info = new Param();
            info.setInt(ProductGroupRelEntity.Info.AID, mockAid);
            info.setInt(ProductGroupRelEntity.Info.UNION_PRI_ID, mockUnionPriId);
            info.setInt(ProductGroupRelEntity.Info.GROUP_ID, 1);
            info.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 1);
            info.setInt(ProductGroupRelEntity.Info.SORT, 2);
            mockList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductGroupRelEntity.Info.GROUP_ID, 2);
            info2.setInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 2);
            info2.setInt(ProductGroupRelEntity.Info.SORT, 1);
            mockList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }
            // 从远端获取数据, 待完善
            return;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            if(needLoadFromDb){

            }else{

            }
            // 从远端获取数据
            Param info = new Param();
            info.setInt(SpuBizSummaryEntity.Info.AID, mockAid);
            info.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, mockUnionPriId);
            info.setInt(SpuBizSummaryEntity.Info.PD_ID, 1);    // 商品id
            info.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, 1);  // 商品业务id
            info.setInt(SpuBizSummaryEntity.Info.MIN_PRICE, 50);   // 最小价格
            info.setInt(SpuBizSummaryEntity.Info.MAX_PRICE, 100);  // 最大价格
            info.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 8888);  // 虚拟销售量
            info.setInt(SpuBizSummaryEntity.Info.SALES, 100);  // 实际销售量
            info.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 20);  // 商品剩余库存
            mockList.add(info);

            Param info2 = info.clone();
            info2.setInt(SpuBizSummaryEntity.Info.PD_ID, 5);           // 商品id
            info2.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, 5);  // 商品业务id
            info2.setInt(SpuBizSummaryEntity.Info.MIN_PRICE, 5000);   // 最小价格
            info2.setInt(SpuBizSummaryEntity.Info.MAX_PRICE, 10000);   // 最大价格
            info2.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 5000);  // 虚拟销售量
            info2.setInt(SpuBizSummaryEntity.Info.SALES, 10);  // 实际销售量
            info2.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 20);  // 商品剩余库存
            mockList.add(info2);
        }
        sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, mockList);

        // 设置 各个表 的本地缓存
        ParamListCache1 cache1 = getInfoCache(unionPriId);
        cache1.put(getMgProductCacheKey(aid, tableName), mockList);

        // 设置 各个表本地 缓存的时间
        Param dataStatusInfo = new Param();
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.DATA_COUNT);
        localDataStatusCache.put(getDataStatusCacheKey(aid, unionPriId, tableName), dataStatusInfo);
        return;
    }


    private FaiList<Param> getSearchResult(int aid, int unionPriId, int tid, Param sorterSizeInfo, ParamMatcher searchMatcher, Ref<Param> localDataStatusCacheInfo){
        FaiList<Param> searchResultList = new FaiList<Param>();
        String tableName = sorterSizeInfo.getString(SorterSizeInfo.DATA_TABLE);
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){

            return searchResultList;
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){

            return searchResultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){

            return searchResultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_GROUP_REL.searchTableName.equals(tableName)){

            return searchResultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){

            return searchResultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){

            return searchResultList;
        }
        return searchResultList;
    }


    private void checkDataStatus(int flow, int aid, int unionPriId, int tid, MgProductSearch.SearchTableNameEnum searchTableNameEnum, ParamMatcher searchMatcher, Ref<Long> maxChangeTime, Ref<Param> remoteDataStatusInfo, Ref<Param> localDataStatusCacheInfo, FaiList<Param> SorterSizeInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        localDataStatusCacheInfo.value = localDataStatusCache.get(getDataStatusCacheKey(aid, unionPriId, searchTableNameEnum.searchTableName));
        remoteDataStatusInfo.value = getDataStatusInfoFromEachSvr(searchTableNameEnum.searchTableName);    // 待发包从各个获取数据
        Log.logDbg("key=%s;searchMatcher=%s;remoteDataStatusInfo=%s;", getDataStatusCacheKey(aid, unionPriId, searchTableNameEnum.searchTableName), searchMatcher.getSql(), remoteDataStatusInfo.value.toJson());
        if(localDataStatusCacheInfo.value != null && remoteDataStatusInfo.value != null){
            if(localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME) < remoteDataStatusInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME) || localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME) < remoteDataStatusInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
                needGetDataFromRemote = true;
            }
        }else if (localDataStatusCacheInfo.value == null && remoteDataStatusInfo.value != null){
            localDataStatusCacheInfo.value = remoteDataStatusInfo.value; // 赋值到新的 cache
            needGetDataFromRemote= true;
        }else if (localDataStatusCacheInfo.value == null && remoteDataStatusInfo.value == null){
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid);
        }
        if(maxChangeTime.value < localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME)){
            maxChangeTime.value = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        }
        if(maxChangeTime.value < localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
            maxChangeTime.value = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        }
        int dataAllSize = localDataStatusCacheInfo.value.getInt(DataStatusCacheInfo.DATA_COUNT);
        long manageDataUpdateTime = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        long vistorDataUpdateTime = localDataStatusCacheInfo.value.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        // 设置需要排序的table
        initSorterSizeInfoList(SorterSizeInfoList, dataAllSize, manageDataUpdateTime, vistorDataUpdateTime, searchTableNameEnum, needGetDataFromRemote);
    }


    ParamMatcher getBasicMatcher(int aid, int unionPriId){
        ParamMatcher paramMatcher = new ParamMatcher();
        paramMatcher.and(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        paramMatcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        return paramMatcher;
    }
    //  各个表从 svr load 数据的排序
    public static int getLoadFromDbThreshold(String tableName){
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Str.isEmpty(conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey))){
            return defaultThreshold;
        }
        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey).getInt(tableName, defaultThreshold);
    }

    //  各个表的本地数据缓存
    //  ConcurrentHashMap<Integer, ParamCache1>  eg: <unionPriId, ParamCache1>
    public static ConcurrentHashMap<Integer, ParamListCache1> mgProductCache = new ConcurrentHashMap<Integer, ParamListCache1>();
    public static String getMgProductCacheKey(int aid, String searchTableName){
        return aid + "-" + searchTableName;
    }

    // 用于从哪个表的数据开始做数据搜索，做各个表的数据量大小排优处理
    public static final class SorterSizeInfo{
        static final String DATA_TABLE = "dt";
        static final String DATA_COUNT = "dc";
        static final String NEED_GET_DATA_FROM_REMOTE = "dgdfr";
        static final String SEARCH_DATA_LIST = "sdl";
    }
    public void initSorterSizeInfoList(FaiList<Param> SorterSizeInfoList, int dataAllSize, long manageDataUpdateTime, long vistorDataUpdateTime, MgProductSearch.SearchTableNameEnum searchTableNameEnum, boolean needGetDataFromRemote){
        Param info = new Param();
        info.setString(SorterSizeInfo.DATA_TABLE, searchTableNameEnum.searchTableName);
        info.setInt(SorterSizeInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(SorterSizeInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME, vistorDataUpdateTime);
        info.setInt(DataStatusCacheInfo.DATA_COUNT, dataAllSize);
        SorterSizeInfoList.add(info);
    }

    // 数据的更新时间和总条数的缓存
    public static ParamCache1 localDataStatusCache = new ParamCache1();
    public static String getDataStatusCacheKey(int aid, int unionPriId, String searchTableName){
        return aid + "-" + unionPriId + "-" + searchTableName;
    }
    public static final class DataStatusCacheInfo {
        static final String MANAGE_DATA_UPDATE_TIME = "mdut";
        static final String VISTOR_DATA_UPDATE_TIME = "vdut";
        static final String DATA_COUNT = "dc";
    }

    // 搜索结果集的缓存
    RedisCacheManager m_cache;
    public static String getResultCacheKey(int aid, int unionPriId, MgProductSearch mgProductSearch){
        // 根据搜索词的 md5
        String key = aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(mgProductSearch.toString(), "utf-8");
        return key;
    }
}
