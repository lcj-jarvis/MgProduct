package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.entity.MgProductSearchResultEntity;
import fai.MgProductSearchSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MgProductSearchService {

    public void initMgProductSearchService(RedisCacheManager cache, ParamCacheRecycle cacheRecycle){
        m_result_cache = cache;
        m_cacheRecycle = cacheRecycle;
        m_cacheRecycle.addParamCache("localDataStatusCache", m_localDataStatusCache);
        m_cacheRecycle.addParamCache("localMgProductSearchMapDataCache", m_localMgProductSearchMapDataCache);
    }

    @SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamStr=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);
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
            Ref<Long> manageDataMaxChangeTime = new Ref<Long>(0L);  // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> vistorDataMaxChangeTime = new Ref<Long>(0L);  // 用于判断搜索结果的缓存数据是否失效

            // 搜索结果的缓存
            Param resultCacheInfo = m_result_cache.getParam(getResultCacheKey(aid, unionPriId, mgProductSearch), MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            long resultCacheTime = 0;
            if(resultCacheInfo != null){
                resultCacheTime = resultCacheInfo.getLong(MgProductSearchResultEntity.Info.CACHE_TIME);
                Log.logDbg("key=%s;resultCacheInfo=%s;", getResultCacheKey(aid, unionPriId, mgProductSearch), resultCacheInfo.toJson());
            }

            /* 后面需要搞为异步获取数据 */
            // 1、在 "商品基础表" mgProduct_xxxx 搜索
            ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
            if(!productBasicSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT, mgProductSearch, productBasicSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }

            // 2、在 "商品业务关系表" mgProductRel_xxxx 搜索
            ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
            if(!productRelSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL, mgProductSearch, productRelSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }

            // 3、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
            ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
            if(!productBindPropDataSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP, mgProductSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }

            // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
            ParamMatcher mgProductBindGroupSearchMatcher = mgProductSearch.getProductBindGroupSearchMatcher(null);
            if(!mgProductBindGroupSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP, mgProductSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }

            // 5、在 "标签业务关系表" mgProductBindLable_xxxx 搜索
            ParamMatcher mgProductBindLableSearchMatcher = mgProductSearch.getProductBindLableSearchMatcher(null);
            if(!mgProductBindLableSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL, mgProductSearch, mgProductBindLableSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }

            // 6、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
            ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
            if(!mgSpuBizSummarySearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY, mgProductSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, sorterSizeInfoList);
            }
            // 如果搜索条件的内容为空，直接抛异常
            if(sorterSizeInfoList.isEmpty()){
                throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList.isEmpty err", flow, aid, unionPriId, tid);
            }

            // 判断缓存的时间，是否需要进行重新搜索缓存
            if(resultCacheTime == 0 || (resultCacheTime < manageDataMaxChangeTime.value || resultCacheTime < vistorDataMaxChangeTime.value)){
                // 初始化需要搜索的数据，从本地和远端判断
                for(Param sorterSizeInfo : sorterSizeInfoList){
                    getNeedSearchDataList(flow, aid, unionPriId, sorterSizeInfo);
                }

                // 根据搜索的表的数据由小到大排序
                ParamComparator compSizeForSorter = new ParamComparator(SorterSizeInfo.DATA_COUNT, false);
                Collections.sort(sorterSizeInfoList, compSizeForSorter);

                FaiList<Param> resultList = null;
                boolean isFirstSearch = true;

                // 排序字段相关的数据
                for(Param sorterSizeInfo : sorterSizeInfoList){
                    resultList = getSearchResult(aid, unionPriId, tid, sorterSizeInfo, mgProductSearch, isFirstSearch, resultList);
                    Log.logDbg("getSearchResult resultList = %s;sorterSizeInfo=%s;", resultList, sorterSizeInfo);
                    if(resultList.isEmpty()){
                        // 搜索结果为空
                        break;
                    }
                    isFirstSearch = false;
                }
                //  根据排序字段对 resultList 进行排序




                resultCacheInfo = new Param();
                resultCacheInfo.setLong(MgProductSearchResultEntity.Info.CACHE_TIME, manageDataMaxChangeTime.value);
                FaiList<Integer> idList = new FaiList<Integer>();
                idList.add(1);
                resultCacheInfo.setList(MgProductSearchResultEntity.Info.ID_LIST, idList);
                resultCacheInfo.setInt(MgProductSearchResultEntity.Info.TOTAL, 30000);
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


    // 根据 map 的缓存重新过滤数据
    private FaiList<Param> searchListFilterBySearchMapList(FaiList<Param> searchList, HashMap<Integer, Param> searchMapList){
        FaiList<Param> filterList = new FaiList<Param>();
        for(Param info : searchList){
            if(searchMapList.containsKey(info.getInt(ProductRelEntity.Info.RL_PD_ID))){
                filterList.add(searchMapList.get(info.getInt(ProductRelEntity.Info.RL_PD_ID)));
            }
        }
        return filterList;
    }

    private FaiList<Param> getSearchResult(int aid, int unionPriId, int tid, Param sorterSizeInfo, MgProductSearch mgProductSearch, boolean isFirstSearch, FaiList<Param> resultList){
        ParamMatcher searchMatcher = (ParamMatcher) sorterSizeInfo.getObject(SorterSizeInfo.SEARCH_MATCHER);
        FaiList<Param> searchList = sorterSizeInfo.getList(SorterSizeInfo.SEARCH_DATA_LIST);
        HashMap<Integer, Param> searchMapList = (HashMap<Integer, Param>)sorterSizeInfo.getObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST);
        // 非第一次搜索，进入 map <id, param> 的过滤逻辑，减少搜索集合，提高性能
        if(!isFirstSearch){
            searchList = searchListFilterBySearchMapList(resultList, searchMapList);
        }
        if(searchList.isEmpty()){
            return searchList;
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        Searcher searcher = new Searcher(searchArg);
        resultList = searcher.getParamList(searchList);
        return resultList;
/*
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){

            return resultList;
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){

            return resultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){

            return resultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){

            return resultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){

            return resultList;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){

            return resultList;
        }
        return resultList;*/
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

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
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

    private void getNeedSearchDataList(int flow, int aid, int unionPriId, Param sorterSizeInfo){
        String tableName = sorterSizeInfo.getString(SorterSizeInfo.DATA_TABLE);
        int dataCount = sorterSizeInfo.getInt(SorterSizeInfo.DATA_COUNT);
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        boolean needLoadFromDb = dataCount > dataLoadFromDbThreshold;
        boolean needGetDataFromRemote = sorterSizeInfo.getBoolean(SorterSizeInfo.NEED_GET_DATA_FROM_REMOTE);

        int mockAid = 6370736;
        int mockUnionPriId = 100;

        FaiList<Param> searchDataList = new FaiList<Param>();   // 需要真正获取的数据
        HashMap<Integer, Param> searchMapDataList = new HashMap<Integer, Param>();
        Log.logDbg("needGetDataFromRemote=%s;needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;", needGetDataFromRemote, needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold);
        if(!needGetDataFromRemote){
            ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
            if(!localMgProductSearchData.containsKey(getLocalMgProductSearchDataCacheKey(aid, tableName))){
                sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, new FaiList<Param>());
                sorterSizeInfo.setObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST, searchDataList);
                Log.logStd("cache1 == null; flow=%s;aid=%d;unionPriId=%d;sorterSizeInfo=%s;", flow, aid, unionPriId, sorterSizeInfo);
            }else{
                sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, localMgProductSearchData.get(getLocalMgProductSearchDataCacheKey(aid, tableName)));
                Param localMgProductSearchMapDataCacheInfo = m_localMgProductSearchMapDataCache.get(getLocalMgProductSearchMapDataCacheKey(aid, unionPriId, tableName));
                if(Str.isEmpty(localMgProductSearchMapDataCacheInfo)){
                    sorterSizeInfo.setObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST, searchDataList);
                }else{
                    sorterSizeInfo.setObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST, localMgProductSearchMapDataCacheInfo.getObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST));
                }
            }
            return;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
            }else{
            }
            Param info = new Param();
            info.setInt(ProductEntity.Info.AID, mockAid);
            info.setInt(ProductEntity.Info.PD_ID, 1);
            info.setInt(ProductEntity.Info.PD_TYPE, 0);
            info.setString(ProductEntity.Info.NAME, "测试商品");
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductEntity.Info.PD_ID, 5);
            info2.setString(ProductEntity.Info.NAME, "测试商品2");
            searchDataList.add(info2);

            faiListToHashMap(ProductEntity.Info.PD_ID, searchDataList, searchMapDataList);
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){

            }else{

            }
            Param info = new Param();
            info.setInt(ProductRelEntity.Info.AID, mockAid);    //  aid
            info.setInt(ProductRelEntity.Info.UNION_PRI_ID, mockUnionPriId);  // 联合主键id
            info.setInt(ProductRelEntity.Info.RL_PD_ID, 1);    //  业务商品id
            info.setInt(ProductRelEntity.Info.PD_ID, 1);       //  商品id
            info.setInt(ProductRelEntity.Info.RL_LIB_ID, 1);   // 库 id
            info.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 00:04:12");   // 录入时间
            info.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.UP);   // 上架状态
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductRelEntity.Info.RL_PD_ID, 5);    //  业务商品id
            info2.setInt(ProductRelEntity.Info.PD_ID, 5);       //  商品id
            info2.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 05:04:12");   // 录入时间
            searchDataList.add(info2);

            faiListToHashMap(ProductRelEntity.Info.RL_PD_ID, searchDataList, searchMapDataList);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){

            }else{

            }
            Param info = new Param();
            info.setInt(ProductBindPropEntity.Info.AID, mockAid);   //  aid
            info.setInt(ProductBindPropEntity.Info.UNION_PRI_ID, mockUnionPriId);  //  业务商品id
            info.setInt(ProductBindPropEntity.Info.PD_ID, 1);  //  商品id
            info.setInt(ProductBindPropEntity.Info.RL_PD_ID, 1);  //  业务商品id
            info.setInt(ProductBindPropEntity.Info.RL_PROP_ID, 1);  //  业务商品参数id
            info.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, 1);  //  商品参数值id
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductBindPropEntity.Info.PD_ID, 5);  //  商品id
            info2.setInt(ProductBindPropEntity.Info.RL_PD_ID, 5);  //  业务商品id
            info2.setInt(ProductBindPropEntity.Info.RL_PROP_ID, 2);  //  业务商品参数id
            info2.setInt(ProductBindPropEntity.Info.PROP_VAL_ID, 2);  //  商品参数值id
            searchDataList.add(info2);

            faiListToHashMap(ProductBindPropEntity.Info.RL_PD_ID, searchDataList, searchMapDataList);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){

            }else{

            }
            Param info = new Param();
            info.setInt(ProductBindGroupEntity.Info.AID, mockAid);
            info.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, mockUnionPriId);
            info.setInt(ProductBindGroupEntity.Info.RL_PD_ID, 1);
            info.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, 1);
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, 2);
            info2.setInt(ProductBindGroupEntity.Info.RL_PD_ID, 5);
            searchDataList.add(info2);

            faiListToHashMap(ProductGroupAssocEntity.Info.RL_PD_ID, searchDataList, searchMapDataList);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){

            }else{

            }
            Param info = new Param();
            info.setInt("aid", mockAid);
            info.setInt("unionPriId", mockUnionPriId);
            info.setInt("rlPdId", 1);  // 商品业务id
            info.setInt("rlLableId", 3);  // 商品业务标签id
            searchDataList.add(info);

            Param info2 = info.clone();
            info.setInt("rlPdId", 5);  // 商品业务id
            info2.setInt("rlLableId", 4);  // 商品业务标签id
            searchDataList.add(info2);
            faiListToHashMap("rlPdId", searchDataList, searchMapDataList);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){

            }else{

            }
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
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(SpuBizSummaryEntity.Info.PD_ID, 5);           // 商品id
            info2.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, 5);  // 商品业务id
            info2.setInt(SpuBizSummaryEntity.Info.MIN_PRICE, 5000);   // 最小价格
            info2.setInt(SpuBizSummaryEntity.Info.MAX_PRICE, 10000);   // 最大价格
            info2.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 5000);  // 虚拟销售量
            info2.setInt(SpuBizSummaryEntity.Info.SALES, 10);  // 实际销售量
            info2.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 20);  // 商品剩余库存
            searchDataList.add(info2);

            faiListToHashMap(SpuBizSummaryEntity.Info.RL_PD_ID, searchDataList, searchMapDataList);
        }
        sorterSizeInfo.setList(SorterSizeInfo.SEARCH_DATA_LIST, searchDataList);
        sorterSizeInfo.setObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST, searchMapDataList);

        // 设置 各个表 的本地缓存
        ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
        localMgProductSearchData.put(getLocalMgProductSearchDataCacheKey(aid, tableName), searchDataList);

        //  各个表 的本地缓存 map 的缓存
        Param localMgProductSearchMapDataCacheInfo = new Param();
        localMgProductSearchMapDataCacheInfo.setObject(SorterSizeInfo.SEARCH_MAP_DATA_LIST, searchMapDataList);
        m_localMgProductSearchMapDataCache.put(getLocalMgProductSearchMapDataCacheKey(aid, unionPriId, tableName), localMgProductSearchMapDataCacheInfo);

        // 设置 各个表本地 缓存的时间
        Param dataStatusInfo = new Param();
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        dataStatusInfo.assign(sorterSizeInfo, DataStatusCacheInfo.DATA_COUNT);
        m_localDataStatusCache.put(getDataStatusCacheKey(aid, unionPriId, tableName), dataStatusInfo);
        return;
    }


    public static void faiListToHashMap(String key, FaiList<Param> searchList, HashMap<Integer, Param> mockMapList){
        for(Param p : searchList){
            mockMapList.put(p.getInt(key), p);
        }
    }


    private void checkDataStatus(int flow, int aid, int unionPriId, int tid, MgProductSearch.SearchTableNameEnum searchTableNameEnum, MgProductSearch mgProductSearch, ParamMatcher searchMatcher, Ref<Long> manageDataMaxChangeTime, Ref<Long> vistorDataMaxChangeTime, FaiList<Param> sorterSizeInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = true;

        //  各种数据状态的本地缓存
        Param localDataStatusCacheInfo = m_localDataStatusCache.get(getDataStatusCacheKey(aid, unionPriId, searchTableNameEnum.searchTableName));
        // 远端各种数据状态
        Param remoteDataStatusInfo = getDataStatusInfoFromEachSvr(searchTableNameEnum.searchTableName);
        Log.logDbg("key=%s;searchMatcher=%s;remoteDataStatusInfo=%s;", getDataStatusCacheKey(aid, unionPriId, searchTableNameEnum.searchTableName), searchMatcher.getSql(), (remoteDataStatusInfo == null) ? "" : remoteDataStatusInfo.toJson());
        if(localDataStatusCacheInfo != null && remoteDataStatusInfo != null){
            isOnlySearchManageData = mgProductSearch.getIsOnlySearchManageData(searchTableNameEnum.searchTableName);
            // 管理态数据变动，影响所有的缓存
            if(localDataStatusCacheInfo.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME)){
                needGetDataFromRemote = true;
            }else{
                // 如果有搜索访客字段，并且是访客字段时间有变动，需要 reload 数据
                if(!isOnlySearchManageData && localDataStatusCacheInfo.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
                    needGetDataFromRemote = true;
                }
            }
        }else if (localDataStatusCacheInfo == null && remoteDataStatusInfo != null){
            // 本地没有了数据，如果进入搜索逻辑，则需要重新reload数据
            localDataStatusCacheInfo = remoteDataStatusInfo; // 赋值到新的 cache
            needGetDataFromRemote = true;
        }else if (localDataStatusCacheInfo == null && remoteDataStatusInfo == null){
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid);
        }
        if(manageDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME)){
            manageDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        }
        if(!isOnlySearchManageData && vistorDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME)){
            vistorDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatusCacheInfo.DATA_COUNT);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME);
        long vistorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME);
        // 设置需要排序的table
        initSorterSizeInfoList(sorterSizeInfoList, dataAllSize, manageDataUpdateTime, vistorDataUpdateTime, searchTableNameEnum, needGetDataFromRemote, searchMatcher);
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

    // 缓存回收器
    private ParamCacheRecycle m_cacheRecycle;

    //  各个表的本地数据缓存
    //  ConcurrentHashMap<Integer, ParamCache1>  eg: <unionPriId, ParamCache1>
    private ParamCache1 m_localMgProductSearchMapDataCache = new ParamCache1();
    public static String getLocalMgProductSearchMapDataCacheKey(int aid, int unionPriId, String searchTableName){
        return unionPriId + "-" + aid + "-" + searchTableName;
    }

    private ConcurrentHashMap<Integer, ParamListCache1> m_localMgProductSearchDataCache = new ConcurrentHashMap<Integer, ParamListCache1>();
    public static String getLocalMgProductSearchDataCacheKey(int aid, String searchTableName){
        return aid + "-" + searchTableName;
    }
    private ParamListCache1 getLocalMgProductSearchDataCache(int unionPriId) {
        ParamListCache1 cache = m_localMgProductSearchDataCache.get(unionPriId);
        if (cache != null) {
            return cache;
        }
        synchronized (m_localMgProductSearchDataCache) {
            // double check
            cache = m_localMgProductSearchDataCache.get(unionPriId);
            if (cache != null) {
                return cache;
            }
            cache = new ParamListCache1();
            m_localMgProductSearchDataCache.put(unionPriId, cache);
            m_cacheRecycle.addParamCache("mgpd-" + unionPriId, cache);
            return cache;
        }
    }

    // 用于从哪个表的数据开始做数据搜索，做各个表的数据量大小排优处理, 由小表的数据到大表的数据做搜索
    public static final class SorterSizeInfo{
        static final String DATA_TABLE = "dt";
        static final String DATA_COUNT = "dc";
        static final String NEED_GET_DATA_FROM_REMOTE = "dgdfr";
        static final String SEARCH_DATA_LIST = "sdl";
        static final String SEARCH_MAP_DATA_LIST = "smdl";
        static final String SEARCH_MATCHER = "sm";
    }
    public void initSorterSizeInfoList(FaiList<Param> sorterSizeInfoList, int dataAllSize, long manageDataUpdateTime, long vistorDataUpdateTime, MgProductSearch.SearchTableNameEnum searchTableNameEnum, boolean needGetDataFromRemote, ParamMatcher searchMatcher){
        Param info = new Param();
        info.setString(SorterSizeInfo.DATA_TABLE, searchTableNameEnum.searchTableName);
        info.setInt(SorterSizeInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(SorterSizeInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatusCacheInfo.MANAGE_DATA_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatusCacheInfo.VISTOR_DATA_UPDATE_TIME, vistorDataUpdateTime);
        info.setInt(DataStatusCacheInfo.DATA_COUNT, dataAllSize);
        info.setObject(SorterSizeInfo.SEARCH_MATCHER, searchMatcher);
        sorterSizeInfoList.add(info);
    }

    // 数据的更新时间和总条数的缓存
    private ParamCache1 m_localDataStatusCache = new ParamCache1();
    public static String getDataStatusCacheKey(int aid, int unionPriId, String searchTableName){
        return aid + "-" + unionPriId + "-" + searchTableName;
    }
    public static final class DataStatusCacheInfo {
        static final String MANAGE_DATA_UPDATE_TIME = "mdut";
        static final String VISTOR_DATA_UPDATE_TIME = "vdut";
        static final String DATA_COUNT = "dc";
    }

    // 搜索结果集的缓存
    private RedisCacheManager m_result_cache;
    public static String getResultCacheKey(int aid, int unionPriId, MgProductSearch mgProductSearch){
        // 根据搜索词的 md5
        String key = aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(mgProductSearch.toString(), "utf-8");
        return key;
    }
}
