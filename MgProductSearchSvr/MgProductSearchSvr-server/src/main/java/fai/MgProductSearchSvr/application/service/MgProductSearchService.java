package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductSearchSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.MgProductSearchSvr.interfaces.entity.*;
import fai.MgProductStoreSvr.interfaces.entity.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class MgProductSearchService {

    public void initMgProductSearchService(RedisCacheManager cache, ParamCacheRecycle cacheRecycle){
        m_result_cache = cache;
        m_cacheRecycle = cacheRecycle;
        m_cacheRecycle.addParamCache("localDataStatusCache", m_localDataStatusCache);
    }

    // 支持的搜索表中，除开商品基础表只有 PD_ID, 其他表都有 PD_ID、RL_PD_ID 这两个字段
    @SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        //Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamStr=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);
        long beginTime = System.currentTimeMillis();
        try{
            Param searchParam = Param.parseParam(searchParamString);
            if(Str.isEmpty(searchParam)){
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; searchParam is null err", flow, aid, unionPriId, tid);
            }
            if(searchParam.isEmpty()){
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; searchParam is empty err", flow, aid, unionPriId, tid);
            }

            MgProductSearch mgProductSearch = new MgProductSearch();
            mgProductSearch.initProductSearch(searchParam);    // 初始化 ProductSearch
            //Log.logDbg("md5=%s;searchParam=%s;", MD5Util.MD5Encode(searchParamString, "utf-8"), searchParam.toJson());

            // 搜索结果的缓存
            String resultCacheKey = getResultCacheKey(aid, unionPriId, searchParamString);
            Param resultCacheInfo = m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            long resultManageCacheTime = 0L;
            long resultVistorCacheTime = 0L;
            if(!Str.isEmpty(resultCacheInfo)){
                resultManageCacheTime = resultCacheInfo.getLong(MgProductSearchResultEntity.Info.MANAGE_DATA_CACHE_TIME, 0L);
                resultVistorCacheTime = resultCacheInfo.getLong(MgProductSearchResultEntity.Info.VISTOR_DATA_CACHE_TIME, 0L);
            }
            //Log.logDbg("searchParamString=%s;resultCacheKey=%s;resultCacheInfo=%s;", searchParamString, resultCacheKey, resultCacheInfo == null ? "null" : resultCacheInfo.toJson());

            // init 需要用到的 client
            MgProductStoreCli mgProductStoreCli = getMgProductStoreCli(flow);
            MgProductBasicCli mgProductBasicCli = getMgProductBasicCli(flow);

            // 后面需要搞为异步获取数据
            FaiList<Param> searchSorterInfoList = new FaiList<Param>();  // 根据搜索的table的数据大小排序，从小到大排序
            Ref<Long> manageDataMaxChangeTime = new Ref<Long>(0L);  // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> vistorDataMaxChangeTime = new Ref<Long>(0L);  // 用于判断搜索结果的缓存数据是否失效


            // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
            ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
            String searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName;
            if(!productBindPropDataSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
            ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
            searchTableName = MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName;
            if(!mgSpuBizSummarySearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 3、在 "标签业务关系表" mgProductBindLable_xxxx 搜索
            ParamMatcher mgProductBindLableSearchMatcher = mgProductSearch.getProductBindLableSearchMatcher(null);
            searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName;
            if(!mgProductBindLableSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindLableSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
            ParamMatcher mgProductBindGroupSearchMatcher = mgProductSearch.getProductBindGroupSearchMatcher(null);
            searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName;
            if(!mgProductBindGroupSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
            ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
            searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
            if(!productRelSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productRelSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 6、在 "商品基础表" mgProduct_xxxx 搜索
            ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
            searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName;
            if(!productBasicSearchMatcher.isEmpty()){
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBasicSearchMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 如果搜索条件的内容为空，直接抛异常
            if(searchSorterInfoList.isEmpty()){
                throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList.isEmpty", flow, aid, unionPriId, tid);
            }

            // 根据搜索的表的数据由小到大排序，提高搜索效率
            ParamComparator compSizeForSorter = new ParamComparator(SearchSorterInfo.DATA_COUNT, false);
            Collections.sort(searchSorterInfoList, compSizeForSorter);

            // 补充的搜索list，比如 排序字段 没有在搜索表中、或者只是搜索了 商品基础表，没有 rlPdId
            ParamComparator paramComparator = mgProductSearch.getParamComparator();
            String comparatorTable = mgProductSearch.getFirstComparatorTable();   // 排序的表
            boolean needCompare = false;
            if(!paramComparator.isEmpty()){
                needCompare = true;
            }
            String supplementSearchTable = "";
            // 排序字段 没有在搜索表中
            // 如果有排序，并且排序字段不是 PD_ID、RL_PD_ID
            if(needCompare && !ProductEntity.Info.PD_ID.equals(mgProductSearch.getFirstComparatorKey()) && !ProductRelEntity.Info.RL_PD_ID.equals(mgProductSearch.getFirstComparatorKey())){
                boolean findComparatorTable = false;
                for(Param searchSorterInfo : searchSorterInfoList){
                    String tableName = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
                    if(tableName.equals(comparatorTable)){
                        findComparatorTable = true;
                    }
                }
                // 没搜索对应的排序表
                if(!findComparatorTable){
                    supplementSearchTable = comparatorTable;
                }
            }
            //  只是搜索了 商品基础表，需要转化为 RL_PD_ID 的搜索，因为返回的结果是 RL_PD_ID
            if(Str.isEmpty(supplementSearchTable) && searchSorterInfoList.size() == 1 && searchSorterInfoList.get(0).getString(SearchSorterInfo.SEARCH_TABLE).equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)){
                // 增加商品业务关系表进行搜索
                supplementSearchTable = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
            }
            if(!Str.isEmpty(supplementSearchTable)){
                ParamMatcher defaultMatcher = new ParamMatcher();   // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, supplementSearchTable, mgProductSearch, defaultMatcher, manageDataMaxChangeTime, vistorDataMaxChangeTime, searchSorterInfoList);
            }

            // 判断缓存的时间，是否需要进行重新搜索缓存
            // 管理态时间变更，影响有管理态字段查询、访客字段查询 结果缓存
            // 访客态时间变更，影响有访客态字段查询 结果缓存
            // resultVistorCacheTime, 搜索条件里面有 访客字段搜索 才会用到赋值更新这个字段值
            if(resultManageCacheTime == 0 || (resultManageCacheTime < manageDataMaxChangeTime.value || (resultVistorCacheTime != 0 && resultVistorCacheTime < manageDataMaxChangeTime.value) || resultVistorCacheTime < vistorDataMaxChangeTime.value)){

                // 初始化需要搜索的数据，从本地缓存获取、或者从远端获取
                // 开始进行 search
                FaiList<Param> resultList = new FaiList<Param>();
                FaiList<Param> comparatorResultList = null;
                FaiList<Param> lastSecondResultList = null;
                String lastSearchTable = "";  // 最后一次搜索的 table
                int searchSorterInfoListSize = searchSorterInfoList.size();
                for(int i = 0; i < searchSorterInfoListSize; i++){
                    Param searchSorterInfo = searchSorterInfoList.get(i);
                    resultList = getSearchDataAndSearchResultList(flow, aid, tid, unionPriId, searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli);
                    lastSearchTable = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
                    if(lastSearchTable.equals(comparatorTable)){
                        comparatorResultList = resultList;
                    }
                    if(i == (searchSorterInfoListSize - 2)){
                        lastSecondResultList = resultList;
                    }
                    Log.logDbg("getSearchResult,lastSearchTable=%s;resultList=%s;", lastSearchTable, resultList);
                    if(resultList == null){
                        // 搜索结果为空
                        resultList = new FaiList<Param>();
                        break;
                    }
                    if(resultList.isEmpty()){
                        // 搜索结果为空
                        break;
                    }
                }

                boolean isFixedRlPdId = false;
                //  根据排序字段对 resultList 进行排序
                if(!paramComparator.isEmpty() && !resultList.isEmpty() && !comparatorResultList.isEmpty()){
                    // 如果最后一次的 搜索的表和排序表不一致，需要转换为 排序表
                    if(!lastSearchTable.equals(comparatorTable)){
                        resultList = searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                        lastSearchTable = comparatorTable;
                    }
                    // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段进行搜索
                    if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) && (ProductRelEntity.Info.RL_PD_ID.equals(mgProductSearch.getFirstComparatorKey()) || mgProductSearch.isNeedSecondComparatorSorting())){
                        if(lastSecondResultList != null){
                            isFixedRlPdId = true;
                            resultListFixedRlPdId(aid, unionPriId, flow, resultList, lastSecondResultList);
                        }else{
                            Log.logErr(rt,"lastSecondResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                        }
                    }
                    Log.logDbg("getSearchResult=%s;lastSearchTable=%s;isNeedSecondComparatorSorting=%s;", isFixedRlPdId, lastSearchTable, mgProductSearch.isNeedSecondComparatorSorting());
                    Collections.sort(resultList, paramComparator); // 进行排序
                }

                //Log.logDbg("getSearchResult 2222,lastSearchTable=%s;resultList=%s;lastSecondResultList=%s;comparatorResultList=%s;", lastSearchTable, resultList, lastSecondResultList, comparatorResultList);
                // 需要根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
                //  MgProductSearch.SearchTableNameEnum.MG_PRODUCT、MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL、MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY 没必要去重
                if(!lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName) && !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName) && !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName)){
                    resultList = removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);
                }
                resultCacheInfo = new Param();
                resultCacheInfo.setLong(MgProductSearchResultEntity.Info.MANAGE_DATA_CACHE_TIME, (resultManageCacheTime < manageDataMaxChangeTime.value ) ? manageDataMaxChangeTime.value : resultManageCacheTime);
                resultCacheInfo.setLong(MgProductSearchResultEntity.Info.VISTOR_DATA_CACHE_TIME, (resultVistorCacheTime < vistorDataMaxChangeTime.value) ? vistorDataMaxChangeTime.value : resultVistorCacheTime);
                resultCacheInfo.setInt(MgProductSearchResultEntity.Info.TOTAL, resultList.size());  // 去重后，得到总的条数

                // 分页
                SearchArg searchArg = new SearchArg();
                Searcher searcher = new Searcher(searchArg);
                mgProductSearch.setSearArgStartAndLimit(searchArg);
                resultList = searcher.getParamList(resultList);

                // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段
                if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) && !isFixedRlPdId){
                    if(lastSecondResultList != null){
                        isFixedRlPdId = true;
                        resultListFixedRlPdId(aid, unionPriId, flow, resultList, lastSecondResultList);
                    }else{
                        Log.logErr(rt,"lastSecondResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                    }
                }
                //Log.logDbg("getSearchResult  3333,lastSearchTable=%s;resultList=%s;lastSecondResultList=%s;", lastSearchTable, resultList, lastSecondResultList);
                // 排重，并且由 Param 转换为 idList
                FaiList<Integer> idList = toIdList(resultList, ProductRelEntity.Info.RL_PD_ID);
                resultCacheInfo.setList(MgProductSearchResultEntity.Info.ID_LIST, idList);

                // 搜索结果进入缓存
                m_result_cache.del(resultCacheKey);
                m_result_cache.setParam(resultCacheKey, resultCacheInfo, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                resultCacheInfo = m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            }
            //Log.logDbg("flow=%s;aid=%d;unionPriId=%d;tid=%d;productCount=%d;", flow, aid, unionPriId, tid, productCount);
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

    // 根据 ProductRelEntity.Info.RL_PD_ID 去重
    private FaiList<Param> removeRepeatedByKey(FaiList<Param> resultList, String resultListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        if(resultList.isEmpty()){
            return filterList;
        }
        HashSet<Integer> idSetList = new HashSet<Integer>();  // 去重集合
        for(Param info : resultList){
            int id = info.getInt(resultListKey);
            if(idSetList.contains(id)){
                continue;
            }
            idSetList.add(id);
            filterList.add(info);
        }
        return filterList;
    }

    // 由ParamList 提取业务商品 idLisst
    private FaiList<Integer> toIdList(FaiList<Param> resultList, String key){
        FaiList<Integer> idList = new FaiList<Integer>();
        for(Param info : resultList){
            Integer keyVal = info.getInt(key);
            if(keyVal != null){
                idList.add(keyVal);
            }
        }
        return idList;
    }

    // 根据 set 的缓存重新过滤数据
    private FaiList<Param> searchListFilterBySearchResultList(FaiList<Param> resultList, String resultListKey, FaiList<Param> searchList, String searchListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        HashSet<Integer> resultSetList = faiListToHashIdSet(resultList, resultListKey);  // 转换为 set 的集合
        for(Param info : searchList){
            if(resultSetList.contains(info.getInt(searchListKey))){
                filterList.add(info);
            }
        }
        return filterList;
    }

    private FaiList<Param> resultListFixedRlPdId(int aid, int unionPriId, int flow, FaiList<Param> resultList, FaiList<Param> lastSecondResultList){
        FaiList<Param> filterList = new FaiList<Param>();
        String key = ProductEntity.Info.PD_ID;
        HashMap<Integer, Param> searchHashMap = faiListToHashMap(lastSecondResultList, key);  // 转换为 set 的集合
        for(Param info : resultList){
            Param matchInfo = searchHashMap.get(info.getInt(key));
            if(matchInfo != null){
                info.assign(matchInfo, ProductRelEntity.Info.RL_PD_ID);
            }else{
                info.assign(info, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
                Log.logErr("matchInfo null err, aid=%d;unionPriId=%d;flow=%d;info=%s;", aid, unionPriId, flow, info);
            }
        }
        return filterList;
    }


    private FaiList<Param> getSearchResult(ParamMatcher searchMatcher, FaiList<Param> searchList, FaiList<Param> resultList, String searchKey){
        // 非第一次搜索，进入 set<id> 的过滤逻辑，减少搜索集合，提高性能
        if(resultList != null && !resultList.isEmpty()){
            searchList = searchListFilterBySearchResultList(resultList, searchKey, searchList, searchKey);
        }
        if(searchList.isEmpty()){
            return searchList;
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        Searcher searcher = new Searcher(searchArg);
        resultList = searcher.getParamList(searchList);
        return resultList;
    }

    private Param getDataStatusInfoFromEachSvr(int aid, int unionPriId, int tid, int flow, String tableName, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli){
        Param remoteDataStatusInfo = new Param();
        remoteDataStatusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 1614096000000L);  // 2021-02-24 00:00:00
        remoteDataStatusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 1614182400000L);  // 2021-02-25 15:34:37
        remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 1000);                           // 1000 的数据量

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 1000);
            return remoteDataStatusInfo;
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 1000);
            // 从远端获取数据, 待完善
            Param testDataStatusInfo = new Param();
            int rt = mgProductBasicCli.getBindPropDataStatus(aid, unionPriId, testDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindPropDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindPropDataStatus, testDataStatusInfo=%s;", testDataStatusInfo);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 5000);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            Param testDataStatusInfo = new Param();
            int rt = mgProductBasicCli.getBindGroupDataStatus(aid, unionPriId, testDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, testDataStatusInfo=%s;", testDataStatusInfo);
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 300);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 3000);
            return remoteDataStatusInfo;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            Param testDataStatusInfo = new Param();
            int rt = mgProductStoreCli.getSpuBizSummaryDataStatus(aid, tid, unionPriId, testDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getSpuBizSummaryDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getSpuBizSummaryDataStatus,testDataStatusInfo=%s;", testDataStatusInfo);
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 1000);
            return remoteDataStatusInfo;
        }
        return remoteDataStatusInfo;
    }

    private FaiList<Param> getSearchDataAndSearchResultList(int flow, int aid, int tid, int unionPriId, Param searchSorterInfo, FaiList<Param> resultList, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli){
        String tableName = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
        // 所有的表都有这个字段，用这个字段作为 filter 的过滤条件
        String searchKey = ProductEntity.Info.PD_ID;
        ParamMatcher searchMatcher = (ParamMatcher) searchSorterInfo.getObject(SearchSorterInfo.SEARCH_MATCHER);
        int dataCount = searchSorterInfo.getInt(SearchSorterInfo.DATA_COUNT);
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        boolean needLoadFromDb = dataCount > dataLoadFromDbThreshold;
        if(!needLoadFromDb && searchMatcher.isEmpty() && !resultList.isEmpty()){
            FaiList<Integer> idList = toIdList(resultList, searchKey);
            searchMatcher.and(searchKey, ParamMatcher.IN, idList);
        }
        boolean needGetDataFromRemote = searchSorterInfo.getBoolean(SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE);
        // 直接从缓存中拿
        if(!needGetDataFromRemote){
            ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
            if(localMgProductSearchData.containsKey(getLocalMgProductSearchDataCacheKey(aid, tableName))){
                FaiList<Param> searchList = localMgProductSearchData.get(getLocalMgProductSearchDataCacheKey(aid, tableName));
                searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, searchList);
                resultList = getSearchResult(searchMatcher, searchList, resultList, searchKey);
                Log.logDbg("needGetDataFromRemote=%s;tableName=%s;searchDataList=%s;", needGetDataFromRemote, tableName, searchSorterInfo.getList(SearchSorterInfo.SEARCH_DATA_LIST));
                return resultList;
            }else{
                // 可能缓存被回收了，乐观点，重新获取数据吧
                // searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, new FaiList<Param>());
                Log.logStd("cache1==null; flow=%s;aid=%d;unionPriId=%d;searchSorterInfo=%s;", flow, aid, unionPriId, searchSorterInfo);
            }
        }

        // 需要发包获取数据
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        // 如果需要发包到svr进行搜索，加上 上一次的搜索结果的 idList
        if(needLoadFromDb && !resultList.isEmpty()){
            FaiList<Integer> idList = toIdList(resultList, searchKey);
            searchArg.matcher.and(searchKey, ParamMatcher.IN, idList);
        }
        FaiList<Param> searchDataList = new FaiList<Param>();   // 需要真正获取的数据

        int mockAid = 6370736;
        int mockUnionPriId = 100;
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
            }else{
            }
            Param info = new Param();
            info.setInt(ProductEntity.Info.AID, mockAid);
            info.setInt(ProductEntity.Info.PD_ID, 1);
            info.setString(ProductEntity.Info.NAME, "测试商品");
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductEntity.Info.PD_ID, 5);
            info2.setString(ProductEntity.Info.NAME, "测试商品2");
            searchDataList.add(info2);
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
            info.setInt(ProductEntity.Info.PD_TYPE, 0);
            info.setInt(ProductRelEntity.Info.PD_ID, 1);       //  商品id
            info.setInt(ProductRelEntity.Info.RL_LIB_ID, 1);   // 库 id
            info.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 00:04:12");   // 录入时间
            info.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.UP);   // 上架状态
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductEntity.Info.PD_TYPE, 1);
            info2.setInt(ProductRelEntity.Info.RL_PD_ID, 5);    //  业务商品id
            info2.setInt(ProductRelEntity.Info.PD_ID, 5);       //  商品id
            info2.setInt(ProductRelEntity.Info.RL_LIB_ID, 5);   // 库 id
            info2.setString(ProductRelEntity.Info.ADD_TIME, "2021-03-01 05:04:12");   // 录入时间
            info2.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DOWN);   // 上架状态
            searchDataList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            FaiList tmpSearchDataList = new FaiList<Param>();
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindPropFromDb(aid, unionPriId, searchArg, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindPropFromDb, tmpSearchDataList=%s;", tmpSearchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindGroupData(aid, unionPriId, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindGroupData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, tmpSearchDataList=%s;", tmpSearchDataList);
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
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            FaiList tmpSearchDataList = new FaiList<Param>();
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindGroupFromDb(aid, unionPriId, searchArg, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchBindGroupFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindGroupFromDb, tmpSearchDataList=%s;", tmpSearchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindGroupData(aid, unionPriId, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindGroupData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, tmpSearchDataList=%s;", tmpSearchDataList);
            }

            Param info = new Param();
            info.setInt(ProductBindGroupEntity.Info.AID, mockAid);
            info.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, mockUnionPriId);
            info.setInt(ProductBindGroupEntity.Info.RL_PD_ID, 1);
            info.setInt(ProductBindGroupEntity.Info.PD_ID, 1);       //  商品id
            info.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, 1);
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, 2);
            info2.setInt(ProductBindGroupEntity.Info.PD_ID, 5);       //  商品id
            info2.setInt(ProductBindGroupEntity.Info.RL_PD_ID, 5);
            searchDataList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
            }else{
            }
            Param info = new Param();
            info.setInt("aid", mockAid);
            info.setInt("unionPriId", mockUnionPriId);
            info.setInt("pdId", 1);  // 商品id
            info.setInt("rlPdId", 1);  // 商品业务id
            info.setInt("rlLableId", 3);  // 商品业务标签id
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt("pdId", 5);  // 商品id
            info2.setInt("rlPdId", 5);  // 商品业务id
            info2.setInt("rlLableId", 4);  // 商品业务标签id
            searchDataList.add(info2);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            FaiList tmpSearchDataList = new FaiList<Param>();
            if(needLoadFromDb){
                int rt = mgProductStoreCli.searchSpuBizSummaryFromDb(aid, tid, unionPriId, searchArg, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchSpuBizSummaryFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchSpuBizSummaryFromDb, tmpSearchDataList=%s;", tmpSearchDataList);
            }else{
                int rt = mgProductStoreCli.getSpuBizSummaryAllData(aid, tid, unionPriId, tmpSearchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getSpuBizSummaryAllData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getSpuBizSummaryAllData, tmpSearchDataList=%s;", tmpSearchDataList);
            }

            Param info = new Param();
            info.setInt(SpuBizSummaryEntity.Info.AID, mockAid);
            info.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, mockUnionPriId);
            info.setInt(SpuBizSummaryEntity.Info.PD_ID, 1);    // 商品id
            info.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, 1);  // 商品业务id
            info.setLong(SpuBizSummaryEntity.Info.MIN_PRICE, 50L);   // 最小价格
            info.setLong(SpuBizSummaryEntity.Info.MAX_PRICE, 100L);  // 最大价格
            info.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 8888);  // 虚拟销售量
            info.setInt(SpuBizSummaryEntity.Info.SALES, 100);  // 实际销售量
            info.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 20);  // 商品剩余库存
            searchDataList.add(info);

            Param info2 = info.clone();
            info2.setInt(SpuBizSummaryEntity.Info.PD_ID, 5);           // 商品id
            info2.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, 5);  // 商品业务id
            info2.setLong(SpuBizSummaryEntity.Info.MIN_PRICE, 5000L);   // 最小价格
            info2.setLong(SpuBizSummaryEntity.Info.MAX_PRICE, 10000L);   // 最大价格
            info2.setInt(SpuBizSummaryEntity.Info.VIRTUAL_SALES, 5000);  // 虚拟销售量
            info2.setInt(SpuBizSummaryEntity.Info.SALES, 10);  // 实际销售量
            info2.setInt(SpuBizSummaryEntity.Info.REMAIN_COUNT, 100);  // 商品剩余库存
            searchDataList.add(info2);
        }
        searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, searchDataList);

        // 全量 load 数据时，才做本地缓存
        if(!needLoadFromDb){
            // 进行搜索
            resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);

            // 设置 各个表 的全量本地缓存
            ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
            localMgProductSearchData.put(getLocalMgProductSearchDataCacheKey(aid, tableName), searchDataList);

            // 设置 各个表本地 缓存的时间
            Param dataStatusInfo = new Param();
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.TOTAL_SIZE);
            m_localDataStatusCache.put(getDataStatusCacheKey(aid, unionPriId, tableName), dataStatusInfo);
        }else{
            // 后面上线后需要干掉，因为已经 把 matcher 放到 client 中，远端已经进行过了搜索
            //  log.logDbg(xxxxxxxx)
            resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);
        }
        Log.logDbg("needGetDataFromRemote=%s;needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;resultList=%s;", needGetDataFromRemote, needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold, resultList);
        return resultList;
    }

    // 根据指定的 key，把 FaiList 转换为 map
    public static HashMap<Integer, Param> faiListToHashMap(FaiList<Param> resultList, String key){
        HashMap<Integer, Param> searchHashMap = new HashMap<Integer, Param>();
        for(Param p : resultList){
            searchHashMap.put(p.getInt(key), p);
        }
        return searchHashMap;
    }

    // 根据指定的 key，把 FaiList 转换为 keySet
    public static HashSet<Integer> faiListToHashIdSet(FaiList<Param> resultList, String key){
        HashSet<Integer> searchSetList = new HashSet<Integer>();
        for(Param p : resultList){
            searchSetList.add(p.getInt(key));
        }
        return searchSetList;
    }


    private void checkDataStatus(int flow, int aid, int unionPriId, int tid, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli, String searchTableName, MgProductSearch mgProductSearch, ParamMatcher searchMatcher, Ref<Long> manageDataMaxChangeTime, Ref<Long> vistorDataMaxChangeTime, FaiList<Param> searchSorterInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;

        //  各种数据状态的本地缓存
        Param localDataStatusCacheInfo = m_localDataStatusCache.get(getDataStatusCacheKey(aid, unionPriId, searchTableName));
        // 远端各种数据状态
        Param remoteDataStatusInfo = getDataStatusInfoFromEachSvr(aid, unionPriId, tid, flow, searchTableName, mgProductBasicCli, mgProductStoreCli);
        // Log.logDbg("key=%s;searchMatcher=%s;remoteDataStatusInfo=%s;", getDataStatusCacheKey(aid, unionPriId, searchTableNameEnum.searchTableName), searchMatcher.getSql(), (remoteDataStatusInfo == null) ? "" : remoteDataStatusInfo.toJson());
        if(!Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            isOnlySearchManageData = mgProductSearch.getIsOnlySearchManageData(searchTableName);
            // 管理态数据变动，影响所有的缓存, 因为管理变动可能会导致访客的数据变动
            if(localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME)){
                needGetDataFromRemote = true;
            }else{
                // 如果有搜索访客字段，并且是访客字段时间有变动，需要 reload 数据
                if(!isOnlySearchManageData && localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME)){
                    needGetDataFromRemote = true;
                }
            }
        }else if (Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 本地没有了数据，如果进入搜索逻辑，则需要重新reload数据
            localDataStatusCacheInfo = remoteDataStatusInfo; // 赋值到新的 cache
            needGetDataFromRemote = true;
        }else if (Str.isEmpty(localDataStatusCacheInfo) && Str.isEmpty(remoteDataStatusInfo)){
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid);
        }

        // 各个表 管理态 修改的最新时间
        if(manageDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME)){
            manageDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        }
        // 各个表 访客态 修改的最新时间
        if(!isOnlySearchManageData && vistorDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME)){
            vistorDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatus.Info.TOTAL_SIZE);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        long vistorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);

        // 设置需要搜索的信息
        initSearchSorterInfoList(searchSorterInfoList, dataAllSize, manageDataUpdateTime, vistorDataUpdateTime, searchTableName, needGetDataFromRemote, searchMatcher);
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

    // 本地缓存回收器
    private ParamCacheRecycle m_cacheRecycle;

    //  各个表的本地数据缓存
    //  ConcurrentHashMap<Integer, ParamListCache1>  eg: <unionPriId, ParamCache1>
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
    public static final class SearchSorterInfo{
        static final String SEARCH_TABLE = "st";
        static final String DATA_COUNT = "dc";
        static final String NEED_GET_DATA_FROM_REMOTE = "ngdfr";
        static final String SEARCH_DATA_LIST = "sdl";
        static final String SEARCH_MATCHER = "sm";
    }
    public void initSearchSorterInfoList(FaiList<Param> searchSorterInfoList, int dataAllSize, long manageDataUpdateTime, long vistorDataUpdateTime, String searchTableName, boolean needGetDataFromRemote, ParamMatcher searchMatcher){
        Param info = new Param();
        info.setString(SearchSorterInfo.SEARCH_TABLE, searchTableName);
        info.setInt(SearchSorterInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, vistorDataUpdateTime);
        info.setInt(DataStatus.Info.TOTAL_SIZE, dataAllSize);
        info.setObject(SearchSorterInfo.SEARCH_MATCHER, searchMatcher);
        searchSorterInfoList.add(info);
    }

    // 数据的更新时间和总条数的缓存
    private ParamCache1 m_localDataStatusCache = new ParamCache1();
    public static String getDataStatusCacheKey(int aid, int unionPriId, String searchTableName){
        return aid + "-" + unionPriId + "-" + searchTableName;
    }

    // 搜索结果集的缓存
    private RedisCacheManager m_result_cache;
    public static String getResultCacheKey(int aid, int unionPriId, String searchParamString){
        // 根据搜索词的 md5
        String key = aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(searchParamString, "utf-8");
        return key;
    }


    // 获取 MgProductBasicCli
    private MgProductBasicCli getMgProductBasicCli(int flow){
        MgProductBasicCli m_cli = new MgProductBasicCli(flow);
        if(!m_cli.init()) {
            Log.logErr("init MgProductBasicCli err, flow=%s;", flow);
            m_cli = null;
        }
        return m_cli;
    }

    // 获取 MgProductPropCli
    private MgProductPropCli getMgProductPropCli(int flow){
        MgProductPropCli m_cli = new MgProductPropCli(flow);
        if(!m_cli.init()) {
            Log.logErr("init MgProductPropCli err, flow=%s;", flow);
            m_cli = null;
        }
        return m_cli;
    }

    // 获取 MgProductGroupCli
    private MgProductGroupCli getMgProductGroupCli(int flow){
        MgProductGroupCli m_cli = new MgProductGroupCli(flow);
        if(!m_cli.init()) {
            Log.logErr("init MgProductGroupCli err, flow=%s;", flow);
            m_cli = null;
        }
        return m_cli;
    }

    // 获取 MgProductStoreCli
    private MgProductStoreCli getMgProductStoreCli(int flow){
        MgProductStoreCli m_cli = new MgProductStoreCli(flow);
        if(!m_cli.init()) {
            Log.logErr("init MgProductStoreCli err, flow=%s;", flow);
            m_cli = null;
        }
        return m_cli;
    }
}
