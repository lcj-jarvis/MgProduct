package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductSearchSvr.domain.comm.CliFactory;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.netkit.FaiClient;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MgProductSearchService {

    private final MgProductSearchProc searchProc = new MgProductSearchProc();

    /**
     * 支持的搜索表中，除开商品基础表只有 PD_ID, 其他表都有 PD_ID、RL_PD_ID 这两个字段
     */
    @SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        long beginTime = System.currentTimeMillis();
        try{
            Param searchParam = Param.parseParam(searchParamString);
            if(Str.isEmpty(searchParam)){
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; searchParam is null err", flow, aid, unionPriId, tid);
            }

            MgProductSearch mgProductSearch = new MgProductSearch();
            // 初始化 ProductSearch , 即初始化查询条件
            mgProductSearch.initProductSearch(searchParam);

            // 搜索结果的缓存
            String resultCacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, searchParamString);
            Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);

            long resultManageCacheTime = 0L;
            long resultVisitorCacheTime = 0L;
            if(!Str.isEmpty(resultCacheInfo)){
                resultManageCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, 0L);
                resultVisitorCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, 0L);
            }

            // 初始化需要用到的 client
            MgProductBasicCli mgProductBasicCli = CliFactory.getCliInstance(flow, MgProductBasicCli.class);
            MgProductStoreCli mgProductStoreCli = CliFactory.getCliInstance(flow, MgProductStoreCli.class);

            // TODO 后面需要搞为异步获取数据
            // 根据搜索的table的数据大小排序，从小到大排序
            FaiList<Param> searchSorterInfoList = new FaiList<Param>();
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<Long>(0L);
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<Long>(0L);

            // 搜索各个表的数据状态
            eachTableCheckDataStatus(flow, aid, unionPriId, tid,
                                    mgProductBasicCli, mgProductStoreCli, mgProductSearch,
                                    manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            // 根据搜索的表的数据(总记录数)由小到大排序，提高搜索效率
            ParamComparator compSizeForSorter = new ParamComparator(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, false);
            searchSorterInfoList.sort(compSizeForSorter);
            //Log.logDbg("searchSorterInfoList = %s;", searchSorterInfoList);

            // 补充的搜索信息list，比如 排序字段 没有在搜索表中、或者只是搜索了 商品基础表，没有 rlPdId
            ParamComparator paramComparator = mgProductSearch.getParamComparator();
            FaiList<Integer> rlPdIdComparatorList = mgProductSearch.getRlPdIdComparatorList();
            boolean rlPdIdComparatorListNotEmpty = !Util.isEmptyList(rlPdIdComparatorList);
            // 排序的表
            String firstComparatorTable = mgProductSearch.getFirstComparatorTable();
            // 排序的字段
            String firstComparatorKey = mgProductSearch.getFirstComparatorKey();
            // 是否需要 跟进 rlPdId 排序
            boolean isNeedSecondComparatorSorting = mgProductSearch.isNeedSecondComparatorSorting();
            boolean needCompare = !paramComparator.isEmpty();
            // 补充搜索排序表
            String supplementSearchTable = "";
            if(needCompare){
                // 如果有排序, 没有rlPdId排序，并且排序字段不是 PD_ID 和 RL_PD_ID
                boolean hasOtherComparatorKey = !rlPdIdComparatorListNotEmpty &&
                    !ProductEntity.Info.PD_ID.equals(firstComparatorKey) &&
                    !ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey);
                if(hasOtherComparatorKey){
                    boolean findComparatorTable = false;
                    for(Param searchSorterInfo : searchSorterInfoList){
                        String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                        if(tableName.equals(firstComparatorTable)){
                            findComparatorTable = true;
                        }
                    }
                    // 没搜索对应的排序表
                    if(!findComparatorTable){
                        // 设置补偿的表为第一排序表
                        supplementSearchTable = firstComparatorTable;
                    }
                }
            }
            //  只是搜索了商品基础表，需要转化为 RL_PD_ID 的搜索，因为返回的结果是 RL_PD_ID
            //  同时 PD_ID 和 RL_PD_ID 都在搜索表，可以作为排序字段
            boolean onlySearchMgProductTable = Str.isEmpty(supplementSearchTable) &&
                searchSorterInfoList.size() == 1 &&
                searchSorterInfoList.get(0).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE)
                    .equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName);
            if(onlySearchMgProductTable){
                // 增加商品业务关系表进行搜索
                supplementSearchTable = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
            }

            // 补充的表，放最后面搜索
            if(!Str.isEmpty(supplementSearchTable)){
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, supplementSearchTable, mgProductSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 重写排序表, 如果有 rlPdIdComparatorList，则 ProductRelEntity.Info.RL_PD_ID 是最优排序表。
            // 或者只是需要  ProductRelEntity.Info.RL_PD_ID 排序
            boolean rewriteComparatorTable = rlPdIdComparatorListNotEmpty || (Str.isEmpty(firstComparatorTable) && isNeedSecondComparatorSorting);
            if(rewriteComparatorTable){
                for(int i = (searchSorterInfoList.size() - 1); i >=0; i--){
                    String searchTable = searchSorterInfoList.get(i).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if(!searchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)){
                        firstComparatorTable = searchTable;
                        firstComparatorKey = ProductRelEntity.Info.RL_PD_ID;
                        break;
                    }
                }
            }

            // 判断缓存的时间，是否需要进行重新搜索缓存
            // 管理态时间变更，影响有管理态字段查询、访客字段查询、结果缓存
            // 访客态时间变更，影响有访客态字段查询 结果缓存
            // resultVisitorCacheTime, 搜索条件里面有 访客字段搜索 才会用到赋值更新这个字段值
            boolean needReload = resultManageCacheTime == 0 || (resultManageCacheTime < manageDataMaxChangeTime.value ||
                (resultVisitorCacheTime != 0 && resultVisitorCacheTime < manageDataMaxChangeTime.value) ||
                resultVisitorCacheTime < visitorDataMaxChangeTime.value);
            Log.logDbg("needReload=%s;resultManageCacheTime=%s;manageDataMaxChangeTime=%s;resultVisitorCacheTime=%s;visitorDataMaxChangeTime=%s;", needReload, resultManageCacheTime, manageDataMaxChangeTime.value, resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            if(needReload) {
                // 初始化需要搜索的数据，从本地缓存获取、或者从远端获取
                // 开始进行 search
                FaiList<Param> resultList = new FaiList<>();
                FaiList<Param> comparatorResultList = null;
                FaiList<Param> includeRlPdIdResultList = null;
                // 最后一次搜索的 table
                String lastSearchTable = "";
                for (Param searchSorterInfo : searchSorterInfoList) {
                    /*
                      SearchData：获取每个表对应的查询条件对应的查询结果
                      resultList：遍历searchSorterInfoList，resultList最后满足
                                  所有表的查询条件的结果（类似于联表查询）。
                    */
                    resultList = searchProc.getSearchDataAndSearchResultList(flow, aid, tid, unionPriId,
                            searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli);
                    // Log.logDbg("searching......,searchTable=%s;resultList=%s;searchSorterInfo=%s;", searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE), resultList, searchSorterInfo);
                    lastSearchTable = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    // Log.logDbg("lastSearchTablexxxx = %s; ", lastSearchTable);
                    if (lastSearchTable.equals(firstComparatorTable)) {
                        comparatorResultList = resultList;
                    }
                    if (!lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)) {
                        //包含rlPdId的查询结果
                        includeRlPdIdResultList = resultList;
                        // Log.logDbg("includeRlPdIdResultList = %s; ", includeRlPdIdResultList);
                    }
                    // Log.logDbg("getSearchResult,lastSearchTable=%s;resultList=%s;", lastSearchTable, resultList);
                    if (resultList == null) {
                        // 搜索结果为空
                        resultList = new FaiList<Param>();
                        break;
                    }
                    if (resultList.isEmpty()) {
                        // 搜索结果为空
                        break;
                    }
                }

                // 根据排序字段对 resultList 进行排序
                boolean isFixedRlPdId = false;
                if(!paramComparator.isEmpty() && !resultList.isEmpty() && !comparatorResultList.isEmpty()){
                    // 如果最后一次的 搜索的表和排序表不一致，需要转换为 排序表
                    if(!Str.isEmpty(firstComparatorTable) && !lastSearchTable.equals(firstComparatorTable)){
                        // 取resultList的pdIdList和comparatorResultList和pdIdList的交集
                        resultList = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                        lastSearchTable = firstComparatorTable;
                    }
                    // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段进行排序
                    if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) &&
                            (ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey) || isNeedSecondComparatorSorting)){
                        if(includeRlPdIdResultList != null){
                            isFixedRlPdId = true;
                            // 设置includeRlPdIdResultList中的rlPdId到resultList中。
                            searchProc.resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                        }else{
                            Log.logErr(rt,"includeRlPdIdResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                        }
                    }
                    // 进行排序
                    resultList.sort(paramComparator);
                    //Log.logDbg("searching......,isFixedRlPdId=%s;lastSearchTable=%s;paramComparator=%s;isNeedSecondComparatorSorting=%s;resultList=%s;includeRlPdIdResultList=%s;", isFixedRlPdId, lastSearchTable, paramComparator.getKeyList(), isNeedSecondComparatorSorting, resultList, includeRlPdIdResultList);
                }

                // Log.logDbg("searching......,lastSearchTable=%s;resultList=%s;includeRlPdIdResultList=%s;comparatorResultList=%s;", lastSearchTable, resultList, includeRlPdIdResultList, comparatorResultList);
                // 需要根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
                // MgProductSearch.SearchTableNameEnum.MG_PRODUCT、
                // MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL、
                // MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY 没必要去重
                boolean needRemoveRepeated = !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName) &&
                    !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName) &&
                    !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName);
                if(needRemoveRepeated){
                    resultList = searchProc.removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);
                }
                resultCacheInfo = new Param();
                // 管理态变更的缓存时间
                resultManageCacheTime =  Math.max(resultManageCacheTime, manageDataMaxChangeTime.value);
                resultCacheInfo.setLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, resultManageCacheTime);

                resultVisitorCacheTime = Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value);
                // 访客态变更的缓存时间
                resultVisitorCacheTime = Math.max(resultVisitorCacheTime, resultManageCacheTime);

                resultCacheInfo.setLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value));
                // 去重后，得到总的条数
                resultCacheInfo.setInt(MgProductSearchResult.Info.TOTAL, resultList.size());

                // 分页
                SearchArg searchArg = new SearchArg();
                Searcher searcher = new Searcher(searchArg);
                mgProductSearch.setSearArgStartAndLimit(searchArg);
                resultList = searcher.getParamList(resultList);

                // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段.如果补充过了,就不用补充了
                //Log.logDbg("resultListFixedRlPdId, lastSearchTable=%s;isFixedRlPdId=%s;resultList=%s;includeRlPdIdResultList=%s;", lastSearchTable, isFixedRlPdId, resultList, includeRlPdIdResultList);
                if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) && !isFixedRlPdId){
                    if(includeRlPdIdResultList != null){
                        // isFixedRlPdId = true;
                        searchProc.resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                    }else{
                        Log.logErr(rt,"includeRlPdIdResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                    }
                }
                // Log.logDbg("getSearchResult  3333,lastSearchTable=%s;resultList=%s;includeRlPdIdResultList=%s;", lastSearchTable, resultList, includeRlPdIdResultList);
                // 排重，并且由 Param 转换为 idList
                FaiList<Integer> idList = searchProc.toIdList(resultList, ProductRelEntity.Info.RL_PD_ID);
                resultCacheInfo.setList(MgProductSearchResult.Info.ID_LIST, idList);

                // 搜索结果进入缓存
                MgProductSearchCache.ResultCache.delCache(resultCacheKey);
                MgProductSearchCache.ResultCache.addCacheInfo(resultCacheKey, resultCacheInfo);
            } else {
                // 从缓存总获取数据
                resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
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

    /**==========================================================checkDataStatus start==========================================================================================================================================================================*/
    private void eachTableCheckDataStatus(int flow, int aid, int unionPriId, int tid,
                                          MgProductBasicCli mgProductBasicCli,
                                          MgProductStoreCli mgProductStoreCli,
                                          MgProductSearch mgProductSearch,
                                          Ref<Long> manageDataMaxChangeTime,
                                          Ref<Long> visitorDataMaxChangeTime,
                                          FaiList<Param> searchSorterInfoList) {
        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName;
        if(!productBindPropDataSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName;
        if(!mgSpuBizSummarySearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 3、"标签业务关系表" mgProductBindTag_xxxx 搜索， 还没有标签功能，暂时没开放
        ParamMatcher mgProductBindTagSearchMatcher = mgProductSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName;
        if (!mgProductBindTagSearchMatcher.isEmpty()) {
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindTagSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }
        
        // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
        ParamMatcher mgProductBindGroupSearchMatcher = mgProductSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName;
        if(!mgProductBindGroupSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
        if(!productRelSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 6、在 "商品基础表" mgProduct_xxxx 搜索
        ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName;
        if(!productBasicSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBasicSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 如果搜索条件的内容为空，直接抛异常
        if(searchSorterInfoList.isEmpty()){
            throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList isEmpty", flow, aid, unionPriId, tid);
        }
    }

    private void checkDataStatus(int flow, int aid, int unionPriId, int tid, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli, String searchTableName, MgProductSearch mgProductSearch, ParamMatcher searchMatcher, Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime, FaiList<Param> searchSorterInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;

        //  各种数据状态的本地缓存
        String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, searchTableName);
        Param localDataStatusCacheInfo = MgProductSearchCache.LocalDataStatusCache.getLocalDataStatusCache(cacheKey);

        // 远端获取各种数据状态
        Param remoteDataStatusInfo = searchProc.getDataStatusInfoFromEachSvr(aid, unionPriId, tid, flow, searchTableName, mgProductBasicCli, mgProductStoreCli);
        if(!Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 是否只查管理态的数据
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
            // 赋值到新的 cache
            localDataStatusCacheInfo = remoteDataStatusInfo;
            needGetDataFromRemote = true;
        }else if (Str.isEmpty(localDataStatusCacheInfo) && Str.isEmpty(remoteDataStatusInfo)){
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;searchTableName=%s;dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid, searchTableName);
        }

        // 各个表 管理态 修改的最新时间
        if(manageDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME)){
            manageDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        }
        // 各个表 访客态 修改的最新时间
        if(!isOnlySearchManageData && visitorDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME)){
            visitorDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatus.Info.TOTAL_SIZE);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        long visitorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);

        // 设置需要搜索的信息
        initSearchSorterInfoList(searchSorterInfoList, dataAllSize, manageDataUpdateTime, visitorDataUpdateTime, searchTableName, needGetDataFromRemote, searchMatcher);
    }


    /**
     * 设置需要搜索的信息
     * @param searchSorterInfoList  保存需要搜索的信息
     * @param dataAllSize 数据的总记录数
     * @param manageDataUpdateTime 管理态最新的更新时间
     * @param visitorDataUpdateTime 访客态最新的更新时间
     * @param searchTableName 搜索的表
     * @param needGetDataFromRemote 是否需要调用其他的cli从db远程获取
     * @param searchMatcher 搜索的条件
     */
    public void initSearchSorterInfoList(FaiList<Param> searchSorterInfoList, int dataAllSize, long manageDataUpdateTime, long visitorDataUpdateTime, String searchTableName, boolean needGetDataFromRemote, ParamMatcher searchMatcher){
        Param info = new Param();
        info.setString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE, searchTableName);
        info.setInt(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(MgProductSearchProc.SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, visitorDataUpdateTime);
        info.setInt(DataStatus.Info.TOTAL_SIZE, dataAllSize);
        info.setObject(MgProductSearchProc.SearchSorterInfo.SEARCH_MATCHER, searchMatcher);
        searchSorterInfoList.add(info);
    }
    /**==========================================================checkDataStatus end==========================================================================================================================================================================*/










































































































    /*@SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        // Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamStr=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);
        long beginTime = System.currentTimeMillis();
        try{
            Param searchParam = Param.parseParam(searchParamString);
            if(Str.isEmpty(searchParam)){
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; searchParam is null err", flow, aid, unionPriId, tid);
            }

            MgProductSearch mgProductSearch = new MgProductSearch();
            // 初始化 ProductSearch
            mgProductSearch.initProductSearch(searchParam);
            // Log.logDbg("md5=%s;searchParam=%s;", MD5Util.MD5Encode(searchParamString, "utf-8"), searchParam.toJson());

            // 搜索结果的缓存
            // String resultCacheKey = getResultCacheKey(aid, unionPriId, searchParamString);
            // Param resultCacheInfo = m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            String resultCacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, searchParamString);
            Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);

            long resultManageCacheTime = 0L;
            long resultVisitorCacheTime = 0L;
            if(!Str.isEmpty(resultCacheInfo)){
                resultManageCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, 0L);
                resultVisitorCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, 0L);
            }
            //Log.logDbg("searchParamString=%s;resultCacheKey=%s;resultCacheInfo=%s;", searchParamString, resultCacheKey, resultCacheInfo == null ? "null" : resultCacheInfo.toJson());

            // init 需要用到的 client
            MgProductBasicCli mgProductBasicCli = CliFactory.getCliInstance(flow, MgProductBasicCli.class);
            MgProductStoreCli mgProductStoreCli = CliFactory.getCliInstance(flow, MgProductStoreCli.class);

            // 后面需要搞为异步获取数据
            // 根据搜索的table的数据大小排序，从小到大排序
            FaiList<Param> searchSorterInfoList = new FaiList<Param>();
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<Long>(0L);
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<Long>(0L);

            //搜索各个表的数据状态
            eachTableCheckDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli,
                    mgProductSearch, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);

            // 根据搜索的表的数据由小到大排序，提高搜索效率
            // ParamComparator compSizeForSorter = new ParamComparator(SearchSorterInfo.DATA_COUNT, false);
            ParamComparator compSizeForSorter = new ParamComparator(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, false);
            //searchSorterInfoList.sort(compSizeForSorter);
            Collections.sort(searchSorterInfoList, compSizeForSorter);
            //Log.logDbg("searchSorterInfoList = =%s;", searchSorterInfoList);

            // 补充的搜索list，比如 排序字段 没有在搜索表中、或者只是搜索了 商品基础表，没有 rlPdId
            ParamComparator paramComparator = mgProductSearch.getParamComparator();
            FaiList<Integer> rlPdIdComparatorList = mgProductSearch.getRlPdIdComparatorList();
            // boolean rlPdIdComparatorListNotEmpty = !Util.isEmptyList(rlPdIdComparatorList);
            boolean rlPdIdComparatorListNotEmpty = rlPdIdComparatorList != null && !rlPdIdComparatorList.isEmpty();
            // 排序的表
            String firstComparatorTable = mgProductSearch.getFirstComparatorTable();
            // 排序的字段
            String firstComparatorKey = mgProductSearch.getFirstComparatorKey();
            // 是否需要 跟进 rlPdId 排序
            boolean isNeedSecondComparatorSorting = mgProductSearch.isNeedSecondComparatorSorting();
            // boolean needCompare = !paramComparator.isEmpty()
            boolean needCompare = false;
            if(!paramComparator.isEmpty()){
                needCompare = true;
            }
            String supplementSearchTable = "";
            //  补充搜索排序表
            if(needCompare){
                // 如果有排序，并且排序字段不是 PD_ID、RL_PD_ID
                // TODO 这个判断条件有点问题，后面再回来看看
                if(!rlPdIdComparatorListNotEmpty &&
                        !ProductEntity.Info.PD_ID.equals(firstComparatorKey) &&
                        !ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey)){
                    boolean findComparatorTable = false;
                    for(Param searchSorterInfo : searchSorterInfoList){
                        //String tableName = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
                        String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                        // findComparatorTable = tableName.equals(firstComparatorTable);
                        if(tableName.equals(firstComparatorTable)){
                            findComparatorTable = true;
                        }
                    }
                    // 没搜索对应的排序表
                    if(!findComparatorTable){
                        supplementSearchTable = firstComparatorTable;
                    }
                }
            }
            //  只是搜索了商品基础表，需要转化为 RL_PD_ID 的搜索，因为返回的结果是 RL_PD_ID
            //  同时 PD_ID 和 RL_PD_ID 都在搜索表，可以作为排序字段
            if(Str.isEmpty(supplementSearchTable) && searchSorterInfoList.size() == 1 &&
                    //searchSorterInfoList.get(0).getString(SearchSorterInfo.SEARCH_TABLE)
                    searchSorterInfoList.get(0).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE)
                            .equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)){
                // 增加商品业务关系表进行搜索
                supplementSearchTable = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
            }

            // 补充的表，放最后面搜索
            if(!Str.isEmpty(supplementSearchTable)){
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, supplementSearchTable, mgProductSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 重写排序表, 如果有 rlPdIdComparatorList，则 ProductRelEntity.Info.RL_PD_ID 是最优排序表。
            // 或者只是需要  ProductRelEntity.Info.RL_PD_ID 排序
            if(rlPdIdComparatorListNotEmpty || (Str.isEmpty(firstComparatorTable) && isNeedSecondComparatorSorting)){
                for(int i = (searchSorterInfoList.size() - 1); i >=0; i--){
                    //String searchTable = searchSorterInfoList.get(i).getString(SearchSorterInfo.SEARCH_TABLE);
                    String searchTable = searchSorterInfoList.get(i).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if(!searchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)){
                        firstComparatorTable = searchTable;
                        firstComparatorKey = ProductRelEntity.Info.RL_PD_ID;
                        break;
                    }
                }
            }

            // 判断缓存的时间，是否需要进行重新搜索缓存
            // 管理态时间变更，影响有管理态字段查询、访客字段查询 结果缓存
            // 访客态时间变更，影响有访客态字段查询 结果缓存
            // resultVistorCacheTime, 搜索条件里面有 访客字段搜索 才会用到赋值更新这个字段值
            boolean needReload = resultManageCacheTime == 0 || (resultManageCacheTime < manageDataMaxChangeTime.value ||
                    (resultVisitorCacheTime != 0 && resultVisitorCacheTime < manageDataMaxChangeTime.value) ||
                    resultVisitorCacheTime < visitorDataMaxChangeTime.value);
            Log.logDbg("needReload=%s;resultManageCacheTime=%s;manageDataMaxChangeTime=%s;resultVistorCacheTime=%s;visitorDataMaxChangeTime=%s;", needReload, resultManageCacheTime, manageDataMaxChangeTime.value, resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            if(needReload){
                // 初始化需要搜索的数据，从本地缓存获取、或者从远端获取
                // 开始进行 search
                FaiList<Param> resultList = new FaiList<Param>();
                FaiList<Param> comparatorResultList = null;
                FaiList<Param> includeRlPdIdResultList = null;
                // 最后一次搜索的 table
                String lastSearchTable = "";
                int searchSorterInfoListSize = searchSorterInfoList.size();
                // Log.logDbg("searchSorterInfoListSize = %s; searchSorterInfoList=%s;", searchSorterInfoListSize, searchSorterInfoList);
                for(int i = 0; i < searchSorterInfoListSize; i++){
                    Param searchSorterInfo = searchSorterInfoList.get(i);
                    //resultList = getSearchDataAndSearchResultList(flow, aid, tid, unionPriId, searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli);
                    resultList = searchProc.getSearchDataAndSearchResultList(flow, aid, tid, unionPriId, searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli);
                    //Log.logDbg("searching......,searchTable=%s;resultList=%s;searchSorterInfo=%s;", searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE), resultList, searchSorterInfo);
                    // lastSearchTable = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
                    lastSearchTable = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    //Log.logDbg("lastSearchTablexxxx = %s; ", lastSearchTable);
                    if(lastSearchTable.equals(firstComparatorTable)){
                        comparatorResultList = resultList;
                    }
                    if(!lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName)){
                        includeRlPdIdResultList = resultList;
                        //Log.logDbg("includeRlPdIdResultList = %s; ", includeRlPdIdResultList);
                    }
                    //Log.logDbg("getSearchResult,lastSearchTable=%s;resultList=%s;", lastSearchTable, resultList);
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

                //  根据排序字段对 resultList 进行排序
                boolean isFixedRlPdId = false;
                if(!paramComparator.isEmpty() && !resultList.isEmpty() && !comparatorResultList.isEmpty()){
                    // 如果最后一次的 搜索的表和排序表不一致，需要转换为 排序表
                    if(!Str.isEmpty(firstComparatorTable) && !lastSearchTable.equals(firstComparatorTable)){
                        // resultList = searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                        resultList = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                        lastSearchTable = firstComparatorTable;
                    }
                    // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段进行排序
                    if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) &&
                            (ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey) || isNeedSecondComparatorSorting)){
                        if(includeRlPdIdResultList != null){
                            isFixedRlPdId = true;
                            searchProc.resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                        }else{
                            Log.logErr(rt,"includeRlPdIdResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                        }
                    }
                    // resultList.sort(paramComparator);
                    Collections.sort(resultList, paramComparator); // 进行排序
                    //Log.logDbg("searching......,isFixedRlPdId=%s;lastSearchTable=%s;paramComparator=%s;isNeedSecondComparatorSorting=%s;resultList=%s;includeRlPdIdResultList=%s;", isFixedRlPdId, lastSearchTable, paramComparator.getKeyList(), isNeedSecondComparatorSorting, resultList, includeRlPdIdResultList);
                }

                // Log.logDbg("searching......,lastSearchTable=%s;resultList=%s;includeRlPdIdResultList=%s;comparatorResultList=%s;", lastSearchTable, resultList, includeRlPdIdResultList, comparatorResultList);
                // 需要根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
                //  MgProductSearch.SearchTableNameEnum.MG_PRODUCT、MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL、MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY 没必要去重
                if(!lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName) &&
                        !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName) &&
                        !lastSearchTable.equals(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName)){
                    //resultList = removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);
                    resultList = searchProc.removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);
                }
                resultCacheInfo = new Param();
                // 管理态变更的缓存时间
                resultManageCacheTime = (resultManageCacheTime < manageDataMaxChangeTime.value) ? manageDataMaxChangeTime.value : resultManageCacheTime;
                resultCacheInfo.setLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, resultManageCacheTime);

                resultVisitorCacheTime = (resultVisitorCacheTime < visitorDataMaxChangeTime.value) ? visitorDataMaxChangeTime.value : resultVisitorCacheTime;
                // 访客态变更的缓存时间
                resultVisitorCacheTime = (resultVisitorCacheTime < resultManageCacheTime) ? resultManageCacheTime : resultVisitorCacheTime;

                resultCacheInfo.setLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, (resultVisitorCacheTime < visitorDataMaxChangeTime.value) ? visitorDataMaxChangeTime.value : resultVisitorCacheTime);
                resultCacheInfo.setInt(MgProductSearchResult.Info.TOTAL, resultList.size());  // 去重后，得到总的条数

                // 分页
                SearchArg searchArg = new SearchArg();
                Searcher searcher = new Searcher(searchArg);
                mgProductSearch.setSearArgStartAndLimit(searchArg);
                resultList = searcher.getParamList(resultList);

                // 判断是否需要补充 ProductRelEntity.Info.RL_PD_ID 字段
                //Log.logDbg("resultListFixedRlPdId, lastSearchTable=%s;isFixedRlPdId=%s;resultList=%s;includeRlPdIdResultList=%s;", lastSearchTable, isFixedRlPdId, resultList, includeRlPdIdResultList);
                if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(lastSearchTable) && !isFixedRlPdId){
                    if(includeRlPdIdResultList != null){
                        isFixedRlPdId = true;
                        //resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                        searchProc.resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                    }else{
                        Log.logErr(rt,"includeRlPdIdResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                    }
                }
                //Log.logDbg("getSearchResult  3333,lastSearchTable=%s;resultList=%s;includeRlPdIdResultList=%s;", lastSearchTable, resultList, includeRlPdIdResultList);
                // 排重，并且由 Param 转换为 idList
                // FaiList<Integer> idList = toIdList(resultList, ProductRelEntity.Info.RL_PD_ID);
                FaiList<Integer> idList = searchProc.toIdList(resultList, ProductRelEntity.Info.RL_PD_ID);
                resultCacheInfo.setList(MgProductSearchResult.Info.ID_LIST, idList);

                // 搜索结果进入缓存
                // m_result_cache.del(resultCacheKey);
                // m_result_cache.setParam(resultCacheKey, resultCacheInfo, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                MgProductSearchCache.ResultCache.delCache(resultCacheKey);
                MgProductSearchCache.ResultCache.addCacheInfo(resultCacheKey, resultCacheInfo);
            }else{
                // 从缓存总获取数据
                //resultCacheInfo = m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
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
    }*/

    /*public void initMgProductSearchService(RedisCacheManager cache, ParamCacheRecycle cacheRecycle){
        m_result_cache = cache;
        m_cacheRecycle = cacheRecycle;
        m_cacheRecycle.addParamCache("localDataStatusCache", m_localDataStatusCache);
    }*/

    /*// 根据 ProductRelEntity.Info.RL_PD_ID 去重
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

    // 由ParamList 提取业务商品 idList
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

    private FaiList<Param> resultListFixedRlPdId(int aid, int unionPriId, int flow, FaiList<Param> resultList, FaiList<Param> includeRlPdIdResultList){
        FaiList<Param> filterList = new FaiList<Param>();
        String key = ProductEntity.Info.PD_ID;
        // 转换为 set 的集合
        HashMap<Integer, Param> searchHashMap = faiListToHashMap(includeRlPdIdResultList, key);
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
        searchList = searcher.getParamList(searchList);
        return searchList;
    }

    private Param getDataStatusInfoFromEachSvr(int aid, int unionPriId, int tid, int flow, String tableName, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli){
        Param remoteDataStatusInfo = new Param();
        //remoteDataStatusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 1614096000000L);  // 2021-02-24 00:00:00
        //remoteDataStatusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 1614182400000L);  // 2021-02-25 15:34:37
        //remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 1000);                           // 1000 的数据量

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductBasicCli.getPdDataStatus(aid, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getPdDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductBasicCli.getPdRelDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdRelDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getPdRelDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            int rt = mgProductBasicCli.getBindPropDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindPropDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindPropDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductBasicCli.getBindGroupDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善, mock 数据
            remoteDataStatusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, 1614096000000L);  // 2021-02-24 00:00:00
            remoteDataStatusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 1614182400000L);  // 2021-02-25 15:34:37
            remoteDataStatusInfo.setInt(DataStatus.Info.TOTAL_SIZE, 0);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductStoreCli.getSpuBizSummaryDataStatus(aid, tid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getSpuBizSummaryDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getSpuBizSummaryDataStatus,remoteDataStatusInfo=%s;", remoteDataStatusInfo);
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
                //Log.logDbg("needGetDataFromRemote=%s;tableName=%s;searchDataList=%s;", needGetDataFromRemote, tableName, searchSorterInfo.getList(SearchSorterInfo.SEARCH_DATA_LIST));
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

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchPdFromDb(aid, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllPdData(aid, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdData, searchDataList=%s;", searchDataList);
            }
        }


        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchPdRelFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdRelFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdRelFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllPdRelData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdRelData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdRelData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindPropFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindPropFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindPropData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindPropData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindPropData, searchDataList=%s;aid=%s;unionPriId=%s;", searchDataList, aid, unionPriId);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindGroupFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchBindGroupFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindGroupFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindGroupData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindGroupData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
            }else{
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductStoreCli.searchSpuBizSummaryFromDb(aid, tid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchSpuBizSummaryFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchSpuBizSummaryFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductStoreCli.getSpuBizSummaryAllData(aid, tid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getSpuBizSummaryAllData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getSpuBizSummaryAllData, searchDataList=%s;", searchDataList);
            }
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
            // 后面上线后需要干掉getSearchResult(searchMatcher, searchDataList, resultList, searchKey) 调用，
            // 因为已经 把 matcher 放到 client 中，远端已经进行过了搜索
            //resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);
            resultList = searchDataList;
        }
        //Log.logDbg("needGetDataFromRemote=%s;needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;resultList=%s;", needGetDataFromRemote, needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold, resultList);
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

    //  各个表从 svr load 数据的排序
    public static int getLoadFromDbThreshold(String tableName){
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Str.isEmpty(conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey))){
            return defaultThreshold;
        }
        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey).getInt(tableName, defaultThreshold);
    }

    // 用于从哪个表的数据开始做数据搜索，做各个表的数据量大小排优处理, 由小表的数据到大表的数据做搜索
    public static final class SearchSorterInfo{
        static final String SEARCH_TABLE = "st";
        static final String DATA_COUNT = "dc";
        static final String NEED_GET_DATA_FROM_REMOTE = "ngdfr";
        static final String SEARCH_DATA_LIST = "sdl";
        static final String SEARCH_MATCHER = "sm";
    }*/

    /*// 本地缓存回收器
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

    // 数据的更新时间和总条数的缓存
    private ParamCache1 m_localDataStatusCache = new ParamCache1();
    public static String getDataStatusCacheKey(int aid, int unionPriId, String searchTableName){
        return aid + "-" + unionPriId + "-" + searchTableName;
    }

    // 搜索结果集的缓存
    private RedisCacheManager m_result_cache;
    public static String getResultCacheKey(int aid, int unionPriId, String searchParamString){
        // 根据搜索词的 md5
        return aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(searchParamString, "utf-8");
    }*/

    /**==========================================================checkDataStatus start==========================================================================================================================================================================
    private void eachTableCheckDataStatus(int flow, int aid, int unionPriId, int tid,
                                          MgProductBasicCli mgProductBasicCli,
                                          MgProductStoreCli mgProductStoreCli,
                                          MgProductSearch mgProductSearch,
                                          Ref<Long> manageDataMaxChangeTime,
                                          Ref<Long> visitorDataMaxChangeTime,
                                          FaiList<Param> searchSorterInfoList) {
        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName;
        if(!productBindPropDataSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName;
        if(!mgSpuBizSummarySearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 3、"标签业务关系表" mgProductBindTag_xxxx 搜索， 还没有标签功能，暂时没开放
        ParamMatcher mgProductBindLableSearchMatcher = mgProductSearch.getProductBindLableSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName;
        if (!mgProductBindLableSearchMatcher.isEmpty()) {
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindLableSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
        ParamMatcher mgProductBindGroupSearchMatcher = mgProductSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName;
        if(!mgProductBindGroupSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
        if(!productRelSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 6、在 "商品基础表" mgProduct_xxxx 搜索
        ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName;
        if(!productBasicSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, searchTableName, mgProductSearch, productBasicSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 如果搜索条件的内容为空，直接抛异常
        if(searchSorterInfoList.isEmpty()){
            throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList.isEmpty", flow, aid, unionPriId, tid);
        }
    }

    private void checkDataStatus(int flow, int aid, int unionPriId, int tid, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli, String searchTableName, MgProductSearch mgProductSearch, ParamMatcher searchMatcher, Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime, FaiList<Param> searchSorterInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;

        //  各种数据状态的本地缓存
        //Param localDataStatusCacheInfo = m_localDataStatusCache.get(getDataStatusCacheKey(aid, unionPriId, searchTableName));
        String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, searchTableName);
        Param localDataStatusCacheInfo = MgProductSearchCache.LocalDataStatusCache.getLocalDataStatusCache(cacheKey);

        // 远端各种数据状态
        //Param remoteDataStatusInfo = getDataStatusInfoFromEachSvr(aid, unionPriId, tid, flow, searchTableName, mgProductBasicCli, mgProductStoreCli);
        Param remoteDataStatusInfo = searchProc.getDataStatusInfoFromEachSvr(aid, unionPriId, tid, flow, searchTableName, mgProductBasicCli, mgProductStoreCli);
        //Log.logDbg("key=%s;searchMatcher=%s;remoteDataStatusInfo=%s;", getDataStatusCacheKey(aid, unionPriId, searchTableName), searchMatcher.getSql(), (remoteDataStatusInfo == null) ? "" : remoteDataStatusInfo.toJson());
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
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;searchTableName=%s;dtaStatusCacheInfo == null && remoteDataStatusInfo == null err", flow, aid, unionPriId, tid, searchTableName);
        }

        // 各个表 管理态 修改的最新时间
        if(manageDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME)){
            manageDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        }
        // 各个表 访客态 修改的最新时间
        if(!isOnlySearchManageData && visitorDataMaxChangeTime.value < localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME)){
            visitorDataMaxChangeTime.value = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatus.Info.TOTAL_SIZE);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        long vistorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);

        // 设置需要搜索的信息
        initSearchSorterInfoList(searchSorterInfoList, dataAllSize, manageDataUpdateTime, vistorDataUpdateTime, searchTableName, needGetDataFromRemote, searchMatcher);
    }

    public void initSearchSorterInfoList(FaiList<Param> searchSorterInfoList, int dataAllSize, long manageDataUpdateTime, long vistorDataUpdateTime, String searchTableName, boolean needGetDataFromRemote, ParamMatcher searchMatcher){
        Param info = new Param();
        info.setString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE, searchTableName);
        info.setInt(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(MgProductSearchProc.SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, vistorDataUpdateTime);
        info.setInt(DataStatus.Info.TOTAL_SIZE, dataAllSize);
        info.setObject(MgProductSearchProc.SearchSorterInfo.SEARCH_MATCHER, searchMatcher);
        searchSorterInfoList.add(info);
    }
    *==========================================================checkDataStatus end==========================================================================================================================================================================
*/
}
