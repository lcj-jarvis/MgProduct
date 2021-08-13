package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.utils.MgProductEsSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductSearchSvr.domain.comm.CliFactory;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc;
import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.app.DocOplogDef;
import fai.app.FaiSearchExDef;
import fai.cli.FaiSearchExCli;
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
    private static final FaiList<String> NOT_HAVE_RLPDID_TABLE = new FaiList<>();

    static {
        NOT_HAVE_RLPDID_TABLE.add(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName);
        NOT_HAVE_RLPDID_TABLE.add(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.searchTableName);
    }

    /**
     * es搜索，搜索字段为name，过滤字段为status （目前门店那边的情景）
     */
    public FaiList<Integer> esSearch(int flow, int aid, int unionPriId, String esSearchParamString) {
        FaiList<Integer> idList = new FaiList<>();
        Param esSearchParam = Param.parseParam(esSearchParamString);
        if (Str.isEmpty(esSearchParam)) {
            Log.logStd("esSearchParam is empty");
            return idList;
        }
        MgProductEsSearch mgProductEsSearch = new MgProductEsSearch();
        mgProductEsSearch.initEsSearch(esSearchParam);

        // 搜索的内容
        // String name = esSearchParam.getString(MgProductEsSearch.EsSearchInfo.NAME);
        // 过滤的内容
        // Integer status = esSearchParam.getInt(MgProductEsSearch.EsSearchInfo.STATUS);
        String name = mgProductEsSearch.getName();
        Integer status = mgProductEsSearch.getStatus();

        FaiSearchExCli cli = new FaiSearchExCli(flow);
        if (!cli.init(FaiSearchExDef.App.MG_PRODUCT, aid)) {
            int rt = Errno.ERROR;
            throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; FaiSearchExCli init err", flow, aid, unionPriId);
        }

        // 设置搜索的内容
        FaiSearchExDef.SearchWord searchWord = FaiSearchExDef.SearchWord.create(name);

        // 搜索字段列表
        FaiList<FaiSearchExDef.SearchField> fields = new FaiList<>();
        // name字段, 并使用了ik分词的字符串
        fields.add(FaiSearchExDef.SearchField.create(MgProductEsSearch.EsSearchInfo.NAME, FaiSearchExDef.SearchField.FieldType.TEXT_IK_CN));

        // 过滤列表
        FaiList<FaiSearchExDef.SearchFilter> filters = new FaiList<>();
        // 等值过滤
        filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchInfo.AID, FaiSearchExDef.SearchField.FieldType.INTEGER, aid));
        filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchInfo.UNIONPRIID, FaiSearchExDef.SearchField.FieldType.INTEGER, unionPriId));
        filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchInfo.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, status));

        // 主键信息列表
        FaiList<DocOplogDef.Docid> resultList = new FaiList<>();
        // 命中条数
        Ref<Long> foundTotalRef = new Ref<>();
        // 全文检索
        cli.fullTextQuery(searchWord, fields, filters, resultList, foundTotalRef);
        Log.logDbg("finish search;aid=%d,unionPriId=%d,fields=%s,filters=%s,resultList=%s,foundTotalSize=%d", aid, unionPriId, fields, filters, resultList, foundTotalRef.value);

        // 获取PdId
        resultList.forEach(docId -> idList.add((Integer) docId.getVal(MgProductEsSearch.EsSearchPrimaryKeyOrder.PDID_ORDER)));
        Log.logDbg("idListOfEsSearch=%s", idList);
        return idList;
    }


    /**
     * 支持的搜索表中，除开商品基础表和商品规格skuCode表只有 PD_ID, 其他表都有 PD_ID、RL_PD_ID 这两个字段
     */
    @SuccessRt(value=Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String esSearchParamString, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        long beginTime = System.currentTimeMillis();
        try{

            //先搜索es, idListOfEsSearch保存es的搜索结果
            FaiList<Integer> idListOfEsSearch = esSearch(flow, aid, unionPriId, esSearchParamString);

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
            MgProductSpecCli mgProductSpecCli = CliFactory.getCliInstance(flow, MgProductSpecCli.class);


            // TODO 后面需要搞为异步获取数据
            // 根据搜索的table的数据大小排序，从小到大排序
            FaiList<Param> searchSorterInfoList = new FaiList<Param>();
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<Long>(0L);
            // 用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<Long>(0L);

            // 搜索各个表的数据状态
            eachTableCheckDataStatus(flow, aid, unionPriId, tid,
                                    mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, mgProductSearch,
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

            /*
              只是搜索了商品基础表和商品规格skuCode表，需要转化为 RL_PD_ID 的搜索（因为有可能需要rlPdId进行排序），
              同时 PD_ID 和 RL_PD_ID 都在搜索表，可以作为排序字段
             */
            boolean onlySearchNotHaveRlPdIdTable = false;
            if (Str.isEmpty(supplementSearchTable) && searchSorterInfoList.size() <= NOT_HAVE_RLPDID_TABLE.size()) {
                int countNotHaveRlPdIdTable = 0;
                for (Param searchSorterInfo:searchSorterInfoList) {
                    String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (NOT_HAVE_RLPDID_TABLE.contains(tableName)) {
                        ++countNotHaveRlPdIdTable;
                    }
                }
                onlySearchNotHaveRlPdIdTable = countNotHaveRlPdIdTable == searchSorterInfoList.size();
            }
            if (onlySearchNotHaveRlPdIdTable) {
                // 增加商品业务关系表进行搜索
                supplementSearchTable = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
            }

            // 补充的表，放最后面搜索
            if(!Str.isEmpty(supplementSearchTable)){
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, supplementSearchTable, mgProductSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 重写排序表, 如果有 rlPdIdComparatorList，则 ProductRelEntity.Info.RL_PD_ID 是最优排序表。
            // 或者只是需要  ProductRelEntity.Info.RL_PD_ID 排序 (如果第一排序表为空，而且设置了第二排序)
            boolean rewriteComparatorTable = rlPdIdComparatorListNotEmpty || (Str.isEmpty(firstComparatorTable) && isNeedSecondComparatorSorting);
            if(rewriteComparatorTable){
                for(int i = (searchSorterInfoList.size() - 1); i >=0; i--){
                    String searchTable = searchSorterInfoList.get(i).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (!NOT_HAVE_RLPDID_TABLE.contains(searchTable)) {
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
                // FaiList<Param> comparatorResultList = null;
                FaiList<Param> comparatorResultList = new FaiList<>();
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
                            searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli);
                    // Log.logDbg("searching......,searchTable=%s;resultList=%s;searchSorterInfo=%s;", searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE), resultList, searchSorterInfo);
                    lastSearchTable = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    // Log.logDbg("lastSearchTablexxxx = %s; ", lastSearchTable);
                    if (lastSearchTable.equals(firstComparatorTable)) {
                        comparatorResultList = resultList;
                    }
                    if (!NOT_HAVE_RLPDID_TABLE.contains(lastSearchTable)) {
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
                    boolean compensateRlPdIdForSort = NOT_HAVE_RLPDID_TABLE.contains(firstComparatorTable) &&
                        (ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey) || isNeedSecondComparatorSorting);
                    if(compensateRlPdIdForSort){
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


                /*
                 需要根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
                 MgProductSearch.SearchTableNameEnum.MG_PRODUCT、
                 MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL、
                 MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY 没必要去重
                 mgProductSpecSkuCode 表中的PdId不是主键，要去重
                 */
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
                if(NOT_HAVE_RLPDID_TABLE.contains(lastSearchTable) && !isFixedRlPdId){
                    if(includeRlPdIdResultList != null){
                        isFixedRlPdId = true;
                        // 设置includeRlPdIdResultList中的rlPdId到resultList中。
                        searchProc.resultListFixedRlPdId(aid, unionPriId, flow, resultList, includeRlPdIdResultList);
                    }else{
                        Log.logErr(rt,"includeRlPdIdResultList null err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                    }
                }
                // Log.logDbg("getSearchResult  3333,lastSearchTable=%s;resultList=%s;includeRlPdIdResultList=%s;", lastSearchTable, resultList, includeRlPdIdResultList);
                // 将原来返回PdIdList
                FaiList<Integer> idList = searchProc.toIdList(resultList, ProductRelEntity.Info.PD_ID);
                resultCacheInfo.setList(MgProductSearchResult.Info.ID_LIST, idList);

                // 搜索结果进入缓存
                MgProductSearchCache.ResultCache.delCache(resultCacheKey);
                MgProductSearchCache.ResultCache.addCacheInfo(resultCacheKey, resultCacheInfo);
            } else {
                // 从缓存总获取数据
                resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
            }

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

    private void eachTableCheckDataStatus(int flow, int aid, int unionPriId, int tid,
                                          MgProductBasicCli mgProductBasicCli,
                                          MgProductStoreCli mgProductStoreCli,
                                          MgProductSpecCli mgProductSpecCli,
                                          MgProductSearch mgProductSearch,
                                          Ref<Long> manageDataMaxChangeTime,
                                          Ref<Long> visitorDataMaxChangeTime,
                                          FaiList<Param> searchSorterInfoList) {
        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher productBindPropDataSearchMatcher = mgProductSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName;
        if(!productBindPropDataSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        ParamMatcher mgSpuBizSummarySearchMatcher = mgProductSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName;
        if(!mgSpuBizSummarySearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 3、"标签业务关系表" mgProductBindTag_xxxx 搜索， 还没有标签功能，暂时没开放
        ParamMatcher mgProductBindTagSearchMatcher = mgProductSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName;
        if (!mgProductBindTagSearchMatcher.isEmpty()) {
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, mgProductBindTagSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }
        
        // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
        ParamMatcher mgProductBindGroupSearchMatcher = mgProductSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName;
        if(!mgProductBindGroupSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
        if(!productRelSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 6、在"商品规格skuCode表" mgProductSpecSkuCode_0xxx 搜索
        ParamMatcher mgProductSpecSkuSearchMatcher = mgProductSearch.getProductSpecSkuCodeSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.searchTableName;
        if (!mgProductSpecSkuSearchMatcher.isEmpty()) {
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, mgProductSpecSkuSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 7、在 "商品基础表" mgProduct_xxxx 搜索
        ParamMatcher productBasicSearchMatcher = mgProductSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName;
        if(!productBasicSearchMatcher.isEmpty()){
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductSearch, productBasicSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 如果搜索条件的内容为空，直接抛异常
        if (searchSorterInfoList.isEmpty()) {
            throw new MgException(Errno.ARGS_ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d;sorterSizeInfoList isEmpty", flow, aid, unionPriId, tid);
        }
    }

    private void checkDataStatus(int flow, int aid, int unionPriId, int tid,
                                 MgProductBasicCli mgProductBasicCli,
                                 MgProductStoreCli mgProductStoreCli,
                                 MgProductSpecCli mgProductSpecCli,
                                 String searchTableName,
                                 MgProductSearch mgProductSearch, ParamMatcher searchMatcher,
                                 Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime,
                                 FaiList<Param> searchSorterInfoList){
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;
        //  各种数据状态的本地缓存
        String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, searchTableName);
        Param localDataStatusCacheInfo = MgProductSearchCache.LocalDataStatusCache.getLocalDataStatusCache(cacheKey);

        // 远端获取各种数据状态
        Param remoteDataStatusInfo = searchProc.getDataStatusInfoFromEachSvr(aid, unionPriId, tid, flow, searchTableName, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli);
        if(!Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 是否只查管理态的数据
            isOnlySearchManageData = mgProductSearch.getIsOnlySearchManageData(searchTableName);
            /*
                （1）管理态数据变动，影响所有的缓存, 因为管理变动可能会导致访客的数据变动
                （2）如果有搜索访客字段，并且是访客字段时间有变动，需要 reload 数据
                 以上两种情况满足其中一个就行
             */
            needGetDataFromRemote = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) ||
                (!isOnlySearchManageData && localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
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
}
