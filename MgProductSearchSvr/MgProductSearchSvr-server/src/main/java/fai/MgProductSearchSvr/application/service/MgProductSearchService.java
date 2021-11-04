package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.async.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicValObj;
import fai.MgProductInfSvr.interfaces.utils.*;
import fai.MgProductSearchSvr.domain.repository.MgProductSearchCache;
import fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc;
import fai.MgProductSpecSvr.interfaces.cli.async.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.async.MgProductStoreCli;
import fai.app.DocOplogDef;
import fai.app.FaiSearchExDef;
import fai.cli.FaiSearchExCli;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.rpc.client.FaiClientProxyFactory;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static fai.MgProductInfSvr.interfaces.utils.MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL;
import static fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc.DbSearchSorterInfo.*;
import static fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc.EsSearchSorterInfo.*;

/**
 * @author Lu
 * 搜索服务
 */
public class MgProductSearchService {

    private final MgProductSearchProc searchProc = new MgProductSearchProc();

    /**
     * 在es中进行搜索
     * @return 返回搜索到的pdIdList
     */
    public Param esSearch(int flow, int aid, int unionPriId, MgProductSearchArg mgProductSearchArg) {
        FaiList<Integer> esSearchResult = new FaiList<>();
        MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
        if (Objects.isNull(mgProductEsSearch) || mgProductEsSearch.isEmpty()) {
            Log.logStd("mgProductEsSearch is null or mgProductEsSearch search condition is empty;Don't need search from es;flow=%d,aid=%d,unionPriId=%d;mgProductEsSearch=%s;", flow, aid, unionPriId, mgProductEsSearch);
            return new Param();
        }

        // 搜索的字段
        String name = mgProductEsSearch.getSearchKeyWord();
        // 过滤的字段
        Integer status = mgProductEsSearch.getUpSalesStatus();

        FaiSearchExCli cli = new FaiSearchExCli(flow);
        if (!cli.init(FaiSearchExDef.App.MG_PRODUCT, aid)) {
            // es 客户端初始化失败直接抛出异常
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d; es cli init err", flow, aid, unionPriId);
        }

        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        if (Objects.isNull(mgProductDbSearch)) {
           // db 搜索条件为空，这里就设置为es里的分页。默认返回前200条数据。如果在初始化查询条件时设置分页，就不采用默认的。
           cli.setSearchLimit(mgProductSearchArg.getStart(), mgProductSearchArg.getLimit());
        } else {
           // db有搜索条件，最后搜索完再做分页。这里先获取前5000条的es搜索结果。
           // 因为运维那边封装的es，如果没有设置分页，start默认是0，limit默认是100。见FaiSearchExCli就知道。
           // 所以没有做分页的时候，也要设置from和limit。不然只返回前100条数据。
           // TODO: 实际上是要获取全部的数据才合理的。因为运维那边的es设置了分片为5000，如果es的命中条数超过5000，就要分多次去获取数据。(可以开多个线程去分次拿，但是效率也不高)
           //  目前先设置为前5000条，已经可以满足门店那边的搜索
           // from：0 limit = 5000;
           cli.setSearchLimit(0, MgProductEsSearch.ONCE_REQUEST_LIMIT);
        }

        // 设置搜索的内容
        FaiSearchExDef.SearchWord searchWord = FaiSearchExDef.SearchWord.create(name);

        // 搜索字段列表
        FaiList<FaiSearchExDef.SearchField> fields = new FaiList<>();
        // 如果关键词用做商品名称搜索
        if (mgProductEsSearch.isEnableSearchProductName()) {
            // name字段, 并使用了ik分词的字符串
            fields.add(FaiSearchExDef.SearchField.create(MgProductEsSearch.EsSearchFields.NAME, FaiSearchExDef.SearchField.FieldType.TEXT_IK_CN));
        }

        // 过滤列表
        FaiList<FaiSearchExDef.SearchFilter> filters = new FaiList<>();
        // 等值过滤
        filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchFields.AID, FaiSearchExDef.SearchField.FieldType.INTEGER, aid));
        filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchFields.UNIONPRIID, FaiSearchExDef.SearchField.FieldType.INTEGER, unionPriId));
        // 状态不为空，才设置状态
        if (status != null) {
            if (status == BaseMgProductSearch.UpSalesStatusEnum.UP_AND_DOWN_SALES.getUpSalesStatus()) {
                // 上架或者下架的，或者两种都有。使用in过滤
                FaiList<Integer> statusList = new FaiList<>();
                statusList.add(ProductBasicValObj.ProductValObj.Status.UP);
                statusList.add(ProductBasicValObj.ProductValObj.Status.DOWN);
                filters.add(FaiSearchExDef.SearchFilter.createIn(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, statusList));
            } else if (status != BaseMgProductSearch.UpSalesStatusEnum.ALL.getUpSalesStatus()) {
                // 全部状态的
                // 上架、下架、删除。使用in过滤
                FaiList<Integer> statusList = new FaiList<>();
                statusList.add(ProductBasicValObj.ProductValObj.Status.UP);
                statusList.add(ProductBasicValObj.ProductValObj.Status.DOWN);
                statusList.add(ProductBasicValObj.ProductValObj.Status.DEL);
                filters.add(FaiSearchExDef.SearchFilter.createIn(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, statusList));
            } else {
                // 非全部的，单独是某种状态.使用等值过滤
                filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, status));
            }
        }


        // 排序列表, 首先根据第一字段排序, 再根据第二字段排序（如果有第二字段排序）
        FaiList<FaiSearchExDef.SearchSort> sorts = new FaiList<>();
        // 在es中排序
        String comparatorKey = mgProductEsSearch.getFirstComparatorKey();
        Integer comparatorKeyType = mgProductEsSearch.getFirstComparatorKeyType();
        byte sortOrder = mgProductEsSearch.isFirstComparatorKeyOrderByDesc() ? FaiSearchExDef.SearchSort.OrderType.DESC : FaiSearchExDef.SearchSort.OrderType.ASC;
        if (!Str.isEmpty(comparatorKey) && comparatorKeyType != null) {
            // 根据第一字段排序
            sorts.add(FaiSearchExDef.SearchSort.create(comparatorKey, comparatorKeyType.byteValue(), sortOrder));
        }

        // 根据第二字段排序
        if (mgProductEsSearch.isNeedSecondComparatorSorting()) {
            comparatorKey = mgProductEsSearch.getSecondComparatorKey();
            comparatorKeyType = mgProductEsSearch.getSecondComparatorKeyType();
            sortOrder = mgProductEsSearch.isSecondComparatorKeyOrderByDesc() ? FaiSearchExDef.SearchSort.OrderType.DESC : FaiSearchExDef.SearchSort.OrderType.ASC;
            if (!Str.isEmpty(comparatorKey) && comparatorKeyType != null) {
                sorts.add(FaiSearchExDef.SearchSort.create(comparatorKey, comparatorKeyType.byteValue(), sortOrder));
            }
        }

        // 主键信息列表
        FaiList<DocOplogDef.Docid> resultList = new FaiList<>();
        // 命中条数
        Ref<Long> foundTotalRef = new Ref<>();
        // 全文检索.
        int rt ;
        if (!Str.isEmpty(mgProductEsSearch.getSearchKeyWord())) {
            // 关键词搜索
            rt = sorts.isEmpty()? cli.fullTextQuery(searchWord, fields, filters, resultList, foundTotalRef) : cli.fullTextQuery(searchWord, fields, filters, sorts, resultList, foundTotalRef);
        } else {
            // 只搜索状态的条件，不搜索关键字
            rt = cli.completelyFilter(filters, sorts, resultList, foundTotalRef);
        }

        if (rt != Errno.OK) {
            throw new MgException(rt, "es search error;flow=%d,aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s", flow, aid, unionPriId, fields, filters, sorts);
        }
        Log.logStd("finish searching es;flow=%d,foundTotalSize=%d,aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s,resultList=%s,", flow, foundTotalRef.value, aid, unionPriId, fields, filters, sorts, resultList);

        // TODO 可以根据命中条数，并发多个线程去获取es数据

        FaiList<Param> esSearchResultInfoList = new FaiList<>();
        resultList.forEach(docId -> {
            Integer pdId = (Integer) docId.getVal(MgProductEsSearch.EsSearchPrimaryKeyOrder.PDID_ORDER);
            if (!esSearchResult.contains(pdId)) {
                esSearchResult.add(pdId);
                esSearchResultInfoList.add(new Param().setInt(ProductEntity.Info.PD_ID, pdId));
            }
        });
        Param esResultInfo = new Param();
        esResultInfo.setList(PDIDLIST_FROME_ES_SEARCH_RESULT, esSearchResult);
        esResultInfo.setList(ES_SEARCH_RESULT, esSearchResultInfoList);
        esResultInfo.setLong(ES_SEARCH_RESULT_TOTAL, foundTotalRef.value);

        Log.logStd(" finish getting pdIdList from es search result;flow=%d,aid=%d,unionPriId=%d,idList=%s", flow, aid, unionPriId, esSearchResult);
        return esResultInfo;
    }

    @SuccessRt(value = Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid,
                              int productCount, String esSearchParamString, String dbSearchParamString, String pageInfoString) throws IOException {
        int rt = Errno.ERROR;
        long beginTime = System.currentTimeMillis();
        try {
            Param esSearchParam = Param.parseParam(esSearchParamString);
            Param dbSearchParam = Param.parseParam(dbSearchParamString);
            Param pageInfo = Param.parseParam(pageInfoString);
            // 处理搜索的条件
            searchProc.processSearchArgs(esSearchParam, dbSearchParam);

            MgProductEsSearch mgProductEsSearch = null;
            MgProductDbSearch mgProductDbSearch = null;
            if (!esSearchParam.isEmpty()) {
                mgProductEsSearch = new MgProductEsSearch();
                mgProductEsSearch.initSearchParam(esSearchParam);
            }
            if (!dbSearchParam.isEmpty()) {
                mgProductDbSearch = new MgProductDbSearch();
                mgProductDbSearch.initSearchParam(dbSearchParam);
            }
            Log.logStd("flow=%d;aid=%d;unionPriId=%d;mgProductEsSearch=%s;MgProductDbSearch=%s", flow, aid, unionPriId, mgProductEsSearch, mgProductDbSearch);

            // 初始化搜索条件
            MgProductSearchArg mgProductSearchArg = new MgProductSearchArg(mgProductEsSearch, mgProductDbSearch)
                .setStart(pageInfo.getInt(MgProductSearchArg.PageInfo.START))
                .setLimit(pageInfo.getInt(MgProductSearchArg.PageInfo.LIMIT));

            // 分页限制，允许最大的分页为200，如果超过的话，直接抛出异常
            boolean overLimit = mgProductSearchArg.getLimit() > MgProductSearchArg.MAX_LIMIT;
            if (overLimit) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;unionPriId=%d;over paging limit error,maxLimit=200", flow, aid, unionPriId);
            }

            // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
            // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);
            FaiBuffer sendBuf = new FaiBuffer(true);
            String cacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, mgProductSearchArg.getEsSearchParam().toJson(), mgProductSearchArg.getDbSearchParam().toJson());
            // es查询条件不为空，mgProductDbSearch为空，看看是否存在缓存。
            // 【注意】如果初始化时候在mgProductDbSearch中设置了排序，mgProductDbSearch也不为null。
            boolean onlySearchInEs = Objects.nonNull(mgProductEsSearch) && !mgProductEsSearch.isEmpty() && Objects.isNull(mgProductDbSearch)
                || Objects.nonNull(mgProductEsSearch) && !mgProductEsSearch.isEmpty() && Objects.nonNull(mgProductDbSearch) && mgProductDbSearch.isEmpty() && mgProductDbSearch.getParamComparator().isEmpty();
            if (onlySearchInEs && MgProductSearchCache.ResultCache.existsCache(cacheKey)) {
                Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(cacheKey);
                // 如果es搜索条件不为空，db条件为空，就去查询es数据来源的表，获取管理态和访客态最新的修改时间,判断缓存是否有效
                searchProc.asyncGetEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                Long cachedManageDataMaxChangeTime = resultCacheInfo.getLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME);
                Long cachedVisitorDataMaxChangeTime = resultCacheInfo.getLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME);
                // 缓存数据是否失效
                boolean expired = cachedManageDataMaxChangeTime < manageDataMaxChangeTime.value ||
                    cachedVisitorDataMaxChangeTime < visitorDataMaxChangeTime.value ||
                    cachedVisitorDataMaxChangeTime < manageDataMaxChangeTime.value;
                if (expired) {
                    // 缓存过期，重新搜索es，加载新的内容到缓存
                    Log.logStd("mgProductDbSearch is null or mgProductDbSearch search conditions is empty and not have comparator;result cache is invalid;need get result from es once again;flow=%d,aid=%d,unionPriId=%d;mgProductDbSearch=%s", flow, aid, unionPriId, mgProductDbSearch);
                    Param esResultInfo = esSearch(flow, aid, unionPriId, mgProductSearchArg);
                    FaiList<Integer> esSearchResult = esResultInfo.getList(PDIDLIST_FROME_ES_SEARCH_RESULT);
                    Long total = esResultInfo.getLong(ES_SEARCH_RESULT_TOTAL);
                    // 添加缓存
                    resultCacheInfo = searchProc.integrateAndAddCache(esSearchResult, total, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                }
                Log.logStd("mgProductDbSearch is null or mgProductDbSearch search conditions is empty and not have comparator;Don't need to unite db search;finish es search;return es searchResult;flow=%d,aid=%d,unionPriId=%d;resultInfo=%s;mgProductDbSearch=%s", flow, aid, unionPriId, resultCacheInfo, mgProductDbSearch);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                rt = Errno.OK;
                return rt;
            }

            /*
               执行到这里说明是以下三种情况的一种
               (1) “es有搜索条件，mgProductDbSearch为null”的这种情况是无缓存
               (2) es可能有搜索条件可能也没有，db的搜索条件不为空
               (3) mgProductEsSearch和mgProductDbSearch都为null,搜索默认的商品业务表
             */
            // 在es中获取到的idList
            Param esResultInfo = esSearch(flow, aid, unionPriId, mgProductSearchArg);
            FaiList<Integer> esSearchResult = esResultInfo.getList(PDIDLIST_FROME_ES_SEARCH_RESULT);

            if (onlySearchInEs) {
                // es 有搜索条件，mgProductDbSearch为null的情况不存在缓存，添加缓存，返回es的搜索结果
                searchProc.asyncGetEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                Long total = esResultInfo.getLong(ES_SEARCH_RESULT_TOTAL);
                Param resultCacheInfo = searchProc.integrateAndAddCache(esSearchResult, total, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                Log.logStd("mgProductDbSearch is null or mgProductDbSearch search conditions is empty and not have comparator;Don't need to unite db search;only search in ES;finish es search;return es searchResult;flow=%d,aid=%d,unionPriId=%d;mgProductEsSearch=%s;mgProductDbSearch=%s;resultInfo=%s", flow, aid, unionPriId, mgProductEsSearch, mgProductDbSearch, resultCacheInfo);
                rt = Errno.OK;
                return rt;
            }

            // 执行到这里说明mgProductEsSearch和mgProductDbSearch都为null，需要走默认的搜索表
            // 或者es的搜索条件可能为空也可能不为空，db的搜索条件不为空，需要联合db查询
            Param resultCacheInfo = dbSearch(flow, aid, unionPriId, tid, mgProductSearchArg, esResultInfo);
            resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
            session.write(sendBuf);
            rt = Errno.OK;
        } finally {
            long endTime = System.currentTimeMillis();
            Log.logDbg("flow=%d;rt=%d;aid=%d;unionPriId=%d;tid=%d;useTimeMillis=%s;", flow, rt, aid, unionPriId, tid, (endTime - beginTime));
        }
        return rt;
    }

    /**
     * es搜索完后整合db的进行搜索
     * @param mgProductSearchArg 搜索条件
     * @param esResultInfo es的搜索结果
     * @return 返回包含idList的Param
     */
    public Param dbSearch(int flow, int aid, int unionPriId, int tid,
                          MgProductSearchArg mgProductSearchArg, Param esResultInfo) {


        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        FaiList<Integer> pdIdFromEsSearch = esResultInfo.getList(PDIDLIST_FROME_ES_SEARCH_RESULT);
        if (Objects.isNull(pdIdFromEsSearch)) {
            pdIdFromEsSearch = new FaiList<>();
        }
        Log.logStd("need to unite db search;begin invoke uniteDbSearch method;flow=%d,aid=%d,unionPriId=%d,mgProductDbSearch=%s,esSearchResult=%s", flow, aid, unionPriId, mgProductDbSearch, pdIdFromEsSearch);

        // 初始化需要用到的 异步client
        MgProductBasicCli asyncMgProductBasicCli = FaiClientProxyFactory.createProxy(MgProductBasicCli.class);
        MgProductStoreCli asyncMgProductStoreCli = FaiClientProxyFactory.createProxy(MgProductStoreCli.class);
        MgProductSpecCli asyncMgProductSpecCli = FaiClientProxyFactory.createProxy(MgProductSpecCli.class);
        if (mgProductSearchArg.isEmpty()) {
            // 如果查询条件为空的话，就搜索MG_PRODUCT_REL表下的aid + unionPriId 所有数据
            Log.logStd("mgProductSearchArg is empty;get data from mgProductRel table;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            // 空搜索条件的时候，添加默认的排序,根据rlPdId进行降序。
            mgProductDbSearch = new MgProductDbSearch().setFirstComparator(ProductRelEntity.Info.RL_PD_ID, MG_PRODUCT_REL.getSearchTableName(),true);
        }

        // 保存 各个表 管理态 修改的最新时间，相当于线程安全的TreeSet，查询的效率和TreeSet一样都是O(logN)
        final ConcurrentSkipListSet<Long> manageDataChangeTimeSet = new ConcurrentSkipListSet<>();
        // 保存 各个表 访客态 修改的最新时间，相当于线程安全的TreeSet，查询的效率和TreeSet一样都是O(logN)
        final ConcurrentSkipListSet<Long> visitorDataChangeTimeSet = new ConcurrentSkipListSet<>();

        // key：searchKeyWord（关键字）所在的表，value：保存该表对应的搜索条件、数据状态、搜索结果等
        final Map<String, Param> searchKeyWordTable_searchInfo = new ConcurrentHashMap<>(16);
        // key：searchKeyWord（关键字）所在的表 value:该表异步获取数据状态返回的CompletableFuture
        Map<String, CompletableFuture> searchKeyWordTable_dataStatusFuture = new HashMap<>(16);
        // key：searchKeyWord（关键字）所在的表 value:该表对应的搜索条件
        Map<String, ParamMatcher> searchKeyWordTable_searchMatcher = new HashMap<>(16);
        // 异步获取"searchKeyWord（关键字）所在的表"的数据状态
        searchProc.asyncGetSearchKeyWordTableDataStatus(flow, aid, unionPriId, tid, mgProductDbSearch, searchKeyWordTable_dataStatusFuture, searchKeyWordTable_searchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // key: 除searchKeyWord（关键字）之外的查询条件所在的表 value：保存该表对应的搜索条件、数据状态、搜索结果等
        final Map<String, Param> table_searchInfo = new ConcurrentHashMap<>(16);
        // key: 除searchKeyWord（关键字）之外的查询条件所在的表 value:该表异步获取数据状态返回的CompletableFuture
        Map<String, CompletableFuture> table_dataStatusFuture = new HashMap<>(16);
        // key: 除searchKeyWord（关键字）之外的查询条件所在的表 value:该表对应的搜索条件
        Map<String, ParamMatcher> table_searchMatcher = new HashMap<>(16);
        // 获取es的搜索条件，用于判断是否需要获取es数据来源的表的数据状态
        MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
        // 异步获取"除searchKeyWord（关键字）之外的查询条件所在的表"的数据状态
        searchProc.asyncGetSearchTableDataStatus(flow, aid, unionPriId, tid, mgProductDbSearch, mgProductEsSearch, table_dataStatusFuture, table_searchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 回调获取数据状态的结果
        CountDownLatch countDownLatch = new CountDownLatch(searchKeyWordTable_dataStatusFuture.size() + table_dataStatusFuture.size());
        searchProc.callbackGetDataStatus(flow, aid, unionPriId, mgProductDbSearch, searchKeyWordTable_dataStatusFuture, searchKeyWordTable_searchMatcher, manageDataChangeTimeSet, visitorDataChangeTimeSet, searchKeyWordTable_searchInfo, countDownLatch);
        searchProc.callbackGetDataStatus(flow, aid, unionPriId, mgProductDbSearch, table_dataStatusFuture, table_searchMatcher, manageDataChangeTimeSet, visitorDataChangeTimeSet, table_searchInfo, countDownLatch);
        long begin = System.currentTimeMillis();
        try {
            // 等待获取数据状态完成，可以设置回调超时时间
            countDownLatch.await();
            long end = System.currentTimeMillis();
            Log.logStd("finish async get all dataStatus;consume " + (end - begin) + "ms;");
        } catch (InterruptedException e) {
            long end = System.currentTimeMillis();
            throw new MgException(Errno.ERROR, "waiting each cli async get all dataStatus timeout;consume " + (end - begin) + "ms");
        }

        // 获取mgProductDbSearch里设置的排序
        ParamComparator paramComparator = mgProductDbSearch.getParamComparator();
        // 是否需要排序
        boolean needCompare = !paramComparator.isEmpty();
        // 自定义排序的key
        String customComparatorKey = mgProductDbSearch.getCustomComparatorKey();
        // 第一排序的key
        String firstComparatorKey = mgProductDbSearch.getFirstComparatorKey();
        // 第二排序的key
        String secondComparatorKey = mgProductDbSearch.getSecondComparatorKey();
        // 是否有自定义的排序
        boolean hasCustomComparator = mgProductDbSearch.hasCustomComparator();
        // 是否存在第一排序
        boolean hasFirstComparator = mgProductDbSearch.hasFirstComparator();
        // 是否需要第二排序
        boolean needSecondComparatorSorting = mgProductDbSearch.isNeedSecondComparatorSorting();
        // 自定义排序的表
        String customComparatorTable = mgProductDbSearch.getCustomComparatorTable();
        // 第一排序表
        String firstComparatorTable = mgProductDbSearch.getFirstComparatorTable();
        // 第二排序的表
        String secondComparatorTable = mgProductDbSearch.getSecondComparatorTable();

        // key：排序字段所在的表，value：保存该表对应的搜索条件、数据状态、搜索结果等
        final Map<String, Param> comparatorTable_searchInfo = new ConcurrentHashMap<>(16);
        // 用于保存排序表，对应获取数据状态的的DefaultFuture
        Map<String, CompletableFuture> comparatorTable_dataStatusFuture = new HashMap<>(16);
        // 用于保存排序表对应的空搜索条件
        Map<String, ParamMatcher> comparatorTable_searchMatcher = new HashMap<>(16);
        if (needCompare) {
            boolean compensateTable = hasCustomComparator && !table_searchInfo.containsKey(customComparatorTable);
            if (compensateTable) {
                Log.logStd("table_searchInfo don't contains customComparatorTable;need add customComparatorTable to comparatorTable_searchInfo for searching;flow=%d,aid=%d,unionPriId=%d;customComparatorTable=%s", flow, aid, unionPriId, customComparatorTable);
                // 搜索的表中未包含自定义排序字段所在的表
                ParamMatcher defaultMatcher = new ParamMatcher();
                comparatorTable_searchMatcher.put(customComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(flow, aid, unionPriId, tid, customComparatorTable, comparatorTable_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }

            compensateTable = hasFirstComparator && !table_searchInfo.containsKey(firstComparatorTable);
            if (compensateTable) {
                Log.logStd("table_searchInfo don't contains firstComparatorTable;need add firstComparatorTable to comparatorTable_searchInfo for searching;flow=%d,aid=%d,unionPriId=%d,firstComparatorTable=%s", flow, aid, unionPriId, firstComparatorTable);
                // 搜索的表中未包含第一排序字段所在的表
                ParamMatcher defaultMatcher = new ParamMatcher();
                comparatorTable_searchMatcher.put(firstComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(flow, aid, unionPriId, tid, firstComparatorTable, comparatorTable_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }

            compensateTable = needSecondComparatorSorting && !table_searchInfo.containsKey(secondComparatorTable);
            if (compensateTable) {
                Log.logStd("table_searchInfo don't contains secondComparatorTable;need add secondComparatorTable to comparatorTable_searchInfo for searching;flow=%d,aid=%d,unionPriId=%d,secondComparatorTable=%s", flow, aid, unionPriId, secondComparatorTable);
                // 搜索的表中未包含第二排序字段所在的表
                ParamMatcher defaultMatcher = new ParamMatcher();
                comparatorTable_searchMatcher.put(secondComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(flow, aid, unionPriId, tid, secondComparatorTable, comparatorTable_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }
        }

        // 获取搜索结果的缓存
        String esSearchParamString = mgProductSearchArg.getEsSearchParam().toJson();
        String dbSearchParamString = mgProductSearchArg.getDbSearchParam().toJson();
        String resultCacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, esSearchParamString, dbSearchParamString);
        Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
        Log.logStd("flow=%d,aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", flow, aid, unionPriId, resultCacheKey, resultCacheInfo);

        long resultManageCacheTime = 0L;
        long resultVisitorCacheTime = 0L;
        if (!Str.isEmpty(resultCacheInfo)) {
            resultManageCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, 0L);
            resultVisitorCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, 0L);
        }

        // 回调获取"补充搜索的排序字段所在的表"的数据状态
        if (!comparatorTable_dataStatusFuture.isEmpty()) {
            countDownLatch = new CountDownLatch(comparatorTable_dataStatusFuture.size());
            searchProc.callbackGetDataStatus(flow, aid, unionPriId, mgProductDbSearch, comparatorTable_dataStatusFuture, comparatorTable_searchMatcher, manageDataChangeTimeSet, visitorDataChangeTimeSet, comparatorTable_searchInfo, countDownLatch);
            try {
                // 阻塞等待完成
                countDownLatch.await();
                Log.logStd("finish async get compareTable dataStatus;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            } catch (InterruptedException e) {
                throw new MgException(Errno.ERROR, "waiting get compareTable dataStatus;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            }
        }

        // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
        // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);
        // 不用非空判断其实也没问题，默认会搜索MgProductRel表，manageDataChangeTimeSet不可能为空
        manageDataMaxChangeTime.value = manageDataChangeTimeSet.last();
        // 该ConcurrentSkipListSet可能没有元素。因为如果last()方法获取不到，则抛出NoSuchElementException，所以保存访客态的要判空
        if (!visitorDataChangeTimeSet.isEmpty()) {
            visitorDataMaxChangeTime.value = visitorDataChangeTimeSet.last();
        }

        /**
         * 管理态时间变更，影响有管理态字段查询、访客字段查询、结果缓存
         * 访客态时间变更，影响有访客态字段查询 结果缓存
         * resultVisitorCacheTime, 搜索条件里面有“访客字段”搜索，才会用到赋值更新这个字段值
         */
        boolean needReload = resultManageCacheTime == 0 || (resultManageCacheTime < manageDataMaxChangeTime.value ||
            (resultVisitorCacheTime != 0 && resultVisitorCacheTime < manageDataMaxChangeTime.value) ||
            resultVisitorCacheTime < visitorDataMaxChangeTime.value);
        Log.logStd("flow=%d;needReload=%s;resultManageCacheTime=%s;manageDataMaxChangeTime=%s;resultVisitorCacheTime=%s;visitorDataMaxChangeTime=%s;", flow, needReload, resultManageCacheTime, manageDataMaxChangeTime.value, resultVisitorCacheTime, visitorDataMaxChangeTime.value);

        // 是否需要重新加载数据，还是从缓存中获取
        if (needReload) {
            Log.logStd("searchResult Cache is inValid;begin to async get data;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            // searchKeyword（关键词）表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
            Map<String, Map<Integer, Param>> searchKeywordTableMappingPdIdParam = new ConcurrentHashMap<>(16);
            // 除searchKeyWord之外的查询条件(包含排序)所在的表 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
            Map<String, Map<Integer, Param>> tableMappingPdIdParam = new ConcurrentHashMap<>(16);
            // 排序字段所在的表 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
            Map<String, Map<Integer, Param>> comparatorTableMappingPdIdParam = new ConcurrentHashMap<>(16);

            // 获取数据
            searchProc.asyncGetData(flow, aid, tid, unionPriId, searchKeyWordTable_searchInfo, searchKeywordTableMappingPdIdParam,
                table_searchInfo, tableMappingPdIdParam, comparatorTable_searchInfo, comparatorTableMappingPdIdParam,
                asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

            // 关键词的搜索是否为空
            boolean hasSearchKeywordSearch = !searchKeyWordTable_searchInfo.isEmpty() ||
                (mgProductEsSearch != null && !Str.isEmpty(mgProductEsSearch.getSearchKeyWord()));
            // 保存最终的结果
            FaiList<Param> resultList = new FaiList<>();
            if (!table_searchInfo.isEmpty()) {
                Log.logStd("Not only have searchKeyword search;have other search conditions;begin to take the intersection and integrate sort fields;flow=%d;aid=%d;unionPriId=%d;comparatorTableMappingPdIdParam=%s", flow, aid, unionPriId, comparatorTableMappingPdIdParam);
                // 关键字搜索的结果取并集，目前关键字搜索主要用于商品名称，商品条形码，es搜索
                Set<Integer> searchKeywordPdIdSearchResult = new HashSet<>();
                searchKeywordTableMappingPdIdParam.values().forEach(pdId_info -> searchKeywordPdIdSearchResult.addAll(pdId_info.keySet()));
                searchKeywordPdIdSearchResult.addAll(pdIdFromEsSearch);
                Log.logStd("flow=%d;searchKeyword searchResultOfPdIds=%s", flow, searchKeywordPdIdSearchResult);

                // 除searchKeyword外的搜索条件的搜索结果，先根据搜索结果条数的大小排序
                ParamComparator sizeComparator = new ParamComparator(SEARCH_RESULT_SIZE, false);
                FaiList<Param> searchInfoList = table_searchInfo.values().stream().sorted(sizeComparator).collect(Collectors.toCollection(FaiList::new));

                // 搜索结果条数最少的searchInfo
                Param searchInfoOfMinSearchResult = searchInfoList.get(0);
                // 搜索结果条数最少的List
                FaiList<Param> minSearchResultList = searchInfoOfMinSearchResult.getList(SEARCH_RESULT_LIST);
                String table = searchInfoOfMinSearchResult.getString(SEARCH_TABLE);
                // 每张表的搜索结果，pdId -> 保存搜索结果的info
                Collection<Map<Integer, Param>> listOfPdIdMappingInfo = tableMappingPdIdParam.values();

                Set<Integer> pdIds = new HashSet<>(minSearchResultList.size());
                for (Param info : minSearchResultList) {
                    Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
                    // 重复的pdId说明已经判断过是否属于交集了
                    if (pdIds.contains(pdId)) {
                        continue;
                    }
                    pdIds.add(pdId);

                    // 是否属于交集
                    boolean find = true;
                    for (Map<Integer, Param> pdIdMappingInfo : listOfPdIdMappingInfo) {
                        if (!pdIdMappingInfo.containsKey(pdId)) {
                            find = false;
                            break;
                        }
                    }

                    if (find && hasSearchKeywordSearch) {
                        // 是否属于searchKeyword的搜索结果
                        find = searchKeywordPdIdSearchResult.contains(pdId);
                    }

                    if (find) {
                        // 取完交集后的结果

                        // 如果未包含自定义排序字段，则整合自定义排序字段到info中
                        if (hasCustomComparator && !table.equals(customComparatorTable)) {
                            // 通过排序表的名称和pdId来获取排序字段的值
                            Map<Integer, Param> pdId_info = tableMappingPdIdParam.get(customComparatorTable);
                            if (pdId_info == null) {
                                pdId_info = comparatorTableMappingPdIdParam.get(customComparatorTable);
                            }

                            Param containCustomComparatorKeyInfo = pdId_info.get(pdId);
                            info.assign(containCustomComparatorKeyInfo, customComparatorKey);
                        }

                        // 如果未包含第一排序字段，则整合第一排序字段到info中
                        if (hasFirstComparator && !table.equals(firstComparatorTable)) {
                            // 通过排序表的名称和pdId来获取排序字段的值
                            Map<Integer, Param> pdId_info = tableMappingPdIdParam.get(firstComparatorTable);
                            if (pdId_info == null) {
                                pdId_info = comparatorTableMappingPdIdParam.get(firstComparatorTable);
                            }

                            Param containFirstComparatorKeyInfo = pdId_info.get(pdId);
                            info.assign(containFirstComparatorKeyInfo, firstComparatorKey);
                        }

                        // 如果未包含第二排序字段，则整合第二排序字段到info中
                        if (needSecondComparatorSorting && !table.equals(secondComparatorTable)) {
                            // 通过排序表的名称和pdId来获取排序字段的值
                            Map<Integer, Param> pdId_info = tableMappingPdIdParam.get(secondComparatorTable);
                            if (pdId_info == null) {
                                pdId_info = comparatorTableMappingPdIdParam.get(secondComparatorTable);
                            }

                            Param containSecondComparatorKeyInfo = pdId_info.get(pdId);
                            info.assign(containSecondComparatorKeyInfo, secondComparatorKey);
                        }
                        // 添加交集后的结果到resultList
                        resultList.add(info);
                    }
                }
            } else if (hasSearchKeywordSearch) {
                Log.logStd("only have searchKeyword search conditions;flow=%d;aid=%d;unionPriId=%d;comparatorTableMappingPdIdParam=%s", flow, aid, unionPriId, comparatorTableMappingPdIdParam);
                // 执行到这里说明只有关键词的搜索
                FaiList<Param> tempList = new FaiList<>();
                searchKeyWordTable_searchInfo.values().forEach(searchInfo -> tempList.addAll(searchInfo.getList(SEARCH_RESULT_LIST)));
                FaiList<Param> esSearchResult = esResultInfo.getList(ES_SEARCH_RESULT);
                if (!Utils.isEmptyList(esSearchResult)) {
                    tempList.addAll(esSearchResult);
                }

                // 整合排序字段
                Set<Integer> pdIds = new HashSet<>(tempList.size());
                for (Param info : tempList) {
                    Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
                    // 重复的pdId就直接跳过，不用整合了
                    if (pdIds.contains(pdId)) {
                        continue;
                    }
                    pdIds.add(pdId);

                    // 有排序字段，但是因为只是只有关键词搜索，说明排序字段的表，就在关键词搜索的表中
                    if (hasCustomComparator && !info.containsKey(customComparatorKey)) {
                        // 通过排序表的名称和pdId来获取排序字段的值
                        Map<Integer, Param> pdId_info = comparatorTableMappingPdIdParam.get(customComparatorTable);
                        Param containCustomComparatorKeyInfo = pdId_info.get(info.getInt(ProductEntity.Info.PD_ID));
                        info.assign(containCustomComparatorKeyInfo, customComparatorKey);
                    }

                    if (hasFirstComparator && !info.containsKey(firstComparatorKey)) {
                        Map<Integer, Param> pdId_info = comparatorTableMappingPdIdParam.get(firstComparatorTable);
                        Param containFirstComparatorKeyInfo = pdId_info.get(info.getInt(ProductEntity.Info.PD_ID));
                        info.assign(containFirstComparatorKeyInfo, firstComparatorKey);
                    }

                    if (needSecondComparatorSorting && !info.containsKey(secondComparatorKey)) {
                        Map<Integer, Param> pdId_info = comparatorTableMappingPdIdParam.get(secondComparatorTable);
                        Param containSecondComparatorKeyInfo = pdId_info.get(info.getInt(ProductEntity.Info.PD_ID));
                        info.assign(containSecondComparatorKeyInfo, secondComparatorKey);
                    }

                    // 添加结果到resultList
                    resultList.add(info);
                }
            }

            // 执行到这里说明取完交集和将排序字段的值整合到Param中
            Log.logStd("finish taking the intersection and integrating sort fields;flow=%d;aid=%d;unionPriId=%d;finalResult=%s", flow, aid, unionPriId, resultList);

            // 排序优先级：自定义排序 > 第一排序 > 第二排序 > es排序

            // 根据es的结果定制排序，保证es的排序优先级最低
            boolean hasComparatorInEs = (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.hasFirstComparator()) ||
                (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.isNeedSecondComparatorSorting());
            if (!resultList.isEmpty() && hasComparatorInEs) {
                Log.logStd("have comparator in es;Sorted by es pdIds;flow=%d;aid=%d;unionPriId=%d;esPdIds=%s", flow, aid, unionPriId, pdIdFromEsSearch);
                // 根据es的结果进行定制排序
                ParamComparator customComparatorOfEsSearchResult = new ParamComparator();
                customComparatorOfEsSearchResult.addKey(ProductEntity.Info.PD_ID, pdIdFromEsSearch);

                // 先根据es的结果进行排序
                resultList.sort(customComparatorOfEsSearchResult);
            }


            // 根据db的结果进行排序
            if (!resultList.isEmpty() && needCompare) {
                Log.logStd("have comparator in db;flow=%d;aid=%d;unionPriId=%d;paramComparator=%s", flow, aid, unionPriId, paramComparator);
                resultList.sort(paramComparator);
            }

            // 管理态变更的缓存时间
            resultManageCacheTime = Math.max(resultManageCacheTime, manageDataMaxChangeTime.value);
            // 访客态变更的缓存时间
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, resultManageCacheTime);

            Log.logStd("before paging result;aid=%d,unionPriId=%d,resultSize=%d,result=%s", aid, unionPriId, resultList.size(), resultList);

            // 分页
            FaiList<Integer> idList = resultList.stream()
                .skip(mgProductSearchArg.getStart())
                .limit(mgProductSearchArg.getLimit())
                .map(info -> info.getInt(ProductEntity.Info.PD_ID))
                .collect(Collectors.toCollection(FaiList::new));

            Log.logStd("finish uniting db search;flow=%d,aid=%d,unionPriId=%d,idList=%s;", flow, aid, unionPriId, idList);
            // 添加缓存
            return searchProc.integrateAndAddCache(idList, resultList.size(), resultManageCacheTime, resultVisitorCacheTime, resultCacheKey);

        } else {
            Log.logStd("finish uniting db search;Don't need to reLoad;get result from cache;flow=%d,aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", flow, aid, unionPriId, resultCacheKey, resultCacheInfo);
            // 从缓存中获取数据
            return resultCacheInfo;
        }
    }
}
