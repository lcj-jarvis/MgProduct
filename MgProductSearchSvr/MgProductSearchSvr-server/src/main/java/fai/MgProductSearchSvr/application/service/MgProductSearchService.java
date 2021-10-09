package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.async.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
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
import java.util.stream.Collectors;

/**
 * @author Lu
 * 搜索服务
 */
public class MgProductSearchService {

    private final MgProductSearchProc searchProc = new MgProductSearchProc();

    // TODO 先保留
    public static final Integer INTEGRATE_COMPARATOR_FIELD_THREAD_NUM = 3;
    /**
     * 线程池最大线程数，CPU密集型最大线程数，设置为CPU核数 + 1
     */
    public static final Integer MAX_THREAD_NUM = Runtime.getRuntime().availableProcessors() + 1;

    public static final Integer KEEP_ALIVE_TIME = 20;

    public static final Integer BLOCKING_DEQUE_CAPACITY = 100;

    /**
     * 在es中进行搜索
     * @return 返回搜索到的pdIdList
     */
    public FaiList<Integer> esSearch(int flow, int aid, int unionPriId, MgProductSearchArg mgProductSearchArg) {
        FaiList<Integer> esSearchResult = new FaiList<>();
        MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
        if (Objects.isNull(mgProductEsSearch)) {
            Log.logStd("mgProductEsSearch is null;Don't need search from es;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            return esSearchResult;
        }

        // 搜索的字段
        String name = mgProductEsSearch.getSearchKeyWord();
        // 过滤的字段
        int status = mgProductEsSearch.getUpSalesStatus();

        FaiSearchExCli cli = new FaiSearchExCli(flow);
        if (!cli.init(FaiSearchExDef.App.MG_PRODUCT, aid)) {
            // es 客户端初始化失败直接抛出异常
            throw new MgException(Errno.ERROR, "flow=%s;aid=%d;unionPriId=%d;tid=%d; es cli init err", flow, aid, unionPriId);
        }
        // 如果只在es里搜索，要进行分页，就使用es的进行设置。如果db有搜索条件的话，就用db那边的分页。
        // db那里的分页不设置的话，默认start=0，limit=200
        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        if (Objects.isNull(mgProductDbSearch)) {
           // db 搜索条件为空，这里就设置为es里的分页。默认返回前200条数据。如果在初始化查询条件时设置了，就采用设置的。
           cli.setSearchLimit(mgProductEsSearch.getStart(), mgProductEsSearch.getLimit());
        } else {
           // db有搜索条件，使用db的分页。这里获取前5000条的es搜索结果。
           // 运维那边封装的es，如果没有设置分页。start默认是0，limit默认是100。见FaiSearchExCli就知道。
            // 所以没有做分页的时候，也要设置from和limit。不然只返回前100条数据。
           // TODO: 实际上是要获取全部的数据才合理的。因为运维那边的es设置了分片为5000，如果es的命中条数超过5000，就要分多次去获取数据。(开多个线程去分次拿)
           //  目前先设置为5000，已经可以满足门店那边的搜索
           // from：0 limit = 5000;
           cli.setSearchLimit(mgProductEsSearch.getStart(), MgProductEsSearch.ONCE_REQUEST_LIMIT);
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
        if (status == BaseMgProductSearch.UpSalesStatusEnum.UP_AND_DOWN_SALES.getUpSalesStatus()) {
            // 上架或者下架的，或者两种都有。使用in过滤
            FaiList<Integer> statusList = new FaiList<>();
            statusList.add(ProductBasicValObj.ProductValObj.Status.UP);
            statusList.add(ProductBasicValObj.ProductValObj.Status.DOWN);
            filters.add(FaiSearchExDef.SearchFilter.createIn(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, statusList));
        } else if (status != BaseMgProductSearch.UpSalesStatusEnum.ALL.getUpSalesStatus()) {
            // 非全部的，单独是某种状态.使用等值过滤
            filters.add(FaiSearchExDef.SearchFilter.createEqual(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, status));
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
        int rt = sorts.isEmpty()? cli.fullTextQuery(searchWord, fields, filters, resultList, foundTotalRef) : cli.fullTextQuery(searchWord, fields, filters, sorts, resultList, foundTotalRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "es search error;flow=%d,aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s", flow, aid, unionPriId, fields, filters, sorts);
        }
        Log.logStd("finish searching es;flow=%d,aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s,resultList=%s,foundTotalSize=%d", flow, aid, unionPriId, fields, filters, sorts, resultList, foundTotalRef.value);

        // TODO 可以根据命中条数，并发多个线程去获取es数据

        Set<Integer> pdIdSet = new HashSet<>();
        // 获取PdId，保证不重复
        resultList.forEach(docId -> pdIdSet.add((Integer) docId.getVal(MgProductEsSearch.EsSearchPrimaryKeyOrder.PDID_ORDER)));
        // TODO 注意要保证确保经过排序的结果不会乱序
        esSearchResult.addAll(pdIdSet);
        Log.logStd(" finish getting pdIdList from es search result;flow=%d,aid=%d,unionPriId=%d,idList=%s", flow, aid, unionPriId, esSearchResult);
        return esSearchResult;
    }

    /**
     * @param esSearchParamString json形式的es的搜索条件
     * @param dbSearchParamString json形式的db的搜索条件
     * @return {@link Errno}
     */
    @SuccessRt(value = Errno.OK)
    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid,
                          int productCount, String esSearchParamString, String dbSearchParamString) throws IOException {
        int rt = Errno.ERROR;
        long beginTime = System.currentTimeMillis();
        try {
            Param esSearchParam = Param.parseParam(esSearchParamString);
            Param dbSearchParam = Param.parseParam(dbSearchParamString);
            // 去除db中和es重复的搜索字段，排序看后面需要看看要不要去掉
            searchProc.removeSameSearchFields(esSearchParam, dbSearchParam);

            // 初始化搜索条件.
            MgProductSearchArg mgProductSearchArg = new MgProductSearchArg();
            mgProductSearchArg.initSearchParam(esSearchParam, dbSearchParam);
            MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
            MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();

            // 目前查询条件为空的话，就搜索MG_PRODUCT_REL表下的aid + unionPriId 所有数据
            boolean overLimit = Objects.nonNull(mgProductDbSearch) && mgProductDbSearch.getLimit() > BaseMgProductSearch.MAX_LIMIT ||
                Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.getLimit() > BaseMgProductSearch.MAX_LIMIT;
            if (overLimit) {
                // 分页限制，允许最大的分页为200，如果超过的话，直接抛出异常
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%d;aid=%d;unionPriId=%d;over paging limit error,maxLimit=200", flow, aid, unionPriId);
            }

            // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
            // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);
            FaiBuffer sendBuf = new FaiBuffer(true);
            String cacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, mgProductSearchArg.getEsSearchParam().toJson(), mgProductSearchArg.getDbSearchParam().toJson());
            // es查询条件不为空，mgProductDbSearch为空，看看是否存在缓存。如果初始化时候在mgProductDbSearch中设置了排序，mgProductDbSearch也就是不为空的。
            boolean onlySearchInEs = Objects.nonNull(mgProductEsSearch) && Objects.isNull(mgProductDbSearch);
            if (onlySearchInEs && MgProductSearchCache.ResultCache.existsCache(cacheKey)) {
                Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(cacheKey);
                // 如果es搜索条件不为空，db条件为空，就去查询es数据来源的表，获取管理态和访客态最新的修改时间
                // 如果当前的时间 - 获取到的管理态或者访客态最新的修改 < 30s ,
                // 就认为此时的缓存是无效的（因为es的数据还没有在db那边同步过来），需要再查询一次es
                // 反之如果 > 30s,就认为缓存生效了。不需要再次查询es。
                searchProc.asyncGetEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                Long latestDataChangeTime = Math.max(manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value);
                Long currentTime = System.currentTimeMillis();
                // 小于30s，认为缓存无效
                if (currentTime - latestDataChangeTime < MgProductSearchCache.ResultCache.INVALID_CACHE_TIME) {
                    Log.logStd("mgProductDbSearch is empty and cache is invalid;need get result from es once again;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    // 重新搜索es，加载新的内容到缓存
                    FaiList<Integer> esSearchResult = esSearch(flow, aid, unionPriId, mgProductSearchArg);
                    // 添加缓存
                    resultCacheInfo = searchProc.integrateAndAddCache(esSearchResult, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                }
                Log.logStd("mgProductDbSearch is null;Don't need to unite db search;finish es search;return es searchResult;flow=%d,aid=%d,unionPriId=%d;resultInfo=%s", flow, aid, unionPriId, resultCacheInfo);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                return Errno.OK;
            }

            /*
               执行到这里说明是以下三种情况的一种
               (1) “es有搜索条件，mgProductDbSearch为null”的这种情况是无缓存
               (2) es可能有搜索条件可能也没有，db的搜索条件不为空
               (3) mgProductEsSearch和mgProductDbSearch都为null
             */
            // 在es中获取到的idList
            FaiList<Integer> esSearchResult = esSearch(flow, aid, unionPriId, mgProductSearchArg);

            // (1) es 查询条件不为空，但是 es 的搜索结果为空，直接返回
            // (2) es 查询条件不为空，mgProductDbSearch为null，添加缓存，返回es的搜索结果
            boolean notUniteDbSearch = (Objects.nonNull(mgProductEsSearch) && Utils.isEmptyList(esSearchResult)) || onlySearchInEs;
            if (notUniteDbSearch) {
                searchProc.asyncGetEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                // 添加缓存
                Param resultCacheInfo = searchProc.integrateAndAddCache(esSearchResult, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                Log.logStd("Don't need to unite db search;finish es search;return es searchResult;flow=%d,aid=%d,unionPriId=%d;mgProductDbSearch=%s;resultInfo=%s", flow, aid, unionPriId, mgProductDbSearch, resultCacheInfo);
                return Errno.OK;
            }

            // 执行到这里说明mgProductEsSearch和mgProductDbSearch都为null，或者es的搜索条件可能为空也可能不为空，db的搜索条件不为空
            // 执行到这里说明要走db的搜索逻辑.联合db查询,得到最终的搜索结果
            Param resultCacheInfo = uniteDbSearch(flow, aid, unionPriId, tid, mgProductSearchArg, esSearchResult);
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
     * @param esSearchResult es的搜索结果
     * @return 返回包含idList的Param
     */
    public Param uniteDbSearch(int flow, int aid, int unionPriId, int tid,
                               MgProductSearchArg mgProductSearchArg, FaiList<Integer> esSearchResult) {
        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        Log.logStd("need to unite db search;begin invoke uniteDbSearch method;flow=%d,aid=%d,unionPriId=%d,mgProductDbSearch=%s,esSearchResult=%s", flow, aid, unionPriId, mgProductDbSearch, esSearchResult);

        // 初始化需要用到的 异步client
        FaiClientProxyFactory.initFlowGenerator(() -> flow);
        MgProductBasicCli asyncMgProductBasicCli = FaiClientProxyFactory.createProxy(MgProductBasicCli.class);
        MgProductStoreCli asyncMgProductStoreCli = FaiClientProxyFactory.createProxy(MgProductStoreCli.class);
        MgProductSpecCli asyncMgProductSpecCli = FaiClientProxyFactory.createProxy(MgProductSpecCli.class);

        // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
        // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);

        if (mgProductSearchArg.isEmpty()) {
            // 搜索条件为空。就搜索MG_PRODUCT_REL中aid + unionPriId下的所有数据
            Log.logStd("mgProductSearchArg is empty;get data from mgProductRel table;flow=%d,aid=%d,unionPriId=%d", flow, aid, unionPriId);
            mgProductDbSearch = new MgProductDbSearch();
        }

        // 表名映射“包含搜索和排序等信息的Param”
        final Map<String, Param> tableMappingSearchAndSortInfo = new ConcurrentHashMap<>();
        // 保存 各个表 管理态 修改的最新时间
        final TreeSet<Long> manageDataChangeTimeSet = new TreeSet<>();
        // 保存 各个表 访客态 修改的最新时间
        final TreeSet<Long> visitorDataChangeTimeSet = new TreeSet<>();
        // 异步搜索各个表的数据状态
        searchProc.asyncCheckDataStatus(flow, aid, unionPriId, tid, mgProductDbSearch, esSearchResult, manageDataChangeTimeSet, visitorDataChangeTimeSet, tableMappingSearchAndSortInfo, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 判断排序的表是否在tableMappingSearchAndSortInfo的搜索表中，如果没有将排序表也作为要搜索的表，添加到tableMappingSearchAndSortInfo中

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

        // 用于保存排序表对应的DefaultFuture
        Map<String, CompletableFuture> tableNameMappingFuture = new HashMap<>(16);
        // 用于保存排序表对应的空查询条件
        Map<String, ParamMatcher> tableNameMappingSearchMatcher = new HashMap<>(16);

        if (needCompare) {
            // 考虑是将自定义的排序表设置默认值，还是根据在这里根据自定义排序字段是rlPdId手动补偿表赋值。
            // 这里采用在mgProductDbSearch设置默认的自定义的排序表为MgProductRel

            // compensateTable为true,说明tableMappingSearchAndSortInfo中未包含该排序字段对应的排序表
            boolean compensateTable = hasCustomComparator && !tableMappingSearchAndSortInfo.containsKey(customComparatorTable);
            // 如果 tableMappingSearchAndSortInfo 未包含自定义排序的表
            if (compensateTable) {
                Log.logStd("tableMappingSearchAndSortInfo don't contain customComparatorTable,need to compensate customComparatorTable;compensateTable=%s,tableMappingSearchAndSortInfo=%s", customComparatorTable, tableMappingSearchAndSortInfo);
                // 异步
                ParamMatcher defaultMatcher = new ParamMatcher();
                tableNameMappingSearchMatcher.put(customComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(aid, unionPriId, tid, customComparatorTable, tableNameMappingFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }

            compensateTable = hasFirstComparator && !tableMappingSearchAndSortInfo.containsKey(firstComparatorTable);
            // 如果 tableMappingSearchAndSortInfo 未包含第一排序的表
            if (compensateTable) {
                Log.logStd("tableMappingSearchAndSortInfo don't contain firstComparatorTable,need compensate firstComparatorTable;compensateTable=%s,tableMappingSearchAndSortInfo=%s", firstComparatorTable, tableMappingSearchAndSortInfo);
                // 异步
                ParamMatcher defaultMatcher = new ParamMatcher();
                tableNameMappingSearchMatcher.put(firstComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(aid, unionPriId, tid, firstComparatorTable, tableNameMappingFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }

            // 第二排序的key默认是rlPdId，默认的排序表是MG_PRODUCT_REL
            compensateTable = needSecondComparatorSorting && !tableMappingSearchAndSortInfo.containsKey(secondComparatorTable);
            // 如果 tableMappingSearchAndSortInfo 未包含第二排序的表
            if (compensateTable) {
                Log.logStd("tableMappingSearchAndSortInfo don't contain secondComparatorTable,need compensate secondComparatorTable;compensateTable=%s,tableMappingSearchAndSortInfo=%s", secondComparatorTable, tableMappingSearchAndSortInfo);
                // 异步
                ParamMatcher defaultMatcher = new ParamMatcher();
                tableNameMappingSearchMatcher.put(secondComparatorTable, defaultMatcher);
                searchProc.asyncGetDataStatus(aid, unionPriId, tid, secondComparatorTable, tableNameMappingFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
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

        // 回调获取结果,等待异步完成
        if (!tableNameMappingFuture.isEmpty()) {
            searchProc.callbackGetDataStatus(flow, aid, unionPriId, tableNameMappingFuture, tableNameMappingSearchMatcher, mgProductDbSearch, manageDataChangeTimeSet, visitorDataChangeTimeSet, tableMappingSearchAndSortInfo);
            Log.logStd("finish async get compareTable dataStatus;flow=%d,aid=%d,unionPriId=%d,tableNameMappingFuture=%s", flow, aid, unionPriId, tableNameMappingFuture);
        }

        // 执行到这里，说明tableMappingSearchAndSortInfo已经包含了所有需要排序的表

        // 不用非空判断其实也没问题，默认会搜索MgProductRel表，manageDataChangeTimeSet不可能为空
        manageDataMaxChangeTime.value = manageDataChangeTimeSet.last();
        // 该TreeSet可能没有元素。因为如果last()方法获取不到，则抛出NoSuchElementException，所以保存访客态的要判空
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
        Log.logStd("needReload=%s;resultManageCacheTime=%s;manageDataMaxChangeTime=%s;resultVisitorCacheTime=%s;visitorDataMaxChangeTime=%s;", needReload, resultManageCacheTime, manageDataMaxChangeTime.value, resultVisitorCacheTime, visitorDataMaxChangeTime.value);

        // 是否需要重新加载
        if (needReload) {
            // 表名 -> pdId -> List<Param> 主要用于排序
            // Map<String, Map<Integer, List<Param>>> tableNameMappingPdIdParamList = new ConcurrentHashMap<>();

            // 表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
            Map<String, Map<Integer, Param>> tableNameMappingPdIdParam = new ConcurrentHashMap<>();
            // 表名映射每张表的pdId的BitSet，主要用于取交集。多线程回调，使用线程安全的Map
            Map<String, BitSet> tableNameMappingPdIdBitSet = new ConcurrentHashMap<>();
            // 保存 searchAndSortInfoList 的集合
            Collection<Param> searchAndSortInfoList = tableMappingSearchAndSortInfo.values();
            // 表名映射“从本地缓存获取数据异步任务”
            Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture = new HashMap<>(16);
            // 表名映射“从远程获取数据的异步任务”
            Map<String, CompletableFuture> tableNameMappingLocalGetDataFuture = new HashMap<>(16);

            // 异步获取数据
            Log.logStd("need to reload;begin async get data;aid=%d,unionPriId=%d", aid, unionPriId);
            searchAndSortInfoList.forEach(searchAndSortInfo -> {
                searchProc.asyncGetData(flow, aid, tid, unionPriId, searchAndSortInfo,
                    tableNameMappingRemoteGetDataFuture,
                    tableNameMappingLocalGetDataFuture,
                    tableNameMappingPdIdParam,
                    tableNameMappingPdIdBitSet,
                    asyncMgProductBasicCli,
                    asyncMgProductStoreCli,
                    asyncMgProductSpecCli);
            });

            // 回调获取结果
            searchProc.callbackGetResultList(flow, aid, unionPriId, tableNameMappingLocalGetDataFuture, tableNameMappingRemoteGetDataFuture, tableMappingSearchAndSortInfo, tableNameMappingPdIdParam, tableNameMappingPdIdBitSet);

            // 保存排序的结果
            List<Param> sortResult = new FaiList<>();
            if (!tableNameMappingPdIdBitSet.isEmpty()) {

                // 取交集。
                // 【注】为什么用BitSet取交集呢？因为如果数据量上来了，BitSet取交集的效率是for循环的上千倍
                BitSet[] pdIdBitSetArrays = tableNameMappingPdIdBitSet.values().toArray(new BitSet[0]);
                BitSet pdIdBitSet = pdIdBitSetArrays[0];
                // 开始取交集,注意这里的i是从1开始
                for (int i = 1; i < pdIdBitSetArrays.length; i++) {
                    pdIdBitSet.and(pdIdBitSetArrays[i]);
                }
                // 取完交集后，这里就是db最终的pdId
                Log.logStd("finish taking the intersection of db searchResult;flow=%d,aid=%d,unionPriId=%d,intersection=%s", flow, aid, unionPriId, pdIdBitSet);

                // 整合es的搜索结果
                // 先手动给一个值,用于测试
                // int inSqlThreshold = 5;
                int inSqlThreshold = searchProc.getInSqlThreshold();
                // 是否需要和es的搜索结果取交集。大于in sql的阈值，就取交集。
                boolean needUnionEs = esSearchResult.size() > inSqlThreshold;
                // 是否有排序在ES中
                MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
                boolean hasComparatorInEs = (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.hasFirstComparator()) ||
                    (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.isNeedSecondComparatorSorting());

                // 是否需要和ES取交集
                if (!pdIdBitSet.isEmpty() && needUnionEs) {
                    BitSet esBitSet = new BitSet();
                    esSearchResult.forEach(pdId -> {
                        if (!esBitSet.get(pdId)) {
                            esBitSet.set(pdId);
                        }
                    });

                    // db的搜索结果和es的搜索结果根据pdId取交集
                    pdIdBitSet.and(esBitSet);

                    // 如果有排序在es。先按照es的排序结果进行定制排序。
                    Log.logStd("finish taking the intersection of es and db searchResult;aid=%d,unionPriId=%d,esResult=%s,intersection=%s", aid, unionPriId, esBitSet, pdIdBitSet);
                }


                // （1）如果es里有排序，sortResult是es的搜索结果和db的搜索结果的交集，
                //     需要将交集sortResult先按照“es里的排序结果”进行排序。因为整合出来的结果是乱序的。
                //  (2)如果es里有排序，sortResult是es的搜索结果作为db的查询条件查询出来的，
                //     那么此时的sortResult也有可能是乱序的。但是已经在es里排序了，所以sortResult要按照“es里的排序结果”进行排序。
                pdIdBitSet.stream().forEach(pdId -> sortResult.add(new Param().setInt(ProductEntity.Info.PD_ID, pdId)));
                if (!sortResult.isEmpty() && hasComparatorInEs) {
                    // 根据es的结果进行定制排序
                    ParamComparator customComparatorOfEsSearchResult = new ParamComparator();
                    customComparatorOfEsSearchResult.addKey(ProductEntity.Info.PD_ID, esSearchResult);

                    // 按照es里的顺序先排好。
                    sortResult.sort(customComparatorOfEsSearchResult);
                    // 按照es的结果排好后，再按MgProductDbSearch里的排序去排

                    Log.logStd("sorted by pdIdOfEs;aid=%d,unionPriId=%d,idListFromEs=%s,sortResult=%s", aid, unionPriId, esSearchResult, sortResult);
                }

                // db里的排序
                if (!sortResult.isEmpty() && needCompare) {

                    // TODO 数据量少的时候，单线程好，数据量大的时候，看不同数据量的耗时，待考虑用多线程的方式还是单线程方式。
                    // 阿里巴巴开发规范: 使用线程池的好处是减少在创建和销毁线程上所花的时间以及系统资源的开销，解决资源不足的问题。
                    //  如果不使用线程池，有可能造成系统创建大量同类线程而导致消耗完内存或者“过度切换”的问题。所以不采用单独new的方式
                    // 3 为整合排序字段到sortResult的线程数
                    /*CountDownLatch countDownLatch = new CountDownLatch(INTEGRATE_COMPARATOR_FIELD_THREAD_NUM);
                    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(INTEGRATE_COMPARATOR_FIELD_THREAD_NUM, MAX_THREAD_NUM, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                        new LinkedBlockingDeque<>(BLOCKING_DEQUE_CAPACITY),
                        Executors.defaultThreadFactory(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
                    try {
                        // 整合自定义排序字段到sortResult的线程
                        threadPoolExecutor.execute(() -> searchProc.integrateComparatorFieldToSortResult(hasCustomComparator, sortResult, tableNameMappingPdIdParam, customComparatorTable, customComparatorKey, countDownLatch));
                        // 整合第一排序字段到sortResult的线程
                        threadPoolExecutor.execute(() -> searchProc.integrateComparatorFieldToSortResult(hasFirstComparator, sortResult, tableNameMappingPdIdParam, firstComparatorTable, firstComparatorKey, countDownLatch));
                        // 整合第二排序字段到sortResult的线程
                        threadPoolExecutor.execute(() -> searchProc.integrateComparatorFieldToSortResult(needSecondComparatorSorting, sortResult, tableNameMappingPdIdParam, secondComparatorTable, secondComparatorKey, countDownLatch));

                        // 等待执行完成
                        countDownLatch.await();
                        Log.logStd("finish integrating all comparatorField to sortResult;flow=%d;aid=%d,unionPriId=%d;sortResult=%s", flow, aid, unionPriId, sortResult);
                    } catch (Exception e) {
                        // 等待执行完成超时
                        throw new MgException(Errno.ERROR, "waiting integrate comparatorField to sortResult error;flow=%d;aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    } finally {
                        // 及时关闭线程池，避免内存泄漏
                        threadPoolExecutor.shutdownNow();
                    }*/

                    // TODO 数据量少的时候，单线程好，数据量大的时候，看不同数据量的耗时，待考虑用多线程的方式还是单线程方式。
                    sortResult.forEach(info -> {
                        searchProc.integrateComparatorFieldToSortResult(hasCustomComparator, info, tableNameMappingPdIdParam, customComparatorTable, customComparatorKey);
                        searchProc.integrateComparatorFieldToSortResult(hasFirstComparator, info, tableNameMappingPdIdParam, firstComparatorTable, firstComparatorKey);
                        searchProc.integrateComparatorFieldToSortResult(needSecondComparatorSorting, info, tableNameMappingPdIdParam, secondComparatorTable, secondComparatorKey);
                    });
                    Log.logStd("finish integrating ComparatorField to sortResult;sortResult=%s", sortResult);

                    // 排序
                    sortResult.sort(paramComparator);
                    Log.logStd("finish sorting;flow=%d;aid=%d,unionPriId=%d,sortResult=%s;", flow, aid, unionPriId, sortResult);
                }
            }

            // 管理态变更的缓存时间
            resultManageCacheTime = Math.max(resultManageCacheTime, manageDataMaxChangeTime.value);
            // 访客态变更的缓存时间
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, resultManageCacheTime);

            Log.logStd("before paging result;aid=%d,unionPriId=%d,totalSize=%d,result=%s", aid, unionPriId, sortResult.size(), sortResult);

            // 分页(旧)
            // SearchArg searchArg = new SearchArg();
            // Searcher searcher = new Searcher(searchArg);
            // mgProductDbSearch.setSearArgStartAndLimit(searchArg);
            // 分页的结果
            // sortResult = searcher.getParamList(sortResult);
            // 将最终结果转化为pdIdList，添加到缓存
            // FaiList<Integer> idList = searchProc.toIdList(sortResult, ProductEntity.Info.PD_ID);

            // 分页（新）
            FaiList<Integer> idList = sortResult.stream()
                .skip(mgProductDbSearch.getStart())
                .limit(mgProductDbSearch.getLimit())
                .map(info -> info.getInt(ProductEntity.Info.PD_ID))
                .collect(Collectors.toCollection(FaiList::new));

            Log.logStd("finish uniting db search;flow=%d,aid=%d,unionPriId=%d,idList=%s;", flow, aid, unionPriId, idList);
            // 添加缓存
            return searchProc.integrateAndAddCache(idList, resultManageCacheTime, resultVisitorCacheTime, resultCacheKey);

        } else {
            Log.logStd("finish uniting db search;Don't need to reLoad;get result from cache;flow=%d,aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", flow, aid, unionPriId, resultCacheKey, resultCacheInfo);
            // 从缓存中获取数据
            return resultCacheInfo;
        }
    }

}
