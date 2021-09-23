package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicValObj;
import fai.MgProductInfSvr.interfaces.utils.*;
import fai.MgProductSearchSvr.domain.comm.CliFactory;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc;
import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.app.DocOplogDef;
import fai.app.FaiSearchExDef;
import fai.cli.FaiSearchExCli;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author Lu
 * 搜索服务
 */
public class MgProductSearchService {

    private final MgProductSearchProc searchProc = new MgProductSearchProc();

    /**
     * 在es中进行搜索
     * @return 不返回FaiList<Integer>,返回FaiList<Param>是为了做分页方便。
     */
    public FaiList<Param> esSearch(int flow, int aid, int unionPriId, MgProductSearchArg mgProductSearchArg) {
        FaiList<Param> esSearchResult = new FaiList<>();
        MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
        if (Objects.isNull(mgProductEsSearch)) {
            Log.logStd("mgProductEsSearch is null;Don't need search from es;aid=%d,unionPriId=%d", aid, unionPriId);
            return esSearchResult;
        }

        // 搜索的字段
        String name = mgProductEsSearch.getSearchKeyWord();
        // 过滤的字段
        int status = mgProductEsSearch.getUpSalesStatus();

        FaiSearchExCli cli = new FaiSearchExCli(flow);
        if (!cli.init(FaiSearchExDef.App.MG_PRODUCT, aid)) {
            int rt = Errno.ERROR;
            throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d; FaiSearchExCli init err", flow, aid, unionPriId);
        }
        // 如果只在es里搜索，要进行分页，就进行设置。如果db有搜索条件的话，就用db那边的分页。
        // db那里的分页会不设置的话，默认start=0，limit=200
        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        if (Objects.isNull(mgProductDbSearch)) {
           // db 搜索条件为空，这里就设置为es里的分页。默认返回前200条数据。如果在初始化查询条件时设置了，就采用设置的。
           cli.setSearchLimit(mgProductEsSearch.getStart(), mgProductEsSearch.getLimit());
        } else {
           // db有搜索条件，使用db的分页。这里获取前5000条的es搜索结果。
            // 运维那边封装的es，如果没有设置分页。start默认是0，limit默认是100。见FaiSearchExCli就知道。所以没有做分页的时候，也要设置from和limit。不然只返回前100条数据。
           // TODO: 实际上是要获取全部的数据才合理的。因为运维那边的es设置了分片为5000，如果es的命中条数超过5000，就要分多次去获取数据。(开多个线程去分次拿)
           //  目前先设置为5000，已经可以满足门店那边的搜索
           // from：0 limit = 5000;
           cli.setSearchLimit(mgProductEsSearch.getStart(), MgProductEsSearch.ONCE_REQUEST_LIMIT);
        }
        // 简化
        // int limit = Objects.isNull(mgProductDbSearch) ? mgProductEsSearch.getLimit() : MgProductEsSearch.ONCE_REQUEST_LIMIT;
        // cli.setSearchLimit(mgProductEsSearch.getStart(), limit);

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
            throw new MgException(rt, "es search error;aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s", aid, unionPriId, fields, filters, sorts);
        }
        Log.logStd("finish search in es;aid=%d,unionPriId=%d,fields=%s,filters=%s,sorts=%s,resultList=%s,foundTotalSize=%d", aid, unionPriId, fields, filters, sorts, resultList, foundTotalRef.value);

        // 获取PdId
        resultList.forEach(docId -> esSearchResult.add(new Param().setInt(ProductEntity.Info.PD_ID, (Integer)docId.getVal(MgProductEsSearch.EsSearchPrimaryKeyOrder.PDID_ORDER))));
        Log.logStd("get idList from es search result finish;aid=%d,unionPriId=%d,idList=%s", aid, unionPriId, esSearchResult);
        return esSearchResult;
    }

    /**
     *  作用
     * （1）如果es中搜索过的字段，db中不再搜索
     */
    public void removeSameSearchFields(Param esSearchParam, Param dbSearchParam) {
        // 避免NPE
        if (esSearchParam == null) {
            esSearchParam = new Param();
        }
        if (dbSearchParam == null) {
            dbSearchParam = new Param();
        }

        /*if (esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD)) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD);
        }*/
        // 如果关键词已经在es用做了商品名称搜索，就不在db中用作商品名称搜索了
        boolean useSearchKeywordAsPdNameInEs = esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD) &&
            esSearchParam.getBoolean(BaseMgProductSearch.BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME, false);
        if (useSearchKeywordAsPdNameInEs) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME);
        }
        // 判断是否有公共的查询字段。es中搜索过了，db中不再进行搜索
        if (esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS)) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS);
        }

        // 不移除以下代码的话：自定义排序 > db里设置的第一排序 > db里设置的第二排序 > es里设置的排序
        // 根据看情况要不要移除吧，目前先移除
        // 判断第一排序或者第二排序是否在es中
//        boolean sortKeyInEs = esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY) ||
//            esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
//        if (sortKeyInEs) {
//            // 如果第一排序或者第二排序在es，则移除db里的第一排序和第二排序
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY);
//            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.FIRST_COMPARATOR_TABLE);
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SECOND_COMPARATOR_KEY);
//            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.SECOND_COMPARATOR_TABLE);
//        }
    }

    /**
     * 获取“es数据来源的表”对应的管理态和访客态最新的修改时间
     * @param manageDataMaxChangeTime 管理态最新的修改时间
     * @param visitorDataMaxChangeTime 访客态最新的修改时间
     */
    public void getEsDataStatus(int flow, int aid, int unionPriId, Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime) {
        // 目前es的数据来源与商品表和商品业务表
        MgProductBasicCli mgProductBasicCli = CliFactory.getCliInstance(flow, MgProductBasicCli.class);
        TreeSet<Long> manageDataChangeTimeSet = new TreeSet<>();
        TreeSet<Long> visitorDataChangeTimeSet = new TreeSet<>();

        // TODO 两个方法还好，方法多了要考虑简化
        Param statusInfo = new Param();
        mgProductBasicCli.getPdDataStatus(aid, statusInfo);
        manageDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
        visitorDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
        statusInfo.clear();

        mgProductBasicCli.getPdRelDataStatus(aid, unionPriId, statusInfo);
        manageDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
        visitorDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));

        manageDataMaxChangeTime.value = Math.max(manageDataMaxChangeTime.value, manageDataChangeTimeSet.last());
        visitorDataMaxChangeTime.value = Math.max(visitorDataMaxChangeTime.value, visitorDataChangeTimeSet.last());
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
            removeSameSearchFields(esSearchParam, dbSearchParam);

            // 初始化搜索条件.
            MgProductSearchArg mgProductSearchArg = new MgProductSearchArg();
            mgProductSearchArg.initSearchParam(esSearchParam, dbSearchParam);
            MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
            MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();

            // 目前查询条件为空的话，就搜索MG_PRODUCT_REL表下的aid + unionPriId 所有数据
            boolean overLimit = Objects.nonNull(mgProductDbSearch) && mgProductDbSearch.getLimit() > BaseMgProductSearch.DEFAULT_LIMIT ||
                Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.getLimit() > BaseMgProductSearch.DEFAULT_LIMIT;
            if (overLimit) {
                // 分页限制，允许最大的分页为200，如果超过的话，直接抛出异常
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d;over limit error,maxLimit=200", flow, aid, unionPriId, tid);
            }

            // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
            // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);
            FaiBuffer sendBuf = new FaiBuffer(true);
            String cacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, mgProductSearchArg.getEsSearchParam().toJson(), mgProductSearchArg.getDbSearchParam().toJson());
            // es查询条件不为空，mgProductDbSearch为空，看看是否存在缓存。如果初始化时候在mgProductDbSearch中设置了排序，mgProductDbSearch也就是不为空的。
            boolean onlySearchInEs = Objects.nonNull(mgProductEsSearch) && Objects.isNull(mgProductDbSearch);
            if (onlySearchInEs) {
                if (MgProductSearchCache.ResultCache.existsCache(cacheKey)) {
                    Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(cacheKey);

                    // 如果es搜索条件不为空，db条件为空，就去查询es数据来源的表，获取管理态和访客态最新的修改时间
                    // 如果当前的时间 - 获取到的管理态或者访客态最新的修改 < 30s ,
                    // 此时的缓存认为是无效的（因为es的数据还没有在db那边同步过来），需要再查询一次es
                    // 反之如果 > 30s,就认为缓存生效了。不需要再次查询es。
                    getEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                    Long latestChangeTime = Math.max(manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value);
                    Long currentTime = System.currentTimeMillis();
                    // 小于30s，认为缓存无效
                    if (currentTime - latestChangeTime < MgProductSearchCache.ResultCache.INVALID_CACHE_TIME) {
                        Log.logStd("mgProductDbSearch is empty and reload cache;need get result from es;aid=%d,unionPriId=%d;", aid, unionPriId);
                        // 重新搜索es，加载新的内容到缓存
                        FaiList<Param> esSearchResult = esSearch(flow, aid, unionPriId, mgProductSearchArg);
                        // 在es中获取到的idList
                        FaiList<Integer> idListFromEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);
                        // 添加缓存
                        resultCacheInfo = integrateAndAddCache(idListFromEs, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                    }
                    Log.logStd("mgProductDbSearch is null;Don't need uniteDbSearch;return es searchResult;aid=%d,unionPriId=%d;resultInfo=%s", aid, unionPriId, resultCacheInfo);
                    resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                    session.write(sendBuf);
                    return Errno.OK;
                }
            }

            // 执行到这里说明“es有搜索条件，mgProductDbSearch为null”的这种情况是无缓存
            // 或者es可能有搜索条件可能也没有，db的搜索条件不为空。
            // 或者mgProductEsSearch和mgProductDbSearch都为null
            FaiList<Param> esSearchResult = esSearch(flow, aid, unionPriId, mgProductSearchArg);
            // 在es中获取到的idList
            FaiList<Integer> idListFromEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);

            // (1) es 查询条件不为空，但是 es 的搜索结果为空，直接返回
            // (2) es 查询条件不为空，mgProductDbSearch为null，添加缓存，返回es的搜索结果
            boolean notUniteDbSearch = (Objects.nonNull(mgProductEsSearch) && Utils.isEmptyList(idListFromEs)) || onlySearchInEs;
            if (notUniteDbSearch) {
                getEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                // 添加缓存
                Param resultCacheInfo = integrateAndAddCache(idListFromEs, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                Log.logStd("Don't need uniteDbSearch;return es searchResult;aid=%d,unionPriId=%d;mgProductDbSearch=%s;resultInfo=%s", aid, unionPriId, mgProductDbSearch, resultCacheInfo);
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
     * 整合并添加缓存
     * @param idList 缓存的pdId List
     * @param manageDataCacheTime 管理态数据最新的修改时间
     * @param visitDataCacheTime  访客态数据的最新修改时间
     * @param cacheKey 缓存的key
     * @return 返回包含缓存结果的Param
     */
    private Param integrateAndAddCache(FaiList<Integer> idList, Long manageDataCacheTime, Long visitDataCacheTime, String cacheKey) {
        // 缓存的数据
        Param resultCacheInfo = new Param();
        resultCacheInfo.setList(MgProductSearchResult.Info.ID_LIST, idList);
        resultCacheInfo.setInt(MgProductSearchResult.Info.TOTAL, idList.size());
        resultCacheInfo.setLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, manageDataCacheTime);
        resultCacheInfo.setLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, visitDataCacheTime);
        // 缓存处理，先删除再添加
        MgProductSearchCache.ResultCache.delCache(cacheKey);
        MgProductSearchCache.ResultCache.addCacheInfo(cacheKey, resultCacheInfo);
        return resultCacheInfo;
    }

    /**
     *  获取各个表的数据状态,同时初始化db的查询条件，以及要搜索哪些表的数据
     * @param manageDataMaxChangeTime 管理态的最大修改时间
     * @param visitorDataMaxChangeTime 访客态的最大修改时间
     * @param searchSorterInfoList 参考{@see initSearchSorterInfoList()}
     */
    private void eachTableCheckDataStatus(int flow, int aid, int unionPriId, int tid,
                                          MgProductBasicCli mgProductBasicCli,
                                          MgProductStoreCli mgProductStoreCli,
                                          MgProductSpecCli mgProductSpecCli,
                                          MgProductDbSearch mgProductDbSearch,
                                          FaiList<Param> esSearchResult,
                                          Ref<Long> manageDataMaxChangeTime,
                                          Ref<Long> visitorDataMaxChangeTime,
                                          FaiList<Param> searchSorterInfoList) {
        // 是否将es的idList直接当作db的查询条件
        ParamMatcher idListFromEsParamMatcher = new ParamMatcher();
        if (!Utils.isEmptyList(esSearchResult)) {
            // 先手动给一个值用于测试
            // int inSqlThreshold = 5;
            int inSqlThreshold = searchProc.getInSqlThreshold();
            if (esSearchResult.size() == 1) {
                Integer pdId = esSearchResult.get(0).getInt(ProductEntity.Info.PD_ID);
                idListFromEsParamMatcher.and(ProductBasicEntity.ProductInfo.PD_ID, ParamMatcher.EQ, pdId);
            } else if (esSearchResult.size() <= inSqlThreshold) {
                FaiList<Integer> idListFromEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);
                idListFromEsParamMatcher.and(ProductBasicEntity.ProductInfo.PD_ID, ParamMatcher.IN, idListFromEs);
            }
        }

        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher productBindPropDataSearchMatcher = mgProductDbSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName();
        if(!productBindPropDataSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                productBindPropDataSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        ParamMatcher mgSpuBizSummarySearchMatcher = mgProductDbSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName();
        if(!mgSpuBizSummarySearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgSpuBizSummarySearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 3、"标签业务关系表" mgProductBindTag_xxxx 搜索， 还没有标签功能，暂时没开放
        ParamMatcher mgProductBindTagSearchMatcher = mgProductDbSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName();
        if (!mgProductBindTagSearchMatcher.isEmpty()) {
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductBindTagSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductBindTagSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
        ParamMatcher mgProductBindGroupSearchMatcher = mgProductDbSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName();
        if(!mgProductBindGroupSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductBindGroupSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        ParamMatcher productRelSearchMatcher = mgProductDbSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName();
        if (mgProductDbSearch.isEmpty()) {
            // 执行到这里说明es和db的搜索条件都为空.因为es搜索条件不为空，db搜索条件为空的情况，在前面已经直接返回es里的搜索结果了。
            // db搜索的表为空，就补充一张MG_PRODUCT_REL作为空搜索条件的表
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }  else {
            // 或者es的搜索条件为空，db的搜索条件不为空，或者es的不为空，db的搜索条件不为空
            if (!productRelSearchMatcher.isEmpty()) {
                if (!idListFromEsParamMatcher.isEmpty()) {
                    // 添加es的idList作为查询条件
                    productRelSearchMatcher.and(idListFromEsParamMatcher);
                }
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }
        }

        // 6、在"商品规格skuCode表" mgProductSpecSkuCode_0xxx 搜索
        ParamMatcher mgProductSpecSkuSearchMatcher = mgProductDbSearch.getProductSpecSkuCodeSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName();
        if (!mgProductSpecSkuSearchMatcher.isEmpty()) {
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductSpecSkuSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductSpecSkuSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 7、在 "商品基础表" mgProduct_xxxx 搜索
        ParamMatcher productBasicSearchMatcher = mgProductDbSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName();
        if(!productBasicSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                productBasicSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productBasicSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
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
                                 MgProductDbSearch mgProductDbSearch, ParamMatcher searchMatcher,
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
            isOnlySearchManageData = mgProductDbSearch.getIsOnlySearchManageData(searchTableName);
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

    /**
     * es搜索完后整合db的进行搜索
     * @param mgProductSearchArg 搜索条件
     * @param esSearchResult es的搜索结果
     * @return 返回包含idList的Param
     */
    public Param uniteDbSearch(int flow, int aid, int unionPriId, int tid,
                               MgProductSearchArg mgProductSearchArg, FaiList<Param> esSearchResult) {
        MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
        Log.logStd("need unite dbSearch;begin invoke uniteDbSearch method;aid=%d,unionPriId=%d,mgProductDbSearch=%s,esSearchResult=%s", aid, unionPriId, mgProductDbSearch, esSearchResult);
        // 初始化需要用到的 client
        MgProductBasicCli mgProductBasicCli = CliFactory.getCliInstance(flow, MgProductBasicCli.class);
        MgProductStoreCli mgProductStoreCli = CliFactory.getCliInstance(flow, MgProductStoreCli.class);
        MgProductSpecCli mgProductSpecCli = CliFactory.getCliInstance(flow, MgProductSpecCli.class);

        // TODO 后面需要搞为异步获取数据
        // 根据搜索的table的数据大小排序，从小到大排序
        FaiList<Param> searchSorterInfoList = new FaiList<Param>();
        // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
        // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
        Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);

        if (mgProductSearchArg.isEmpty()) {
            // 搜索条件为空。就搜索MG_PRODUCT_REL中aid + unionPriId下的所有数据
            Log.logStd("mgProductSearchArg is empty;get data from mgProductRel table;aid=%d,unionPriId=%d", aid, unionPriId);
            mgProductDbSearch = new MgProductDbSearch();
        }

        // 搜索各个表的数据状态
        eachTableCheckDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli,
            mgProductDbSearch, esSearchResult, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);

        // 根据搜索的表的数据(总记录数)由小到大排序，提高搜索效率
        ParamComparator compSizeForSorter = new ParamComparator(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, false);
        searchSorterInfoList.sort(compSizeForSorter);

        // 判断排序的表是否在searchSorterInfoList的搜索表中，如果没有将排序表也作为要搜索的表，添加到searchSorterInfoList

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
        if (needCompare) {
            // 考虑是将自定义的排序表设置默认值，还是根据在这里根据自定义排序字段是rlPdId手动补偿表赋值。
            // 这里采用在mgProductDbSearch设置默认的自定义的排序表为MgProductRel

            // compensateTable为true说明searchSorterInfoList中未包含该排序字段对应的排序表
            boolean compensateTable = findComparatorTable(hasCustomComparator, customComparatorTable, searchSorterInfoList);
            // 如果 searchSorterInfoList 未包含自定义排序的表
            if (compensateTable) {
                Log.logStd("searchSorterInfoList don't contain customComparatorTable,need compensate customComparatorTable;compensateTable=%s,searchSorterInfoList=%s", customComparatorTable, searchSorterInfoList);
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, customComparatorTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            compensateTable = findComparatorTable(hasFirstComparator, firstComparatorTable, searchSorterInfoList);
            // 如果 searchSorterInfoList 未包含第一排序的表
            if (compensateTable) {
                Log.logStd("searchSorterInfoList don't contain firstComparatorTable,need compensate firstComparatorTable;compensateTable=%s,searchSorterInfoList=%s", firstComparatorTable, searchSorterInfoList);
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, firstComparatorTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 第二排序的key默认是rlPdId，默认的排序表是MG_PRODUCT_REL
            compensateTable = findComparatorTable(needSecondComparatorSorting, secondComparatorTable, searchSorterInfoList);
            // 如果 searchSorterInfoList 未包含第二排序的表
            if (compensateTable) {
                Log.logStd("searchSorterInfoList don't contain  secondComparatorTable,need compensate secondComparatorTable;compensateTable=%s,searchSorterInfoList=%s", secondComparatorTable, searchSorterInfoList);
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, secondComparatorTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }
        }

        // 执行到这里，说明searchSorterInfoList已经包含了所有需要排序的表

        // 获取搜索结果的缓存
        String esSearchParamString = mgProductSearchArg.getEsSearchParam().toJson();
        String dbSearchParamString = mgProductSearchArg.getDbSearchParam().toJson();
        String resultCacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, esSearchParamString, dbSearchParamString);
        Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
        Log.logStd("aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", aid, unionPriId, resultCacheKey, resultCacheInfo);

        long resultManageCacheTime = 0L;
        long resultVisitorCacheTime = 0L;
        if (!Str.isEmpty(resultCacheInfo)) {
            resultManageCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, 0L);
            resultVisitorCacheTime = resultCacheInfo.getLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, 0L);
        }

        // 判断缓存的时间，是否需要进行重新搜索缓存
        // 管理态时间变更，影响有管理态字段查询、访客字段查询、结果缓存
        // 访客态时间变更，影响有访客态字段查询 结果缓存
        // resultVisitorCacheTime, 搜索条件里面有 访客字段搜索 才会用到赋值更新这个字段值
        boolean needReload = resultManageCacheTime == 0 || (resultManageCacheTime < manageDataMaxChangeTime.value ||
            (resultVisitorCacheTime != 0 && resultVisitorCacheTime < manageDataMaxChangeTime.value) ||
            resultVisitorCacheTime < visitorDataMaxChangeTime.value);
        Log.logDbg("needReload=%s;resultManageCacheTime=%s;manageDataMaxChangeTime=%s;resultVisitorCacheTime=%s;visitorDataMaxChangeTime=%s;", needReload, resultManageCacheTime, manageDataMaxChangeTime.value, resultVisitorCacheTime, visitorDataMaxChangeTime.value);

        // 重新加载
        if (needReload) {
            // 初始化需要搜索的数据，从本地缓存获取、或者从远端获取。
            FaiList<Param> resultList = new FaiList<>();
            // 保存自定义排序的结果
            FaiList<Param> customComparatorResultList = new FaiList<>();
            // 保存第一排序的结果
            FaiList<Param> firstComparatorResultList = new FaiList<>();
            // 保存第二排序的结果
            FaiList<Param> secondComparatorResultList = new FaiList<>();
            // 正在查询的表
            String searchingTable = "";

            Log.logStd("Don't get from cache;need reload;aid=%d,unionPriId=%d,searchSorterInfoList=%s", aid, unionPriId, searchSorterInfoList);
            // 开始搜索
            for (Param searchSorterInfo : searchSorterInfoList) {
                // SearchData：获取每个表对应的查询条件对应的查询结果
                // resultList：遍历searchSorterInfoList，resultList是最后满足"所有表的查询条件"的结果（类似于联表查询）。可以理解为搜索结果根据pdId去交集
                resultList = searchProc.getSearchDataAndSearchResultList(flow, aid, tid, unionPriId,
                    searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli);
                searchingTable = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);

                // 存在自定义排序，并且是对应的排序表的结果
                if (hasCustomComparator && customComparatorTable.equals(searchingTable)) {
                    customComparatorResultList = resultList;
                    Log.logStd("find customComparatorResultList;aid=%d,unionPriId=%d,customComparatorKey=%s,customComparatorTable=%s,customComparatorResultList=%s", aid, unionPriId, customComparatorKey, customComparatorTable, customComparatorResultList);
                }
                // 存在第一排序
                if (hasFirstComparator && firstComparatorTable.equals(searchingTable)) {
                    firstComparatorResultList = resultList;
                    Log.logStd("find firstComparatorResultList;aid=%d,unionPriId=%d,firstComparatorKey=%s,firstComparatorTable=%s,firstComparatorResultList=%s", aid, unionPriId, firstComparatorKey, firstComparatorTable, firstComparatorResultList);
                }
                // 需要第二排序
                if (needSecondComparatorSorting && secondComparatorTable.equals(searchingTable)) {
                    secondComparatorResultList = resultList;
                    Log.logStd("find secondComparatorResultList;aid=%d,unionPriId=%d,secondComparatorKey=%s,secondComparatorTable=%s,secondComparatorResultList=%s", aid, unionPriId, secondComparatorKey, secondComparatorTable, secondComparatorResultList);
                }

                if (resultList == null) {
                    // 搜索结果为空。直接结束搜索
                    resultList = new FaiList<>();
                    break;
                }
                if (resultList.isEmpty()) {
                    // 搜索结果为空。直接结束搜索
                    break;
                }
            }
            Log.logStd("search from db finish;aid=%d,unionPriId=%d,searchSorterInfoList=%s,resultList=%s", aid, unionPriId, searchSorterInfoList, resultList);
            // 最后一张搜索的表
            String lastSearchTable = searchingTable;
            // 排序的结果不为空
            boolean comparatorResultListNotExists = Utils.isEmptyList(customComparatorResultList) &&
                Utils.isEmptyList(firstComparatorResultList) && Utils.isEmptyList(secondComparatorResultList);

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

            if (needUnionEs) {
                // es的搜索结果和db的搜索结果根据pdId取交集
                resultList = searchProc.searchListFilterBySearchResultList(esSearchResult, ProductEntity.Info.PD_ID, resultList, ProductEntity.Info.PD_ID);
                // 如果有排序在es。先按照es的排序结果进行定制排序。
                Log.logStd("union es and db searchResult;aid=%d,unionPriId=%d,esSearchResult=%s,unionResult=%s", aid, unionPriId, esSearchResult, resultList);
            }


            // （1）如果es里有排序，resultList是es的搜索结果和db的搜索结果的交集，
            //     将交集resultList先按照“es里的排序结果”进行排序。因为整合出来的结果是乱序的。
            //  (2)如果es里有排序，resultList是es的搜索结果作为db的查询条件查询出来的，
            //     那么此时的resultList也有可能是乱序的。但是已经在es里排序了，所以resultList要按照“es里的排序结果”进行排序。
            if (!resultList.isEmpty() && hasComparatorInEs) {
                FaiList<Integer> sortedIdListOfEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);
                ParamComparator customComparatorOfEs = new ParamComparator();
                customComparatorOfEs.addKey(ProductEntity.Info.PD_ID, sortedIdListOfEs);
                // 按照es里的顺序先排好。
                resultList.sort(customComparatorOfEs);
                // 按照es的结果排好后，再按MgProductDbSearch里的排序去排

                Log.logStd("sort by pdIdOfEs;aid=%d,unionPriId=%d,sortedIdListOfEs=%s,sortResult=%s", aid, unionPriId, sortedIdListOfEs, resultList);
            }

            // 开始MgProductDbSearch里的排序
            if (needCompare && !resultList.isEmpty() && !comparatorResultListNotExists) {
                // 保存自定义排序的结果List
                FaiList<Param> listForCustomComparator = new FaiList<>();
                // 保存包含第一排序字段的结果的List
                FaiList<Param> listForFirstComparator = new FaiList<>();
                // 保存包含第二排序字段的结果的List
                FaiList<Param> listForSecondComparator = new FaiList<>();
                // 包含排序字段的结果的List和最终的结果根据pdId结果取交集
                boolean needGetListForSort = hasCustomComparator && !Utils.isEmptyList(customComparatorResultList) && !lastSearchTable.equals(customComparatorTable);
                if (needGetListForSort) {
                    // 根据pdId取交集
                    listForCustomComparator = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, customComparatorResultList, ProductEntity.Info.PD_ID);
                }

                needGetListForSort = hasFirstComparator && !Utils.isEmptyList(firstComparatorResultList) && !lastSearchTable.equals(firstComparatorTable);
                if (needGetListForSort) {
                    listForFirstComparator = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, firstComparatorResultList, ProductEntity.Info.PD_ID);
                }

                needGetListForSort = needSecondComparatorSorting && !Utils.isEmptyList(secondComparatorResultList) && !lastSearchTable.equals(secondComparatorTable);
                if (needGetListForSort) {
                    listForSecondComparator = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, secondComparatorResultList, ProductEntity.Info.PD_ID);
                }

                // 以上取交集也是可以减少下面循环的数据量

                // 整合自定义排序的字段到resultList中
                if (!Utils.isEmptyList(listForCustomComparator)) {
                    resultList = integrateComparatorFieldToResultList(customComparatorKey, listForCustomComparator, resultList);
                }

                // 整合第一排序字段到resultList中
                if (!Utils.isEmptyList(listForFirstComparator)) {
                    resultList = integrateComparatorFieldToResultList(firstComparatorKey, listForFirstComparator, resultList);
                }

                // 整合第二排序字段到resultList中
                if (!Utils.isEmptyList(listForSecondComparator)) {
                    resultList = integrateComparatorFieldToResultList(secondComparatorKey, listForSecondComparator, resultList);
                }

                // 最终进行排序
                resultList.sort(paramComparator);
                Log.logStd("finish sorted by contain in MgProductDbSearch's ComparatorKey;aid=%d,unionPriId=%d,paramComparator=%s,sortResultList=%s", aid, unionPriId, paramComparator.toJson(), resultList);
            }

            // 根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
            resultList = searchProc.removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);

            // 管理态变更的缓存时间
            resultManageCacheTime = Math.max(resultManageCacheTime, manageDataMaxChangeTime.value);
            // 访客态变更的缓存时间
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, resultManageCacheTime);
            // 分页
            SearchArg searchArg = new SearchArg();
            Searcher searcher = new Searcher(searchArg);
            mgProductDbSearch.setSearArgStartAndLimit(searchArg);
            // 分页的结果
            resultList = searcher.getParamList(resultList);
            // 将最终结果转化为pdIdList，添加到缓存
            FaiList<Integer> idList = searchProc.toIdList(resultList, ProductEntity.Info.PD_ID);

            Log.logStd("unionDbSearch finish;aid=%d,unionPriId=%d,searchSorterInfoList=%s,final search resultList=%s;", aid, unionPriId, searchSorterInfoList, resultList);
            // 添加缓存
            return integrateAndAddCache(idList, resultManageCacheTime, resultVisitorCacheTime, resultCacheKey);
        } else {
            Log.logStd("unionDbSearch finish;Don't need reLoad;get result from cache;aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", aid, unionPriId, resultCacheKey, resultCacheInfo);
            // 从缓存中获取数据
            return resultCacheInfo;
        }
    }

    /**
     * @param hasComparator  是否存在排序，true表示存在
     * @param comparatorTable 排序字段对应的表
     * @param searchSorterInfoList 包含要搜索的表等信息{@see initSearchSorterInfoList()}
     * @return true表示存在排序和排序表，而且排序字段对应的表没有包含在searchSorterInfoList中
     */
    public boolean findComparatorTable(boolean hasComparator, String comparatorTable, FaiList<Param> searchSorterInfoList) {
        boolean existInSearchSorterInfoList = false;
        if (hasComparator) {
            for (Param searchSorterInfo : searchSorterInfoList) {
                String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                if (tableName.equals(comparatorTable)) {
                    existInSearchSorterInfoList = true;
                    break;
                }
            }
        }
        return hasComparator && !existInSearchSorterInfoList && !Str.isEmpty(comparatorTable);
    }

    /**
     * 整合排序的字段到resultList
     * @param comparatorKey 排序字段
     * @param listForComparator 包含排序字段的结果
     * @param resultList 未包含排序字段的结果
     * @return 整合排序的字段到resultList后的resultList
     */
    public FaiList<Param> integrateComparatorFieldToResultList(String comparatorKey, FaiList<Param> listForComparator, FaiList<Param> resultList) {
        // Map<Integer, FaiList<Param>> pdIdMappingInfoList = new HashMap<>(listForComparator.size() * 4 / 3 + 1);
        /*      pdId          rlGroupId
         *       1                 1
         *       1                 2
         *       1                 3
         *       2                 1
         *       2                 2
         *       2                 3
         *       ......
         */
        // pdId一对一、一对多都可以满足
        /*listForComparator.forEach(info -> {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            FaiList<Param> infoList = Objects.isNull(pdIdMappingInfoList.get(pdId))? new FaiList<>() : pdIdMappingInfoList.get(pdId);
            infoList.add(info);
            pdIdMappingInfoList.put(pdId, infoList);
        });*/

        Map<Integer, List<Param>> pdIdMappingInfoList = listForComparator.stream().collect(Collectors.groupingBy(info -> info.getInt(ProductEntity.Info.PD_ID)));

        // resultList肯定不为空。因为在调用方法之前做了非空判断
        FaiList<Param> tempList = new FaiList<>();
        for (Param info:resultList) {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            List<Param> infoList = pdIdMappingInfoList.get(pdId);
            if (!Utils.isEmptyList(infoList)) {
                infoList.forEach(containComparatorKeyInfo -> {
                    // 将排序字段赋值到resultList的Param中
                    info.assign(containComparatorKeyInfo, comparatorKey);
                    // 注意这里不要直接将info赋值过去
                    Param newInfo = info.clone();
                    tempList.add(newInfo);
                });
            }
        }

        resultList = tempList;
        return resultList;
    }
}
