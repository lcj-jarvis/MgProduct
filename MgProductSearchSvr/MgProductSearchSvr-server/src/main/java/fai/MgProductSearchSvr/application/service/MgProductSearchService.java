package fai.MgProductSearchSvr.application.service;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
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
import java.util.*;

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
        if (status == BaseMgProductSearch.UpSalesStatusEnum.UP_AND_DOWN_SALES.upSalesStatus) {
            // 上架或者下架的，或者两种都有。使用in过滤
            FaiList<Integer> statusList = new FaiList<>();
            statusList.add(ProductBasicValObj.ProductValObj.Status.UP);
            statusList.add(ProductBasicValObj.ProductValObj.Status.DOWN);
            filters.add(FaiSearchExDef.SearchFilter.createIn(MgProductEsSearch.EsSearchFields.STATUS, FaiSearchExDef.SearchField.FieldType.INTEGER, statusList));
        } else if (status != BaseMgProductSearch.UpSalesStatusEnum.ALL.upSalesStatus) {
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
     * （2）设置排序：如果有自定义的排序，则自定义的排序优先级最高，es里的第一、二排序无效，db里的第一、第二排序无效
     *             如果无自定义的排序，此时如果es里设置了排序，就移除db里的排序
     */
    public void removeSameSearchFieldsAndSetSort(Param esSearchParam, Param dbSearchParam) {
        // 避免NPE
        if (esSearchParam == null) {
            esSearchParam = new Param();
        }
        if (dbSearchParam == null) {
            dbSearchParam = new Param();
        }
        // 判断是否有公共的查询字段
        if (esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD)) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD);
        }
        if (esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS)) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS);
        }

        // 设置排序
        boolean hasCustomKey = dbSearchParam.containsKey(MgProductDbSearch.DbSearchInfo.CUSTOM_COMPARATOR_KEY) &&
            dbSearchParam.containsKey(MgProductDbSearch.DbSearchInfo.CUSTOM_COMPARATOR_LIST);
        if (hasCustomKey) {
            // 有自定义的排序的话，es和db里的第一和第二排序都失效
            esSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY);
            esSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
            esSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SECOND_COMPARATOR_KEY);

            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY);
            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.FIRST_COMPARATOR_TABLE);
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SECOND_COMPARATOR_KEY);
            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.SECOND_COMPARATOR_TABLE);
        } else {
            // 没有自定义的排序
            // 判断第一排序或者第二排序是否在es中
            boolean sortKeyInEs = esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY) ||
                esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
            if (sortKeyInEs) {
                // 如果排序在es，则移除db里的第一排序和第二排序
                dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY);
                dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.FIRST_COMPARATOR_TABLE);
                dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
                dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SECOND_COMPARATOR_KEY);
                dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.SECOND_COMPARATOR_TABLE);
            }
        }
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
            // 去除db中和es重复的搜索字段和设置排序等
            removeSameSearchFieldsAndSetSort(esSearchParam, dbSearchParam);

            // 初始化搜索条件.
            MgProductSearchArg mgProductSearchArg = new MgProductSearchArg();
            mgProductSearchArg.initSearchParam(esSearchParam, dbSearchParam);
            MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
            MgProductDbSearch mgProductDbSearch = mgProductSearchArg.getMgProductDbSearch();
            // 判断查询条件是否为空
            if (mgProductSearchArg.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d; searchParam is null err", flow, aid, unionPriId);
            }
            boolean overLimit = Objects.nonNull(mgProductDbSearch) && mgProductDbSearch.getLimit() > BaseMgProductSearch.MAX_LIMIT ||
                Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.getLimit() > BaseMgProductSearch.MAX_LIMIT;
            if (overLimit) {
                // 允许最大的分页为200，如果超过的话，直接抛出异常
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "flow=%s;aid=%d;unionPriId=%d;tid=%d;over limit error,maxLimit=200", flow, aid, unionPriId, tid);
            }

            // 记录了管理态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> manageDataMaxChangeTime = new Ref<>(0L);
            // 记录了访客态数据的最新改变时间，用于判断搜索结果的缓存数据是否失效
            Ref<Long> visitorDataMaxChangeTime = new Ref<>(0L);
            FaiBuffer sendBuf = new FaiBuffer(true);
            String cacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, mgProductSearchArg.getEsSearchParam().toJson(), mgProductSearchArg.getDbSearchParam().toJson());
            // es查询条件不为空，db的查询条件为空，看看是否存在缓存
            if (Objects.isNull(mgProductDbSearch) || mgProductDbSearch.isEmpty()) {
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
                    Log.logStd("mgProductDbSearch is empty;Don't need uniteDbSearch;return es searchResult;aid=%d,unionPriId=%d;resultInfo=%s", aid, unionPriId, resultCacheInfo);
                    resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                    session.write(sendBuf);
                    return Errno.OK;
                }
            }

            // 执行到这里说明“es有搜索条件，db无搜索条件”的这种情况是无缓存 或者db的搜索条件不为空。
            FaiList<Param> esSearchResult = esSearch(flow, aid, unionPriId, mgProductSearchArg);
            // 在es中获取到的idList
            FaiList<Integer> idListFromEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);

            // (1) es 查询条件不为空，但是 es 的搜索结果为空，直接返回
            // (2) es 查询条件不为空，db的查询条件为空，添加缓存，返回es的搜索结果
            boolean notUniteDbSearch = (Objects.nonNull(mgProductEsSearch) && Utils.isEmptyList(idListFromEs)) ||
                Objects.isNull(mgProductDbSearch) || mgProductDbSearch.isEmpty();
            if (notUniteDbSearch) {
                getEsDataStatus(flow, aid, unionPriId, manageDataMaxChangeTime, visitorDataMaxChangeTime);
                // 添加缓存
                Param resultCacheInfo = integrateAndAddCache(idListFromEs, manageDataMaxChangeTime.value, visitorDataMaxChangeTime.value, cacheKey);
                resultCacheInfo.toBuffer(sendBuf, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
                session.write(sendBuf);
                Log.logStd("Don't need uniteDbSearch;return es searchResult;aid=%d,unionPriId=%d;mgProductDbSearch=%s;resultInfo=%s", aid, unionPriId, mgProductDbSearch, resultCacheInfo);
                return Errno.OK;
            }

            // 执行到这里说明db有查询条件了,继续走db的搜索逻辑.联合db查询,得到最终的搜索结果
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
     *  获取各个表的数据状态,同时初始化db的查询条件
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
        String searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName;
        if(!productBindPropDataSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                productBindPropDataSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productBindPropDataSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        ParamMatcher mgSpuBizSummarySearchMatcher = mgProductDbSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName;
        if(!mgSpuBizSummarySearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgSpuBizSummarySearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgSpuBizSummarySearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 3、"标签业务关系表" mgProductBindTag_xxxx 搜索， 还没有标签功能，暂时没开放
        ParamMatcher mgProductBindTagSearchMatcher = mgProductDbSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName;
        if (!mgProductBindTagSearchMatcher.isEmpty()) {
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductBindTagSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductBindTagSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 4、在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
        ParamMatcher mgProductBindGroupSearchMatcher = mgProductDbSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName;
        if(!mgProductBindGroupSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductBindGroupSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductBindGroupSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        ParamMatcher productRelSearchMatcher = mgProductDbSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
        if(!productRelSearchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                productRelSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, productRelSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 6、在"商品规格skuCode表" mgProductSpecSkuCode_0xxx 搜索
        ParamMatcher mgProductSpecSkuSearchMatcher = mgProductDbSearch.getProductSpecSkuCodeSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.searchTableName;
        if (!mgProductSpecSkuSearchMatcher.isEmpty()) {
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                mgProductSpecSkuSearchMatcher.and(idListFromEsParamMatcher);
            }
            checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, searchTableName, mgProductDbSearch, mgProductSpecSkuSearchMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
        }

        // 7、在 "商品基础表" mgProduct_xxxx 搜索
        ParamMatcher productBasicSearchMatcher = mgProductDbSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName;
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

        // 搜索各个表的数据状态
        eachTableCheckDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli,
            mgProductDbSearch, esSearchResult, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);

        // 根据搜索的表的数据(总记录数)由小到大排序，提高搜索效率
        ParamComparator compSizeForSorter = new ParamComparator(MgProductSearchProc.SearchSorterInfo.DATA_COUNT, false);
        searchSorterInfoList.sort(compSizeForSorter);

        // 获取搜索结果的缓存(为什么在排序之前呢？因为有可能排序的表会被重写，这样不同条件下的缓存的key可能会一致。所以在排序逻辑之前)
        String esSearchParamString = mgProductSearchArg.getEsSearchParam().toJson();
        String dbSearchParamString = mgProductSearchArg.getDbSearchParam().toJson();
        String resultCacheKey = MgProductSearchCache.ResultCache.getResultCacheKey(aid, unionPriId, esSearchParamString, dbSearchParamString);
        Param resultCacheInfo = MgProductSearchCache.ResultCache.getCacheInfo(resultCacheKey);
        Log.logStd("aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", aid, unionPriId, resultCacheKey, resultCacheInfo);

        // 补充的搜索信息list，比如排序字段 没有在搜索表中、或者只是搜索了没有包含rlPdId的表

        // TODO 后面将排序的逻辑抽成一个方法
        /*
           如果设置了自定义排序，paramComparator只包含自定义的排序。其他的排序无效
           如果没设置自定义排序，此时如果有第一排序，则第一排序就生效，否则第一排序就不生效。第二排序就要看是否开启。
         */
        ParamComparator paramComparator = mgProductDbSearch.getParamComparator();
        // 是否需要排序
        boolean needCompare = !paramComparator.isEmpty();
        // 自定义排序的key
        String customComparatorKey = mgProductDbSearch.getCustomComparatorKey();
        // 第一排序的key
        String firstComparatorKey = mgProductDbSearch.getFirstComparatorKey();
        // 第二排序的key
        String secondComparatorKey = mgProductDbSearch.getSecondComparatorKey();
        // 补偿的排序表
        String supplementSearchTable = "";
        // 是否有自定义的排序
        boolean hasCustomComparator = mgProductDbSearch.hasCustomComparator();
        // 是否存在第一排序
        boolean hasFirstComparator = mgProductDbSearch.hasFirstComparator();
        // 是否有除PdId，RlPdId之外的“其他自定义的排序字段”。
        boolean hasOtherCustomComparator = !ProductEntity.Info.PD_ID.equals(customComparatorKey) &&
            !ProductRelEntity.Info.RL_PD_ID.equals(customComparatorKey);
        // 是否有除PdId，RlPdId之外的“其他第一排序字段”
        boolean hasOtherFirstComparatorKey = !ProductEntity.Info.PD_ID.equals(firstComparatorKey) &&
            !ProductRelEntity.Info.RL_PD_ID.equals(firstComparatorKey);
        // 是否有除PdId，RlPdId之外的“其他第二排序字段”
        boolean hasOtherSecondComparatorKey = !ProductEntity.Info.PD_ID.equals(secondComparatorKey) &&
            !ProductRelEntity.Info.RL_PD_ID.equals(secondComparatorKey);
        // 是否需要第二排序
        boolean needSecondComparatorSorting = mgProductDbSearch.isNeedSecondComparatorSorting();
        if (needCompare) {
            // 执行到这里说明：
            // （1）如果设置了自定义排序，paramComparator只包含自定义的排序。其他的排序无效
            // （2）如果没设置自定义排序，此时如果有第一排序，则第一排序就生效，第二排序看是否开启
            // （3）如果没设置自定义排序，也没第一排序，则第二排序一定存在。

            // TODO 待优化代码，提取公共的for循环
            boolean findComparatorTable = false;
            if (hasCustomComparator && hasOtherCustomComparator) {
                // 有其他自定义排序的字段。该自定义排序的字段不是PdId，也不是RlPdId。必须设置该字段所在的表。
                String customComparatorTable = mgProductDbSearch.getCustomComparatorTable();
                for (Param searchSorterInfo : searchSorterInfoList) {
                    String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (tableName.equals(customComparatorTable)) {
                        findComparatorTable = true;
                    }
                }
                // 之前搜索的表中未包含自定义排序表
                if (!findComparatorTable) {
                    // 将自定义的排序表设置为补偿表
                    supplementSearchTable = customComparatorTable;
                }
            }

            // 无自定义的排序，存在第一排序，就走第一排序逻辑
            if (hasFirstComparator && hasOtherFirstComparatorKey) {
                // 无其他自定义的排序,并且第一排序字段不是 PD_ID 和 RL_PD_ID,是其他的排序字段
                // 如果是PdId的话，其实不用补偿，每张表都有PdId

                // 其他排序字段所在的表
                String otherComparatorKeyTable = mgProductDbSearch.getFirstComparatorTable();
                for (Param searchSorterInfo : searchSorterInfoList) {
                    String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (tableName.equals(otherComparatorKeyTable)) {
                        findComparatorTable = true;
                    }
                }
                // 之前搜索的表中未包含“其他第一排序字段所在的表”
                if (!findComparatorTable) {
                    // 将“其他第一排序字段所在的表”设置为补偿表
                    supplementSearchTable = otherComparatorKeyTable;
                }
            }

            // 将补偿的排序表放到最后
            // 如果补偿的排序表是“其他第一排序字段所在的表”，并有“其他第二排序字段所在的表”，
            // 则此时补偿的“其他第一排序字段所在的表”就在倒数第二的位置，因为有“其他第二排序字段所在的表”
            if (!Str.isEmpty(supplementSearchTable)) {
                // 后面会根据上一次的搜索结果，搜索会加上对应的 in PD_ID 进行搜索
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, supplementSearchTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 需要第二排序，并且是除PdId，rlPdId之外的排序字段
            if (needSecondComparatorSorting && hasOtherSecondComparatorKey) {
                String secondComparatorTable = mgProductDbSearch.getSecondComparatorTable();
                boolean findSecondComparatorTable = false;
                for (Param searchSorterInfo : searchSorterInfoList) {
                    String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (tableName.equals(secondComparatorTable)) {
                        findSecondComparatorTable = true;
                    }
                }
                // 之前搜索的表中没包含“其他第二排序字段所在的表”
                if (!findSecondComparatorTable) {
                    // 将“其他第二排序字段所在的表"设置为补偿表
                    supplementSearchTable = mgProductDbSearch.getSecondComparatorTable();
                    // 这里的supplementSearchTable肯定不为空（因为怎样都会有默认的第二排序表）
                    // 将第二排序字段对应的表放到最后
                    ParamMatcher defaultMatcher = new ParamMatcher();
                    checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, supplementSearchTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
                }
            }
        }

        /*
         * 需要重新排序表的情况：
         * (1)自定义的排序字段是PdId或者是RlPdId。
         * (2)不存在自定义排序，存在第一排序，并且第一排序字段是PdId或者是RlPdId。
         * (3)不存在自定义排序，第二排序字段是PdId或者是RlPdId。（第一排序字段可能存在，也可能不存在）
         */
        boolean rewriteComparatorTable = (hasCustomComparator && !hasOtherCustomComparator) ||
            (hasFirstComparator && !hasOtherFirstComparatorKey) ||
            (needSecondComparatorSorting && !hasOtherSecondComparatorKey);
        if (rewriteComparatorTable) {
            // 将排序表设置为包含为最后一张包含RlPdId的表（所有的表都包含PdId，如果排序字段是PdId，随便一张表做排序都行）

            // 如果只是搜索了包含PdId的表，但是要根据rlPdId排序。所以这里要补偿一张表包含rlPdId的表。
            boolean onlySearchNotHaveRlPdIdTable = false;
            if (searchSorterInfoList.size() <= MgProductDbSearch.NOT_HAVE_RLPDID_TABLE.size()) {
                int countNotHaveRlPdIdTable = 0;
                for (Param searchSorterInfo : searchSorterInfoList) {
                    String tableName = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                    if (MgProductDbSearch.NOT_HAVE_RLPDID_TABLE.contains(tableName)) {
                        ++countNotHaveRlPdIdTable;
                    }
                }
                onlySearchNotHaveRlPdIdTable = countNotHaveRlPdIdTable == searchSorterInfoList.size();
            }
            // 只是搜索了包含PdId的表，补偿一张表包含rlPdId的表。
            if (onlySearchNotHaveRlPdIdTable) {
                // 补偿一张包含rlPdId的表进行搜索
                supplementSearchTable = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;
                Log.logStd("only search notHaveRlPdIdTable;comparatorKey is pdId or rlPdId,need supplement searchTable;aid=%d,unionPriId=%d,supplementSearchTable=%s;", aid, unionPriId, supplementSearchTable);
                // 将补偿的表放到最后
                ParamMatcher defaultMatcher = new ParamMatcher();
                checkDataStatus(flow, aid, unionPriId, tid, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli, supplementSearchTable, mgProductDbSearch, defaultMatcher, manageDataMaxChangeTime, visitorDataMaxChangeTime, searchSorterInfoList);
            }

            // 设置排序的表（从后往前遍历，取第一张既包含PdId又包含rlPdId的表）
            int size = searchSorterInfoList.size();
            for (int i = size - 1; i >= 0; i--) {
                String tableName = searchSorterInfoList.get(i).getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);
                if (!MgProductDbSearch.NOT_HAVE_RLPDID_TABLE.contains(tableName)) {
                    // 如果该排序的表是自定义的，就设置在自定义上。如果是第一排序的，就设置在第一排序上。
                    if (hasCustomComparator) {
                        mgProductDbSearch.setCustomComparatorTable(tableName);
                        Log.logStd("aid=%d,unionPriId=%d,customComparatorKey=%s;rewrite customComparatorTable=%s", aid, unionPriId, customComparatorKey, tableName);
                    }

                    // 非rlPdId，pdId排序字段的才可以重写
                    if (hasFirstComparator && !hasOtherFirstComparatorKey) {
                        mgProductDbSearch.setFirstComparatorTable(tableName);
                        Log.logStd("aid=%d,unionPriId=%d,firstComparatorKey=%s;rewrite firstComparatorTable=%s", aid, unionPriId, firstComparatorKey, tableName);
                    }

                    // 如果需要第二排序的话，并且第二排序字段是rlPdId或者PdId才能改变第二排序表
                    // 因为有可能第一排序字段是rlPdId，此时第二排序字段是其他的排序字段，rewriteComparatorTable也是true，也会进入重写排序表的逻辑，
                    // 所以要加!hasOtherSecondComparatorKey条件进行设置，防止误改变了“其他第二排序字段的表”
                    if (needSecondComparatorSorting && !hasOtherSecondComparatorKey) {
                        mgProductDbSearch.setSecondComparatorTable(tableName);
                        Log.logStd("aid=%d,unionPriId=%d,secondComparatorKey=%s;rewrite secondComparatorTable=%s", aid, unionPriId, secondComparatorKey, tableName);
                    }
                    break;
                }
            }
        }

        // 执行到这里，对应的排序表都设置好了

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
            // 自定义排序的表
            String customComparatorTable = mgProductDbSearch.getCustomComparatorTable();
            // 第一排序表
            String firstComparatorTable = mgProductDbSearch.getFirstComparatorTable();
            // 第二排序的表
            String secondComparatorTable = mgProductDbSearch.getSecondComparatorTable();

            Log.logStd("Don't get from cache;need reload;aid=%d,unionPriId=%d,searchSorterInfoList=%s", aid, unionPriId, searchSorterInfoList);
            // 开始搜索
            for (Param searchSorterInfo : searchSorterInfoList) {
                // SearchData：获取每个表对应的查询条件对应的查询结果
                // resultList：遍历searchSorterInfoList，resultList是最后满足"所有表的查询条件"的结果（类似于联表查询）。
                resultList = searchProc.getSearchDataAndSearchResultList(flow, aid, tid, unionPriId,
                    searchSorterInfo, resultList, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli);
                searchingTable = searchSorterInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE);

                // 存在自定义排序，并且是对应的排序表的结果
                if (hasCustomComparator && customComparatorTable.equals(searchingTable)) {
                    customComparatorResultList = resultList;
                    Log.logStd("find customComparatorResultList;aid=%d,unionPriId=%d,customComparatorKey=%s,customComparatorTable=%s,customComparatorResultList=%s", aid, unionPriId, customComparatorKey, customComparatorTable, customComparatorResultList);
                }
                // 无自定义排序，存在第一排序
                if (hasFirstComparator && firstComparatorTable.equals(searchingTable)) {
                    firstComparatorResultList = resultList;
                    Log.logStd("find firstComparatorResultList;aid=%d,unionPriId=%d,firstComparatorKey=%s,firstComparatorTable=%s,firstComparatorResultList=%s", aid, unionPriId, firstComparatorKey, firstComparatorTable, firstComparatorResultList);
                }
                // 无自定义排序，需要第二排序
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
            // 最后一张搜索的表
            String lastSearchTable = searchingTable;
            // 排序的结果不为空
            boolean comparatorResultListNotExists = Utils.isEmptyList(customComparatorResultList) &&
                Utils.isEmptyList(firstComparatorResultList) && Utils.isEmptyList(secondComparatorResultList);
            // 开始排序
            if (needCompare && !resultList.isEmpty() && !comparatorResultListNotExists) {
                //自定义排序优先级最高
                // 如果存在自定义的排序，并且排序的结果不为空
                if (hasCustomComparator && !Utils.isEmptyList(customComparatorResultList)) {
                    // 获取包含自定义排序字段的排序结果
                    resultList = getListForSort(lastSearchTable, hasOtherCustomComparator, customComparatorTable, resultList, customComparatorResultList);
                } else {
                    // 保存包含第一排序字段的结果的List
                    FaiList<Param> listForFirstComparator = new FaiList<>();
                    // 保存包含第二排序字段的结果的List
                    FaiList<Param> listForSecondComparator = new FaiList<>();
                    // 不存在自定义的排序。存在第一排序
                    if (hasFirstComparator && !Utils.isEmptyList(firstComparatorResultList)) {
                        listForFirstComparator = getListForSort(lastSearchTable, hasOtherFirstComparatorKey, firstComparatorTable, resultList, firstComparatorResultList);
                        Log.logStd("get listForFirstComparator;aid=%d,unionPriId=%d,listForFirstComparator=%s", aid, unionPriId, listForFirstComparator);
                    }

                    // 不存在自定义的排序。需要第二排序
                    if (needSecondComparatorSorting && !Utils.isEmptyList(secondComparatorResultList)) {
                        listForSecondComparator = getListForSort(lastSearchTable, hasOtherSecondComparatorKey, secondComparatorTable, resultList, secondComparatorResultList);
                        Log.logStd("get listForSecondComparator;aid=%d,unionPriId=%d,listForSecondComparator=%s", aid, unionPriId, listForSecondComparator);
                    }

                    // 整合第一排序字段和第二排序字段到同一个List保存的Param中。
                    if (hasFirstComparator && needSecondComparatorSorting) {
                        // 第一排序字段和第二排序字段不一样才整合。如果一样就不用整合。
                        if (!firstComparatorKey.equals(secondComparatorKey)) {
                            // 第一排序字段和第二排序字段同时存在才需要整合
                            // 使用map尽量提高效率。初始化容量，减少扩容次数
                            Map<Integer, Param> pdIdMappingParam = new HashMap<>(listForSecondComparator.size() * 4 / 3 + 1);

                            // TODO 判断以pdId为key是否会导致数据丢失.如果pdId和第二排序字段是一对一的话，目前这样就没有问题。
                            // 比如第一字段排序是rlGroupId，第二字段排序是rlPdId。这样即使Map的key重复了，value覆盖的话也没关系
                            // 因为rlPdId和pdId具有一对一的确定关系。
                            // 但是第一排序字段是rlGroupId，第二排序字段是rlTagId这种，就会存在问题。
                            // 如果Map的key重复了，value覆盖的话，因为pdId和rlTagId是一对多的关系，这样覆盖的话，就会有问题。
                            // 但是目前第二排序都是能够唯一确定的排序，一般和pdId都是一对一的关系。暂时这样写是没问题的。
                            listForSecondComparator.forEach(info -> pdIdMappingParam.put(info.getInt(ProductEntity.Info.PD_ID), info));
                            Log.logStd("pdId mapping secondComparatorParam;aid=%d,unionPriId=%d,pdIdMappingParam=%s", aid, unionPriId, pdIdMappingParam);

                            for (Param info : listForFirstComparator) {
                                Integer pdId = info.getInt(ProductEntity.Info.PD_ID);

                                if (pdIdMappingParam.containsKey(pdId)) {
                                    Param infoInListForSecondComparator = pdIdMappingParam.get(pdId);
                                    // 将第二排序字段的值复制到第一排序的每个结果里
                                    info.assign(infoInListForSecondComparator, secondComparatorKey);
                                }
                            }
                        }
                        resultList = listForFirstComparator;
                    } else if (!hasFirstComparator && needSecondComparatorSorting) {
                        // 只有第二排序
                        resultList = listForSecondComparator;
                    } else if (hasFirstComparator) {
                        // 只有第一排序
                        resultList = listForFirstComparator;
                    }
                }
                // 进行排序
                resultList.sort(paramComparator);
            }

            // 根据 ProductEntity.Info.PD_ID 对搜索结果数据去重
            resultList = searchProc.removeRepeatedByKey(resultList, ProductEntity.Info.PD_ID);

            // 保存es和db取交集后的结果
            FaiList<Param> unionResultList = new FaiList<>();
            // 看看要不要和es的结果取交集。
            // 先手动给一个值,用于测试
            // int inSqlThreshold = 5;
            int inSqlThreshold = searchProc.getInSqlThreshold();
            // 大于in sql的阈值，就取交集。
            boolean needUnionEs = esSearchResult.size() > inSqlThreshold;
            // 是否有排序在ES中
            MgProductEsSearch mgProductEsSearch = mgProductSearchArg.getMgProductEsSearch();
            boolean comparatorInEs = (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.hasFirstComparator()) ||
                (Objects.nonNull(mgProductEsSearch) && mgProductEsSearch.isNeedSecondComparatorSorting());
            if (needUnionEs) {
                Log.logStd("Don't use in sql,need union es result;aid=%d,unionPriId=%d,esSearchResult=%s,dbResultList=&s", aid, unionPriId, esSearchResult, resultList);
                // 不用进行非空判断，idListFromEs.size() > inSqlThreshold，就证明非空了
                unionResultList = unionEsSearchResult(esSearchResult, resultList, comparatorInEs);
            } else {
                // 不需要取交集。但是已经在es中进行排序了，并且这个在es中排序后的结果，已经作为db的查询条件了。
                // 那么此时db得到的搜索结果就是未排序的。
                // 所以“此时db得到的搜索结果”要根据es的搜索结果的顺序进行“自定义的排序”。
                if (comparatorInEs) {
                    // es排序好的pdId
                    FaiList<Integer> sortedIdListOfEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);
                    Log.logStd("Don't need union es result,already sorted in es;aid=%d,unionPriId=%d,sortedIdListOfEs=%s,dbResultList=&s", aid, unionPriId, sortedIdListOfEs, resultList);
                    ParamComparator customComparatorOfEs = new ParamComparator();
                    customComparatorOfEs.addKey(ProductEntity.Info.PD_ID, sortedIdListOfEs);
                    resultList.sort(customComparatorOfEs);
                }
            }
            Log.logStd("finish sort resultList;aid=%d,unionPriId=%d;resultList=%s;", aid, unionPriId, resultList);

            // 管理态变更的缓存时间
            resultManageCacheTime = Math.max(resultManageCacheTime, manageDataMaxChangeTime.value);
            // 访客态变更的缓存时间
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, visitorDataMaxChangeTime.value);
            resultVisitorCacheTime = Math.max(resultVisitorCacheTime, resultManageCacheTime);
            // 最终整合的搜索结果
            FaiList<Param> finalResultList = needUnionEs ? unionResultList : resultList;
            // 分页
            SearchArg searchArg = new SearchArg();
            Searcher searcher = new Searcher(searchArg);
            mgProductDbSearch.setSearArgStartAndLimit(searchArg);
            // 分页的结果
            finalResultList = searcher.getParamList(finalResultList);
            // 将最终结果转化为pdIdList，添加到缓存
            FaiList<Integer> idList = searchProc.toIdList(finalResultList, ProductEntity.Info.PD_ID);

            // log输出太长了，不方便查看。测试输出便于查看。
            /*searchSorterInfoList.forEach(searchInfo -> {
                System.out.println("搜索的表：" + searchInfo.getString(MgProductSearchProc.SearchSorterInfo.SEARCH_TABLE));
                System.out.println("搜索表中的totalSize: " + searchInfo.getInt(DataStatus.Info.TOTAL_SIZE));
                System.out.println("是否需要重新加载db：" + searchInfo.getBoolean(MgProductSearchProc.SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE));
                System.out.println("要搜索的表对应的搜索条件：" + searchInfo.getObject(MgProductSearchProc.SearchSorterInfo.SEARCH_MATCHER));
                System.out.println("============================");
            });
            System.out.println("最终结果：" + finalResultList);
            finalResultList.forEach(System.out::println);*/

            Log.logStd("unionDbSearch finish;aid=%d,unionPriId=%d,searchSorterInfoList=%s,final search resultList=%s;", aid, unionPriId, searchSorterInfoList, finalResultList);
            // 添加缓存
            return integrateAndAddCache(idList, resultManageCacheTime, resultVisitorCacheTime, resultCacheKey);
        } else {
            Log.logStd("unionDbSearch finish;Don't need reLoad;get result from cache;aid=%d,unionPriId=%d,resultCacheKey=%s,resultCacheInfo=%s", aid, unionPriId, resultCacheKey, resultCacheInfo);
            // 从缓存中获取数据
            return resultCacheInfo;
        }
    }

    /**
     * Db的搜索结果和ES的搜索结果根据PdId取交集
     * @param esSearchResult Es的搜索结果
     * @param dbSearchResult Db的搜索结果
     * @param comparatorInEs 是否有排序在ES中，true表示有，false表示没有
     * @return 返回交集后的结果
     *
     *   TODO 考虑是否会存在并发修改异常的问题.因为idListTemp和idListFromEs都是非线程安全的。
     *        目前他们都是局部变量，不存在并发修改异常的问题。
     *        如果出现并发修改异常，就使用写时复制类,或者不适用remove方式，将结果另存。
     *        CopyOnWriteArraySet<Integer> objects = new CopyOnWriteArraySet<>();
     */
    public FaiList<Param> unionEsSearchResult(FaiList<Param> esSearchResult, FaiList<Param> dbSearchResult, boolean comparatorInEs) {
        // 将不排序的数据添加到Set中，排序好的数据还是在原集合中
        Set<Integer> idListTemp = new HashSet<>();
        FaiList<Integer> idListFromEs = searchProc.toIdList(esSearchResult, ProductEntity.Info.PD_ID);
        FaiList<Integer> idListFromDb = searchProc.toIdList(dbSearchResult, ProductEntity.Info.PD_ID);
        if (comparatorInEs) {
            // 排序在es
            idListTemp.addAll(idListFromDb);
            esSearchResult.removeIf(info -> !idListTemp.contains(info.getInt(ProductEntity.Info.PD_ID)));
            return esSearchResult;
        }
        // 执行到这里说明 存在自定义排序，或者第一排序，第一排序在db ，或者 es 和 db 都没有排序。
        // 此时都用db的PdIdList进行remove，这样不管有没有排序，都可以保证其顺序。
        // 如果 es 和 db 都没有排序，尽量减少取交集的时间复杂度，其实addAll也要遍历，不管addAll哪个，时间应该差不多。
        idListTemp.addAll(idListFromEs);
        dbSearchResult.removeIf(info -> !idListTemp.contains(info.getInt(ProductEntity.Info.PD_ID)));
        return dbSearchResult;
    }

    /**
     * 获取包含排序字段的结果
     * @param lastSearchTable 搜索的最后一张表
     * @param hasOtherComparatorKey 除rlPdId，PdId之外的排序字段
     * @param comparatorTable 排序字段对应的排序表
     * @param resultList 最后的搜索结果（不一定包含排序字段）
     * @param comparatorResultList 包含排序字段的搜索结果
     * @return 返回包含排序字段的最终搜索结果。
     *          即根据PdId将resultList和comparatorResultList取交集，返回comparatorResultList属于交集部分的内容。
     *
     * 注释的解释是以自定义的排序为例写的，第一排序和第二排序可以类比理解。
     */
    public FaiList<Param> getListForSort(String lastSearchTable, boolean hasOtherComparatorKey, String comparatorTable,
                                         FaiList<Param> resultList, FaiList<Param> comparatorResultList) {
        /*FaiList<Param> listForSort = resultList;
        // 如果最后搜索的表不包含rlPdId。
        if (NOT_HAVE_RLPDID_TABLE.contains(lastSearchTable)) {
            if (hasOtherComparatorKey) {
                // 自定义排序字段是其他字段。

                // 如果最后搜索的表，不是“其他自定义排序字段对应的搜索表”，说明最后的结果不是要排序的结果(因为不一定包含排序的字段)
                // 所以和排序的结果取交集，返回交集在排序结果中的那部分
                if (!lastSearchTable.equals(comparatorTable)) {
                    listForSort = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                }
            } else {
                // 如果最后搜索的表不包含rlPdId。但是自定义排序的排序字段是rlPdId或者PdId的

                // 取resultList的PdIdList和customComparatorResultList和PdIdList的交集。
                // 交集取的是包含rlPdId的customComparatorResultList部分。
                listForSort = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
            }
        } else {
            // 最后搜索的表，包含rlPdId。
            // 此时如果排序的字段是rlPdId或者PdId，最后结果的resultList就是要排序的结果了。

            if (hasOtherComparatorKey) {
                // 如果最后搜索的表，不是“其他自定义排序字段对应的搜索表”，说明最后的结果不是要排序的结果(因为不一定包含排序的字段)
                // 所以和排序的结果取交集，返回交集在排序结果中的那部分
                if (!lastSearchTable.equals(comparatorTable)) {
                    listForSort = searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                }
            }
        }
        return listForSort;*/

        // 简化
        // 如果最后搜索的表不包含rlPdId。
        if (MgProductDbSearch.NOT_HAVE_RLPDID_TABLE.contains(lastSearchTable)) {
            if (hasOtherComparatorKey) {
                // 自定义排序字段是其他字段。

                // 如果最后搜索的表，不是“其他自定义排序字段对应的搜索表”，说明最后的结果不是要排序的结果(因为不一定包含排序的字段)
                // 所以和排序的结果取交集，返回交集在排序结果中的那部分
                // 如果是的话，resultList就是排序的结果
                if (!lastSearchTable.equals(comparatorTable)) {
                    return searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
                }
            } else {
                // 如果最后搜索的表不包含rlPdId。但是自定义排序的排序字段是rlPdId或者PdId的

                // 取resultList的PdIdList和customComparatorResultList和PdIdList的交集。
                // 交集取的是包含rlPdId的customComparatorResultList部分。
                return searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
            }
        }
        // 最后搜索的表，包含rlPdId。
        // 此时如果排序的字段是rlPdId或者PdId，最后结果的resultList就是要排序的结果了。
        if (hasOtherComparatorKey) {
            // 如果最后搜索的表，不是“其他自定义排序字段对应的搜索表”，说明最后的结果不是要排序的结果(因为不一定包含排序的字段)
            // 所以和排序的结果取交集，返回交集在排序结果中的那部分
            if (!lastSearchTable.equals(comparatorTable)) {
                return searchProc.searchListFilterBySearchResultList(resultList, ProductEntity.Info.PD_ID, comparatorResultList, ProductEntity.Info.PD_ID);
            }
        }
        return resultList;
    }
}
