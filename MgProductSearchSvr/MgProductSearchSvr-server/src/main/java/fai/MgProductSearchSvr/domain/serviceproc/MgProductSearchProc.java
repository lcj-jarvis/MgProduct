package fai.MgProductSearchSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.async.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.utils.BaseMgProductSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductDbSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.comm.ParseData;
import fai.MgProductSearchSvr.domain.repository.MgProductSearchCache;
import fai.MgProductSpecSvr.interfaces.cli.async.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.async.MgProductStoreCli;
import fai.comm.jnetkit.server.fai.RemoteStandResult;
import fai.comm.rpc.client.DefaultFuture;
import fai.comm.rpc.client.FaiClientProxyFactory;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc.DbSearchSorterInfo.*;

/**
 * @author Lu
 * @date 2021-08-06 9:48
 */
public class MgProductSearchProc {

    /**
     * 异步获取“es数据来源的表”对应的管理态和访客态最新的修改时间
     * @param manageDataMaxChangeTime 管理态最新的修改时间
     * @param visitorDataMaxChangeTime 访客态最新的修改时间
     */
    public void asyncGetEsDataStatus(int flow, int aid, int unionPriId, Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime) {
        // final避免后面改到，ConcurrentSkipListSet，相当于线程安全的TreeSet，可以避免加锁
        final ConcurrentSkipListSet<Long> manageDataChangeTimeSet = new ConcurrentSkipListSet<>();
        final ConcurrentSkipListSet<Long> visitorDataChangeTimeSet = new ConcurrentSkipListSet<>();

        FaiClientProxyFactory.initFlowGenerator(() -> flow);
        // 目前es的数据来源与商品表和商品业务表，所以搜索这两张表的数据状态
        MgProductBasicCli asyncMgProductBasicCli = FaiClientProxyFactory.createProxy(MgProductBasicCli.class);

        // 异步获取数据状态
        DefaultFuture pdDataStatus = asyncMgProductBasicCli.getPdDataStatus(flow, aid);
        DefaultFuture pdRelDataStatus = asyncMgProductBasicCli.getPdRelDataStatus(flow, aid, unionPriId);

        Map<String, CompletableFuture> tableNameMappingFuture = new HashMap<>(16);
        tableNameMappingFuture.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName(), pdDataStatus);
        tableNameMappingFuture.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName(), pdRelDataStatus);

        CountDownLatch countDownLatch = new CountDownLatch(tableNameMappingFuture.size());
        tableNameMappingFuture.forEach((tableName, dataStatusFuture) -> {
            // TODO: 注意whenCompleteAsync默认使用的是ForkJoinPool线程池，当然也可以使用自定义的线程池
            // 异步回调获取结果
            dataStatusFuture.whenCompleteAsync((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess()) {
                    Integer getParamKey = ParseData.TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.get(tableName);
                    Param statusInfo = result.getObject(getParamKey, Param.class);
                    manageDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
                    visitorDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
                } else {
                    int rt = result.getRt();
                    throw new MgException(rt, "es get " + tableName + " dataStatus error;flow=%d,aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                }
                Log.logStd("finish es get " + tableName + " dataStatus;flow=%d,aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                // 回调完成就减一
                countDownLatch.countDown();
            });
        });

        try {
            // 同步阻塞等待回调执行完成
            countDownLatch.await();
        } catch (Exception e) {
            throw new MgException(Errno.ERROR, "waiting get es dataStatus time out;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
        }

        manageDataMaxChangeTime.value = Math.max(manageDataMaxChangeTime.value, manageDataChangeTimeSet.last());
        visitorDataMaxChangeTime.value = Math.max(visitorDataMaxChangeTime.value, visitorDataChangeTimeSet.last());
    }

    /**
     * 异步获取要搜索的表的数据状态
     */
    public void asyncCheckDataStatus(int flow, int aid, int unionPriId, int tid,
                                     MgProductDbSearch mgProductDbSearch,
                                     FaiList<Integer> esSearchResult,
                                     final Set<Long> manageDataChangeTimeSet,
                                     final Set<Long> visitorDataChangeTimeSet,
                                     final Map<String, Param> tableMappingSearchAndSortInfo,
                                     MgProductBasicCli asyncMgProductBasicCli,
                                     MgProductStoreCli asyncMgProductStoreCli,
                                     MgProductSpecCli asyncMgProductSpecCli) {

        // 是否将es的idList直接当作db的查询条件
        ParamMatcher idListFromEsParamMatcher = new ParamMatcher();
        if (!Utils.isEmptyList(esSearchResult)) {
            int inSqlThreshold = getInSqlThreshold();
            if (esSearchResult.size() == 1) {
                Integer pdId = esSearchResult.get(0);
                idListFromEsParamMatcher.and(ProductBasicEntity.ProductInfo.PD_ID, ParamMatcher.EQ, pdId);
            } else if (esSearchResult.size() <= inSqlThreshold) {
                idListFromEsParamMatcher.and(ProductBasicEntity.ProductInfo.PD_ID, ParamMatcher.IN, esSearchResult);
            }
        }

        // 表名映射各个异步获取数据状态返回的CompletableFuture
        Map<String, CompletableFuture> tableNameMappingFuture = new HashMap<>();
        // 保存每个表对应的查询条件
        Map<String, ParamMatcher> tableNameMappingSearchMatcher = new HashMap<>();

        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher searchMatcher = mgProductDbSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 3、"商品与标签关联表" mgProductBindTag_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 4、在 "商品与分类关联表" mgProductBindGroup_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName();
        if (mgProductDbSearch.isEmpty()) {
            // 执行到这里说明es和db的搜索条件都为空.因为es搜索条件不为空，db搜索条件为空的情况，在前面已经直接返回es里的搜索结果了。
            // db搜索的表为空，就补充一张MG_PRODUCT_REL作为空搜索条件的表
            tableNameMappingSearchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, tableNameMappingFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }  else {
            // 或者es的搜索条件为空，db的搜索条件不为空，或者es的不为空，db的搜索条件不为空
            asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 6、在"商品规格skuCode表" mgProductSpecSkuCode_0xxx 搜索
        searchMatcher = mgProductDbSearch.getProductSpecSkuCodeSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 7、在 "商品基础表" mgProduct_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName();
        asyncGetDataStatusForEachTable(flow, aid, unionPriId, tid, searchTableName, searchMatcher, idListFromEsParamMatcher, tableNameMappingFuture, tableNameMappingSearchMatcher, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);

        // 等待回调执行完成，获取结果
        callbackGetDataStatus(flow, aid, unionPriId, tableNameMappingFuture, tableNameMappingSearchMatcher, mgProductDbSearch, manageDataChangeTimeSet, visitorDataChangeTimeSet, tableMappingSearchAndSortInfo);
    }


    /**
     * 根据tableName异步获取该表的数据状态
     * @param tableName 要获取数据状态的表
     * @param tableNameMappingFuture 表名映射获取数据状态的DefaultFuture
     */
    public void asyncGetDataStatus(int flow, int aid, int unionPriId, int tid,
                                    String tableName,
                                    Map<String, CompletableFuture> tableNameMappingFuture,
                                    MgProductBasicCli asyncMgProductBasicCli,
                                    MgProductStoreCli asyncMgProductStoreCli,
                                    MgProductSpecCli asyncMgProductSpecCli) {
        // 从远端获取数据
        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture pdDaStatus = asyncMgProductBasicCli.getPdDataStatus(flow, aid);
            tableNameMappingFuture.put(tableName, pdDaStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture pdRelDataStatus = asyncMgProductBasicCli.getPdRelDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, pdRelDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindPropDataStatus = asyncMgProductBasicCli.getBindPropDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindPropDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindGroupDataStatus = asyncMgProductBasicCli.getBindGroupDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindGroupDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindTagDataStatus = asyncMgProductBasicCli.getBindTagDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindTagDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture spuBizSummaryDataStatus = asyncMgProductStoreCli.getSpuBizSummaryDataStatus(flow, aid, tid, unionPriId);
            tableNameMappingFuture.put(tableName, spuBizSummaryDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture skuCodeDataStatus = asyncMgProductSpecCli.getSkuCodeDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, skuCodeDataStatus);
        }
    }

    private void asyncGetDataStatusForEachTable(int flow, int aid, int unionPriId, int tid,
                                                String tableName,
                                                ParamMatcher searchMatcher,
                                                ParamMatcher idListFromEsParamMatcher,
                                                Map<String, CompletableFuture> tableNameMappingFuture,
                                                Map<String, ParamMatcher> tableNameMappingSearchMatcher,
                                                MgProductBasicCli asyncMgProductBasicCli,
                                                MgProductStoreCli asyncMgProductStoreCli,
                                                MgProductSpecCli asyncMgProductSpecCli) {
        if(!searchMatcher.isEmpty()){
            if (!idListFromEsParamMatcher.isEmpty()) {
                // 添加es的idList作为查询条件
                idListFromEsParamMatcher.and(idListFromEsParamMatcher);
            }
            tableNameMappingSearchMatcher.put(tableName, searchMatcher);
            // 异步获取数据状态
            asyncGetDataStatus(flow, aid, unionPriId, tid, tableName, tableNameMappingFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }
    }

    /**
     * 回调获取各个表的数据状态，并保存每张表对应的搜索和排序等信息到tableMappingSearchAndSortInfo
     * @param tableNameMappingFuture 保存了表名映射获取数据状态的DefaultFuture
     * @param tableNameMappingSearchMatcher 保存了表名映射对应的搜索条件
     * @param manageDataChangeTimeSet 用于保存每张表关联态的修改时间
     * @param visitorDataChangeTimeSet 用于保存每张表访客态的修改时间
     * @param tableMappingSearchAndSortInfo 表名映射“包含搜索和排序等信息的Param”
     */
    public void callbackGetDataStatus(int flow, int aid, int unionPriId,
                                      Map<String, CompletableFuture> tableNameMappingFuture,
                                      Map<String, ParamMatcher> tableNameMappingSearchMatcher,
                                      MgProductDbSearch mgProductDbSearch,
                                      final Set<Long> manageDataChangeTimeSet,
                                      final Set<Long> visitorDataChangeTimeSet,
                                      final Map<String, Param> tableMappingSearchAndSortInfo) {
        /**
         * 为什么不用CompletableFuture的allOf方法呢，而用CountDownLatch呢?
         * 因为CompletableFuture的allOf方法只是等待所有的CompletableFuture返回结果就停止阻塞，
         * 而不是等待CompletableFuture的whenComplete回调执行完才停止阻塞。
         */
        CountDownLatch countDownLatch = new CountDownLatch(tableNameMappingFuture.size());
        tableNameMappingFuture.forEach((tableName, defaultFuture) -> {
            defaultFuture.whenCompleteAsync((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess()) {
                    Log.logStd(" begin to callback get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    // 获取缓存在本地的数据状态
                    String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
                    Param localDataStatusCacheInfo = MgProductSearchCache.LocalDataStatusCache.getLocalDataStatusCache(cacheKey);

                    // 获取远程的数据状态
                    Integer getParamKey = ParseData.TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.get(tableName);
                    Param remoteDataStatusInfo = result.getObject(getParamKey, Param.class);

                    // 该表对应的查询条件
                    ParamMatcher matcher = tableNameMappingSearchMatcher.get(tableName);
                    initSearchAndSortInfoTask(tableName, matcher, localDataStatusCacheInfo, remoteDataStatusInfo, mgProductDbSearch, manageDataChangeTimeSet, visitorDataChangeTimeSet, tableMappingSearchAndSortInfo);

                    Log.logStd(" finish getting " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    // countDownLatch减一
                    countDownLatch.countDown();
                } else {
                    int rt = result.getRt();
                    throw new MgException(rt, "get " + tableName + " table dataStatus error;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                }
            });
        });

        // 阻塞等待回调执行完成
        long begin = System.currentTimeMillis();
        try {
            // 可以设置回调超时时间
            countDownLatch.await();
            long end = System.currentTimeMillis();
            Log.logStd("finish async get all dataStatus;consume " + (end - begin) + "ms;tableNameMappingFuture=%s", tableNameMappingFuture);
        } catch (InterruptedException e) {
            long end = System.currentTimeMillis();
            throw new MgException(Errno.ERROR, "waiting each cli async get all dataStatus timeout;consume " + (end - begin) + "ms", tableNameMappingFuture);
        }
    }


    /**
     * 初始化每张表的搜索、排序、数据状态等信息，并保存这些信息到tableMappingSearchAndSortInfo中
     * @param tableName 表名
     * @param searchMatcher 该表对应的搜索条件
     * @param localDataStatusCacheInfo 从本地缓存获取的数据状态
     * @param remoteDataStatusInfo 从远程获取的数据状态
     * @param manageDataChangeTimeSet 用于保存每张表关联态的修改时间
     * @param visitorDataChangeTimeSet 用于保存每张表访客态的修改时间
     * @param tableMappingSearchAndSortInfo 表名映射“包含搜索和排序等信息的Param”
     */
    public void initSearchAndSortInfoTask(String tableName,
                                          ParamMatcher searchMatcher,
                                          Param localDataStatusCacheInfo,
                                          Param remoteDataStatusInfo,
                                          MgProductDbSearch mgProductDbSearch,
                                          final Set<Long> manageDataChangeTimeSet,
                                          final Set<Long> visitorDataChangeTimeSet,
                                          final Map<String, Param> tableMappingSearchAndSortInfo) {
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;
        if(!Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 是否只查管理态的数据
            isOnlySearchManageData = mgProductDbSearch.getIsOnlySearchManageData(tableName);
            /*
                （1）管理态数据变动，影响所有的缓存, 因为管理变动可能会导致访客的数据变动
                （2）如果有搜索访客字段，并且是访客字段时间有变动，需要 reload 数据
                 以上两种情况满足其中一个就行
             */
            needGetDataFromRemote = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) ||
                (!isOnlySearchManageData && localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
            if (needGetDataFromRemote) {
                localDataStatusCacheInfo = remoteDataStatusInfo;
            }
        } else if (Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 本地没有数据，如果进入搜索逻辑，则需要重新reload数据，赋值到新的 cache
            localDataStatusCacheInfo = remoteDataStatusInfo;
            needGetDataFromRemote = true;
        } else if (Str.isEmpty(localDataStatusCacheInfo) && Str.isEmpty(remoteDataStatusInfo)){
            throw new MgException(Errno.ERROR, "searchTableName=%s;localDataStatusCacheInfo == null && remoteDataStatusInfo == null err", tableName);
        }

        // 这里的Set为ConcurrentSkipListSet，相当于线程安全的TreeSet，不用加锁
        manageDataChangeTimeSet.add(localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
        if (!isOnlySearchManageData) {
            // 这里的Set为ConcurrentSkipListSet，相当于线程安全的TreeSet，不用加锁
            visitorDataChangeTimeSet.add(localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatus.Info.TOTAL_SIZE);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        long visitorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);

        // 设置需要搜索的信息
        initTableMappingSearchAndSortInfo(tableMappingSearchAndSortInfo, dataAllSize, manageDataUpdateTime, visitorDataUpdateTime, tableName, needGetDataFromRemote, searchMatcher);
    }

    /**
     * 设置需要搜索的信息和排序
     * @param tableMappingSearchAndSortInfo 线程安全的Map,表名映射“包含搜索和排序等信息的Param”
     * @param dataAllSize 数据的总记录数
     * @param manageDataUpdateTime 管理态最新的更新时间
     * @param visitorDataUpdateTime 访客态最新的更新时间
     * @param searchTableName 搜索的表
     * @param needGetDataFromRemote 是否需要调用远程的cli从db获取数据
     * @param searchMatcher 搜索的条件
     */
    private void initTableMappingSearchAndSortInfo(final Map<String, Param> tableMappingSearchAndSortInfo, int dataAllSize, long manageDataUpdateTime, long visitorDataUpdateTime, String searchTableName, boolean needGetDataFromRemote, ParamMatcher searchMatcher) {
        Param info = new Param();
        info.setString(DbSearchSorterInfo.SEARCH_TABLE, searchTableName);
        info.setInt(DbSearchSorterInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(DbSearchSorterInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, visitorDataUpdateTime);
        info.setInt(DataStatus.Info.TOTAL_SIZE, dataAllSize);
        info.setObject(SEARCH_MATCHER, searchMatcher);
        // 线程安全的Map
        tableMappingSearchAndSortInfo.put(searchTableName, info);
    }

    /**
     * 整合并添加搜索结果到缓存
     * @param idList 缓存的pdId List
     * @param manageDataCacheTime 管理态数据最新的修改时间
     * @param visitDataCacheTime  访客态数据的最新修改时间
     * @param cacheKey 缓存的key
     * @return 返回包含缓存结果的Param
     */
    public Param integrateAndAddCache(FaiList<Integer> idList, Long manageDataCacheTime, Long visitDataCacheTime, String cacheKey) {
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
     * TODO 【注意】搜索结果返回的只是部分字段，如果要排序，要注意排序字段是否存在搜索结果中
     * 从远端异步获取数据
     * @param tableName 搜索的表
     * @param searchArg 搜索条件
     * @param addDataToLocalCache true表示全量load数据，并将数据到本地缓存
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     */
    private void asyncGetDataFromRemote(int flow, int aid, int tid, int unionPriId,
                                       String tableName, SearchArg searchArg, boolean addDataToLocalCache,
                                       Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                                       MgProductBasicCli asyncMgProductBasicCli,
                                       MgProductStoreCli asyncMgProductStoreCli,
                                       MgProductSpecCli asyncMgProductSpecCli) {

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdData(flow, aid) : asyncMgProductBasicCli.searchPdFromDb(flow, aid, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdRelDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdRelData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchPdRelFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdRelDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindPropDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllBindPropData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindPropFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindPropDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindGroupDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllBindGroupData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindGroupFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindGroupDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindTagDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdBindTagData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindTagFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindTagDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture spuBizSummaryDataFuture = addDataToLocalCache ? asyncMgProductStoreCli.getSpuBizSummaryAllData(flow, aid, tid, unionPriId) : asyncMgProductStoreCli.searchSpuBizSummaryFromDb(flow, aid, tid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, spuBizSummaryDataFuture);
        }

        // 目前提供的接口查询结果，只有这三个字段，Info.SKU_CODE, Info.PD_ID, Info.SKU_ID
        if (MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)) {
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%d;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture skuCodeDataFuture = addDataToLocalCache ? asyncMgProductSpecCli.getSkuCodeAllData(flow, aid, unionPriId) : asyncMgProductSpecCli.searchSkuCodeFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, skuCodeDataFuture);
        }
    }

    /**
     * 将dataList使用搜索条件searchMatcher过滤，同时将过滤后的结果保存至tableNameMappingPdIdBitSet便于取交集
     * @param dataList 被过滤的结果
     * @param recvSearchResult 接收搜索条件searchMatcher过滤后的结果
     */
    public void prepareData(String tableName, ParamMatcher searchMatcher,
                            FaiList<Param> dataList,
                            FaiList<Param> recvSearchResult,
                            Map<String, Map<Integer, Param>> tableNameMappingPdIdParam,
                            Map<String, BitSet> tableNameMappingPdIdBitSet) {
        // 使用BitSet 是为了提高取交集的效率.最好初始化一下BitSet的大小。默认是64
        BitSet pdIdBitSet = new BitSet();
        // 目前pdId 和 排序字段都是一对一的场景
        Map<Integer, Param> pdIdMappingParam = new HashMap<>(dataList.size());

        for (Param info : dataList) {
            // 使用搜索条件过滤
            if (searchMatcher != null && !searchMatcher.isEmpty() && !searchMatcher.match(info)) {
                continue;
            }

            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            // BitSet 不包含，才保存
            if (!pdIdBitSet.get(pdId)) {
                pdIdBitSet.set(pdId);
            }

            if (!pdIdMappingParam.containsKey(pdId)) {
                pdIdMappingParam.put(pdId, info);
            }

            if (recvSearchResult != null) {
                recvSearchResult.add(info);
            }
        }

        Log.logStd("finish adding pdId to BitSet;tableName=%s;pdIdBitSet=%s", tableName, pdIdBitSet);
        // 主要用于排序
        tableNameMappingPdIdParam.put(tableName, pdIdMappingParam);
        // 主要用于取交集
        tableNameMappingPdIdBitSet.put(tableName, pdIdBitSet);
    }


    /**
     * 从本地缓存中获取搜索结果
     * @param tableName 表名
     * @param searchMatcher 搜索条件
     * @param localCacheDataList 本地缓存的结果
     * @param tableNameMappingPdIdParam 表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
     * @param tableNameMappingPdIdBitSet 表名映射每张表的pdId的BitSet，主要用于取交集。多线程回调，使用线程安全的Map
     * @return 返回本地缓存保存的搜索结果
     */
    public FaiList<Param> getSearchResult(String tableName, ParamMatcher searchMatcher,
                                          FaiList<Param> localCacheDataList,
                                          Map<String, Map<Integer, Param>> tableNameMappingPdIdParam,
                                          Map<String, BitSet> tableNameMappingPdIdBitSet) {

        // 用搜索条件过滤后的结果
        FaiList<Param> searchResult = new FaiList<>();
        prepareData(tableName, searchMatcher, localCacheDataList, searchResult, tableNameMappingPdIdParam, tableNameMappingPdIdBitSet);
        return searchResult;
    }

    /**
     * 异步获取搜索结果
     * @param searchAndSortInfo 保存搜索和排序等信息
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     * @param tableNameMappingLocalGetDataFuture 表名映射“从本地缓存获取数据异步任务”
     * @param tableNameMappingPdIdParamList 表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
     * @param tableNameMappingPdIdBitSet 表名映射每张表的pdId的BitSet，主要用于取交集。多线程回调，使用线程安全的Map
     */
    public void asyncGetData(int flow, int aid, int tid, int unionPriId,
                             Param searchAndSortInfo,
                             Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                             Map<String, CompletableFuture> tableNameMappingLocalGetDataFuture,
                             Map<String, Map<Integer, Param>> tableNameMappingPdIdParamList,
                             Map<String, BitSet> tableNameMappingPdIdBitSet,
                             MgProductBasicCli asyncMgProductBasicCli,
                             MgProductStoreCli asyncMgProductStoreCli,
                             MgProductSpecCli asyncMgProductSpecCli) {

        String tableName = searchAndSortInfo.getString(DbSearchSorterInfo.SEARCH_TABLE);
        // 获取查询条件
        ParamMatcher searchMatcher = (ParamMatcher) searchAndSortInfo.getObject(DbSearchSorterInfo.SEARCH_MATCHER);
        int dataCount = searchAndSortInfo.getInt(DbSearchSorterInfo.DATA_COUNT);
        // 全量缓存的阈值
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        // 添加数据到本地缓存
        boolean addDataToLocalCache = dataCount <= dataLoadFromDbThreshold;
        searchAndSortInfo.setBoolean(DbSearchSorterInfo.ADD_DATA_TO_LOCAL_CACHE, addDataToLocalCache);
        // 是否需要从远端获取数据
        boolean needGetDataFromRemote = searchAndSortInfo.getBoolean(DbSearchSorterInfo.NEED_GET_DATA_FROM_REMOTE);
        // 直接从缓存拿
        if (!needGetDataFromRemote) {
            // 从一级缓存获取数据
            ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
            String cacheKey = MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName);
            if (localMgProductSearchData.containsKey(cacheKey)) {
                FaiList<Param> localCacheDataList = localMgProductSearchData.get(cacheKey);
                // 保存缓存的结果到searchAndSortInfo
                searchAndSortInfo.setList(DbSearchSorterInfo.CACHE_DATA_LIST, localCacheDataList);
                // 从缓存拿的也异步执行
                CompletableFuture<FaiList<Param>> localGetDataFuture = CompletableFuture.supplyAsync(() -> getSearchResult(tableName, searchMatcher, localCacheDataList, tableNameMappingPdIdParamList, tableNameMappingPdIdBitSet));
                // 添加到表名映射“从本地缓存获取数据异步任务”，用于回调
                tableNameMappingLocalGetDataFuture.put(tableName, localGetDataFuture);
                return;
            } else {
                // 可能缓存被回收，重新获取数据
                Log.logStd("cache1==null; flow=%s;aid=%d;unionPriId=%d;searchSorterInfo=%s;", flow, aid, unionPriId, searchAndSortInfo);
            }
        }

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        // 从远端异步获取数据
        asyncGetDataFromRemote(flow, aid, tid, unionPriId, tableName, searchArg, addDataToLocalCache, tableNameMappingRemoteGetDataFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
    }

    /**
     * 回调获取搜索结果
     * @param tableNameMappingLocalGetDataFuture 表名映射“从本地缓存获取数据异步任务”
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     * @param tableMappingSearchAndSortInfo  表名映射“包含搜索和排序等信息的Param”
     * @param tableNameMappingPdIdParam  表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
     * @param tableNameMappingPdIdBitSet 表名映射每张表的pdId的BitSet，主要用于取交集。多线程回调，使用线程安全的Map
     */
    public void callbackGetResultList(int flow, int aid, int unionPriId,
                                      Map<String, CompletableFuture> tableNameMappingLocalGetDataFuture,
                                      Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                                      final Map<String, Param> tableMappingSearchAndSortInfo,
                                      Map<String, Map<Integer, Param>> tableNameMappingPdIdParam,
                                      Map<String, BitSet> tableNameMappingPdIdBitSet) {

        CountDownLatch countDownLatch = new CountDownLatch(tableNameMappingLocalGetDataFuture.size() + tableNameMappingRemoteGetDataFuture.size());
        // “从本地缓存获取数据的异步任务”回调获取结果
        tableNameMappingLocalGetDataFuture.forEach((tableName, localGetDataFuture) -> {
            localGetDataFuture.whenCompleteAsync((BiConsumer<FaiList<Param>, Throwable>) (searchResultList, throwable) -> {
                Param searchAndSortInfo = tableMappingSearchAndSortInfo.get(tableName);
                searchAndSortInfo.setList(SEARCH_RESULT_LIST, searchResultList);
                Log.logStd("localGetDataFuture finish get data;aid=%d;unionPriId=%d;tableName=%s;searchResultList=%s", aid, unionPriId, tableName, searchResultList);
                countDownLatch.countDown();
            });
        });

        // “从远程获取数据的异步任务”回调获取结果
        tableNameMappingRemoteGetDataFuture.forEach((tableName, remoteGetDataFuture) -> {
            remoteGetDataFuture.whenCompleteAsync((BiConsumer<RemoteStandResult, Throwable>)(remoteStandResult, ex) -> {
                if (remoteStandResult.isSuccess()) {
                    // TODO 注意Errno.NOT_FOUND的情况是否会返回null，目前涉及到的服务的接口搜索不到，不会返回null
                    FaiList<Param> searchResultList = remoteStandResult.getObject(ParseData.TABLE_NAME_MAPPING_PARSE_DATA_KEY.get(tableName), FaiList.class);
                    Param searchAndSortInfo = tableMappingSearchAndSortInfo.get(tableName);
                    Boolean addDataToLocalCache = searchAndSortInfo.getBoolean(ADD_DATA_TO_LOCAL_CACHE);
                    if (addDataToLocalCache) {
                        // 说明全量搜索

                        // 设置本地缓存和数据状态缓存
                        searchAndSortInfo.setList(CACHE_DATA_LIST, searchResultList);
                        // 设置各个表的全量数据到本地缓存
                        ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
                        // 线程安全类
                        localMgProductSearchData.put(MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName), searchResultList);

                        // 设置各个表的本地缓存时间
                        Param dataStatusInfo = new Param();
                        dataStatusInfo.assign(searchAndSortInfo, DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
                        dataStatusInfo.assign(searchAndSortInfo, DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
                        dataStatusInfo.assign(searchAndSortInfo, DataStatus.Info.TOTAL_SIZE);
                        // 添加数据状态缓存
                        String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
                        // TODO:待考虑是否为线程安全，目前来看是线程安全，不用加锁
                        MgProductSearchCache.LocalDataStatusCache.addLocalDataStatusCache(cacheKey, dataStatusInfo);

                        // 此时的searchResultList还没有使用搜索条件过滤，使用搜索条件过滤
                        ParamMatcher searchMatcher = (ParamMatcher) searchAndSortInfo.getObject(SEARCH_MATCHER);
                        // 使用搜索条件过滤后的最终结果
                        searchResultList = getSearchResult(tableName, searchMatcher, searchResultList, tableNameMappingPdIdParam, tableNameMappingPdIdBitSet);
                    }  else {
                        // 说明不是全量搜索
                        Log.logStd(tableName + " table data(aid+unionPriId) total size is more than LoadFromDbThreshold;Don't need addData to localCache;flow=%d;aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                        // 不做本地缓存的也添加数据到BitSet
                        prepareData(tableName, null, searchResultList, null, tableNameMappingPdIdParam, tableNameMappingPdIdBitSet);
                    }

                    // 保存搜索结果到searchAndSortInfo
                    searchAndSortInfo.setList(SEARCH_RESULT_LIST, searchResultList);
                    Log.logStd(" finish remote get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;searchResultList=%s", flow, aid, unionPriId, searchResultList);
                    countDownLatch.countDown();
                } else {
                    int rt = remoteStandResult.getRt();
                    throw new MgException(rt, "remote get " + tableName + " table data error;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                }
            });
        });

        // 阻塞等待回调执行完成
        long begin = System.currentTimeMillis();
        try {
            countDownLatch.await();
            long end = System.currentTimeMillis();
            Log.logStd("each table finish searching data;consume " + (end - begin) + "ms");
        } catch (InterruptedException e) {
            long end = System.currentTimeMillis();
            throw new MgException("waiting for each table search data time out;consume " + (end - begin) + "ms");
        }

    }

    /**
     * 整合排序字段的值到sortResult保存的Param中（用于单线程执行，先保留）
     * @param needCompare 是否需要排序
     * @param info 用于保存排序字段的值
     * @param tableNameMappingPdIdParam 表名 -> pdId -> Param 主要用于排序，Param中包含排序字段的值，目前排序的场景都是 pdId 与排序字段是一对一
     * @param comparatorTable 排序字段所在的表
     * @param comparatorKey 排序字段
     */
    public void integrateComparatorFieldToSortResult(boolean needCompare, Param info, Map<String, Map<Integer, Param>> tableNameMappingPdIdParam, String comparatorTable, String comparatorKey) {
        if (needCompare && !ProductEntity.Info.PD_ID.equals(comparatorKey)) {
            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            Map<Integer, Param> pdIdMapComparatorInfo = tableNameMappingPdIdParam.get(comparatorTable);
            Param comparatorInfo = pdIdMapComparatorInfo.get(pdId);
            info.assign(comparatorInfo, comparatorKey);
        }
    }


    /**
     * 各表从db全量load数据的阈值
     */
    public static int getLoadFromDbThreshold(String tableName){
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Str.isEmpty(conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey))){
            return defaultThreshold;
        }

        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey)
            .getInt(tableName, defaultThreshold);
    }

    /**
     * 使用es中获取到PdId使用In Sql的阈值。
     */
    public int getInSqlThreshold() {
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Objects.isNull(conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey))){
            return defaultThreshold;
        }
        return conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey, defaultThreshold);
    }


    public static final class DbSearchSorterInfo {
        public static final String SEARCH_TABLE = "searchTable";
        public static final String SEARCH_MATCHER = "searchMatcher";
        public static final String NEED_GET_DATA_FROM_REMOTE = "needGetDataFromRemote";
        public static final String DATA_COUNT = "dataCount";
        public static final String CACHE_DATA_LIST = "cacheDataList";
        public static final String SEARCH_RESULT_LIST = "searchResultList";
        public static final String ADD_DATA_TO_LOCAL_CACHE = "addDataToLocalCache";
    }

    public static final class EsSearchSorterInfo {
        public static final String ES_SEARCH_RESULT = "esSearchResult";
        public static final String ES_SEARCH_RESULT_BITSET = "esSearchResultBitset";
    }
}
