package fai.MgProductInfSvr.interfaces.utils;

import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicValObj;
import fai.MgProductInfSvr.interfaces.entity.ProductPropEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.comm.util.*;

import java.util.Calendar;

public class MgProductSearch {
    public static final class Info {
        public static final String UP_SALES_STATUS = "upSalesStatus";  // 上下架状态，默认是获取上架的状态
        public static final String RL_GROUP_ID_LIST = "rlGroupIdList";     // 商品的分类idList
        public static final String RL_LABLE_ID_LIST = "rlLableIdList";     //  商品标签idList
        public static final String RL_PD_ID_LIST = "rlPdIdList";      // 商品业务idList
        public static final String TYPE_LIST = "typeList";            // 商品类型：实物、卡密、酒店
        public static final String RL_LIB_ID_LIST = "rlLibIdList";               //  在哪些库搜索，默认是全部库

        public static final String SEARCH_KEY_WORD = "searchKeyWord";        // 搜索关键词
        public static final String ENABLE_SEARCH_PRODUCT_NAME = "enableSearchProductName";  // 是否允许搜索商品名称, 默认是 false
        public static final String ENABLE_SEARCH_PRODUCT_PROP = "enableSearchProductProp";  // 是否允许搜索商品参数, 默认是 false
        public static final String KEY_WORD_SEARCH_IN_PROP_ID_LIST = "keyWordSearchInPropIdList";   //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
        public static final String ENABLE_SEARCH_PRODUCT_REMARK = "enableSearchProductRemark";  // 是否允许搜索商品详情, 默认是 false
        public static final String SEARCH_PRODUCT_REMARK_KEY_LIST = "searchProductRemarkKeyList";   // 搜索商品详情 keyList, 可能区分 mobi key、site key，合适由各个项目传入进来

        public static final String RL_PROP_VAL_ID_LIST = "rlPropValIdList";  //  根据 参数值 搜索进行 like 搜索

        public static final String ADD_TIME_BEGIN = "addTimeBegin";   // 搜索商品  开始录入时间, 需要传入 "xxxx-xx-xx xx:xx:xx" 的格式
        public static final String ADD_TIME_END = "addTimeEnd";        // 搜索商品 结束录入时间，需要传入 "xxxx-xx-xx xx:xx:xx" 的格式

        public static final String PRICE_BEGIN = "priceBegin";   // 搜索商品  开始价格
        public static final String PRICE_END = "priceEnd";        // 搜索商品 结束价格

        public static final String SALES_BEGIN = "salesBegin";   // 搜索商品 开始 销量
        public static final String SALES_END = "salesEnd";        // 搜索商品 结束 销量

        public static final String REMAIN_COUNT_BEGIN = "remainCountBegin";   // 搜索商品 开始 库存
        public static final String REMAIN_COUNT_END = "remainCountEnd";        // 搜索商品 结束 库存

        public static final String START = "start";  //  分页位置
        public static final String LIMIT = "limit";  //  分页条数

        public static final String RL_PD_ID_COMPARATOR_LIST  = "rlPdIdComparatorList";  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了

        public static final String FIRST_COMPARATOR_TABLE = "firstComparatorTable";    // 第一排序字段的table
        public static final String FIRST_COMPARATOR_KEY = "firstComparatorKey";     // 第一排序字段
        public static final String FIRST_COMPARATOR_KEY_ORDER_BY_DESC = "firstComparatorKeyOrderByDesc";  // 顺序 还是 倒序，默认是顺序

        public static final String NEED_SECOND_COMPARATOR_SORTING = "needSecondComparatorSorting";  // 是否需要第二排序
        public static final String SECOND_COMPARATOR_TABLE = "secondComparatorTable";  // 第二排序字段的table
        public static final String SECOND_COMPARATOR_KEY = "secondComparatorKey";  // 第二排序字段必须是能够确定唯一的排序字段的，如果不能，会重写为 id 倒序排序
        public static final String SECOND_COMPARATOR_KEY_ORDER_BY_DESC = "secondComparatorKeyOrderByDesc";  // 顺序 还是 倒序，默认是顺序
    }

    //  上下架搜索参数封装
    public enum UpSalesStatusEnum{
        ALL(0), // 全部
        UP_SALES(1),  // 上架
        DOWN_SALES(2), // 下架
        DELETE(3), // 删除
        UP_AND_DOWN_SALES(4); // 上架和下架的
        public int upSalesStatus;
        private UpSalesStatusEnum(int upSalesStatus){
            this.upSalesStatus = upSalesStatus;
        }
        public int getUpSalesStatus(){
            return upSalesStatus;
        }
    }

    // 排序指定 table, table 待完善, 需要作为缓存的key，为了节省内存，所以使用缩写
    public enum SearchTableNameEnum{
        MG_PRODUCT("pd"),    // 对应 mgProduct， 商品基础表
        MG_PRODUCT_REL("pdr"),  // 对应 mgProductRel， 商品关系表
        MG_PRODUCT_BIND_PROP("pbp"),  // 对应 mgProductBindProp，参数值绑定商品关系表
        MG_PRODUCT_BIND_GROUP("pbg"),  // 对应 mgProductGroupRel，分类绑定商品关系表
        MG_PRODUCT_LABLE_REL("plr"), // 对应 mgProductLableRel，标签绑定商品关系表
        MG_SPU_BIZ_SUMMARY("sbs");  // 对应 mgSpuBizSummary，商品 spu 销售汇总表

        public String searchTableName;
        private SearchTableNameEnum(String searchTableName) {
            this.searchTableName = searchTableName;
        }
        public String getSortTableName(){
            return searchTableName;
        }
    }

    // 上下架开始
    private int upSalesStatus = UpSalesStatusEnum.ALL.upSalesStatus;           //  默认是全部的, 对应 ProductRelValObj.Status
    private FaiList<Integer> rlGroupIdList;  // 业务商品分类 IdList
    private FaiList<Integer> rlLableIdList;   //  业务商品标签搜索
    private FaiList<Integer> rlPdIdList;      // 业务商品 IdList
    private FaiList<Integer> typeList;       // 商品类型：实物、卡密、酒店
    private Calendar addTimeBegin;
    private Calendar addTimeEnd;           // 如果搜索的 创建时间 开始 和 创建时间 结束是相同的，则走 EQ 的逻辑
    private FaiList<Integer> rlLibIdList;        //  在哪些库搜索，默认是全部库
    private String searchKeyWord;            //  搜索的关键字，会关联商品名称、商品对应的参数
    private boolean enableSearchProductName = false;   // 是否允许搜索商品名称, 默认是 false
    private boolean enableSearchProductProp = false;   // 是否允许搜索商品参数, 默认是 false
    private FaiList<Integer> keyWordSearchInPropIdList;    //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
    private boolean enableSearchProductRemark = false;   // 是否允许搜索商品详情, 默认是 false
    private FaiList<String> searchProductRemarkKeyList;  //  在哪些富文本详情下搜索, 与 searchKeyWord 匹配使用
    private FaiList<Integer> rlPropValIdList;   //  根据 参数值 搜索

    private Long priceBegin;   //  最小交易价格
    private Long priceEnd;     //  最大交易价格，如果最小交易价格和最大交易价格一样, 则是等于比较

    private Integer salesBegin;  //  最小的销量
    private Integer salesEnd;    //  最大的销量，如果最小销量和销量一样, 则是等于比较

    private Integer remainCountBegin;  //  最小的库存
    private Integer remainCountEnd;    //  最大的库存，如果最小库存和库存一样, 则是等于比较

    private FaiList<Integer> rlPdIdComparatorList;  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
    private String firstComparatorTable;  // 第一排序字段的table
    private String firstComparatorKey;   // 第一排序字段
    private boolean firstComparatorKeyOrderByDesc = false;   // 第一排序字段的顺序, 默认顺序

    // 第二的排序，需要选择是否使用
    private boolean needSecondComparatorSorting = false;   // 是否启动第二字段排序，就是默认 ProductBasicEntity.ProductInfo.RL_PD_ID 排序
    private String secondComparatorTable = SearchTableNameEnum.MG_PRODUCT.searchTableName;   // 第二排序字段
    private String secondComparatorKey = ProductBasicEntity.ProductInfo.RL_PD_ID;                  // 第二排序字段，业务商品id，能够确定唯一的排序, 相当于创建时间排序了
    private boolean secondComparatorKeyOrderByDesc = false;   // 第二排序字段的顺序, 默认顺序

    private int start = 0; // 搜索的开始位置
    private int limit = 100;  // 分页限制条数开始，默认最大是 100


    // 把查询条件转换为 Param
    public Param getSearchParam(){
        Param param = new Param();
        param.setInt(Info.UP_SALES_STATUS, upSalesStatus);     // 上下架
        param.setList(Info.RL_GROUP_ID_LIST, rlGroupIdList);   // 业务商品分类
        param.setList(Info.RL_LABLE_ID_LIST, rlLableIdList);   // 业务商品标签
        param.setList(Info.RL_PD_ID_LIST, rlPdIdList);          // priceEnd
        param.setList(Info.TYPE_LIST, typeList);                // 商品类型：实物、卡密、酒店， 对应 ProductEntity.Info.PD_TYPE 字段
        param.setList(Info.RL_LIB_ID_LIST, rlLibIdList);           //  在哪些库搜索，默认是全部库
        param.setString(Info.SEARCH_KEY_WORD, searchKeyWord);  // 商品搜索关键词
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_NAME, enableSearchProductName);   // 是否允许搜索商品名称, 默认是 false
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_PROP, enableSearchProductProp);   // 是否允许搜索商品参数, 默认是 false
        param.setList(Info.KEY_WORD_SEARCH_IN_PROP_ID_LIST, keyWordSearchInPropIdList);     //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_REMARK, enableSearchProductRemark);  // 是否允许搜索商品详情, 默认是 false
        param.setList(Info.SEARCH_PRODUCT_REMARK_KEY_LIST, searchProductRemarkKeyList);

        param.setList(Info.RL_PROP_VAL_ID_LIST, rlPropValIdList);  //  根据 参数值 搜索
        param.setCalendar(Info.ADD_TIME_BEGIN, addTimeBegin);  // 搜索商品  开始录入时间
        param.setCalendar(Info.ADD_TIME_END, addTimeEnd);      // 搜索商品  结束录入时间
        param.setLong(Info.PRICE_BEGIN, priceBegin);     // 搜索商品 开始价格
        param.setLong(Info.PRICE_END, priceEnd);         // 搜索商品  结束价格
        param.setInt(Info.SALES_BEGIN, salesBegin);      // 搜索商品 开始 销量
        param.setInt(Info.SALES_END, salesEnd);          // 搜索商品 结束 销量
        param.setInt(Info.REMAIN_COUNT_BEGIN, remainCountBegin);      // 搜索商品 开始 库存
        param.setInt(Info.REMAIN_COUNT_END, remainCountEnd);          // 搜索商品 结束 库存

        param.setList(Info.RL_PD_ID_COMPARATOR_LIST, rlPdIdComparatorList);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        param.setString(Info.FIRST_COMPARATOR_TABLE, firstComparatorTable); // 第一排序table
        param.setString(Info.FIRST_COMPARATOR_KEY, firstComparatorKey);  // 第一排序字段
        param.setBoolean(Info.FIRST_COMPARATOR_KEY_ORDER_BY_DESC, firstComparatorKeyOrderByDesc);  // 第一排序字段的顺序

        param.setBoolean(Info.NEED_SECOND_COMPARATOR_SORTING, needSecondComparatorSorting);  // 是否开启默认的第二排序
        param.setString(Info.SECOND_COMPARATOR_TABLE, secondComparatorTable);         // 第二排序table
        param.setString(Info.SECOND_COMPARATOR_KEY, secondComparatorKey);             // 第二排序字段
        param.setBoolean(Info.SECOND_COMPARATOR_KEY_ORDER_BY_DESC, secondComparatorKeyOrderByDesc);  // 第二排序字段的顺序

        param.setInt(Info.START, start);   // 分页获取的起始位置
        param.setInt(Info.LIMIT, limit);   // 分页获取的起始位置
        return param;
    }

    public void initProductSearch(Param searchParam) {
        this.upSalesStatus = searchParam.getInt(Info.UP_SALES_STATUS);   // 上下架
        this.rlGroupIdList = searchParam.getList(Info.RL_GROUP_ID_LIST); // 业务商品分类
        this.rlLableIdList = searchParam.getList(Info.RL_LABLE_ID_LIST); // 业务商品标签
        this.rlPdIdList = searchParam.getList(Info.RL_PD_ID_LIST);        // 业务商品idList
        this.typeList = searchParam.getList(Info.TYPE_LIST);               // 商品类型：实物、卡密、酒店

        this.rlLibIdList = searchParam.getList(Info.RL_LIB_ID_LIST);                       //  在哪些库搜索，默认是全部库
        this.searchKeyWord = searchParam.getString(Info.SEARCH_KEY_WORD);   // 商品搜索关键词
        this.enableSearchProductName = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_NAME);  // 是否允许搜索商品名称, 默认是 false
        this.enableSearchProductProp = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_PROP);  // 是否允许搜索商品参数, 默认是 false
        this.keyWordSearchInPropIdList = searchParam.getList(Info.KEY_WORD_SEARCH_IN_PROP_ID_LIST);  //  在哪些参数下筛选
        this.enableSearchProductRemark = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_REMARK); // 是否允许搜索商品详情, 默认是 false
        this.searchProductRemarkKeyList = searchParam.getList(Info.SEARCH_PRODUCT_REMARK_KEY_LIST);
        this.rlPropValIdList = searchParam.getList(Info.RL_PROP_VAL_ID_LIST);   // 根据 参数值 搜索
        this.addTimeBegin = searchParam.getCalendar(Info.ADD_TIME_BEGIN);   // 搜索商品开始 录入时间
        this.addTimeEnd = searchParam.getCalendar(Info.ADD_TIME_END);       // 搜索商品结束 录入时间
        this.priceBegin = searchParam.getLong(Info.PRICE_BEGIN);          // 搜索商品开始 价格
        this.priceEnd = searchParam.getLong(Info.PRICE_END);              // 搜索商品结束 价格
        this.salesBegin = searchParam.getInt(Info.SALES_BEGIN);           // 搜索商品 开始 销量
        this.salesEnd = searchParam.getInt(Info.SALES_END);               // 搜索商品 结束 销量
        this.remainCountBegin = searchParam.getInt(Info.REMAIN_COUNT_BEGIN);  // 搜索商品 开始 库存
        this.remainCountEnd = searchParam.getInt(Info.REMAIN_COUNT_END);      // 搜索商品 结束 库存

        this.rlPdIdComparatorList = searchParam.getList(Info.RL_PD_ID_COMPARATOR_LIST);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        this.firstComparatorTable = searchParam.getString(Info.FIRST_COMPARATOR_TABLE);   // 第一排序字段table
        this.firstComparatorKey = searchParam.getString(Info.FIRST_COMPARATOR_KEY);   // 第一排序字段
        this.firstComparatorKeyOrderByDesc = searchParam.getBoolean(Info.FIRST_COMPARATOR_KEY_ORDER_BY_DESC);   // 第一排序字段的顺序
        this.needSecondComparatorSorting = searchParam.getBoolean(Info.NEED_SECOND_COMPARATOR_SORTING);   // 是否开启默认的第二排序
        this.secondComparatorTable = searchParam.getString(Info.SECOND_COMPARATOR_TABLE);   // 第二排序字段table
        this.secondComparatorKey = searchParam.getString(Info.SECOND_COMPARATOR_KEY);    // 第二排序字段
        this.secondComparatorKeyOrderByDesc = searchParam.getBoolean(Info.SECOND_COMPARATOR_KEY_ORDER_BY_DESC);   // 第二排序字段的顺序

        this.start = searchParam.getInt(Info.START);    // 分页获取的起始位置
        this.limit = searchParam.getInt(Info.LIMIT);    // 分页获取的起始位置
    }


    // 判断是否搜索访客态的数据
    public boolean getIsOnlySearchManageData(String tableName){
        boolean isOnlySearchManageData = true;
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            //  searchKeyWord 都是管理态修改的数据
            // 判断排序字段是否包含访客态字段
            return isOnlySearchManageData;
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            //  rlPdIdList、 typeList 、 rlLibIdList 、 upSalesStatus、 addTimeBegin、 addTimeEnd 都是管理态修改的数据
            // 判断排序字段是否包含访客态字段
            return isOnlySearchManageData;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // rlPropValIdList 都是管理态修改的数据
            // 判断排序字段是否包含访客态字段
            // 访客态字段:  ProductBindPropEntity.VISITOR_FIELDS
            return isOnlySearchManageData;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // rlGroupIdList 都是管理态修改的数据
            // 判断排序字段是否包含访客态字段
            //  访客态字段: ProductBindGroupEntity.VISITOR_FIELDS
            return isOnlySearchManageData;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_LABLE_REL.searchTableName.equals(tableName)){
            // rlLableId 都是管理态修改的数据
            // 判断排序字段是否包含访客态字段
            return isOnlySearchManageData;
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // priceBegin 、 priceEnd 都是管理态修改的数据
            // salesBegin 、salesEnd、remainCountBegin、remainCountEnd 是访客可能变动的数据
            // 访客态字段: SpuBizSummaryEntity.VISITOR_FIELDS
            if(salesBegin != null || salesEnd != null){
                isOnlySearchManageData = false;
            }
            if(remainCountBegin != null || remainCountEnd != null){
                isOnlySearchManageData = false;
            }

            // 判断排序字段是否包含访客字段
            ParamComparator paramComparator = getParamComparator();
            String comparatorTable = getFirstComparatorTable();
            String firstComparatorKey = getFirstComparatorKey();
            if(!paramComparator.isEmpty() && MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(comparatorTable) && ProductStoreEntity.SpuBizSummaryInfo.VISITOR_FIELDS.contains(firstComparatorKey)){
                isOnlySearchManageData = false;
            }
            return isOnlySearchManageData;
        }
        return isOnlySearchManageData;
    }

    // 根据 matcher 判断是否有搜索条件
    public boolean isEmpty(){
        return getProductRemarkSearchOrMatcher(null).isEmpty() && getProductPropValSearchOrMatcher(null).isEmpty() &&
                getProductBasicSearchOrMatcher(null).isEmpty() && getProductBasicSearchMatcher(null).isEmpty() &&
                getProductBindPropSearchMatcher(null).isEmpty() && getProductBindGroupSearchMatcher(null).isEmpty() &&
                getProductBindLableSearchMatcher(null).isEmpty() && getProductSpuBizSummarySearchMatcher(null).isEmpty();
    }

    // 在 "在商品 富文本 字段"  搜索
    public ParamMatcher getProductRemarkSearchOrMatcher(ParamMatcher paramMatcher){
        ParamMatcher paramMatcherOr = new ParamMatcher();
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName && searchProductRemarkKeyList != null && !searchProductRemarkKeyList.isEmpty()){
            //  商品名称, 名称 like 查询
            for(String remarkKey : searchProductRemarkKeyList){
                paramMatcherOr.or(remarkKey, ParamMatcher.LK, searchKeyWord);
            }
        }
        if(paramMatcher == null){
            return paramMatcherOr;
        }else{
            paramMatcher.and(paramMatcherOr);
            return paramMatcher;
        }
    }

    // 在 "参数值表" mgProductPropVal_xxxx 搜索
    public ParamMatcher getProductPropValSearchOrMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductProp && keyWordSearchInPropIdList != null && !keyWordSearchInPropIdList.isEmpty()){
            if(keyWordSearchInPropIdList.size() == 1){
                paramMatcher.and(ProductPropEntity.PropValInfo.PROP_ID, ParamMatcher.EQ, keyWordSearchInPropIdList.get(0));
            }else{
                paramMatcher.and(ProductPropEntity.PropValInfo.PROP_ID, ParamMatcher.IN, keyWordSearchInPropIdList);
            }
            // 对参数值进行 like 搜索
            paramMatcher.and(ProductPropEntity.PropValInfo.VAL, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    // 在 "商品基础表" mgProduct_xxxx 搜索
    public ParamMatcher getProductBasicSearchOrMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductRemark){
            //  商品名称, 名称 like 查询
            paramMatcher.and(ProductBasicEntity.ProductInfo.NAME, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    // 在 "商品基础表" mgProduct_xxxx 搜索
    public ParamMatcher getProductBasicSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName){
            //  商品名称, 名称 like 查询
            paramMatcher.and(ProductBasicEntity.ProductInfo.NAME, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    // 在 "商品业务关系表" mgProductRel_xxxx 搜索
    public ParamMatcher getProductRelSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 从 mgProduct_xxxx 冗余字段查询，商品类型
        if(typeList != null && !typeList.isEmpty()){
            if(typeList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.PD_TYPE, ParamMatcher.EQ, typeList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.PD_TYPE, ParamMatcher.IN, typeList);
            }
        }
        // 业务商品idList
        if(rlPdIdList != null && !rlPdIdList.isEmpty()){
            if(rlPdIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_PD_ID, ParamMatcher.EQ, rlPdIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_PD_ID, ParamMatcher.IN, rlPdIdList);
            }
        }

        //  商品库
        if(rlLibIdList != null && !rlLibIdList.isEmpty()){
            if(rlLibIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_LIB_ID, ParamMatcher.EQ, rlLibIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_LIB_ID, ParamMatcher.IN, rlLibIdList);
            }
        }

        // 上架或者下架的
        if (upSalesStatus == UpSalesStatusEnum.UP_AND_DOWN_SALES.upSalesStatus){
            FaiList<Integer> statusList = new FaiList<Integer>();
            statusList.add(ProductBasicValObj.ProductRelValObj.Status.UP);
            statusList.add(ProductBasicValObj.ProductRelValObj.Status.DOWN);
            paramMatcher.and(ProductBasicEntity.ProductInfo.STATUS, ParamMatcher.IN, statusList);
        }else if(upSalesStatus != UpSalesStatusEnum.ALL.upSalesStatus){   //  非全部的
            paramMatcher.and(ProductBasicEntity.ProductInfo.STATUS, ParamMatcher.EQ, upSalesStatus);
        }

        //  商品录入时间
        if(addTimeBegin != null || addTimeEnd != null){
            if(addTimeBegin != null && addTimeEnd != null && addTimeBegin.getTimeInMillis() == addTimeEnd.getTimeInMillis()){
                paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.EQ, addTimeBegin);
            }else{
                //  创建时间大于
                if(addTimeBegin != null){
                    paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.GE, addTimeBegin);
                }
                //  创建时间小于
                if(addTimeEnd != null){
                    paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.LE, addTimeEnd);
                }
            }
        }
        return paramMatcher;
    }

    // 在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
    public ParamMatcher getProductBindPropSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(rlPropValIdList != null && !rlPropValIdList.isEmpty()){
            if(rlPropValIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.EQ, rlPropValIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.IN, rlPropValIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "分类业务关系表" mgProductBindGroup_xxxx 搜索
    public ParamMatcher getProductBindGroupSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品分类
        if(rlGroupIdList != null && !rlGroupIdList.isEmpty()){
            if(rlGroupIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.BindGroupInfo.RL_GROUP_ID, ParamMatcher.EQ, rlGroupIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.BindGroupInfo.RL_GROUP_ID, ParamMatcher.IN, rlGroupIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "标签业务关系表" mgProductBindLable_xxxx 搜索
    public ParamMatcher getProductBindLableSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品标签
        if(rlLableIdList != null && !rlLableIdList.isEmpty()){
            if(rlLableIdList.size() == 1){
                paramMatcher.and("rlLableId", ParamMatcher.EQ, rlLableIdList.get(0));
            }else{
                paramMatcher.and("rlLableId", ParamMatcher.IN, rlLableIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
    public ParamMatcher getProductSpuBizSummarySearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 商品销量
        if(salesBegin != null || salesEnd != null){
            if(salesBegin != null && salesEnd != null && salesBegin.intValue() == salesEnd.intValue()){
                paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.EQ, salesBegin);
            }else{
                if(salesBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.GE, salesBegin);
                }
                if(salesEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.LE, salesEnd);
                }
            }
        }

        // 商品库存
        if(remainCountBegin != null || remainCountEnd != null){
            if(remainCountBegin != null && remainCountEnd != null && remainCountBegin.intValue() == remainCountEnd.intValue()){
                paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.EQ, remainCountBegin);
            }else{
                if(remainCountBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.GE, remainCountBegin);
                }
                if(remainCountEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.LE, remainCountEnd);
                }
            }
        }
        // 商品价格
        if(priceBegin != null || priceEnd != null){
            if(priceBegin != null && priceEnd != null){
                if(priceBegin.longValue() == priceEnd.longValue()){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.EQ, priceBegin);
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.EQ, priceBegin);
                }else{
                    ParamMatcher priceMatcher = new ParamMatcher();
                    ParamMatcher minPriceMatcher = new ParamMatcher();
                    minPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.GE, priceBegin);
                    minPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.LE, priceEnd);
                    priceMatcher.or(minPriceMatcher);

                    ParamMatcher maxPriceMatcher = new ParamMatcher();
                    maxPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.GE, priceBegin);
                    maxPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.LE, priceEnd);
                    priceMatcher.or(maxPriceMatcher);
                    paramMatcher.and(priceMatcher);
                }
            }else{
                if(priceBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.GE, priceBegin);
                }
                if(priceEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.LE, priceEnd);
                }
            }
        }
        return paramMatcher;
    }


    public FaiList<Integer> getRlLableIdList() {
        return rlLableIdList;
    }
    public MgProductSearch setRlLableIdList(FaiList<Integer> rlLableIdList) {
        this.rlLableIdList = rlLableIdList;
        return this;
    }
    public MgProductSearch setRlPdIdList(FaiList<Integer> rlPdIdList) {
        this.rlPdIdList = rlPdIdList;
        return this;
    }
    public FaiList<Integer> getRlPdIdList() {
        return rlPdIdList;
    }

    public MgProductSearch setUpSalesStatus(UpSalesStatusEnum upSalesStatusEnum){
        this.upSalesStatus = upSalesStatusEnum.upSalesStatus;
        return this;
    }
    public int getUpSalesStatus(){
        return this.upSalesStatus;
    }
    public FaiList<Integer> getTypeList() {
        return typeList;
    }
    public MgProductSearch setTypeList(FaiList<Integer> typeList) {
        this.typeList = typeList;
        return this;
    }
    public FaiList<Integer> getRlGroupIdList() {
        return rlGroupIdList;
    }

    public MgProductSearch setRlGroupIdList(FaiList<Integer> rlGroupIdList) {
        this.rlGroupIdList = rlGroupIdList;
        return this;
    }

    public FaiList<Integer> getRlLibIdList() {
        return rlLibIdList;
    }
    public MgProductSearch setRlLibIdList(FaiList<Integer> rlLibIdList) {
        this.rlLibIdList = rlLibIdList;
        return this;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }
    // 如果商品参数的搜索rlPropIdList没有设置的话，那就只是代表商品名称like搜索
    public MgProductSearch setSearchKeyWord(String searchKeyWord, boolean enableSearchProductName, boolean enableSearchProductProp, FaiList<Integer> keyWordSearchInPropIdList, boolean enableSearchProductRemark, FaiList<String> searchProductRemarkKeyList) {
        this.searchKeyWord = searchKeyWord;
        this.enableSearchProductName = enableSearchProductName;
        this.enableSearchProductProp = enableSearchProductProp;
        this.keyWordSearchInPropIdList = keyWordSearchInPropIdList;   //  在指定 propId 进行 like  搜索
        this.enableSearchProductRemark = enableSearchProductRemark;
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }

    public boolean getEnableSearchProductName() {
        return enableSearchProductName;
    }
    public MgProductSearch setEnableSearchProductName(boolean enableSearchProductName) {
        this.enableSearchProductName = enableSearchProductName;
        return this;
    }
    public boolean getEnableSearchProductProp() {
        return enableSearchProductProp;
    }
    public MgProductSearch setEnableSearchProductProp(boolean enableSearchProductProp) {
        this.enableSearchProductProp = enableSearchProductProp;
        return this;
    }
    public boolean getEnableSearchProductRemark() {
        return enableSearchProductRemark;
    }
    public MgProductSearch setEnableSearchProductRemark(boolean enableSearchProductRemark) {
        this.enableSearchProductRemark = enableSearchProductRemark;
        return this;
    }
    public FaiList<String> getSearchProductRemarkKeyList() {
        return searchProductRemarkKeyList;
    }
    public MgProductSearch setSearchProductRemarkKeyList(FaiList<String> searchProductRemarkKeyList) {
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }
    public MgProductSearch setKeyWordSearchInPropIdList(FaiList<Integer> keyWordSearchInPropIdList) {
        this.keyWordSearchInPropIdList = keyWordSearchInPropIdList;
        return this;
    }

    public FaiList<Integer> getKeyWordSearchInPropIdList() {
        return keyWordSearchInPropIdList;
    }
    public FaiList<Integer> getRlPropValIdList() {
        return rlPropValIdList;
    }
    public MgProductSearch setRlPropValIdList(FaiList<Integer> rlPropValIdList) {
        this.rlPropValIdList = rlPropValIdList;
        return this;
    }

    // 商品创建时间范围
    public MgProductSearch setAddTime(Calendar addTimeBegin, Calendar addTimeEnd) {
        this.addTimeBegin = addTimeBegin;
        this.addTimeEnd = addTimeEnd;
        return this;
    }
    public Calendar getAddTimeBegin() {
        return addTimeBegin;
    }
    public Calendar getAddTimeEnd() {
        return addTimeEnd;
    }

    // 价格范围
    public MgProductSearch setPrice(Long priceBegin, Long priceEnd) {
        this.priceBegin = priceBegin;
        this.priceEnd = priceEnd;
        return this;
    }
    public Long getPriceBegin() {
        return priceBegin;
    }
    public Long getPriceEnd() {
        return priceEnd;
    }


    // 销量范围
    public MgProductSearch setSales(Integer salesBegin, Integer salesEnd) {
        this.salesBegin = salesBegin;
        this.salesEnd = salesEnd;
        return this;
    }
    public Integer getSalesBegin() {
        return salesBegin;
    }
    public Integer getSalesEnd() {
        return salesEnd;
    }

    // 库存范围
    public MgProductSearch setRemainCount(Integer remainCountBegin, Integer remainCountEnd) {
        this.remainCountBegin = remainCountBegin;
        this.remainCountEnd = remainCountEnd;
        return this;
    }
    public Integer getRemainCountBegin() {
        return remainCountBegin;
    }
    public Integer getRemainCountEnd() {
        return remainCountEnd;
    }

    //  把排序转换为 ParamComparator
    // 暂时不允许第一排序和第二排序字段key是一样的，如果第二排序字段key重复，只有第一排序字段生效，可以看 setSecondComparator
    public ParamComparator getParamComparator(){
        ParamComparator paramComparator = new ParamComparator();
        // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        if(rlPdIdComparatorList != null && !rlPdIdComparatorList.isEmpty()){
            paramComparator.addKey(ProductBasicEntity.ProductInfo.RL_PD_ID, rlPdIdComparatorList);
            // 第二排序
            if(needSecondComparatorSorting){
                paramComparator.addKey(secondComparatorKey, secondComparatorKeyOrderByDesc);
            }
        }else{
            // 第一排序
            if(!Str.isEmpty(this.firstComparatorTable) && !Str.isEmpty(this.firstComparatorKey)){
                paramComparator.addKey(this.firstComparatorKey, this.firstComparatorKeyOrderByDesc);
            }
            // 第二排序
            if(needSecondComparatorSorting){
                paramComparator.addKey(secondComparatorKey, secondComparatorKeyOrderByDesc);
            }
        }
        return paramComparator;
    }

    // 设置一个排序后，支持是否开启默认的第二个能够确认 唯一 排序字：secondComparatorKey = ProductBasicEntity.ProductInfo.RL_PD_ID，相当于创建时间排序
    public MgProductSearch setComparator(Pair<SearchTableNameEnum, String> comparatorTableAndKey, boolean desc){
        return setComparator(comparatorTableAndKey, desc, false, false);
    }
    public MgProductSearch setComparator(Pair<SearchTableNameEnum, String> comparatorTableAndKey, boolean desc, boolean needSecondComparatorSorting, boolean rlPdIdSortingDesc){
        if(Str.isEmpty(comparatorTableAndKey.first.searchTableName) || Str.isEmpty(comparatorTableAndKey.second)){
            return this;
        }
        this.firstComparatorTable = comparatorTableAndKey.first.searchTableName;
        this.firstComparatorKey = comparatorTableAndKey.second;
        this.firstComparatorKeyOrderByDesc = desc;
        this.needSecondComparatorSorting = needSecondComparatorSorting;
        this.secondComparatorKeyOrderByDesc = rlPdIdSortingDesc;
        return this;
    }

    public FaiList<Integer> getRlPdIdComparatorList() {
        return rlPdIdComparatorList;
    }
    public MgProductSearch setRlPdIdComparatorList(FaiList<Integer> rlPdIdComparatorList) {
        return setRlPdIdComparatorList(rlPdIdComparatorList, false, false);
    }
    public MgProductSearch setRlPdIdComparatorList(FaiList<Integer> rlPdIdComparatorList, boolean needSecondComparatorSorting, boolean rlPdIdSortingDesc){
        this.rlPdIdComparatorList = rlPdIdComparatorList;
        this.needSecondComparatorSorting = needSecondComparatorSorting;
        this.secondComparatorKeyOrderByDesc = rlPdIdSortingDesc;
        return this;
    }

    public String getFirstComparatorTable() {
        if(rlPdIdComparatorList == null || rlPdIdComparatorList.isEmpty()){
            return firstComparatorTable;
        }
        return null;
    }
    public String getFirstComparatorKey(){
        if(rlPdIdComparatorList == null || rlPdIdComparatorList.isEmpty()){
            return this.firstComparatorKey;
        }
        return null;
    }
    public boolean getFirstComparatorKeyOrderByDesc(){
        return this.firstComparatorKeyOrderByDesc;
    }

    public String getSecondComparatorTable() {
        return secondComparatorTable;
    }
    public String getSecondComparatorKey(){
        return this.secondComparatorKey;
    }
    public boolean getSecondComparatorKeyOrderByDesc(){
        return this.secondComparatorKeyOrderByDesc;
    }
    public boolean isNeedSecondComparatorSorting() {
        return needSecondComparatorSorting;
    }

    //  把把查询条件转换为 SearchArg
    public void setSearArgStartAndLimit(SearchArg searchArg){
        if(searchArg == null){
            return;
        }
        searchArg.start = this.start;
        searchArg.limit = this.limit;
    }

    public MgProductSearch setStart(int start){
        this.start = start >= 0 ? start : this.start;
        return this;
    }
    public int getStart(){
        return this.start;
    }

    public MgProductSearch setLimit(int limit){
        this.limit = limit >= 0 ? limit : this.limit;
        return this;
    }
    public int getLimit(){
        return this.limit;
    }

}
