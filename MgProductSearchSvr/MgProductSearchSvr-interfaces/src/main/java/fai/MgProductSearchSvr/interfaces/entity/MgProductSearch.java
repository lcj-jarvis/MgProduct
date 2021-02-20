package fai.MgProductSearchSvr.interfaces.entity;

import fai.comm.util.*;
import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductStoreSvr.interfaces.entity.*;

import java.math.BigInteger;

public class MgProductSearch {
    public static final class Info {
        public static final String UP_SALES_STATUS = "upSalesStatus";  // 上下架状态，默认是获取上架的状态
        public static final String RL_GROUP_ID_LIST = "rlGroupIdList";     // 商品的分类idList
        public static final String RL_LABLE_ID_LIST = "rlLableIdList";     //  商品标签idList
        public static final String RL_PD_ID_LIST = "rlPdIdList";      // 商品业务idList
        public static final String TYPE_LIST = "typeList";            // 商品类型：实物、卡密、酒店
        public static final String ADD_TIME_BEGIN = "addTimeBegin";   // 搜索商品  开始录入时间
        public static final String ADD_TIME_END = "addTimeEnd";        // 搜索商品 结束录入时间
        public static final String PRICE_BEGIN = "priceBegin";   // 搜索商品  开始价格
        public static final String PRICE_END = "priceEnd";        // 搜索商品 结束价格
        public static final String RL_LIB_ID = "rlLibId";               //  在哪个库搜索，默认是全部库

        public static final String ENABLE_PRODUCT_NAME = "enableProductName";  // 是否允许搜索商品名称, 默认是 true
        public static final String ENABLE_PRODUCT_PROP = "enableProductProp";  // 是否允许搜索商品参数, 默认是 true
        public static final String ENABLE_PRODUCT_REMARK = "enableProductRemark";  // 是否允许搜索商品详情, 默认是 true



        public static final String SEARCH_KEY_WORD = "searchKeyWord";        // 搜索关键词
        public static final String RL_LIB_AND_RL_PROP_ID_LIST = "rlLibAndrlPropIdList";   //  库与参数相关的搜索, 与 searchKeyWord 匹配使用


        public static final String START = "start";  //  分页位置
        public static final String LIMIT = "limit";  //  分页条数
        public static final String RL_PD_ID_COMPARATOR_LIST  = "rlPdIdComparatorList";  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        public static final String FIRST_COMPARATOR_KEY = "firstComparatorKey";     // 第一排序字段
        public static final String FIRST_COMPARATOR_KEY_ORDER = "firstComparatorKeyOrder";  // 顺序 还是 倒序，默认是顺序
        public static final String SECOND_COMPARATOR_KEY = "secondComparatorKey";  // 第二排序字段必须是能够确定唯一的排序字段的，如果不能，会重写为 id 倒序排序
        public static final String SECOND_COMPARATOR_KEY_ORDER = "secondComparatorKeyOrder";  // 顺序 还是 倒序，默认是顺序
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

    // 上下架开始
    private int upSalesStatus = 1;           //  默认是上架的, 对应 ProductRelValObj.Status
    private FaiList<Integer> rlGroupIdList;  // 业务商品分类 IdList
    private FaiList<Integer> rlLableIdList;   //  业务商品标签搜索
    private FaiList<Integer> rlPdIdList;      // 业务商品 IdList
    private FaiList<Integer> typeList;       // 商品类型：实物、卡密、酒店
    private String addTimeBegin;
    private String addTimeEnd;           // 如果搜索的 创建时间 开始 和 创建时间 结束是相同的，则走 EQ 的逻辑

    private int rlLibId = -1;        //  在哪个库搜索，默认是全部库
    private boolean enableProductName = true;   // 是否允许搜索商品名称, 默认是 true
    private boolean enableProductProp = true;   // 是否允许搜索商品参数, 默认是 true
    private boolean enableProductRemark = true;   // 是否允许搜索商品详情, 默认是 true
    private String searchKeyWord;            //  搜索的关键字，会关联商品名称、商品对应的参数
    private FaiList<Pair<Integer, FaiList<Integer>>> rlLibAndrlPropIdList;  // 商品库 和 参数搜索 [{libId,[rlPropId]}]


    private Long priceBegin;   //  最小交易价格
    private Long priceEnd;     //  最大交易价格，如果最小交易价格和最大交易价格一样，
    private int start = 0; // 搜索的开始位置
    private int limit = 100;  // 分页限制条数开始，默认最大是 100
    private FaiList<Integer> rlPdIdComparatorList;  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
    private String firstComparatorKey;   // 第一排序字段
    private boolean firstComparatorKeyOrder = false;   // 第一排序字段的顺序
    private String secondComparatorKey;                  // 第二排序字段
    private boolean secondComparatorKeyOrder = false;   // 第二排序字段的顺序


    // 把查询条件转换为 Param
    public Param getSearchParam(){
        Param param = new Param();
        param.setInt(Info.UP_SALES_STATUS, upSalesStatus);     // 上下架
        param.setList(Info.RL_GROUP_ID_LIST, rlGroupIdList);   // 业务商品分类
        param.setList(Info.RL_LABLE_ID_LIST, rlLableIdList);   // 业务商品标签
        param.setList(Info.RL_PD_ID_LIST, rlPdIdList);          // priceEnd
        param.setList(Info.TYPE_LIST, typeList);                // 商品类型：实物、卡密、酒店

        param.setInt(Info.RL_LIB_ID, rlLibId);           //  在哪个库搜索，默认是全部库
        param.setBoolean(Info.ENABLE_PRODUCT_NAME, enableProductName);   // 是否允许搜索商品名称, 默认是 true
        param.setBoolean(Info.ENABLE_PRODUCT_PROP, enableProductProp);   // 是否允许搜索商品参数, 默认是 true
        param.setBoolean(Info.ENABLE_PRODUCT_REMARK, enableProductRemark);  // 是否允许搜索商品详情, 默认是 true
        param.setString(Info.SEARCH_KEY_WORD, searchKeyWord);  // 商品搜索关键词
        param.setList(Info.RL_LIB_AND_RL_PROP_ID_LIST, rlLibAndrlPropIdList);

        param.setString(Info.ADD_TIME_BEGIN, addTimeBegin);  // 搜索商品  开始录入时间
        param.setString(Info.ADD_TIME_END, addTimeEnd);      // 搜索商品  结束录入时间

        param.setLong(Info.PRICE_BEGIN, priceBegin);     // 搜索商品 开始价格
        param.setLong(Info.PRICE_END, priceEnd);         // 搜索商品  结束价格


        param.setInt(Info.START, start);   // 分页获取的起始位置
        param.setInt(Info.LIMIT, limit);   // 分页获取的起始位置
        param.setList(Info.RL_PD_ID_COMPARATOR_LIST, rlPdIdComparatorList);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        param.setString(Info.FIRST_COMPARATOR_KEY, firstComparatorKey);  // 第一排序字段
        param.setBoolean(Info.FIRST_COMPARATOR_KEY_ORDER, firstComparatorKeyOrder);  // 第一排序字段的顺序
        param.setString(Info.SECOND_COMPARATOR_KEY, secondComparatorKey);             // 第二排序字段
        param.setBoolean(Info.SECOND_COMPARATOR_KEY_ORDER, secondComparatorKeyOrder); // 第二排序字段的顺序
        return param;
    }

    public void initProductSearch(Param searchParam) {
        this.upSalesStatus = searchParam.getInt(Info.UP_SALES_STATUS);   // 上下架
        this.rlGroupIdList = searchParam.getList(Info.RL_GROUP_ID_LIST); // 业务商品分类
        this.rlLableIdList = searchParam.getList(Info.RL_LABLE_ID_LIST); // 业务商品标签
        this.rlPdIdList = searchParam.getList(Info.RL_PD_ID_LIST);        // 业务商品idList
        this.typeList = searchParam.getList(Info.TYPE_LIST);               // 商品类型：实物、卡密、酒店

        this.rlLibId = searchParam.getInt(Info.RL_LIB_ID);                       //  在哪个库搜索，默认是全部库
        this.enableProductName = searchParam.getBoolean(Info.ENABLE_PRODUCT_NAME);  // 是否允许搜索商品名称, 默认是 true
        this.enableProductProp = searchParam.getBoolean(Info.ENABLE_PRODUCT_PROP);  // 是否允许搜索商品参数, 默认是 true
        this.enableProductRemark = searchParam.getBoolean(Info.ENABLE_PRODUCT_REMARK); // 是否允许搜索商品详情, 默认是 true
        this.searchKeyWord = searchParam.getString(Info.SEARCH_KEY_WORD);   // 商品搜索关键词
        this.rlLibAndrlPropIdList = searchParam.getList(Info.RL_LIB_AND_RL_PROP_ID_LIST);  //  搜索词在商品库与商品参数筛选

        this.addTimeBegin = searchParam.getString(Info.ADD_TIME_BEGIN);   // 搜索商品开始 录入时间
        this.addTimeEnd = searchParam.getString(Info.ADD_TIME_END);       // 搜索商品结束 录入时间

        this.priceBegin = searchParam.getLong(Info.PRICE_BEGIN);          // 搜索商品结束 价格
        this.priceEnd = searchParam.getLong(Info.PRICE_END);              // 搜索商品结束 价格

        this.start = searchParam.getInt(Info.START);    // 分页获取的起始位置
        this.limit = searchParam.getInt(Info.LIMIT);    // 分页获取的起始位置
        this.rlPdIdComparatorList = searchParam.getList(Info.RL_PD_ID_COMPARATOR_LIST);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        this.firstComparatorKey = searchParam.getString(Info.FIRST_COMPARATOR_KEY);   // 第一排序字段
        this.firstComparatorKeyOrder = searchParam.getBoolean(Info.FIRST_COMPARATOR_KEY_ORDER);   // 第一排序字段的顺序
        this.secondComparatorKey = searchParam.getString(Info.SECOND_COMPARATOR_KEY);    // 第二排序字段
        this.secondComparatorKeyOrder = searchParam.getBoolean(Info.SECOND_COMPARATOR_KEY_ORDER);   // 第二排序字段的顺序
    }

    //  把查询条件转换为 Matcher, 如果传入空的 paramMatcher,会创建一个新的 paramMatcher
    public ParamMatcher getParamMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品idList
        if(rlPdIdList != null && !rlPdIdList.isEmpty()){
            paramMatcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIdList);
        }
        // 业务商品分类
        if(rlGroupIdList != null && !rlGroupIdList.isEmpty()){
            paramMatcher.and("rlGroupId", ParamMatcher.IN, rlGroupIdList);
        }
        // 业务商品标签
        if(rlLableIdList != null && !rlLableIdList.isEmpty()){
            paramMatcher.and("rlLableId", ParamMatcher.IN, rlLableIdList);
        }
        // 上架或者下架的
        if (upSalesStatus == UpSalesStatusEnum.UP_AND_DOWN_SALES.upSalesStatus){
            FaiList<Integer> statusList = new FaiList<Integer>();
            statusList.add(ProductRelValObj.Status.UP);
            statusList.add(ProductRelValObj.Status.DOWN);
            paramMatcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.IN, statusList);
        }else if(upSalesStatus != UpSalesStatusEnum.ALL.upSalesStatus){   //  非全部的
            paramMatcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.EQ, upSalesStatus);
        }
        // 业务商品标签
        if(typeList != null && !typeList.isEmpty()){
            paramMatcher.and("productType", ParamMatcher.IN, typeList);
        }

        // 商品价格
        if(priceBegin != null || priceEnd != null){
            if(priceBegin != null && priceEnd != null && priceBegin.longValue() == priceEnd.longValue()){
                paramMatcher.and(BizSalesSummaryEntity.Info.MIN_PRICE, ParamMatcher.EQ, priceBegin);
                paramMatcher.and(BizSalesSummaryEntity.Info.MAX_PRICE, ParamMatcher.EQ, priceBegin);
            }else{
                if(priceBegin != null){
                    paramMatcher.and(BizSalesSummaryEntity.Info.MIN_PRICE, ParamMatcher.GE, priceBegin);
                }
                if(priceEnd != null){
                    paramMatcher.and(BizSalesSummaryEntity.Info.MIN_PRICE, ParamMatcher.LE, priceEnd);
                }
            }
        }

        // 商品录入时间
        if(!Str.isEmpty(addTimeBegin) || !Str.isEmpty(addTimeEnd)){
            if(Str.equals(addTimeBegin, addTimeEnd)){
                paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.EQ, addTimeBegin);
            }else{
                //  创建时间大于
                if(!Str.isEmpty(addTimeBegin)){
                    paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.GE, addTimeBegin);
                }
                //  创建时间小于
                if(!Str.isEmpty(addTimeEnd)){
                    paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.LE, addTimeEnd);
                }
            }
        }

        // 商品库
        if(rlLibId != -1){
            paramMatcher.and(ProductRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, rlLibId);
        }

        //  商品名称, 名称 like 查询
        if(!Str.isEmpty(searchKeyWord) && (enableProductName || enableProductProp || enableProductRemark)){
            ParamMatcher paramMatcher_or = new ParamMatcher();
            if(enableProductName){
                paramMatcher_or.and("name", ParamMatcher.LK, searchKeyWord);
            }
            // 库的参数 like 查询
            if(rlLibAndrlPropIdList != null && !rlLibAndrlPropIdList.isEmpty()){
                for(Pair p : rlLibAndrlPropIdList){
                    int rlLidId = (Integer) p.first;
                    FaiList<Integer> rlPropIdList = (FaiList<Integer>) p.second;
                }
            }
            paramMatcher.and(paramMatcher_or);
        }
        return paramMatcher;
    }

    //  把排序转换为 ParamComparator
    public ParamComparator getParamComparator(){
        ParamComparator paramComparator = new ParamComparator();
        // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        if(rlPdIdComparatorList != null && !rlPdIdComparatorList.isEmpty()){
            paramComparator.addKey(ProductRelEntity.Info.RL_PD_ID, rlPdIdComparatorList);
            paramComparator.addKey(ProductRelEntity.Info.RL_PD_ID, true);   // 如果 rlPdIdComparatorList 排不了序的数据，按业务商品 id 重新排序
        }else{
            if(!Str.isEmpty(this.firstComparatorKey)){
                paramComparator.addKey(this.firstComparatorKey, this.firstComparatorKeyOrder);
            }
            if(!Str.isEmpty(this.secondComparatorKey)){
                paramComparator.addKey(this.secondComparatorKey, this.secondComparatorKeyOrder);
            }
            //  如果没有设置排序，默认是 id 倒序
            if(paramComparator.isEmpty()){
                paramComparator.addKey(ProductRelEntity.Info.RL_PD_ID, true);
            }
        }
        return paramComparator;
    }

    //  把把查询条件转换为 SearchArg
    public SearchArg getSearArg(SearchArg searchArg){
        if(searchArg == null){
            searchArg = new SearchArg();
        }
        searchArg.start = this.start;
        searchArg.limit = this.limit;
        searchArg.matcher = getParamMatcher(searchArg.matcher);
        searchArg.cmpor = getParamComparator();
        return searchArg;
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

    public int getRlLibId() {
        return rlLibId;
    }
    public MgProductSearch setRlLibId(int rlLibId) {
        this.rlLibId = rlLibId;
        return this;
    }

    public boolean isEnableProductName() {
        return enableProductName;
    }
    public MgProductSearch setEnableProductName(boolean enableProductName) {
        this.enableProductName = enableProductName;
        return this;
    }
    public boolean isEnableProductProp() {
        return enableProductProp;
    }
    public MgProductSearch setEnableProductProp(boolean enableProductProp) {
        this.enableProductProp = enableProductProp;
        return this;
    }
    public boolean isEnableProductRemark() {
        return enableProductRemark;
    }
    public MgProductSearch setEnableProductRemark(boolean enableProductRemark) {
        this.enableProductRemark = enableProductRemark;
        return this;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }
    // 如果商品参数的搜索libAndrlPropIdList没有设置的话，那就只是代表商品名称like搜索
    public MgProductSearch setSearchKeyWord(String searchKeyWord, FaiList<Pair<Integer, FaiList<Integer>>> rlLibAndrlPropIdList) {
        this.searchKeyWord = searchKeyWord;
        this.rlLibAndrlPropIdList = rlLibAndrlPropIdList;
        return this;
    }
    public FaiList<Pair<Integer, FaiList<Integer>>> getRlLibAndrlPropIdList() {
        return rlLibAndrlPropIdList;
    }

    public MgProductSearch setAddTime(String addTimeBegin, String addTimeEnd) {
        this.addTimeBegin = addTimeBegin;
        this.addTimeEnd = addTimeEnd;
        return this;
    }
    public String getAddTimeBegin() {
        return addTimeBegin;
    }
    public String getAddTimeEnd() {
        return addTimeEnd;
    }

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


    public FaiList<Integer> getRlPdIdComparatorList() {
        return rlPdIdComparatorList;
    }
    public void setRlPdIdComparatorList(FaiList<Integer> rlPdIdComparatorList) {
        this.rlPdIdComparatorList = rlPdIdComparatorList;
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

    public MgProductSearch setFirstComparatorKey(String firstComparatorKey, boolean desc){
        this.firstComparatorKey = firstComparatorKey;
        this.firstComparatorKeyOrder = desc;
        return this;
    }
    public String getFirstComparatorKey(){
        return this.firstComparatorKey;
    }
    public boolean getFirstComparatorKeyOrder(){
        return this.firstComparatorKeyOrder;
    }

    public void setSecondComparatorKey(String secondComparatorKey, boolean desc){
        this.secondComparatorKey = secondComparatorKey;
        this.secondComparatorKeyOrder = desc;
    }
    public String getSecondComparatorKey(){
        return this.secondComparatorKey;
    }
    public boolean getSecondComparatorKeyOrder(){
        return this.secondComparatorKeyOrder;
    }

}
