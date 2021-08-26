package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.Param;

/**
 * @author LuChaoJi
 * @date 2021-08-16 18:01
 *
 * 抽象的查询父类
 * 包含db和es的公共查询字段
 */
public abstract class BaseMgProductSearch {

    /**
     * 搜索的关键字，会关联商品名称、商品对应的参数
     */
    protected String searchKeyWord;

    /**
     * 上下架开始
     * 默认是全部的, 对应 ProductRelValObj.Status
     */
    protected int upSalesStatus = UpSalesStatusEnum.ALL.upSalesStatus;

    /**
     * 搜索的开始位置
     */
    protected int start = 0;

    /**
     * 分页限制条数开始，默认最大是 100
     */
    protected int limit = 100;

    /**
     * 第一排序字段
     */
    protected String firstComparatorKey;

    /**
     * 第一排序字段的顺序, 默认顺序
     */
    protected boolean firstComparatorKeyOrderByDesc = false;

    /**
     * 选择是否使用第二的排序
     */
    protected boolean needSecondComparatorSorting = false;

    /**
     * 第二排序字段
     */
    protected String secondComparatorKey;

    /**
     * 第二排序字段的顺序，默认顺序
     */
    protected boolean secondComparatorKeyOrderByDesc = false;

    /**
     * 默认不在es中进行排序，在DB中进行排序
     * true：在db中排序
     * false：在es中排序
     */
    protected boolean comparatorInEs = false;

    /**
     * 上下架搜索参数封装
     */
    public enum UpSalesStatusEnum{
        // 全部的状态
        ALL(0),
        // 上架
        UP_SALES(1),
        // 下架
        DOWN_SALES(2),
        // 删除
        DELETE(3),
        // 上架和下架的
        UP_AND_DOWN_SALES(4);
        public int upSalesStatus;
        private UpSalesStatusEnum(int upSalesStatus){
            this.upSalesStatus = upSalesStatus;
        }
        public int getUpSalesStatus(){
            return upSalesStatus;
        }
    }

    /**
     * 把查询条件转换为 Param
     * @return 包含查询条件的 Param
     */
    public abstract Param getSearchParam();

    /**
     * 初始化查询条件
     * @param searchParam 查询条件
     */
    public abstract void initSearchParam(Param searchParam);

    public String getSearchKeyWord() {
        return searchKeyWord;
    }

    public int getUpSalesStatus() {
        return upSalesStatus;
    }

    public String getFirstComparatorKey() {
        return firstComparatorKey;
    }

    public boolean isFirstComparatorKeyOrderByDesc() {
        return firstComparatorKeyOrderByDesc;
    }

    public boolean isNeedSecondComparatorSorting() {
        return needSecondComparatorSorting;
    }

    public String getSecondComparatorKey() {
        return secondComparatorKey;
    }

    public boolean isSecondComparatorKeyOrderByDesc() {
        return secondComparatorKeyOrderByDesc;
    }

    public static final class BaseSearchInfo {
        /**
         * 第一排序字段
         */
        public static final String FIRST_COMPARATOR_KEY = "firstComparatorKey";

        /**
         * 升序还是倒序，默认是升序
         */
        public static final String FIRST_COMPARATOR_KEY_ORDER_BY_DESC = "firstComparatorKeyOrderByDesc";

        /**
         * 是否需要第二排序
         */
        public static final String NEED_SECOND_COMPARATOR_SORTING = "needSecondComparatorSorting";

        /**
         * 第二排序字段必须是能够确定唯一的排序字段的，如果不能，会重写为 id 倒序排序
         */
        public static final String SECOND_COMPARATOR_KEY = "secondComparatorKey";

        /**
         * 升序还是倒序，默认是升序
         */
        public static final String SECOND_COMPARATOR_KEY_ORDER_BY_DESC = "secondComparatorKeyOrderByDesc";

        /**
         * 分页位置
         */
        public static final String START = "start";

        /**
         * 分页条数
         */
        public static final String LIMIT = "limit";
    }

    /**
     * 初始化公共的查询条件
     * @param baseSearchParam
     */
    public void initBaseSearchParam(Param baseSearchParam) {
        this.firstComparatorKey = baseSearchParam.getString(BaseSearchInfo.FIRST_COMPARATOR_KEY);
        this.firstComparatorKeyOrderByDesc = baseSearchParam.getBoolean(BaseSearchInfo.FIRST_COMPARATOR_KEY_ORDER_BY_DESC);
        this.needSecondComparatorSorting = baseSearchParam.getBoolean(BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
        this.secondComparatorKey = baseSearchParam.getString(BaseSearchInfo.SECOND_COMPARATOR_KEY);
        this.secondComparatorKeyOrderByDesc = baseSearchParam.getBoolean(BaseSearchInfo.SECOND_COMPARATOR_KEY_ORDER_BY_DESC);
        this.start = baseSearchParam.getInt(BaseSearchInfo.LIMIT);
        this.limit = baseSearchParam.getInt(BaseSearchInfo.LIMIT);
    }
}
