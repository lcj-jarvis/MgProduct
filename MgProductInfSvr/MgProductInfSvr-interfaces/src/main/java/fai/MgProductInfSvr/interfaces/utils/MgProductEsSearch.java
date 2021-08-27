package fai.MgProductInfSvr.interfaces.utils;

import fai.app.FaiSearchExDef;
import fai.comm.util.Param;
import fai.comm.util.Str;

/**
 * @author LuChaoJi
 * @date 2021-08-13 17:02
 *
 * Es专用的搜索类
 */
public class MgProductEsSearch extends BaseMgProductSearch{

    /**
     * es中第一排序字段的类型。设置了第一排序字段，则必填第一排序字段对应的类型
     * 参考 FaiSearchExDef.SearchField.FieldType
     * {@link FaiSearchExDef.SearchField.FieldType}
     * 如：
     *  FaiSearchExDef.SearchField.FieldType.INTEGER
     *  FaiSearchExDef.SearchField.FieldType.CALENDAR
     *  ...
     */
    private Byte firstComparatorKeyType;

    /**
     * es中第二排序字段的类型。设置了第二排序字段，则必填第二排序字段对应的类型
     * 参考 FaiSearchExDef.SearchField.FieldType
     * {@link FaiSearchExDef.SearchField.FieldType}
     * 如：
     *  FaiSearchExDef.SearchField.FieldType.INTEGER
     *  FaiSearchExDef.SearchField.FieldType.CALENDAR
     */
    private Byte secondComparatorKeyType;


    /**
     * 把查询条件转换为 Param
     */
    @Override
    public Param getSearchParam() {
        // 先获取公共搜索条件的Param
        Param esParam = getBaseSearchParam();

        esParam.setByte(EsSearchInfo.FIRST_COMPARATOR_KEY_TYPE, firstComparatorKeyType);
        esParam.setByte(EsSearchInfo.SECOND_COMPARATOR_KEY_TYPE, secondComparatorKeyType);

        return esParam;
    }

    /**
     * 初始化es的搜索字段和过滤字段等
     * @param esSearchParam 搜索的内容
     */
    @Override
    public void initSearchParam(Param esSearchParam) {
        // 先初始化公共搜索字段。
        initBaseSearchParam(esSearchParam);

        this.firstComparatorKeyType = esSearchParam.getByte(EsSearchInfo.FIRST_COMPARATOR_KEY_TYPE);
        this.secondComparatorKeyType = esSearchParam.getByte(EsSearchInfo.SECOND_COMPARATOR_KEY_TYPE);
    }

    public static final class EsSearchInfo {
        // es 第一排序字段类型
        public static final String FIRST_COMPARATOR_KEY_TYPE = "firstComparatorKeyType";
        // es 第二排序字段类型
        public static final String SECOND_COMPARATOR_KEY_TYPE = "secondComparatorKeyType";
    }

    /**
     * Docid中的主键顺序
     */
    public static final class EsSearchPrimaryKeyOrder {
        public static final Integer AID_ORDER = 0;
        public static final Integer PDID_ORDER = 1;
        public static final Integer UNIONPRIID_ORDER = 2;
    }

    /**
     * es中的字段
     */
    public static final class EsSearchFields {
        public static final String AID = "aid";
        public static final String PDID = "pdId";
        public static final String UNIONPRIID = "unionPriId";
        // 商品状态（对应商品的业务表），对应upSalesStatus的值
        public static final String STATUS = "status";
        // 如果enableSearchProductName为true的话，searchKeyword的值就是商品的名称的值
        public static final String NAME = "name";
    }

    public byte getFirstComparatorKeyType() {
        return firstComparatorKeyType;
    }

    public byte getSecondComparatorKeyType() {
        return secondComparatorKeyType;
    }

    public boolean hasFirstComparator() {
        return !Str.isEmpty(firstComparatorKey) && firstComparatorKeyType != null ;
    }
}