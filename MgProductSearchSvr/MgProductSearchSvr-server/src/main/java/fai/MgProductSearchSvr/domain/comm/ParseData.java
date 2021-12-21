package fai.MgProductSearchSvr.domain.comm;

import fai.MgProductBasicSvr.interfaces.dto.*;
import fai.MgProductInfSvr.interfaces.utils.MgProductDbSearch;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuCodeDao;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lu
 * @date 2021-10-08 16:11
 */
public class ParseData {

    /**
     * 通过Map的value解析到获取返回的Param
     */
    public static final Map<String, Integer> TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY = new HashMap<>(16);

    public static final Map<String, Integer> TABLE_NAME_MAPPING_PARSE_DATA_KEY = new HashMap<>(16);

    static  {
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName(), ProductDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName(), ProductRelDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName(), ProductBindGroupDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName(), ProductBindPropDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName(), ProductBindTagDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName(), SpuBizSummaryDto.Key.DATA_STATUS);
        TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName(), ProductSpecSkuCodeDao.Key.DATA_STATUS);

        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName(), ProductDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName(), ProductRelDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName(), ProductBindGroupDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName(), ProductBindPropDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName(), ProductBindTagDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName(), SpuBizSummaryDto.Key.INFO_LIST);
        TABLE_NAME_MAPPING_PARSE_DATA_KEY.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName(), ProductSpecSkuCodeDao.Key.INFO_LIST);
    }

}
