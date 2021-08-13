package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.Param;

/**
 * @author LuChaoJi
 * @date 2021-08-13 17:02
 *
 * Es专用的搜索类
 */
public class MgProductEsSearch {

    /**
     * es中的字段
     */
    public static final class EsSearchInfo {
        public static final String AID = "aid";
        public static final String PDID = "pdId";
        public static final String UNIONPRIID = "unionPriId";
        public static final String STATUS = "status"; // 商品状态（对应商品的业务表）
        public static final String NAME = "name"; // 商品的名称
    }

    /**
     * Docid中的主键顺序
     */
    public static final class EsSearchPrimaryKeyOrder {
        public static final Integer AID_ORDER = 0;
        public static final Integer PDID_ORDER = 1;
        public static final Integer UNIONPRIID_ORDER = 2;
    }

    private String name; // 商品名称
    private Integer status; // 商品状态（商品业务表的）

    /**
     * 把查询条件转换为 Param
     */
    public Param getEsSearchParam(){
        return new Param()
            .setString(EsSearchInfo.NAME, name)
            .setInt(EsSearchInfo.STATUS, status);
    }

    /**
     * 初始话es的搜索字段和过滤字段等
     * @param esSearchParam 搜索的内容
     */
    public void initEsSearch(Param esSearchParam) {
        this.name = esSearchParam.getString(MgProductEsSearch.EsSearchInfo.NAME);
        this.status = esSearchParam.getInt(MgProductEsSearch.EsSearchInfo.STATUS);
    }

    public String getName() {
        return name;
    }

    public Integer getStatus() {
        return status;
    }
}
