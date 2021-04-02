package fai.MgProductInfSvr.interfaces.entity;

/**
 * 规格服务 - 实体
 */
public class ProductSpecEntity {
    /**
     * 产品规格
     */
    public static final class SpecInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String SC_STR_ID = SpecStrInfo.SC_STR_ID;
        public static final String NAME = SpecStrInfo.NAME;
        public static final String PD_SC_ID = "pdScId";                             // int 商品规格 id
        public static final String SOURCE_TID = "sourceTid";                        // int 创建产品规格的 项目id
        public static final String SORT = "sort";                                   // int 排序
        public static final String FLAG = "flag";                                   // int flag
        /**
         * @see ProductSpecValObj.Spec.InPdScValList
         */
        public static final String IN_PD_SC_VAL_LIST = "inPdScValList";             // 规格值(FaiList<Param>([{psScStrId1 , checked},{psScStrId2 , checked}])), 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
    /**
     * 产品规格SKU
     */
    public static final class SpecSkuInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_PD_ID = "rlPdId";                             // int 商品业务 id
        public static final String SKU_ID = "skuId";                                // bigInt SKU id（aid下自增），系统对内, 也做排序
        public static final String SORT = "sort";                                   // int 排序
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格的 项目id
        public static final String SKU_CODE = "skuCode";                              // TODO
        /**
         * 每个元素值:
         * @see SpecStrInfo#SC_STR_ID
         */
        public static final String IN_PD_SC_STR_ID_LIST = "inPdScStrIdList";        // 规格值 id FaiList<Integer>, 支持最大 15 种规格
        /**
         * 每个元素值:
         * @see SpecStrInfo#NAME
         */
        public static final String IN_PD_SC_STR_NAME_LIST = "inPdScStrNameList";    // 规格值 name FaiList<String>, 支持最大 15 种规格
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
        public static final String STATUS = "status";                               // tinyint 状态
        /**
         * 每个元素值:
         * @see SkuCodeInfo#SKU_CODE
         */
        public static final String SKU_CODE_LIST = "skuCodeList";                   // sku编码集合FaiList<String>，最大支持10个

        public static final String SPU = "spu";                                     // boolean 零时数据不入库
    }
    /**
     * 条件
     */
    public static final class Condition {
        public static final String FUZZY = "fuzzy";                                 // boolean 模糊搜索
        public static final String RETURN_FULL_INFO = "returnFullInfo";             // boolean 返回全部信息
    }

    /**
     * 规格模板
     */
    public static final class SpecTempInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_TP_SC_ID = SpecTempBizRelInfo.RL_TP_SC_ID;
        public static final String RL_LIB_ID = SpecTempBizRelInfo.RL_LIB_ID;
        public static final String TP_SC_ID = SpecTempBizRelInfo.TP_SC_ID;
        public static final String NAME = "name";                                   // varchar(100) 规格模板名称
        public static final String SOURCE_TID = "sourceTid";                        // int 创建规格模板的 项目id
        public static final String FLAG = SpecTempBizRelInfo.FLAG;
        public static final String SORT = SpecTempBizRelInfo.SORT;
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
    /**
     * 规格模板详情
     */
    public static final class SpecTempDetailInfo{
        public static final String AID = "aid";                                     // int 企业aid
        public static final String RL_TP_SC_ID = SpecTempBizRelInfo.RL_TP_SC_ID;
        public static final String SC_STR_ID = SpecStrInfo.SC_STR_ID;
        public static final String NAME = SpecStrInfo.NAME;
        public static final String TP_SC_DT_ID = "tpScDtId";                        // int 规格模板详情 id
        public static final String SORT = "sort";                                   // int 规格排序
        public static final String FLAG = "flag";                                   // int flag
        /**
         * @see ProductSpecValObj.SpecTempDetail.InScValList
         * */
        public static final String IN_SC_VAL_LIST = "inScValList";                  // 规格值(FaiList<Param>) 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    /**
     * 不对外暴露
     * 规格相关字符串
     */
    private static final class SpecStrInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id （aid下自增）
        public static final String NAME = "name";                                   // varchar(100) 规格(值)名称
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
    /**
     * 不对外暴露
     * 规格模板业务关联
     */
    private static final class SpecTempBizRelInfo {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String UNION_PRI_ID = "unionPriId";                     // int 联合主键 id
        public static final String RL_LIB_ID = "rlLibId";                           // int 库业务 id
        public static final String RL_TP_SC_ID = "rlTpScId";                        // int 规格模板业务 Id
        public static final String TP_SC_ID = "tpScId";                             // int 规格模板 id
        public static final String SORT = "sort";                                   // int 排序
        public static final String FLAG = "flag";                                   // int flag
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }

    /**
     * 不对外暴露
     * 条码
     */
    private static final class SkuCodeInfo {
        public static final String SKU_CODE = "skuCode";                            // String 条码 值
        public static final String AID = "aid";                                     // int 企业aid
        public static final String UNION_PRI_ID = "unionPriId";                     // int 创建商品的unionPriId
        public static final String SKU_ID = "skuId";                                // long SKU id
    }

}
