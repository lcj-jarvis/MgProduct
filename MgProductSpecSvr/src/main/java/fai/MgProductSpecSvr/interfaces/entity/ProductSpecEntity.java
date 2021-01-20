package fai.MgProductSpecSvr.interfaces.entity;

/**
 * 产品规格
 */
public class ProductSpecEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String PD_ID = "pdId";                                  // int 商品 id
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id
        public static final String NAME = SpecStrEntity.Info.NAME;
        public static final String PD_SC_ID = "pdScId";                             // int 商品规格 id
        public static final String SOURCE_TID = "sourceTid";                        // int 创建产品规格的 项目id
        public static final String SOURCE_UNION_PRI_ID = "sourceUnionPriId";        // int 创建产品规格的 联合主键id
        public static final String SORT = "sort";                                   // int 排序
        public static final String FLAG = "flag";                                   // int flag
        /**
         * @see ProductSpecValObj.InPdScValList
         */
        public static final String IN_PD_SC_VAL_LIST = "inPdScValList";             // 规格值(FaiList<Param>([{psScStrId1 , checked},{psScStrId2 , checked}])), 支持最大50个值
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
        public static final String SYS_UPDATE_TIME = "sysUpdateTime";               // datetime 修改时间
    }
}
