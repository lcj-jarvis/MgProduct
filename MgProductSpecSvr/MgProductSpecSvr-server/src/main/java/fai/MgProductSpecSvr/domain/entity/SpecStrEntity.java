package fai.MgProductSpecSvr.domain.entity;

/**
 * 规格相关字符串
 */
public class SpecStrEntity {
    public static final class Info {
        public static final String AID = "aid";                                     // int 企业aid
        public static final String SC_STR_ID = "scStrId";                           // int 规格字符串 id （aid下自增）
        public static final String NAME = "name";                                   // varchar(100) 规格名称
        public static final String SYS_CREATE_TIME = "sysCreateTime";               // datetime 创建时间
    }
}
