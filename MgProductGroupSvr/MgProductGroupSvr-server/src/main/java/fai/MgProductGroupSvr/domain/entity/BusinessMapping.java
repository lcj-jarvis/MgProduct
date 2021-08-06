package fai.MgProductGroupSvr.domain.entity;

import fai.comm.middleground.FaiValObj;

/**
 * 业务映射
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/3 14:42
 */
public enum  BusinessMapping {

    SITE("SITE", FaiValObj.TermId.SITE),
    HD("HD", FaiValObj.TermId.HD),
    CD("CD", FaiValObj.TermId.CD),
    TS("TS", FaiValObj.TermId.TS),
    YK("YK", FaiValObj.TermId.YK),
    MEDIA("MEDIA", FaiValObj.TermId.MEDIA),
    OPT("OPT", FaiValObj.TermId.OPT),
    KC("KC", FaiValObj.TermId.KC),
    MP("MP", FaiValObj.TermId.MP),
    MALL("MALL", FaiValObj.TermId.MALL),
    QZ("QZ", FaiValObj.TermId.QZ),
    EDU("EDU", FaiValObj.TermId.EDU),;

    /** 业务名称 */
    private String name;
    /** 业务对应的值 */
    private int tid;

    BusinessMapping(String name, int tid) {
        this.name = name;
        this.tid = tid;
    }

    public static String getName(int tid) {
        for (BusinessMapping bs : BusinessMapping.values()) {
            if (bs.tid == tid) {
                return bs.name;
            }
        }
        return null;
    }
}
