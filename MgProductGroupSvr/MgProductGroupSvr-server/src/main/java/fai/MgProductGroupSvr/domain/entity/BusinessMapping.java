package fai.MgProductGroupSvr.domain.entity;

/**
 * 业务映射
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/3 14:42
 */
public enum  BusinessMapping {

    SITE("SITE", 1),
    HD("HD", 2),
    CD("CD", 3),
    TS("TS", 4),
    YK("YK", 5),
    MEDIA("MEDIA", 6),
    OPT("OPT", 7),
    KC("KC", 8),
    MP("MP", 9),
    MALL("MALL", 10),
    QZ("QZ", 11),
    EDU("EDU", 12),;

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
