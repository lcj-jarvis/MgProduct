package fai.MgProductBasicSvr.domain.common.gfw;


import fai.comm.util.Gfw;

public class SiteGfw extends MgGfw {
    private static SiteGfw siteGfw = new SiteGfw();

    private SiteGfw() {
        this.type = Gfw.Type.PRODUCT;
    }

    public static SiteGfw getInstance() {
        return siteGfw;
    }
}
