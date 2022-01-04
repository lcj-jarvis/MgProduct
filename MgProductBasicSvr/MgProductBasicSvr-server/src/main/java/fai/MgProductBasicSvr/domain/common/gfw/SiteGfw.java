package fai.MgProductBasicSvr.domain.common.gfw;


import fai.comm.util.FaiList;
import fai.comm.util.Gfw;
import fai.comm.util.Param;

public class SiteGfw extends MgGfw {
    private static SiteGfw siteGfw = new SiteGfw();

    private SiteGfw() {
        this.type = Gfw.Type.PRODUCT;
    }

    public static SiteGfw getInstance() {
        return siteGfw;
    }

    @Override
    protected FaiList<Param> buildLogData(int aid, int siteId, Param info) {
        return null;
    }
}
