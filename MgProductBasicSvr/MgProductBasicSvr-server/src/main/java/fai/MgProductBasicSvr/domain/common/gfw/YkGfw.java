package fai.MgProductBasicSvr.domain.common.gfw;


import fai.comm.util.Gfw;

public class YkGfw extends MgGfw {
    private static YkGfw ykGfw = new YkGfw();

    private YkGfw() {
        this.type = Gfw.Type.YK_PRODUCT;
    }

    public static YkGfw getInstance() {
        return ykGfw;
    }
}
