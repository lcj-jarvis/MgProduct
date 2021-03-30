package fai.MgProductInfUtil;

import fai.middleground.infutil.MgInternalCli;

/**
 * 商品中台内部接口cli
 */
public class MgProductInternalCli extends MgInternalCli {
    public MgProductInternalCli(int flow, String name) {
        //-Dfai.MgProductInfUtil.MgProductInternalCli.allow=true
        super(flow, name, "fai.MgProductInfUtil.MgProductInternalCli.allow");
    }

}
