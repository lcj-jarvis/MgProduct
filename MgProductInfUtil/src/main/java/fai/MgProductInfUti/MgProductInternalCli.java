package fai.MgProductInfUti;

import fai.middleground.infutil.MgInternalCli;

public class MgProductInternalCli extends MgInternalCli {
    public MgProductInternalCli(int flow, String name) {
        //-Dfai.MgProductInfUti.MgProductInternalCli.allow=true
        super(flow, name, "fai.MgProductInfUti.MgProductInternalCli.allow");
    }

}
