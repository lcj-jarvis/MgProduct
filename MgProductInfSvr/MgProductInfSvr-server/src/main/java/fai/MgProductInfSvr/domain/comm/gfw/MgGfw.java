package fai.MgProductInfSvr.domain.comm.gfw;

import fai.comm.util.FaiList;
import fai.comm.util.Param;

public abstract class MgGfw {
    protected int type;

    // 子类可重写该方法实现记录不同字段
    protected abstract void writeRichLog(int aid, int siteId, int rlPdId, FaiList<Param> list);
}
