package fai.MgProductSpecSvr.application.service;

import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.serviceProc.SpecStrProc;
import fai.MgProductSpecSvr.interfaces.dto.SpecStrDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;

public class SpecStrService  extends ServicePub {

    /**
     * 跟据规格字符串id集 获取规格字符串
     */
    public int getScStrInfoList(FaiSession session, int flow, int aid, int tid, FaiList<Integer> strIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || strIdList == null || strIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;tid=%s;strIdList=%s;", flow, aid, tid, strIdList);
                return rt;
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
            try {
                SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
                rt = specStrProc.getList(aid, strIdList, listRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally{
                specStrDaoCtrl.closeDao();
            }
            FaiList<Param> scStrInfoList = listRef.value;
            FaiBuffer sendBuf = new FaiBuffer(true);
            scStrInfoList.toBuffer(sendBuf, SpecStrDto.Key.INFO_LIST, SpecStrDto.getInfoDto());
            session.write(sendBuf);
            Log.logDbg("ok;flow=%s;aid=%s;strIdList=%s;", flow, aid, strIdList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
