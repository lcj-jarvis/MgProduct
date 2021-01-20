package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.SalesSummaryEntity;
import fai.MgProductStoreSvr.domain.repository.BizSalesSummaryDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.SalesSummaryDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;

public class SalesSummaryProc {
    public SalesSummaryProc(SalesSummaryDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int report(int aid, int pdId, Param info) {
        if(aid <= 0 || pdId <= 0 || info == null || info.isEmpty()){
            Log.logStd("arg error;flow=%d;aid=%s;pdId=%s;info=%s;", m_flow, aid, pdId, info);
            return Errno.ARGS_ERROR;
        }
        int rt = Errno.ERROR;

        ParamMatcher matcher = new ParamMatcher(SalesSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SalesSummaryEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        Ref<Param> infoRef = new Ref<>();
        rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logErr(rt,"selectFirst err;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
            return rt;
        }
        Param oldInfo = infoRef.value;

        Calendar now = Calendar.getInstance();
        info.setCalendar(SalesSummaryEntity.Info.SYS_UPDATE_TIME, now);
        if(oldInfo.isEmpty()){
            info.setInt(SalesSummaryEntity.Info.AID, aid);
            info.setInt(SalesSummaryEntity.Info.PD_ID, pdId);
            info.setCalendar(SalesSummaryEntity.Info.SYS_CREATE_TIME, now);
            rt = m_daoCtrl.insert(info);
            if(rt != Errno.OK){
                Log.logErr(rt,"insert err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return rt;
            }
        }else {
            ParamUpdater updater = new ParamUpdater(info);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logErr(rt,"update err;flow=%s;aid=%s;pdId=%s;info=%s", m_flow, aid, pdId, info);
                return rt;
            }
        }

        Log.logStd("ok!;flow=%s;aid=%s;pdId=%s;", m_flow, aid, pdId);
        return rt;
    }
    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher(SalesSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SalesSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }
    public int getInfoListFromDao(int aid, FaiList<Integer> pdIdList, Ref<FaiList<Param>> listRef) {
        if(aid <= 0 || pdIdList == null || pdIdList.isEmpty() || listRef == null){
            Log.logStd("arg error;flow=%d;aid=%s;pdIdList=%s;listRef=%s;", m_flow, aid, pdIdList, listRef);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(SalesSummaryEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SalesSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND){
            Log.logDbg(rt, "select err;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logDbg(rt,"ok!;flow=%s;aid=%s;pdIdList=%s;", m_flow, aid, pdIdList);
        return rt;
    }

    private int m_flow;
    private SalesSummaryDaoCtrl m_daoCtrl;



}
