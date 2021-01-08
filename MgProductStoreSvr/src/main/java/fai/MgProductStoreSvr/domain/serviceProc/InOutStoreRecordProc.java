package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.repository.InOutStoreRecordDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;

public class InOutStoreRecordProc {
    public InOutStoreRecordProc(InOutStoreRecordDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }


    public int batchAdd(int aid, FaiList<Param> infoList) {
        if(aid <= 0 || infoList == null || infoList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;infoList=%s;", m_flow, aid, infoList);
            return Errno.ARGS_ERROR;
        }
        FaiList<Param> dataList = new FaiList<>(infoList.size());
        Calendar now = Calendar.getInstance();
        String yyMMdd = Parser.parseString(now, "yyMMdd");
        for (Param info : infoList) {
            int unionPriId = info.getInt(InOutStoreRecordEntity.Info.UNION_PRI_ID);
            int pdId = info.getInt(InOutStoreRecordEntity.Info.PD_ID);
            long skuId = info.getLong(InOutStoreRecordEntity.Info.SKU_ID);
            int rlPdId = info.getInt(InOutStoreRecordEntity.Info.RL_PD_ID);
            int optType = info.getInt(InOutStoreRecordEntity.Info.OPT_TYPE);
            int changeCount = info.getInt(InOutStoreRecordEntity.Info.CHANGE_COUNT);
            Integer ioStoreRecId = m_daoCtrl.buildId();
            if(ioStoreRecId == null){
                Log.logErr("buildId err aid=%s", aid);
                return Errno.ERROR;
            }
            Param data = new Param();
            data.setInt(InOutStoreRecordEntity.Info.AID, aid);
            data.setInt(InOutStoreRecordEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(InOutStoreRecordEntity.Info.PD_ID, pdId);
            data.setLong(InOutStoreRecordEntity.Info.SKU_ID, skuId);
            data.setInt(InOutStoreRecordEntity.Info.RL_PD_ID, rlPdId);
            data.setInt(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ioStoreRecId);
            data.setInt(InOutStoreRecordEntity.Info.OPT_TYPE, optType);

            data.assign(info, InOutStoreRecordEntity.Info.C_TYPE);
            data.assign(info, InOutStoreRecordEntity.Info.S_TYPE);
            data.setInt(InOutStoreRecordEntity.Info.CHANGE_COUNT, changeCount);
            data.assign(info, InOutStoreRecordEntity.Info.PRICE);
            data.setString(InOutStoreRecordEntity.Info.NUMBER, yyMMdd+String.format("%04d", ioStoreRecId));
            data.assign(info, InOutStoreRecordEntity.Info.OPT_SID);
            data.assign(info, InOutStoreRecordEntity.Info.HEAD_SID);
            data.assign(info, InOutStoreRecordEntity.Info.OPT_TIME);
            data.assign(info, InOutStoreRecordEntity.Info.FLAG);
            data.assign(info, InOutStoreRecordEntity.Info.REMARK);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_INT_PROP1);
            data.assign(info, InOutStoreRecordEntity.Info.KEEP_PROP1);
            data.setCalendar(InOutStoreRecordEntity.Info.SYS_UPDATE_TIME, now);
            data.setCalendar(InOutStoreRecordEntity.Info.SYS_CREATE_TIME, now);
            dataList.add(data);
        }

        int rt = m_daoCtrl.batchInsert(dataList, null, true);
        if(rt != Errno.OK){
            Log.logErr(rt,"batchInsert err;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }

        Log.logStd("batchAdd ok;flow=%d;aid=%d;", m_flow, aid);
        return rt;
    }
    public int del(int aid, FaiList<Integer> idList) {
        if(aid <= 0 || idList == null || idList.isEmpty()){
            Log.logStd("batchAdd error;flow=%d;aid=%s;idList=%s;", m_flow, aid, idList);
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher(InOutStoreRecordEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID, ParamMatcher.IN, idList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logErr(rt,"delete err;flow=%d;aid=%d;", m_flow, aid);
            return rt;
        }
        Log.logStd("del ok;flow=%d;aid=%d;idList=%s;", m_flow, aid, idList);
        return rt;
    }


    public int clearIdBuilderCache(int aid){
        int rt = m_daoCtrl.clearIdBuilderCache(aid);
        return rt;
    }

    private int m_flow;
    private InOutStoreRecordDaoCtrl m_daoCtrl;


}
