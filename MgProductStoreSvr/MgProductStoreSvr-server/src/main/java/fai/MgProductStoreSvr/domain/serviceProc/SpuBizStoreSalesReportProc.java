package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.SpuBizStoreSalesReportEntity;
import fai.MgProductStoreSvr.domain.entity.SpuBizStoreSalesReportValObj;
import fai.MgProductStoreSvr.domain.repository.SpuBizStoreSalesReportDaoCtrl;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SpuBizStoreSalesReportProc {
    public SpuBizStoreSalesReportProc(SpuBizStoreSalesReportDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int addReportCountTask(int aid, int unionPriId, FaiList<Integer> pdIdList) {
        return addReportTask(aid, unionPriId, pdIdList, SpuBizStoreSalesReportValObj.Flag.REPORT_COUNT);
    }

    private int addReportTask(int aid, int unionPriId, FaiList<Integer> pdIdList, int ... flagBits){
        if(flagBits == null){
            return Errno.ARGS_ERROR;
        }
        int flag = 0;
        for (int flagBit : flagBits) {
            flag |= flagBit;
        }
        Set<Integer> pdIdSet = new HashSet<>(pdIdList);
        int rt = Errno.ERROR;
        for (Integer pdId : pdIdSet) {
            Param data = new Param();
            data.setInt(SpuBizStoreSalesReportEntity.Info.AID, aid);
            data.setInt(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, unionPriId);
            data.setInt(SpuBizStoreSalesReportEntity.Info.PD_ID, pdId);
            data.setInt(SpuBizStoreSalesReportEntity.Info.FLAG, flag);
            ParamUpdater updater = new ParamUpdater();
            updater.add(SpuBizStoreSalesReportEntity.Info.FLAG, ParamUpdater.LOR, flag);
            rt = m_daoCtrl.replace(data, updater);
            if(rt != Errno.OK){
                Log.logStd(rt,"addReportCountTask err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
                return rt;
            }
        }
        for (Integer pdId : pdIdSet) {
            rt = sendMq(aid, unionPriId, pdId);
            if(rt != Errno.OK){
                return rt;
            }
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdIdList=%s;flagBits=%s", m_flow, aid, unionPriId, pdIdList, Arrays.toString(flagBits));
        return rt;
    }

    private int sendMq(int aid, int unionPriId, int pdId){
        Param sendInfo = new Param();
        sendInfo.setInt(SpuBizStoreSalesReportEntity.Info.AID, aid);
        sendInfo.setInt(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, unionPriId);
        sendInfo.setInt(SpuBizStoreSalesReportEntity.Info.PD_ID, pdId);
        FaiMqMessage message = new FaiMqMessage();
        // 指定topic
        message.setTopic(MqConfig.SpuBizReport.TOPIC);
        // 指定tag
        message.setTag(MqConfig.SpuBizReport.TAG);
        // 添加流水号
        message.setFlow(m_flow);
        // aid-unionPriId-pdId 做幂等
        message.setKey(aid+"-"+unionPriId+"-"+pdId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.SpuBizReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e, MqConfig.SpuBizReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s", message.getFlow(), message.getMsgId());
        }
        return Errno.ERROR;
    }


    public int getInfo(int aid, int unionPriId, int pdId, Ref<Param> infoRef) {
        if(infoRef == null){
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(SpuBizStoreSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpuBizStoreSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = matcher;
        int rt = m_daoCtrl.selectFirst(searchArg, infoRef);
        if(rt != Errno.OK){
            Log.logStd(rt, "selectFirst err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
            return rt;
        }
        return rt;
    }

    private int m_flow;
    private SpuBizStoreSalesReportDaoCtrl m_daoCtrl;

    /**
     * 处理过程中如果无其他类型数据上报任务产生，就正常删除。
     * 否则就只去掉上报过的数据类型的标志。
     */
    public int del(int aid, int unionPriId, int pdId, int flag) {
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(SpuBizStoreSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(SpuBizStoreSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(SpuBizStoreSalesReportEntity.Info.FLAG, ParamMatcher.EQ, flag);
        Ref<Integer> rowCountRef = new Ref<>(0);
        int rt = m_daoCtrl.delete(matcher, rowCountRef);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;flag=%s;", m_flow, aid, unionPriId, pdId, flag);
            return rt;
        }
        if(rowCountRef.value == 0){ // 说明上报过程中产生了新的类型的数据需要上报
            matcher = new ParamMatcher();
            matcher.and(SpuBizStoreSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(SpuBizStoreSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(SpuBizStoreSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            ParamUpdater updater = new ParamUpdater(SpuBizStoreSalesReportEntity.Info.FLAG, ParamUpdater.LAND, ~flag);
            rt = m_daoCtrl.update(updater, matcher);
            if(rt != Errno.OK){
                Log.logStd(rt, "update err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;flag=%s;", m_flow, aid, unionPriId, pdId, flag);
                return rt;
            }
        }
        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdId=%s;flag=%s;", m_flow, aid, unionPriId, pdId, flag);
        return rt;
    }

    public int batchDel(int aid, FaiList<Integer> pdIdList) {
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(SpuBizStoreSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(SpuBizStoreSalesReportEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }

}
