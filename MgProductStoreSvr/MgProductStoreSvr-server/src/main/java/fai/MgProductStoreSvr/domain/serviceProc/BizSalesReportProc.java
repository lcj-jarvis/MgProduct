package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.BizSalesReportEntity;
import fai.MgProductStoreSvr.domain.entity.BizSalesReportValObj;
import fai.MgProductStoreSvr.domain.repository.BizSalesReportDaoCtrl;
import fai.MgProductStoreSvr.interfaces.conf.MqConfig;
import fai.comm.mq.api.MqFactory;
import fai.comm.mq.api.Producer;
import fai.comm.mq.api.SendResult;
import fai.comm.mq.exception.MqClientException;
import fai.comm.mq.message.FaiMqMessage;
import fai.comm.util.*;

import java.util.Arrays;

public class BizSalesReportProc {
    public BizSalesReportProc(BizSalesReportDaoCtrl daoCtrl, int flow) {
        m_daoCtrl = daoCtrl;
        m_flow = flow;
    }

    public int addReportCountTask(int aid, int unionPriId, int pdId) {
        return addReportTask(aid, unionPriId, pdId, BizSalesReportValObj.Flag.REPORT_COUNT);
    }
    public int addReportPriceTask(int aid, int unionPriId, int pdId) {
        return addReportTask(aid, unionPriId, pdId, BizSalesReportValObj.Flag.REPORT_PRICE);
    }

    private int addReportTask(int aid, int unionPriId, int pdId, int ... flagBits){
        if(flagBits == null){
            return Errno.ARGS_ERROR;
        }
        int flag = 0;
        for (int flagBit : flagBits) {
            flag |= flagBit;
        }
        Param data = new Param();
        data.setInt(BizSalesReportEntity.Info.AID, aid);
        data.setInt(BizSalesReportEntity.Info.UNION_PRI_ID, unionPriId);
        data.setInt(BizSalesReportEntity.Info.PD_ID, pdId);
        data.setInt(BizSalesReportEntity.Info.FLAG, flag);
        ParamUpdater updater = new ParamUpdater();
        updater.add(BizSalesReportEntity.Info.FLAG, ParamUpdater.LOR, flag);
        int rt = m_daoCtrl.replace(data, updater);
        if(rt != Errno.OK){
            Log.logStd(rt,"addReportCountTask err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;", m_flow, aid, unionPriId, pdId);
            return rt;
        }

        rt = sendMq(aid, unionPriId, pdId);
        if(rt != Errno.OK){
            return rt;
        }

        Log.logStd("ok;flow=%s;aid=%s;unionPriId=%s;pdId=%s;flagBits=%s", m_flow, aid, unionPriId, pdId, Arrays.toString(flagBits));
        return rt;
    }

    private int sendMq(int aid, int unionPriId, int pdId){
        Param sendInfo = new Param();
        sendInfo.setInt(BizSalesReportEntity.Info.AID, aid);
        sendInfo.setInt(BizSalesReportEntity.Info.UNION_PRI_ID, unionPriId);
        sendInfo.setInt(BizSalesReportEntity.Info.PD_ID, pdId);
        FaiMqMessage message = new FaiMqMessage();
        // 指定topic
        message.setTopic(MqConfig.BizSalesReport.TOPIC);
        // 指定tag
        message.setTag(MqConfig.BizSalesReport.TAG);
        // 添加流水号
        message.setFlow(m_flow);
        // aid-unionPriId-pdId 做幂等
        message.setKey(aid+"-"+unionPriId+"-"+pdId);
        // 消息体
        message.setBody(sendInfo);
        Producer producer = MqFactory.getProducer(MqConfig.BizSalesReport.PRODUCER);
        try {
            // 发送成功返回SendResult对象
            SendResult send = producer.send(message);
            return Errno.OK;
        } catch (MqClientException e) {
            // 发送失败会抛出异常,业务方自己处理,入库或者告警
            Log.logErr(e,MqConfig.BizSalesReport.PRODUCER+" send message err; messageFlow=%d, msgId=%s", message.getFlow(), message.getMsgId());
        }
        return Errno.ERROR;
    }


    public int getInfo(int aid, int unionPriId, int pdId, Ref<Param> infoRef) {
        if(infoRef == null){
            return Errno.ARGS_ERROR;
        }
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(BizSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(BizSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(BizSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
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
    private BizSalesReportDaoCtrl m_daoCtrl;

    /**
     * 处理过程中如果无其他类型数据上报任务产生，就正常删除。
     * 否则就只去掉上报过的数据类型的标志。
     */
    public int del(int aid, int unionPriId, int pdId, int flag) {
        ParamMatcher matcher = new ParamMatcher();
        matcher.and(BizSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(BizSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        matcher.and(BizSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
        matcher.and(BizSalesReportEntity.Info.FLAG, ParamMatcher.EQ, flag);
        Ref<Integer> rowCountRef = new Ref<>(0);
        int rt = m_daoCtrl.delete(matcher, rowCountRef);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;unionPriId=%s;pdId=%s;flag=%s;", m_flow, aid, unionPriId, pdId, flag);
            return rt;
        }
        if(rowCountRef.value == 0){ // 说明上报过程中产生了新的类型的数据需要上报
            matcher = new ParamMatcher();
            matcher.and(BizSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
            matcher.and(BizSalesReportEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
            matcher.and(BizSalesReportEntity.Info.PD_ID, ParamMatcher.EQ, pdId);
            ParamUpdater updater = new ParamUpdater(BizSalesReportEntity.Info.FLAG, ParamUpdater.LAND, ~flag);
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
        matcher.and(BizSalesReportEntity.Info.AID, ParamMatcher.EQ, aid);
        matcher.and(BizSalesReportEntity.Info.PD_ID, ParamMatcher.IN, pdIdList);
        int rt = m_daoCtrl.delete(matcher);
        if(rt != Errno.OK){
            Log.logStd(rt, "delete err;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
            return rt;
        }
        Log.logStd("ok;flow=%s;aid=%s;pdIdList;", m_flow, aid, pdIdList);
        return rt;
    }

}
