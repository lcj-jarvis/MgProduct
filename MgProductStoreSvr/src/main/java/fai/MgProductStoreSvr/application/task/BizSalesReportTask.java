package fai.MgProductStoreSvr.application.task;


import fai.MgProductStoreSvr.application.MgProductStoreSvr;
import fai.MgProductStoreSvr.domain.entity.BizSalesReportEntity;
import fai.MgProductStoreSvr.domain.repository.DaoProxy;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.comm.util.*;

import java.util.concurrent.TimeUnit;

public class BizSalesReportTask implements Runnable{
    public BizSalesReportTask(DaoProxy daoProxy, MgProductStoreSvr.BizSalesReportTaskOption taskOption) {
        this.m_daoProxy = daoProxy;
        this.m_taskOption = taskOption;
    }

    @Override
    public void run() {
        /*try {
            TimeUnit.MINUTES.sleep(m_taskOption.getStartSleepMinutes());
        } catch (InterruptedException e) {
            Log.logErr(e, "BizSalesReportTask InterruptedException");
        }
        int batchSize = m_taskOption.getBatchSize();
        int flow = 508050001;
        while (true){
            Log.logStd("BizSalesReportTask loop");
            Dao dao = null;
            try {
                MgProductStoreCli cli = createCli(flow);
                dao = m_daoProxy.getTaskDao(flow, 0);

                Dao.SelectArg selectArg = new Dao.SelectArg();
                selectArg.table = TABLE;
                selectArg.searchArg.limit = batchSize;
                FaiList<Param> list = dao.select(selectArg);
                dao.close();
                if(list == null){
                    Oss.logAlarm("BizSalesReportTask dao select err");
                    try {
                        TimeUnit.SECONDS.sleep(m_taskOption.getErrSleepSeconds());
                        continue;
                    } catch (InterruptedException e) {
                        Log.logErr(e, "BizSalesReportTask InterruptedException");
                    }
                }
                boolean last = list.size() != batchSize;
                if(last){
                    boolean markTaskIsEmpty = BizSalesReportCacheCtrl.markTaskIsEmpty();
                    Log.logStd("markTaskIsEmpty=%s", markTaskIsEmpty);
                }
                int errCount = 0;
                for (Param info : list) {
                    int aid = info.getInt(BizSalesReportEntity.Info.AID);
                    int unionPriId = info.getInt(BizSalesReportEntity.Info.UNION_PRI_ID);
                    int pdId = info.getInt(BizSalesReportEntity.Info.PD_ID);
                    int rt = cli.reportBizSales(aid, unionPriId, pdId);
                    if(rt != Errno.OK){
                        errCount++;
                    }
                }
                if(errCount != 0){
                    Log.logStd("BizSalesReportTask reportBizSales errCount=%s;",errCount);
                }
                if(last){
                    boolean isFirst = true;
                    while (true){
                        if(!BizSalesReportCacheCtrl.taskIsEmpty()){
                            break;
                        }
                        if(isFirst){
                            selectArg = new Dao.SelectArg();
                            selectArg.table = TABLE;
                            selectArg.field = BizSalesReportEntity.Info.AID;
                            selectArg.searchArg.limit = 1;
                            dao = m_daoProxy.getTaskDao(flow, 0);
                            Param info = dao.selectFirst(selectArg);
                            dao.close();
                            if(info == null){
                                Oss.logAlarm("BizSalesReportTask dao select err");
                            }else {
                                int aid = info.getInt(BizSalesReportEntity.Info.AID, 0);
                                if(aid != 0){ // 说明查询过程中有新的上报任务
                                    Log.logStd("new task aid=%s", aid);
                                    break;
                                }
                                isFirst = false;
                            }
                        }
                        try {
                            TimeUnit.SECONDS.sleep(m_taskOption.getEmptyTaskSleepSeconds());
                        } catch (InterruptedException e) {
                            Log.logErr(e, "BizSalesReportTask InterruptedException");
                        }
                    }
                }
            }catch (Exception e){
                Log.logErr(e);
            }finally {
                if(dao != null){
                    dao.close();
                }
            }
        }*/
    }

    private MgProductStoreCli createCli(int flow){
        MgProductStoreCli mgProductStoreCli = new MgProductStoreCli(flow);
        if(!mgProductStoreCli.init()){
            return null;
        }
        return mgProductStoreCli;
    }

    private DaoProxy m_daoProxy;
    MgProductStoreSvr.BizSalesReportTaskOption m_taskOption;
    private static final String TABLE = "bizSalesReport";
}
