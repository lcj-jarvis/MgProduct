package fai.MgProductStoreSvr.application.task;

import fai.MgProductStoreSvr.application.MgProductStoreSvr;
import fai.MgProductStoreSvr.domain.entity.HoldingRecordEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSalesSkuValObj;
import fai.MgProductStoreSvr.domain.repository.HoldingRecordDaoCtrl;
import fai.MgProductStoreSvr.domain.repository.TableDBMapping;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.comm.middleground.repository.DaoProxy;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

// 补偿
public class HoldingStoreMakeUpTask implements Runnable{
    public HoldingStoreMakeUpTask(DaoProxy daoProxy, MgProductStoreSvr.HoldingStoreMakeUpTaskOption taskOption) {
        this.m_daoProxy = daoProxy;
        this.m_taskOption = taskOption;
    }

    @Override
    public void run() {
        try {
            TimeUnit.MINUTES.sleep(m_taskOption.getStartSleepMinutes());
        } catch (InterruptedException e) {
            Log.logErr(e, "BizSalesReportTask InterruptedException");
        }
        int flow = 508050002;
        boolean isFirst = true;
        while (true) {
            Log.logStd("HoldingStoreMakeUpTask loop");
            if(!isFirst){
                try {
                    TimeUnit.SECONDS.sleep(m_taskOption.getIntervalSeconds());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            isFirst = false;
            Dao dao = null;
            try {
                MgProductStoreCli cli = createCli(flow);
                dao = m_daoProxy.getDaoPool(0, GROUP).getDao();
                Calendar now = Calendar.getInstance();
                Dao.SelectArg selectArg = new Dao.SelectArg();
                selectArg.table = TABLE;
                ParamMatcher matcher = new ParamMatcher(HoldingRecordEntity.Info.ALREADY_DEL, ParamMatcher.EQ, false); // 未删除
                matcher.and(HoldingRecordEntity.Info.EXPIRE_TIME, ParamMatcher.LT, now);
                selectArg.searchArg.matcher = matcher;
                FaiList<Param> list = dao.select(selectArg);
                if(list == null){
                    continue;
                }
                int errCount = 0;
                for (Param info : list) {
                    int aid = info.getInt(HoldingRecordEntity.Info.AID);
                    int unionPriId = info.getInt(HoldingRecordEntity.Info.UNION_PRI_ID);
                    int skuId = info.getInt(HoldingRecordEntity.Info.SKU_ID);
                    int rlOrderId = info.getInt(HoldingRecordEntity.Info.RL_ORDER_ID);
                    int count = info.getInt(HoldingRecordEntity.Info.COUNT);
                    Log.logDbg("whalelog  info=%s", info);
                    int rt = cli.makeUpStore(aid, unionPriId, skuId, rlOrderId, count, StoreSalesSkuValObj.ReduceMode.HOLDING);
                    if(rt != Errno.OK){
                        errCount++;
                    }
                }
                if(errCount != 0){
                    Log.logStd("HoldingStoreMakeUpTask makeUpStore errCount=%s;",errCount);
                }
            }catch (Exception e){
                Log.logErr(e);
            }finally {
                dao.close();
            }
        }
    }

    private MgProductStoreCli createCli(int flow){
        MgProductStoreCli mgProductStoreCli = new MgProductStoreCli(flow);
        if(!mgProductStoreCli.init()){
            return null;
        }
        return mgProductStoreCli;
    }

    private DaoProxy m_daoProxy;
    MgProductStoreSvr.HoldingStoreMakeUpTaskOption m_taskOption;
    private static final String TABLE = TableDBMapping.TableEnum.MG_HOLDING_RECORD.getTable();
    private static final String GROUP = TableDBMapping.TableEnum.MG_HOLDING_RECORD.getTaskGroup();
}
