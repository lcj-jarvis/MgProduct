package fai.job.MgProductJob.handler;

import fai.comm.job.annotations.JobHandler;
import fai.comm.job.config.FaiJobConfigAdapter;
import fai.comm.job.handler.IJobHandler;
import fai.comm.job.util.LogHelper;
import fai.comm.util.*;
import fai.job.MgProductJob.config.DelSagaConfig;
import fai.mgproduct.comm.entity.SagaEntity;

import java.util.Calendar;

/**
 * 商品中台统一实现方式
 * 一张saga操作记录表
 * 各数据表有10张saga数据表
 */
@JobHandler(func = "mgPdDelSaga")
public class DelSagaHandler implements IJobHandler {
    @Override
    public boolean handle() throws Exception {
        int rt;

        boolean success = true;

        DelSagaConfig.JobOption jobOption = DelSagaConfig.getJobOption();
        int keepMonths = jobOption.getKeepMonths();

        Calendar keepMonAgo = Calendar.getInstance();
        keepMonAgo.add(Calendar.MONTH, -keepMonths);

        // 删除保留期限外的数据
        ParamMatcher sagaMatcher = new ParamMatcher(SagaEntity.Info.SYS_CREATE_TIME, ParamMatcher.LE, keepMonAgo);
        ParamMatcher matcher = new ParamMatcher(SagaEntity.Common.SAGA_TIME, ParamMatcher.LE, keepMonAgo);
        LogHelper.logErr( "start delSaga;matcher=%s;sagaMatcher=%s;", matcher.toJson(), sagaMatcher.toJson());

        FaiList<DelSagaConfig.SagaDBOption> dbList = jobOption.getSagaDBList();
        for(DelSagaConfig.SagaDBOption sagaDBOption : dbList) {
            String dbName = sagaDBOption.getDbName();
            DaoPool daoPool = FaiJobConfigAdapter.getDaoPoolByName(dbName);
            Dao dao = daoPool.getDao();
            if (dao == null) {
                rt = Errno.DAO_CONN_ERROR;
                LogHelper.logErr(rt, "dao conn err; db=%s", dbName);
                return false;
            }
            String sagaTable = sagaDBOption.getSagaTable();

            // 删除saga操作表
            rt = dao.delete(sagaTable, sagaMatcher);
            if(rt != Errno.OK){
                LogHelper.logErr( rt, "delSaga err;dbName=%s;", dbName);
                success = false;
                continue;
            }
            LogHelper.logStd( "delSaga ok;dbName=%s;", dbName);

            // 清除业务数据表对应saga数据
            FaiList<String> tablePrefixs = sagaDBOption.getTablePrefixs();
            for(String tablePre : tablePrefixs) {
                for(int i=0; i<100; i++) {
                    String table = getSagaTable(tablePre, i);
                    rt = dao.delete(table, matcher);
                    if(rt != Errno.OK){
                        LogHelper.logErr( rt, "delSaga err;dbName=%s;table=%s;", dbName, table);
                        success = false;
                        continue;
                    }
                    LogHelper.logStd( "delSaga ok;dbName=%s;table=%s;", dbName, table);
                }
            }
        }
        LogHelper.logErr( "delSaga end;");
        return success;
    }

    public static String getSagaTable(String tablePre, int num) {
        return tablePre + "_" + String.format("%03d", num % 100);
    }
}
