package fai.job.MgProductJob.config;

import fai.comm.job.JobApplication;
import fai.comm.job.annotations.FaiListTypeMapping;
import fai.comm.job.annotations.ParamKeyMapping;
import fai.comm.util.FaiList;

public class DelSagaConfig{
    public static final JobOption getJobOption() {
        return s_jobOption;
    }
    @ParamKeyMapping(path = ".mgDelSaga")
    public static class JobOption {
        private int keepMonths = 6; // 数据保留几个月, 默认6个月
        private FaiList<SagaDBOption> sagaDBList = null;

        public int getKeepMonths() {
            return keepMonths;
        }

        public void setKeepMonths(int keepMonths) {
            this.keepMonths = keepMonths;
        }

        public FaiList<SagaDBOption> getSagaDBList() {
            return sagaDBList;
        }

        public void setSagaDBList(@FaiListTypeMapping(type = SagaDBOption.class) FaiList<SagaDBOption> dbList) {
            this.sagaDBList = dbList;
        }
    }

    public static class SagaDBOption {
        private String dbName;
        private String sagaTable;
        private FaiList<String> tablePrefixs;

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getSagaTable() {
            return sagaTable;
        }

        public void setSagaTable(String sagaTable) {
            this.sagaTable = sagaTable;
        }

        public FaiList<String> getTablePrefixs() {
            return tablePrefixs;
        }

        public void setTablePrefixs(@FaiListTypeMapping(type = String.class)FaiList<String> tablePrefixs) {
            this.tablePrefixs = tablePrefixs;
        }
    }

    private static JobOption s_jobOption = JobApplication.getFaiJobConfig().getConfigObject(JobOption.class);
}
