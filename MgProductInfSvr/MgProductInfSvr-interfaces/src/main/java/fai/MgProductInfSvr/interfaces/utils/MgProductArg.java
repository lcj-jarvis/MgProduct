package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;

import java.util.Calendar;

public class MgProductArg {
    private int aid;
    private int tid;
    private int siteId;
    private int lgId;
    private int keepPriId1;
    private FaiList<Param> primaryKeys;

    private FaiList<Param> addList;
    private boolean isBiz;
    private int rlPdId;
    private FaiList<Integer> rlPdIds;
    private Calendar optTime;
    private long costPrice;
    private FaiList<Param> skuList;
    private FaiList<ParamUpdater> updaterList;
    private Param combined;

    private MgProductArg(Builder builder) {
        this.aid = builder.aid;
        this.tid = builder.tid;
        this.siteId = builder.siteId;
        this.lgId = builder.lgId;
        this.keepPriId1 = builder.keepPriId1;

        this.primaryKeys = builder.primaryKeys;
        this.updaterList = builder.updaterList;
        this.addList = builder.addList;
        this.isBiz = builder.isBiz;
        this.rlPdId = builder.rlPdId;
        this.rlPdIds = builder.rlPdIds;
        this.optTime = builder.optTime;
        this.costPrice = builder.costPrice;
        this.skuList = builder.skuList;
        this.combined = builder.combined;
    }

    public int getAid() {
        return aid;
    }

    public int getTid() {
        return tid;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getLgId() {
        return lgId;
    }

    public int getKeepPriId1() {
        return keepPriId1;
    }

    public FaiList<Param> getPrimaryKeys() {
        return primaryKeys;
    }

    public FaiList<ParamUpdater> getUpdaterList() {
        return updaterList;
    }

    public FaiList<Param> getAddList() {
        return addList;
    }

    public FaiList<Param> getSkuList() {
        return skuList;
    }

    public boolean getIsBiz() {
        return isBiz;
    }

    public int getRlPdId() {
        return rlPdId;
    }

    public FaiList<Integer> getRlPdIds() {
        return rlPdIds;
    }

    public Calendar getOptTime() {
        return optTime;
    }

    public long getCostPrice() {
        return costPrice;
    }

    public Param getCombined() {
        return combined;
    }

    private static abstract class TopBuilder {
        protected int aid;
        protected int tid;
        protected int siteId;
        protected int lgId;
        protected int keepPriId1;

        protected FaiList<Param> addList;
        protected FaiList<Param> primaryKeys;
        protected FaiList<ParamUpdater> updaterList;
        protected Param combined;

        public abstract Builder setAddList(FaiList<Param> addList);
        public abstract Builder setPrimaryList(FaiList<Param> primaryKeys);
        public abstract Builder setUpdaterList(FaiList<ParamUpdater> updaterList);
        public abstract Builder setCombined(Param combined);
    }

    private static abstract class BasicBuilder extends TopBuilder {
        protected int rlPdId;
        protected FaiList<Integer> rlPdIds;

        public abstract Builder setRlPdId(int rlPdId);
        public abstract Builder setRlPdIds(FaiList<Integer> rlPdIds);
    }

    private static abstract class GroupBuilder extends BasicBuilder {
    }

    private static abstract class PropBuilder extends GroupBuilder {
    }

    private static abstract class SpecBuilder extends PropBuilder {
        protected FaiList<Param> skuList;
        protected boolean onlyGetChecked;
        protected boolean withSpu;
        protected String skuCode;
        protected FaiList<String> skuCodes;

        public abstract Builder setSkuList(FaiList<Param> skuList);
        public abstract Builder setOnlyGetChecked(boolean onlyGetChecked);
        public abstract Builder setWithSpu(boolean withSpu);
        public abstract Builder setSkuCode(String skuCode);
        public abstract Builder setSkuCodes(FaiList<String> skuCodes);
    }

    private static abstract class StoreBuilder extends SpecBuilder {
        protected Calendar optTime;
        protected boolean isBiz;
        protected long costPrice;

        public abstract Builder setOptTime(Calendar optTime);
        public abstract Builder setIsBiz(boolean isBiz);
        public abstract Builder setCostPrice(long costPrice);
    }

    public static class Builder extends StoreBuilder {
        public Builder (int aid, int tid, int siteId, int lgId, int keepPriId1) {
            this.aid = aid;
            this.tid = tid;
            this.siteId = siteId;
            this.lgId = lgId;
            this.keepPriId1 = keepPriId1;
        }

        @Override
        public Builder setPrimaryList(FaiList<Param> primaryKeys) {
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                throw new RuntimeException();
            }
            this.primaryKeys = primaryKeys;
            return this;
        }

        @Override
        public Builder setUpdaterList(FaiList<ParamUpdater> updaterList) {
            if(updaterList == null || updaterList.isEmpty()) {
                throw new RuntimeException();
            }
            this.updaterList = updaterList;
            return this;
        }

        @Override
        public Builder setCombined(Param combined) {
            this.combined = combined;
            return this;
        }

        @Override
        public Builder setSkuList(FaiList<Param> skuList) {
            if(skuList == null || skuList.isEmpty()) {
                throw new RuntimeException();
            }
            this.skuList = skuList;
            return this;
        }

        @Override
        public Builder setOnlyGetChecked(boolean onlyGetChecked) {
            this.onlyGetChecked = onlyGetChecked;
            return this;
        }

        @Override
        public Builder setWithSpu(boolean withSpu) {
            this.withSpu = withSpu;
            return this;
        }

        @Override
        public Builder setSkuCode(String skuCode) {
            this.skuCode = skuCode;
            return this;
        }

        @Override
        public Builder setSkuCodes(FaiList<String> skuCodes) {
            this.skuCodes = skuCodes;
            return this;
        }

        @Override
        public Builder setAddList(FaiList<Param> addList) {
            if(addList == null || addList.isEmpty()) {
                throw new RuntimeException();
            }
            this.addList = addList;
            return this;
        }

        @Override
        public Builder setIsBiz(boolean isBiz) {
            this.isBiz = isBiz;
            return this;
        }

        @Override
        public Builder setRlPdId(int rlPdId) {
            this.rlPdId = rlPdId;
            return this;
        }

        @Override
        public Builder setRlPdIds(FaiList<Integer> rlPdIds) {
            this.rlPdIds = rlPdIds;
            return this;
        }

        @Override
        public Builder setCostPrice(long costPrice) {
            this.costPrice = costPrice;
            return this;
        }

        @Override
        public Builder setOptTime(Calendar optTime) {
            this.optTime = optTime;
            return this;
        }

        public MgProductArg build() {
            return new MgProductArg(this);
        }
    }
}
