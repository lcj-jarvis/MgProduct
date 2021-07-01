package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;

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
    private FaiList<Long> skuIds;
    private FaiList<ParamUpdater> updaterList;
    private Param combined;
    private ParamUpdater combinedUpdater;
    private Param inOutStoreRecordInfo;
    private FaiList<Param> importProductList;
    private MgProductSearch mgProductSearch;
    private Param addInfo;
    private Param bindRlPdInfo;
    private Param pdRelInfo;
    private FaiList<Param> pdRelInfoList;
    private ParamUpdater updater;
    private boolean softDel;
    private SearchArg searchArg;

    private int rlLibId;
    private FaiList<Integer> rlPropIds;
    private FaiList<Param> propIdsAndValIds;
    private FaiList<Param> addValList;
    private FaiList<ParamUpdater> setValList;
    private FaiList<Integer> delValList;
    private int rlPropId;

    private FaiList<Integer> rlGroupIds;
    private FaiList<Integer> addRlGroupIds;
    private FaiList<Integer> delRlGroupIds;
    private FaiList<String> useOwnerFields;
    private String skuCode;
    private FaiList<String> skuCodes;
    private Param condition;
    private boolean withSpu;
    private long skuId;
    private boolean onlyGetChecked;

    private FaiList<Param> skuStoreSales;

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
        this.combinedUpdater = builder.combinedUpdater;
        this.inOutStoreRecordInfo = builder.inOutStoreRecordInfo;
        this.mgProductSearch = builder.mgProductSearch;
        this.importProductList = builder.importProductList;
        this.addInfo = builder.addInfo;
        this.bindRlPdInfo = builder.bindRlPdInfo;
        this.pdRelInfo = builder.pdRelInfo;
        this.pdRelInfoList = builder.pdRelInfoList;
        this.searchArg = builder.searchArg;
        this.updater = builder.updater;
        this.softDel = builder.softDel;

        this.rlLibId = builder.rlLibId;
        this.rlPropIds = builder.rlPropIds;
        this.propIdsAndValIds = builder.propIdsAndValIds;
        this.addValList = builder.addValList;
        this.setValList = builder.setValList;
        this.delValList = builder.delValList;
        this.rlPropId = builder.rlPropId;

        this.rlGroupIds = builder.rlGroupIds;
        this.addRlGroupIds = builder.addRlGroupIds;
        this.delRlGroupIds = builder.delRlGroupIds;
        this.skuIds = builder.skuIds;
        this.useOwnerFields = builder.useOwnerFields;
        this.skuCode = builder.skuCode;
        this.skuCodes = builder.skuCodes;
        this.condition = builder.condition;
        this.withSpu = builder.withSpu;
        this.skuId = builder.skuId;
        this.onlyGetChecked = builder.onlyGetChecked;

        this.skuStoreSales = builder.skuStoreSales;
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

    public ParamUpdater getCombinedUpdater() {
        return combinedUpdater;
    }

    public Param getInOutStoreRecordInfo() {
        return inOutStoreRecordInfo;
    }

    public int getRlLibId() {
        return rlLibId;
    }

    public FaiList<Integer> getRlPropIds() {
        return rlPropIds;
    }

    public FaiList<Integer> getRlGroupIds() {
        return rlGroupIds;
    }

    public MgProductSearch getMgProductSearch() {
        return mgProductSearch;
    }

    public FaiList<Param> getImportProductList() {
        return importProductList;
    }

    public Param getAddInfo() {
        return addInfo;
    }

    public Param getBindRlPdInfo() {
        return bindRlPdInfo;
    }

    public Param getPdRelInfo() {
        return pdRelInfo;
    }

    public FaiList<Param> getPdRelInfoList() {
        return pdRelInfoList;
    }

    public ParamUpdater getUpdater() {
        return updater;
    }

    public boolean getSoftDel() {
        return softDel;
    }

    public FaiList<Param> getPropIdsAndValIds() {
        return propIdsAndValIds;
    }

    public SearchArg getSearchArg() {
        return searchArg;
    }

    public FaiList<Param> getAddValList() {
        return addValList;
    }

    public int getRlPropId() {
        return rlPropId;
    }

    public FaiList<ParamUpdater> getSetValList() {
        return setValList;
    }

    public FaiList<Integer> getDelValList() {
        return delValList;
    }

    public FaiList<Integer> getAddRlGroupIds() {
        return addRlGroupIds;
    }

    public FaiList<Integer> getDelRlGroupIds() {
        return delRlGroupIds;
    }

    public FaiList<Long> getSkuIds() {
        return skuIds;
    }

    public FaiList<String> getUseOwnerFields() {
        return useOwnerFields;
    }

    public String getSkuCode() {
        return skuCode;
    }

    public FaiList<String> getSkuCodes() {
        return skuCodes;
    }

    public Param getCondition() {
        return condition;
    }

    public boolean getWithSpu() {
        return withSpu;
    }

    public long getSkuId() {
        return skuId;
    }

    public boolean getOnlyGetChecked() {
        return onlyGetChecked;
    }

    public FaiList<Param> getSkuStoreSales() {
        return skuStoreSales;
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
        protected ParamUpdater combinedUpdater;
        protected MgProductSearch mgProductSearch;
        protected FaiList<Param> importProductList;
        protected ParamUpdater updater;
        protected boolean softDel;
        protected SearchArg searchArg;

        public abstract Builder setAddList(FaiList<Param> addList);
        public abstract Builder setPrimaryList(FaiList<Param> primaryKeys);
        public abstract Builder setUpdaterList(FaiList<ParamUpdater> updaterList);
        public abstract Builder setCombined(Param combined);
        public abstract Builder setCombinedUpdater(ParamUpdater combinedUpdater);
        public abstract Builder setMgProductSearch(MgProductSearch mgProductSearch);
        public abstract Builder setImportProductList(FaiList<Param> importProductList);
        public abstract Builder setUpdater(ParamUpdater updater);
        public abstract Builder setSoftDel(boolean softDel);
        public abstract Builder setSearchArg(SearchArg searchArg);
    }

    private static abstract class BasicBuilder extends TopBuilder {
        protected int rlPdId;
        protected FaiList<Integer> rlPdIds;
        protected Param addInfo;
        protected Param bindRlPdInfo;
        protected Param pdRelInfo;
        protected FaiList<Param> pdRelInfoList;

        public abstract Builder setRlPdId(int rlPdId);
        public abstract Builder setRlPdIds(FaiList<Integer> rlPdIds);
        public abstract Builder setAddInfo(Param addInfo);
        public abstract Builder setBindRlPdInfo(Param bindRlPdInfo);
        public abstract Builder setPdRelInfo(Param pdRelInfo);
        public abstract Builder setPdRelInfoList(FaiList<Param> pdRelInfoList);
    }

    private static abstract class GroupBuilder extends BasicBuilder {
        protected FaiList<Integer> rlGroupIds;
        protected FaiList<Integer> addRlGroupIds;
        protected FaiList<Integer> delRlGroupIds;

        public abstract Builder setRlGroupIds(FaiList<Integer> rlGroupIds);
        public abstract Builder setAddRlGroupIds(FaiList<Integer> addRlGroupIds);
        public abstract Builder setDelRlGroupIds(FaiList<Integer> delRlGroupIds);
    }

    private static abstract class PropBuilder extends GroupBuilder {
        protected int rlLibId;
        protected FaiList<Integer> rlPropIds;
        protected FaiList<Param> propIdsAndValIds;

        protected FaiList<Param> addValList;
        protected FaiList<ParamUpdater> setValList;
        protected FaiList<Integer> delValList;
        protected int rlPropId;

        public abstract Builder setRlLibId(int rlLibId);
        public abstract Builder setRlPropIds(FaiList<Integer> rlPropIds);
        public abstract Builder setPropIdsAndValIds(FaiList<Param> propIdsAndValIds);
        public abstract Builder setAddValList(FaiList<Param> addValList);
        public abstract Builder setSetValList(FaiList<ParamUpdater> setValList);
        public abstract Builder setDelValList(FaiList<Integer> delValList);
        public abstract Builder setRlPropId(int rlPropId);
    }

    private static abstract class SpecBuilder extends PropBuilder {
        protected FaiList<Param> skuList;
        protected boolean onlyGetChecked;
        protected boolean withSpu;
        protected String skuCode;
        protected FaiList<String> skuCodes;
        protected FaiList<Long> skuIds;
        protected Param condition;
        protected long skuId;

        public abstract Builder setSkuList(FaiList<Param> skuList);
        public abstract Builder setOnlyGetChecked(boolean onlyGetChecked);
        public abstract Builder setWithSpu(boolean withSpu);
        public abstract Builder setSkuCode(String skuCode);
        public abstract Builder setSkuCodes(FaiList<String> skuCodes);
        public abstract Builder setSkuIds(FaiList<Long> skuIds);
        public abstract Builder setCondition(Param condition);
        public abstract Builder setSkuId(long skuId);
    }

    private static abstract class StoreBuilder extends SpecBuilder {
        protected Calendar optTime;
        protected boolean isBiz;
        protected long costPrice;
        protected Param inOutStoreRecordInfo;
        protected FaiList<String> useOwnerFields;
        protected FaiList<Param> skuStoreSales;

        public abstract Builder setOptTime(Calendar optTime);
        public abstract Builder setIsBiz(boolean isBiz);
        public abstract Builder setCostPrice(long costPrice);
        public abstract Builder setInOutStoreRecordInfo(Param inOutStoreRecordInfo);
        public abstract Builder setUseOwnerFields(FaiList<String> useOwnerFields);
        public abstract Builder setSkuStoreSales(FaiList<Param> skuStoreSales);
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
        public Builder setCombinedUpdater(ParamUpdater combinedUpdater) {
            this.combinedUpdater = combinedUpdater;
            return this;
        }

        @Override
        public Builder setMgProductSearch(MgProductSearch mgProductSearch) {
            this.mgProductSearch = mgProductSearch;
            return this;
        }

        @Override
        public Builder setImportProductList(FaiList<Param> importProductList) {
            this.importProductList = importProductList;
            return this;
        }

        @Override
        public Builder setUpdater(ParamUpdater updater) {
            this.updater = updater;
            return this;
        }

        @Override
        public Builder setSoftDel(boolean softDel) {
            this.softDel = softDel;
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
        public Builder setSkuIds(FaiList<Long> skuIds) {
            this.skuIds = skuIds;
            return this;
        }

        @Override
        public Builder setCondition(Param condition) {
            this.condition = condition;
            return this;
        }

        @Override
        public Builder setSkuId(long skuId) {
            this.skuId = skuId;
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
        public Builder setAddInfo(Param addInfo) {
            this.addInfo = addInfo;
            return this;
        }

        @Override
        public Builder setBindRlPdInfo(Param bindRlPdInfo) {
            this.bindRlPdInfo = bindRlPdInfo;
            return this;
        }

        @Override
        public Builder setPdRelInfo(Param pdRelInfo) {
            this.pdRelInfo = pdRelInfo;
            return this;
        }

        @Override
        public Builder setPdRelInfoList(FaiList<Param> pdRelInfoList) {
            this.pdRelInfoList = pdRelInfoList;
            return this;
        }

        @Override
        public Builder setCostPrice(long costPrice) {
            this.costPrice = costPrice;
            return this;
        }

        @Override
        public Builder setInOutStoreRecordInfo(Param inOutStoreRecordInfo) {
            this.inOutStoreRecordInfo = inOutStoreRecordInfo;
            return this;
        }

        @Override
        public Builder setUseOwnerFields(FaiList<String> useOwnerFields) {
            this.useOwnerFields = useOwnerFields;
            return this;
        }

        @Override
        public Builder setSkuStoreSales(FaiList<Param> skuStoreSales) {
            this.skuStoreSales = skuStoreSales;
            return this;
        }

        @Override
        public Builder setOptTime(Calendar optTime) {
            this.optTime = optTime;
            return this;
        }

        @Override
        public Builder setRlLibId(int rlLibId) {
            this.rlLibId = rlLibId;
            return this;
        }

        @Override
        public Builder setRlPropIds(FaiList<Integer> rlPropIds) {
            this.rlPropIds = rlPropIds;
            return this;
        }

        @Override
        public Builder setPropIdsAndValIds(FaiList<Param> propIdsAndValIds) {
            this.propIdsAndValIds = propIdsAndValIds;
            return this;
        }

        @Override
        public Builder setSearchArg(SearchArg searchArg) {
            this.searchArg = searchArg;
            return this;
        }

        @Override
        public Builder setAddValList(FaiList<Param> addValList) {
            this.addValList = addValList;
            return this;
        }

        @Override
        public Builder setSetValList(FaiList<ParamUpdater> setValList) {
            this.setValList = setValList;
            return this;
        }

        @Override
        public Builder setDelValList(FaiList<Integer> delValList) {
            this.delValList = delValList;
            return this;
        }

        @Override
        public Builder setRlPropId(int rlPropId) {
            this.rlPropId = rlPropId;
            return this;
        }

        @Override
        public Builder setRlGroupIds(FaiList<Integer> rlGroupIds) {
            this.rlGroupIds = rlGroupIds;
            return this;
        }

        @Override
        public Builder setAddRlGroupIds(FaiList<Integer> addRlGroupIds) {
            this.addRlGroupIds = addRlGroupIds;
            return this;
        }

        @Override
        public Builder setDelRlGroupIds(FaiList<Integer> delRlGroupIds) {
            this.delRlGroupIds = delRlGroupIds;
            return this;
        }

        public MgProductArg build() {
            return new MgProductArg(this);
        }
    }
}
