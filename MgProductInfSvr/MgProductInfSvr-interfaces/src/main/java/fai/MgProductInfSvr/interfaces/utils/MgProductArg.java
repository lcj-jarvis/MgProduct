package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.comm.util.SearchArg;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class MgProductArg {
    // 记录设置值的变量，目前用于toString
    private Map<String, Object> usedVar;

    private int aid;
    private int tid;
    private int siteId;
    private int lgId;
    private int keepPriId1;
    private FaiList<Param> primaryKeys;

    private String xid;
    private int fromAid;
    private Param option;
    private Param fromPrimaryKey;
    private int rlBackupId;
    private int rlRestoreId;

    private FaiList<Param> addList;
    private FaiList<Integer> delIdList;
    private boolean isBiz;
    private int rlPdId;
    private FaiList<Integer> rlPdIds;
    private Calendar optTime;
    private long costPrice;
    private FaiList<Param> skuList;
    private FaiList<Param> spuList;
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
    private int rlTpScId;
    private FaiList<Integer> rlTpScIds;
    private FaiList<Integer> tpScDtIds;
    private boolean onlyGetChecked;
    private int sysType;
    private int groupLevel;
    private FaiList<Param> treeDataList;

    private FaiList<Param> skuStoreSales;
    private FaiList<Param> skuIdCounts;
    private String rlOrderCode;
    private int reduceMode;
    private int expireTimeSeconds;
    private String rlRefundId;

    private FaiList<Integer> delRelLibIds;

    private FaiList<Integer> rlTagIds;
    private FaiList<Integer> addRlTagIds;
    private FaiList<Integer> delRlTagIds;


    private MgProductArg(Builder builder) {
        this.usedVar = builder.usedVar;
        this.aid = builder.aid;
        this.tid = builder.tid;
        this.siteId = builder.siteId;
        this.lgId = builder.lgId;
        this.keepPriId1 = builder.keepPriId1;

        this.xid = builder.xid;
        this.fromAid = builder.fromAid;
        this.option = builder.option;
        this.fromPrimaryKey = builder.fromPrimaryKey;
        this.rlBackupId = builder.rlBackupId;
        this.rlRestoreId = builder.rlRestoreId;

        this.primaryKeys = builder.primaryKeys;
        this.updaterList = builder.updaterList;
        this.addList = builder.addList;
        this.delIdList = builder.delIdList;
        this.isBiz = builder.isBiz;
        this.rlPdId = builder.rlPdId;
        this.rlPdIds = builder.rlPdIds;
        this.optTime = builder.optTime;
        this.costPrice = builder.costPrice;
        this.skuList = builder.skuList;
        this.spuList = builder.spuList;
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
        this.rlTpScId = builder.rlTpScId;
        this.rlTpScIds = builder.rlTpScIds;
        this.tpScDtIds = builder.tpScDtIds;
        this.onlyGetChecked = builder.onlyGetChecked;
        this.sysType = builder.sysType;
        this.groupLevel = builder.groupLevel;
        this.treeDataList = builder.treeDataList;

        this.skuStoreSales = builder.skuStoreSales;
        this.skuIdCounts = builder.skuIdCounts;
        this.rlOrderCode = builder.rlOrderCode;
        this.reduceMode = builder.reduceMode;
        this.expireTimeSeconds = builder.expireTimeSeconds;
        this.rlRefundId = builder.rlRefundId;

        this.delRelLibIds = builder.delRelLibIds;

        this.rlTagIds = builder.rlTagIds;
        this.addRlTagIds = builder.addRlTagIds;
        this.delRlTagIds = builder.delRlTagIds;
    }

    public String getXid() {
        return xid;
    }

    public int getFromAid() {
        return fromAid;
    }

    public Param getOption() {
        return option;
    }

    public Param getFromPrimaryKey() {
        return fromPrimaryKey;
    }

    public int getRlBackupId() {
        return rlBackupId;
    }

    public int getRlRestoreId() {
        return rlRestoreId;
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

    public FaiList<Integer> getDelIdList() {
        return delIdList;
    }

    public FaiList<Param> getSkuList() {
        return skuList;
    }

    public FaiList<Param> getSpuList() {
        return spuList;
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

    public int getRlTpScId() {
        return rlTpScId;
    }

    public FaiList<Integer> getRlTpScIds() {
        return rlTpScIds;
    }

    public FaiList<Integer> getTpScDtIds() {
        return tpScDtIds;
    }

    public boolean getOnlyGetChecked() {
        return onlyGetChecked;
    }

    public FaiList<Param> getSkuStoreSales() {
        return skuStoreSales;
    }

    public FaiList<Param> getSkuIdCounts() {
        return skuIdCounts;
    }

    public String getRlOrderCode() {
        return rlOrderCode;
    }

    public int getReduceMode() {
        return reduceMode;
    }

    public int getExpireTimeSeconds() {
        return expireTimeSeconds;
    }

    public String getRlRefundId() {
        return rlRefundId;
    }

    public FaiList<Integer> getDelRelLibIds() {
        return delRelLibIds;
    }

    public FaiList<Integer> getRlTagIds() {
        return rlTagIds;
    }

    public FaiList<Integer> getAddRlTagIds() {
        return addRlTagIds;
    }

    public FaiList<Integer> getDelRlTagIds() {
        return delRlTagIds;
    }

    public int getSysType() {
        return sysType;
    }

    public FaiList<Param> getTreeDataList() {
        return treeDataList;
    }

    public int getGroupLevel() {
        return groupLevel;
    }

    private static abstract class TopBuilder {
        protected int aid;
        protected int tid;
        protected int siteId;
        protected int lgId;
        protected int keepPriId1;

        protected String xid = "";
        protected int fromAid;
        protected Param option;
        protected Param fromPrimaryKey;
        protected int rlBackupId;
        protected int rlRestoreId;

        protected FaiList<Param> addList;
        protected FaiList<Integer> delIdList;
        protected FaiList<Param> primaryKeys;
        protected FaiList<ParamUpdater> updaterList;
        protected Param combined;
        protected ParamUpdater combinedUpdater;
        protected MgProductSearch mgProductSearch;
        protected FaiList<Param> importProductList;
        protected ParamUpdater updater;
        protected boolean softDel;
        protected SearchArg searchArg;
        protected int sysType;

        public abstract Builder setXid(String xid);

        public abstract Builder setFromAid(int fromAid);
        public abstract Builder setOption(Param option);
        public abstract Builder setFromPrimaryKey(Param fromPrimaryKey);
        public abstract Builder setRlBackupId(int rlBackupId);
        public abstract Builder setRlRestoreId(int rlRestoreId);
        public abstract Builder setAddList(FaiList<Param> addList);
        public abstract Builder setDelIdList(FaiList<Integer> delIdList);
        public abstract Builder setPrimaryList(FaiList<Param> primaryKeys);
        public abstract Builder setUpdaterList(FaiList<ParamUpdater> updaterList);
        public abstract Builder setCombined(Param combined);
        public abstract Builder setCombinedUpdater(ParamUpdater combinedUpdater);
        public abstract Builder setMgProductSearch(MgProductSearch mgProductSearch);
        public abstract Builder setImportProductList(FaiList<Param> importProductList);
        public abstract Builder setUpdater(ParamUpdater updater);
        public abstract Builder setSoftDel(boolean softDel);
        public abstract Builder setSearchArg(SearchArg searchArg);
        public abstract Builder setSysType(int sysType);
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
        protected int groupLevel;
        protected FaiList<Param> treeDataList;

        public abstract Builder setRlGroupIds(FaiList<Integer> rlGroupIds);
        public abstract Builder setAddRlGroupIds(FaiList<Integer> addRlGroupIds);
        public abstract Builder setDelRlGroupIds(FaiList<Integer> delRlGroupIds);
        public abstract Builder setGroupLevel(int groupLevel);
        public abstract Builder setTreeDataList(FaiList<Param> treeDataList);
    }

    private static abstract class LibBuilder extends GroupBuilder {
        protected FaiList<Integer> delRelLibIds;

        public abstract Builder setDelRelLibIds(FaiList<Integer> delRelLibIds);
    }

    private static abstract class TagBuilder extends LibBuilder {
        protected FaiList<Integer> rlTagIds;
        protected FaiList<Integer> addRlTagIds;
        protected FaiList<Integer> delRlTagIds;

        public abstract Builder setRlTagIds(FaiList<Integer> rlTagIds);
        public abstract Builder setAddRlTagIds(FaiList<Integer> addRlTagIds);
        public abstract Builder setDelRlTagIds(FaiList<Integer> delRlTagIds);
    }

    private static abstract class PropBuilder extends TagBuilder {
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
        protected FaiList<Param> spuList;
        protected boolean onlyGetChecked;
        protected boolean withSpu;
        protected String skuCode;
        protected FaiList<String> skuCodes;
        protected FaiList<Long> skuIds;
        protected Param condition;
        protected long skuId;
        protected int rlTpScId;
        protected FaiList<Integer> rlTpScIds;
        protected FaiList<Integer> tpScDtIds;

        public abstract Builder setSkuList(FaiList<Param> skuList);
        public abstract Builder setSpuList(FaiList<Param> spuList);
        public abstract Builder setOnlyGetChecked(boolean onlyGetChecked);
        public abstract Builder setWithSpu(boolean withSpu);
        public abstract Builder setSkuCode(String skuCode);
        public abstract Builder setSkuCodes(FaiList<String> skuCodes);
        public abstract Builder setSkuIds(FaiList<Long> skuIds);
        public abstract Builder setCondition(Param condition);
        public abstract Builder setSkuId(long skuId);
        public abstract Builder setRlTpScId(int rlTpScId);
        public abstract Builder setRlTpScIds(FaiList<Integer> rlTpScIds);
        public abstract Builder setTpScDtIds(FaiList<Integer> tpScDtIds);
    }

    private static abstract class StoreBuilder extends SpecBuilder {
        protected Calendar optTime;
        protected boolean isBiz;
        protected long costPrice;
        protected Param inOutStoreRecordInfo;
        protected FaiList<String> useOwnerFields;
        protected FaiList<Param> skuStoreSales;
        protected FaiList<Param> skuIdCounts;
        protected String rlOrderCode;
        protected int reduceMode;
        protected int expireTimeSeconds;
        protected String rlRefundId;

        public abstract Builder setOptTime(Calendar optTime);
        public abstract Builder setIsBiz(boolean isBiz);
        public abstract Builder setCostPrice(long costPrice);
        public abstract Builder setInOutStoreRecordInfo(Param inOutStoreRecordInfo);
        public abstract Builder setUseOwnerFields(FaiList<String> useOwnerFields);
        public abstract Builder setSkuStoreSales(FaiList<Param> skuStoreSales);

        public abstract Builder setSkuIdCounts(FaiList<Param> skuIdCounts);
        public abstract Builder setRlOrderCode(String rlOrderCode);
        public abstract Builder setReduceMode(int reduceMode);
        public abstract Builder setExpireTimeSeconds(int expireTimeSeconds);
        public abstract Builder setRlRefundId(String rlRefundId);
    }

    public static class Builder extends StoreBuilder {
        protected Map<String, Object> usedVar = new HashMap<String, Object>();

        public Builder (int aid, int tid, int siteId, int lgId, int keepPriId1) {
            this.aid = aid;
            this.tid = tid;
            this.siteId = siteId;
            this.lgId = lgId;
            this.keepPriId1 = keepPriId1;
        }

        private void record(String key, Object obj) {
            usedVar.put(key, obj);
        }

        @Override
        public Builder setPrimaryList(FaiList<Param> primaryKeys) {
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                throw new RuntimeException();
            }
            this.primaryKeys = primaryKeys;
            record("primaryKeys", this.primaryKeys);
            return this;
        }

        @Override
        public Builder setUpdaterList(FaiList<ParamUpdater> updaterList) {
            if(updaterList == null) {
                throw new RuntimeException();
            }
            this.updaterList = updaterList;
            record("updaterList", this.updaterList);
            return this;
        }

        @Override
        public Builder setCombined(Param combined) {
            this.combined = combined;
            record("combined", this.combined);
            return this;
        }

        @Override
        public Builder setCombinedUpdater(ParamUpdater combinedUpdater) {
            this.combinedUpdater = combinedUpdater;
            record("combinedUpdater", this.combinedUpdater);
            return this;
        }

        @Override
        public Builder setMgProductSearch(MgProductSearch mgProductSearch) {
            this.mgProductSearch = mgProductSearch;
            record("mgProductSearch", this.mgProductSearch);
            return this;
        }

        @Override
        public Builder setImportProductList(FaiList<Param> importProductList) {
            this.importProductList = importProductList;
            record("importProductList", this.importProductList);
            return this;
        }

        @Override
        public Builder setUpdater(ParamUpdater updater) {
            this.updater = updater;
            record("updater", this.updater);
            return this;
        }

        @Override
        public Builder setSoftDel(boolean softDel) {
            this.softDel = softDel;
            record("softDel", this.softDel);
            return this;
        }

        @Override
        public Builder setSkuList(FaiList<Param> skuList) {
            if(skuList == null || skuList.isEmpty()) {
                throw new RuntimeException();
            }
            this.skuList = skuList;
            record("skuList", this.skuList);
            return this;
        }

        @Override
        public Builder setSpuList(FaiList<Param> spuList) {
            if(spuList == null || spuList.isEmpty()) {
                throw new RuntimeException();
            }
            this.spuList = spuList;
            record("spuList", this.spuList);
            return this;
        }

        @Override
        public Builder setOnlyGetChecked(boolean onlyGetChecked) {
            this.onlyGetChecked = onlyGetChecked;
            record("onlyGetChecked", this.onlyGetChecked);
            return this;
        }

        @Override
        public Builder setWithSpu(boolean withSpu) {
            this.withSpu = withSpu;
            record("withSpu", this.withSpu);
            return this;
        }

        @Override
        public Builder setSkuCode(String skuCode) {
            this.skuCode = skuCode;
            record("skuCode", this.skuCode);
            return this;
        }

        @Override
        public Builder setSkuCodes(FaiList<String> skuCodes) {
            this.skuCodes = skuCodes;
            record("skuCodes", this.skuCodes);
            return this;
        }

        @Override
        public Builder setSkuIds(FaiList<Long> skuIds) {
            this.skuIds = skuIds;
            record("skuIds", this.skuIds);
            return this;
        }

        @Override
        public Builder setCondition(Param condition) {
            this.condition = condition;
            record("condition", this.condition);
            return this;
        }

        @Override
        public Builder setSkuId(long skuId) {
            this.skuId = skuId;
            record("skuId", this.skuId);
            return this;
        }

        @Override
        public Builder setRlTpScId(int rlTpScId) {
            this.rlTpScId = rlTpScId;
            record("rlTpScId", rlTpScId);
            return this;
        }

        @Override
        public Builder setRlTpScIds(FaiList<Integer> rlTpScIds) {
            this.rlTpScIds = rlTpScIds;
            record("rlTpScIds", rlTpScIds);
            return this;
        }

        @Override
        public Builder setTpScDtIds(FaiList<Integer> tpScDtIds) {
            this.tpScDtIds = tpScDtIds;
            record("tpScDtIds", this.tpScDtIds);
            return this;
        }

        @Override
        public Builder setXid(String xid) {
            this.xid = xid;
            record("xid", this.xid);
            return this;
        }

        @Override
        public Builder setFromAid(int fromAid) {
            this.fromAid = fromAid;
            record("fromAid", this.fromAid);
            return this;
        }

        @Override
        public Builder setOption(Param option) {
            this.option = option;
            record("option", this.option);
            return this;
        }

        @Override
        public Builder setFromPrimaryKey(Param fromPrimaryKey) {
            this.fromPrimaryKey = fromPrimaryKey;
            record("fromPrimaryKey", this.fromPrimaryKey);
            return this;
        }

        @Override
        public Builder setRlBackupId(int rlBackupId) {
            this.rlBackupId = rlBackupId;
            record("rlBackupId", this.rlBackupId);
            return this;
        }

        @Override
        public Builder setRlRestoreId(int rlRestoreId) {
            this.rlRestoreId = rlRestoreId;
            record("rlRestoreId", this.rlRestoreId);
            return this;
        }

        @Override
        public Builder setAddList(FaiList<Param> addList) {
            if(addList == null || addList.isEmpty()) {
                throw new RuntimeException();
            }
            this.addList = addList;
            record("addList", this.addList);
            return this;
        }

        @Override
        public Builder setDelIdList(FaiList<Integer> delIdList) {
            if(delIdList == null || delIdList.isEmpty()) {
                throw new RuntimeException();
            }
            this.delIdList = delIdList;
            record("delIdList", this.delIdList);
            return this;
        }

        @Override
        public Builder setIsBiz(boolean isBiz) {
            this.isBiz = isBiz;
            record("isBiz", this.isBiz);
            return this;
        }

        @Override
        public Builder setRlPdId(int rlPdId) {
            this.rlPdId = rlPdId;
            record("rlPdId", this.rlPdId);
            return this;
        }

        @Override
        public Builder setRlPdIds(FaiList<Integer> rlPdIds) {
            this.rlPdIds = rlPdIds;
            record("rlPdIds", this.rlPdIds);
            return this;
        }

        @Override
        public Builder setAddInfo(Param addInfo) {
            this.addInfo = addInfo;
            record("addInfo", this.addInfo);
            return this;
        }

        @Override
        public Builder setBindRlPdInfo(Param bindRlPdInfo) {
            this.bindRlPdInfo = bindRlPdInfo;
            record("bindRlPdInfo", this.bindRlPdInfo);
            return this;
        }

        @Override
        public Builder setPdRelInfo(Param pdRelInfo) {
            this.pdRelInfo = pdRelInfo;
            record("pdRelInfo", this.pdRelInfo);
            return this;
        }

        @Override
        public Builder setPdRelInfoList(FaiList<Param> pdRelInfoList) {
            this.pdRelInfoList = pdRelInfoList;
            record("pdRelInfoList", this.pdRelInfoList);
            return this;
        }

        @Override
        public Builder setCostPrice(long costPrice) {
            this.costPrice = costPrice;
            record("costPrice", this.costPrice);
            return this;
        }

        @Override
        public Builder setInOutStoreRecordInfo(Param inOutStoreRecordInfo) {
            this.inOutStoreRecordInfo = inOutStoreRecordInfo;
            record("inOutStoreRecordInfo", this.inOutStoreRecordInfo);
            return this;
        }

        @Override
        public Builder setUseOwnerFields(FaiList<String> useOwnerFields) {
            this.useOwnerFields = useOwnerFields;
            record("useOwnerFields", this.useOwnerFields);
            return this;
        }

        @Override
        public Builder setSkuStoreSales(FaiList<Param> skuStoreSales) {
            this.skuStoreSales = skuStoreSales;
            record("skuStoreSales", this.skuStoreSales);
            return this;
        }

        @Override
        public Builder setSkuIdCounts(FaiList<Param> skuIdCounts) {
            this.skuIdCounts = skuIdCounts;
            record("skuIdCounts", skuIdCounts);
            return this;
        }

        @Override
        public Builder setRlOrderCode(String rlOrderCode) {
            this.rlOrderCode = rlOrderCode;
            record("rlOrderCode", rlOrderCode);
            return this;
        }

        @Override
        public Builder setReduceMode(int reduceMode) {
            this.reduceMode = reduceMode;
            record("reduceMode", reduceMode);
            return this;
        }

        @Override
        public Builder setExpireTimeSeconds(int expireTimeSeconds) {
            this.expireTimeSeconds = expireTimeSeconds;
            record("expireTimeSeconds", expireTimeSeconds);
            return this;
        }

        @Override
        public Builder setRlRefundId(String rlRefundId) {
            this.rlRefundId = rlRefundId;
            record("rlRefundId", rlRefundId);
            return this;
        }

        @Override
        public Builder setOptTime(Calendar optTime) {
            this.optTime = optTime;
            record("optTime", this.optTime);
            return this;
        }

        @Override
        public Builder setRlLibId(int rlLibId) {
            this.rlLibId = rlLibId;
            record("rlLibId", this.rlLibId);
            return this;
        }

        @Override
        public Builder setRlPropIds(FaiList<Integer> rlPropIds) {
            this.rlPropIds = rlPropIds;
            record("rlPropIds", this.rlPropIds);
            return this;
        }

        @Override
        public Builder setPropIdsAndValIds(FaiList<Param> propIdsAndValIds) {
            this.propIdsAndValIds = propIdsAndValIds;
            record("propIdsAndValIds", this.propIdsAndValIds);
            return this;
        }

        @Override
        public Builder setSearchArg(SearchArg searchArg) {
            this.searchArg = searchArg;
            record("searchArg", this.searchArg);
            return this;
        }

        @Override
        public Builder setSysType(int sysType) {
            this.sysType = sysType;
            record("sysType", this.sysType);
            return this;
        }

        @Override
        public Builder setAddValList(FaiList<Param> addValList) {
            this.addValList = addValList;
            record("addValList", this.addValList);
            return this;
        }

        @Override
        public Builder setSetValList(FaiList<ParamUpdater> setValList) {
            this.setValList = setValList;
            record("setValList", this.setValList);
            return this;
        }

        @Override
        public Builder setDelValList(FaiList<Integer> delValList) {
            this.delValList = delValList;
            record("delValList", this.delValList);
            return this;
        }

        @Override
        public Builder setRlPropId(int rlPropId) {
            this.rlPropId = rlPropId;
            record("rlPropId", this.rlPropId);
            return this;
        }

        @Override
        public Builder setRlGroupIds(FaiList<Integer> rlGroupIds) {
            this.rlGroupIds = rlGroupIds;
            record("rlGroupIds", this.rlGroupIds);
            return this;
        }

        @Override
        public Builder setAddRlGroupIds(FaiList<Integer> addRlGroupIds) {
            this.addRlGroupIds = addRlGroupIds;
            record("addRlGroupIds", this.addRlGroupIds);
            return this;
        }

        @Override
        public Builder setDelRlGroupIds(FaiList<Integer> delRlGroupIds) {
            this.delRlGroupIds = delRlGroupIds;
            record("delRlGroupIds", this.delRlGroupIds);
            return this;
        }

        @Override
        public Builder setGroupLevel(int groupLevel) {
            this.groupLevel = groupLevel;
            record("groupLevel", this.groupLevel);
            return this;
        }

        @Override
        public Builder setTreeDataList(FaiList<Param> treeDataList) {
            this.treeDataList = treeDataList;
            record("treeDataList", this.treeDataList);
            return this;
        }

        @Override
        public Builder setDelRelLibIds(FaiList<Integer> delRelLibIds) {
            this.delRelLibIds = delRelLibIds;
            record("delRelLibIds", this.delRelLibIds);
            return this;
        }

        @Override
        public Builder setRlTagIds(FaiList<Integer> rlTagIds) {
            this.rlTagIds = rlTagIds;
            record("rlTagIds", this.rlTagIds);
            return this;
        }

        @Override
        public Builder setAddRlTagIds(FaiList<Integer> addRlTagIds) {
            this.addRlTagIds = addRlTagIds;
            record("addRlTagIds", this.addRlTagIds);
            return this;
        }

        @Override
        public Builder setDelRlTagIds(FaiList<Integer> delRlTagIds) {
            this.delRlTagIds = delRlTagIds;
            record("delRlTagIds", this.delRlTagIds);
            return this;
        }

        public MgProductArg build() {
            return new MgProductArg(this);
        }
    }

    @Override
    public String toString() {
        return usedVar.toString();
    }
}
