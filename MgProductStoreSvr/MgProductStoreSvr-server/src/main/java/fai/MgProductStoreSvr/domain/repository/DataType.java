package fai.MgProductStoreSvr.domain.repository;

public enum DataType{
    Visitor("V")
    , Manage("M")
    ;
    String label;

    DataType(String label) {
        this.label = label;
    }
    public String getLabel(){
        return label;
    }
}