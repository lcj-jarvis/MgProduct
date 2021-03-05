package fai.MgProductInfSvr.domain.serviceproc;

import fai.comm.util.Log;
import fai.comm.util.SearchArg;

public abstract class AbstractProductProc {
    protected void logErrWithPrintInvoked(int rt, String fmt, Object... args){
        Log.logErr(rt, fmt+String.format(";invoked::%s", getInvoked()), args);
    }

    private String getInvoked(){
        Throwable throwable = new Throwable();
        StackTraceElement[] elemList = throwable.getStackTrace();
        StackTraceElement stackTraceElement = elemList[3];
        String className = stackTraceElement.getClassName();
        int lastIndexOf = className.lastIndexOf('.');
        String simpleClassName = className;
        if(lastIndexOf >= 0){
            simpleClassName = className.substring(lastIndexOf+1);
        }
        String methodName = stackTraceElement.getMethodName();
        int lineNumber = stackTraceElement.getLineNumber();
        return simpleClassName+"::"+methodName+":"+lineNumber;
    }

    protected String getSearchArgInfo(SearchArg searchArg){
        if(searchArg == null){
            return null;
        }
        String info = "";
        if(searchArg.matcher != null){
            info+=";matcher="+searchArg.matcher.toJson();
        }
        if(searchArg.cmpor != null){
            info += ";cmpor="+searchArg.cmpor.toJson();
        }
        info+=";start="+searchArg.start;
        info+=";limit="+searchArg.limit;
        return info;
    }
}
