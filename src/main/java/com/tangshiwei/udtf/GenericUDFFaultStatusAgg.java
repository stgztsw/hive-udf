package com.tangshiwei.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GenericUDFFaultStatusAgg extends GenericUDTF{
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFFaultStatusAgg.class);

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        LOG.debug("initializing GenericUDFGetSplits");
        validateInput(arguments);

        List<String> names = Arrays
                .asList("incident_end_time", "status", "simple_status");
        List<ObjectInspector> fieldOIs = Arrays
                .<ObjectInspector> asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        LOG.debug("done initializing GenericUDFFaultTimeSplit");
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);
    }

    protected void validateInput(ObjectInspector[] arguments)
            throws UDFArgumentLengthException, UDFArgumentTypeException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The function fault_time_split accepts 1 arguments.");
        } else if (!(arguments[0] instanceof StringObjectInspector)) {
            LOG.error("Got " + arguments[0].getTypeName() + " instead of string.");
            throw new UDFArgumentTypeException(0, "\""
                    + "string\" is expected at function fault_time_split, " + "but \""
                    + arguments[0].getTypeName() + "\" is found");
        }
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String[] retCols = new String[3];
        String[] nullCols = {null, null, null};

        if (args == null || args.length != 1) {
            forward(nullCols);
            return;
        }

        String input = args[0].toString();
        String[] times = input.split(";")[0].split(",");
        String[] status = input.split(";")[1].split(",");
        String[] finalizeds = input.split(";")[2].split(",");
        String[] ranks = input.split(";")[3].split(",");
        if (times.length != status.length || times.length != finalizeds.length || times.length != ranks.length) {
            LOG.error("size of time, status and ranks are not equal value");
            forward(nullCols);
            return;
        }
        List<FaultStatus> faultStatusList = new ArrayList<>();
        for (int i=0; i<status.length; i++) {
            FaultStatus faultTime = new FaultStatus(times[i], status[i], finalizeds[i], ranks[i]);
            faultStatusList.add(faultTime);
        }
        Collections.sort(faultStatusList);
        List<String> allStatus = new ArrayList<>();
        int faultStatusListLen = faultStatusList.size();
        for(int faultStatusListIndex = 0; faultStatusListIndex < faultStatusListLen; faultStatusListIndex++){
            FaultStatus faultStatusEle = faultStatusList.get(faultStatusListIndex);
            if (faultStatusEle.getFinalized().equals("1")) {
                retCols[0] =faultStatusEle.getEventTime();
            }
            allStatus.add(faultStatusEle.getStatus());
        }
        retCols[1] = String.join(",", allStatus);
        retCols[2] = simplify(allStatus);
        forward(retCols);
    }

    private String simplify(List<String> allStatus) {
        if (allStatus == null || allStatus.isEmpty()) {
            return "";
        }
        List<String> rtv = new ArrayList<>();
        if (allStatus.size() == 1) {
            return allStatus.get(0);
        }
        String [] a = allStatus.toArray(new String[0]);
        int index = 1;
        rtv.add(a[0]);
        while (index < a.length) {
            if (("1".equals(a[index]) || "11".equals(a[index]) || "0".equals(a[index]))
                    && a[index-1].equals(a[index])) {
                index++;
                continue;
            }
            rtv.add(a[index]);
            index++;
        }
        return String.join(",", rtv);
    }

    @Override
    public void close() throws HiveException {

    }

    class FaultStatus implements Comparable<FaultStatus>{
        private String eventTime;
        private String status;
        private String finalized;
        private String rank;

        public FaultStatus(String eventTime, String status, String finalized, String rank) {
            this.eventTime = eventTime;
            this.status = status;
            this.finalized = finalized;
            this.rank = rank;
        }

        public String getEventTime() {
            return eventTime;
        }

        public void setEventTime(String eventTime) {
            this.eventTime = eventTime;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getRank() {
            return rank;
        }

        public void setRank(String rank) {
            this.rank = rank;
        }

        public String getFinalized() {
            return finalized;
        }

        public void setFinalized(String finalized) {
            this.finalized = finalized;
        }

        @Override
        public int compareTo(FaultStatus o) {
            int eventTimeCompare = this.eventTime.compareTo(o.eventTime);
            int rankCompare = this.rank.compareTo(o.rank);
            return eventTimeCompare == 0 ? rankCompare : eventTimeCompare;
        }
    }
//    public static void main(String[] args) throws HiveException {
//        GenericUDFFaultStatusAgg fun = new GenericUDFFaultStatusAgg();
//        String[] s = {"2022-08-31 04:41:56,2022-09-01 04:41:56,2022-09-01 12:52:47,2022-09-01 12:52:47;11,11,11,11;0,0,1,1;1,2,3,4"};
//        fun.process(s);
//    }
}
