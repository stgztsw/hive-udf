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

public class GenericUDFFaultTimeSplit extends GenericUDTF {

    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFFaultTimeSplit.class);


    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        LOG.debug("initializing GenericUDFGetSplits");
        validateInput(arguments);

        List<String> names = Arrays
                .asList("start_time", "end_time", "status", "simple_status");
        List<ObjectInspector> fieldOIs = Arrays
                .<ObjectInspector> asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
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
        String[] retCols = new String[4];
        String[] nullCols = {null, null, null, null};

        if (args == null || args.length != 1) {
            forward(nullCols);
            return;
        }
        try {
            String input = args[0].toString();
            String[] times = input.split(";")[0].split(",");
            String[] status = input.split(";")[1].split(",");
            String[] ranks = input.split(";")[2].split(",");
            if (times.length != status.length || times.length != ranks.length) {
                LOG.error("size of time, status and ranks are not equal value");
                forward(nullCols);
                return;
            }
            List<FaultTime> faultTimes = new ArrayList<>();
            for (int i=0; i<status.length; i++) {
                FaultTime faultTime = new FaultTime(times[i], status[i], ranks[i]);
                faultTimes.add(faultTime);
            }
            Collections.sort(faultTimes);
            int index =0;
            List<String> allStatus = new ArrayList<>();
            for (int i=0; i<faultTimes.size(); i++) {
                FaultTime f = faultTimes.get(i);
                if (!"2".equals(f.getStatus()) && !"12".equals(f.getStatus())) {
                    if (index == 0) {
                        retCols[0] = f.getStartTime();
                    }
                    allStatus.add(f.getStatus());
                    if (i == faultTimes.size()-1) {
                        retCols[1] = f.getStartTime();
                        retCols[2] = String.join(",", allStatus);
                        retCols[3] = simplify(allStatus);
                        forward(retCols);
//                        System.out.println(String.join(";", retCols));
                        index = 0;
                        allStatus.clear();
                    }
                    index++;
                } else {
                    if (index == 0) {
                        retCols[0] = f.getStartTime();
                    }
                    retCols[1] = f.getStartTime();
                    allStatus.add(f.getStatus());
                    retCols[2] = String.join(",", allStatus);
                    retCols[3] = simplify(allStatus);
                    forward(retCols);
//                    System.out.println(String.join(";", retCols));
                    index = 0;
                    allStatus.clear();
                }
            }
        } catch (Throwable e) {
            LOG.error("size of time and status are not equal value" + e);
            forward(nullCols);
        }
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

    class FaultTime implements Comparable<FaultTime>{

        private String startTime;

        private String status;

        private String rank;

        public FaultTime(String startTime, String status, String rank) {
            this.startTime = startTime;
            this.status = status;
            this.rank = rank;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
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

        @Override
        public int compareTo(FaultTime o) {
            int startTimeCompare = this.startTime.compareTo(o.startTime);
            int rankCompare = this.rank.compareTo(o.rank);
            return startTimeCompare == 0 ? rankCompare : startTimeCompare;
        }
    }

//    public static void main(String[] args) throws HiveException {
//        GenericUDFFaultTimeSplit fun = new GenericUDFFaultTimeSplit();
//        String[] s = {"2022-08-31 04:41:56,2022-09-01 02:52:47,2022-09-01 05:47:07,2022-09-02 04:21:03,2022-09-02 07:02:57,2022-09-02 03:49:10,2022-08-30 23:26:01,2022-08-30 11:41:54,2022-08-31 04:41:56,2022-08-30 11:02:42,2022-08-27 11:41:55,2022-08-30 17:11:52;12,12,12,12,12,12,12,12,11,12,11,12;2,1,1,1,1,1,1,1,1,1,1,1"};
//        fun.process(s);
//    }
}
