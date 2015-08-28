package org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad;

import org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.input.DataLoderThreadSiddhi;
import org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.input.history.BasicDataSource;
import org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.performance.PerfStats;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import javax.sql.DataSource;

/**
 * Created by miyurud on 4/17/15.
 */
public class LinearRoadSiddhi {
    private static PerfStats perfStats1 = new PerfStats();
    private static long lastEventTime1 = -1;
    private static long startTime;
    private static String baseToll = "2";
    private static final  String TOLL_NOTIFICATION = "0";
    private static final  String ACCIDENT_NOTIFICATION = "1";
    private static final  String ACC_BALANCE_RESPONSE = "2";
    private static final  String DAILY_EXPENSE_RESPONSE = "3";

    DataSource dataSource = new BasicDataSource();

    public static void main(String[] args){
        LinearRoadSiddhi queryObj = new LinearRoadSiddhi();
        queryObj.run();
    }

    public void run(){

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource("cepDataSource", dataSource);

        siddhiManager.setExtension("math:abs", org.wso2.siddhi.extension.math.AbsFunctionExtension.class);
        String positionReportsStream = "";
        positionReportsStream = "define stream dailyExpenseReportRequestsStream ( iij_timestamp LONG, timeOfRecord LONG, vid INT, xway INT, qid INT, day INT);";
        positionReportsStream += "define stream accountBalanceReportRequestsStream ( iij_timestamp LONG, timeOfRecord LONG, vid INT, qid INT);";
        positionReportsStream += "define stream positionReportsStream ( iij_timestamp LONG, timeOfRecord LONG, vid INT, speed INT, xway INT, lane INT, dir INT, mile INT, ofst INT);";

        //The accident detection is responsible for maintaining the Stopped cars, Smashed cars, and Accident tables.
        positionReportsStream += "define table StoppedCarsTable ( vid INT, xway INT, lane INT, dir INT, mile INT, ofst INT );";
        positionReportsStream += "define table SmashedCars ( vid INT );"; //The smashed cars table is used to prevent multiple accident alerts from the same accident.
        positionReportsStream += "define table Accidents ( vid INT );";
        positionReportsStream += "define table CarTolls (mile INT, lav DOUBLE, toll DOUBLE);";
        positionReportsStream += "define table NOVTable (mile INT, nov LONG);";//NOVTable
        positionReportsStream += "define table LAVTable (mile INT, lav DOUBLE);";//LAVTable
        positionReportsStream += "define table VehicleInfo (vid INT, toll DOUBLE);";//LAVTable

        positionReportsStream += "define stream zeroSpeedStream ( vid INT, timeOfRecord LONG, xway INT, lane INT, dir INT, mile INT, ofst INT );";
        positionReportsStream += "define stream accidentAlertsStream(type INT, vid INT, xway INT, mile INT, dir INT);";
        positionReportsStream += "define stream tollNotifcationStreamNOV ( vid INT, timeOfRecord LONG, mile INT, xway INT, nov LONG, lane INT );";
        positionReportsStream += "define stream tollNotifcationStreamLAV ( vid INT, timeOfRecord LONG, mile INT, xway INT, lav DOUBLE, lane INT );";
        positionReportsStream += "define stream tollNotifcationStream ( type INT, vid INT, toll DOUBLE, lav DOUBLE );";
        positionReportsStream += "define stream accountBalanceResponseStream ( type INT, qid INT, balance INT );";
        positionReportsStream += "define stream dailyExpenseResponseStream( type INT, qid INT, dailyexp INT );";
        positionReportsStream += "@from(eventtable = 'rdbms' , datasource.name = 'cepDataSource' , table.name = 'history') define table history (carid INT, day INT, xway INT, dailyexp INT);";

        //Are there at least two cars at this position and are they stopped in 4 reports

                //Accident detection
                //First we select all the cars that are stopped
        String  queryAccDetect  = "from positionReportsStream [speed == 0] select vid, timeOfRecord, xway, lane, dir, mile, ofst  insert into zeroSpeedStream;";
                //We need to select only the position reports which are not present in the Stopped cars table.
                queryAccDetect += "from positionReportsStream[not(vid == StoppedCarsTable.vid) in StoppedCarsTable] select vid, xway, lane, dir, mile, ofst insert into intermStream1;";
                //Next, we add the cars which were not present in the Stopped cars table.
                queryAccDetect += "from intermStream1 select vid, xway, lane, dir, mile, ofst insert into StoppedCarsTable;";
                //Also we select the cars which were already in the table.
                queryAccDetect += "from positionReportsStream[(vid == StoppedCarsTable.vid) in StoppedCarsTable] select vid, xway, lane, dir, mile, ofst insert into intermStream2;";
                //then we update the records already there in the Stopped cars table.
                queryAccDetect += "from intermStream2 update StoppedCarsTable on vid == StoppedCarsTable.vid;";
                //We take a time window of 120 seconds and then count the number of occurrence of a vehicle's ID within that 120 seconds time frame.
                queryAccDetect += "from zeroSpeedStream#window.externalTime(timeOfRecord, 120 sec) select vid, count(vid) as vcnt, xway, lane, dir, mile, ofst group by vid insert into smashedStream;";
                //If we can find more than four occurrences of the same vehicle ID across 120 second time frame, with another vehicle stopped at the same location, we determine this as an accident scenario.
                queryAccDetect += "from every (e1=smashedStream[vcnt >= 4] -> e2=smashedStream[xway == e1.xway and vcnt >= 4 and mile == e1.mile and ofst == e1.ofst and lane == e1.lane and dir == e1.dir]) select e1.vid as vid1, e2.vid as vid2, e1.mile,e1.xway, e1.dir insert into intermStream3;";
                //Once we detect an accident we need to notify all the vehicles 5 miles upstream that there is a downstream accident so that it can check an alternative route.
                //the notification should only be sent to the vehicles on the same express way.
                queryAccDetect += "from intermStream3#window.length(1) join StoppedCarsTable [math:abs(mile - StoppedCarsTable.mile) < 5 and intermStream2.xway == xway] select 1 as type, StoppedCarsTable.vid, StoppedCarsTable.xway, StoppedCarsTable.mile, StoppedCarsTable.dir insert into accidentAlertsStream;";

                //Toll calculation
                //If a vehicle enters a new segment, a toll for that segment is calculated and the vehicle is notified of that toll. We use an event pattern detection to find out whether a vehicle is entering
                //a new segment or not.
        String  querySegmentStat = "from every (e1=positionReportsStream -> e2=positionReportsStream[e1.mile != e2.mile]) select e1.vid, e1.timeOfRecord * 1000 as timeOfRecord, e1.mile, e2.mile as mile2, e1.xway, e1.dir, e1.lane, e2.speed as speed insert into intermStream4;";

                //querySegmentStat += "from tollNotifcationStream select vid, timeOfRecord, mile, xway, 10 as toll insert into CarTolls;";
                //We need to compare the new segment number with the one we already stored in the table, and then we send CarTolls.toll value which was originally calculated when the vehicle entered the segment
                //across the output stream.
                //querySegmentStat += "from intermStream4#window.length(1) join CarTolls [vid == CarTolls.vid and mile != CarTolls.mile] select CarTolls.vid, CarTolls.mile, CarTolls.xway, CarTolls.toll insert into intermStream5;";

        String  queryNOV = "from intermStream4#window.externalTime(timeOfRecord, 60000) select mile, count(vid) as nov group by mile insert into NOVTable;";

        String  queryLAV = "from positionReportsStream#window.externalTime(timeOfRecord, 300000) select mile, avg(speed) as lav group by mile insert into LAVTable;";

                queryLAV += "from intermStream4#window.length(1) join NOVTable on mile == NOVTable.mile select intermStream4.vid, intermStream4.timeOfRecord, NOVTable.mile, intermStream4.xway, NOVTable.nov, intermStream4.lane insert into tollNotifcationStreamNOV;";

                queryLAV += "from intermStream4#window.length(1) join LAVTable on mile == LAVTable.mile select intermStream4.vid, intermStream4.timeOfRecord, LAVTable.mile, intermStream4.xway, LAVTable.lav, intermStream4.lane insert into tollNotifcationStreamLAV;";

                querySegmentStat += "from tollNotifcationStreamNOV#window.length(1) join tollNotifcationStreamLAV on (tollNotifcationStreamNOV.mile == tollNotifcationStreamLAV.mile and tollNotifcationStreamNOV.vid == tollNotifcationStreamLAV.vid) select tollNotifcationStreamNOV.vid, tollNotifcationStreamNOV.timeOfRecord, tollNotifcationStreamNOV.mile, tollNotifcationStreamNOV.xway, tollNotifcationStreamLAV.lav, tollNotifcationStreamNOV.nov, tollNotifcationStreamNOV.lane  insert into tollNotifcationStreamNOVLAV;";

                querySegmentStat += "from tollNotifcationStreamNOVLAV [(lav > 40 or nov < 50) and lane == 0] select mile, lav, 0d as toll insert into CarTolls;";

                querySegmentStat += "from tollNotifcationStreamNOVLAV [(lav < 40 or nov > 50) and lane != 0] select mile, lav, " + baseToll + "d*(nov-50)*(nov-50) as toll insert into CarTolls;";

                //Next, we need to calculate the toll for that segment and the vehicle needs to be notified about the toll.
                querySegmentStat += "from intermStream4#window.length(1) join CarTolls on (intermStream4.mile == CarTolls.mile) select 0 as type, intermStream4.vid, CarTolls.lav, CarTolls.toll insert into tollNotifcationStream2;";
                //This is to filter the duplicate events.
                querySegmentStat += "from every (e1=tollNotifcationStream2 -> e2=tollNotifcationStream2[e1.vid != e2.vid or e1.toll != e2.toll]) select 0 as type, e2.vid, e2.toll, e2.lav insert into tollNotifcationStream;";

                //VehicleInfo
                querySegmentStat += "from tollNotifcationStream select vid, toll insert into VehicleInfo;";

                //History queries
                //Account balance query
                //At the start of the simulation, every vehicle’s account balance is zero, and thereafter the account balance at time t is the sum of all tolls assessed as of t.
                String  queryAccBalance = "from accountBalanceReportRequestsStream#window.length(1) join VehicleInfo on (vid == VehicleInfo.vid) select 2 as type, accountBalanceReportRequestsStream.qid, convert(sum(VehicleInfo.toll), 'INT') as balance group by VehicleInfo.vid insert into accountBalanceResponseStream;";

                //Daily expenditure query
                String dailyExpenseQuery = "from dailyExpenseReportRequestsStream#window.length(1) join history on ((dailyExpenseReportRequestsStream.vid == history.carid)and(dailyExpenseReportRequestsStream.day == history.day)) select 3 as type, dailyExpenseReportRequestsStream.qid as qid, sum(history.dailyexp) as dailyexp group by dailyExpenseReportRequestsStream.day insert into dailyExpenseResponseStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(positionReportsStream+queryAccDetect+querySegmentStat+queryNOV+queryLAV+queryAccBalance+dailyExpenseQuery);

        InputHandler inputHandlerPositionReportsStream = executionPlanRuntime.getInputHandler("positionReportsStream");
        InputHandler inputHandlerAccountBalanceReportRequestStream = executionPlanRuntime.getInputHandler("accountBalanceReportRequestsStream");
        InputHandler inputHandlerDailyExpenseReportsStream = executionPlanRuntime.getInputHandler("dailyExpenseReportRequestsStream");

        //Once an accident is detected an accident notification is sent to every car that comes in proximity of the accident (five segments upstream from the accident.).
        executionPlanRuntime.addCallback("accidentAlertsStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();

                    System.out.println(dt[0] + "," + dt[1] + "," + dt[2] + "," + dt[3] + "," + dt[4]);
                }
            }
        });

        //Everytime when a vehicle enters a new segment a toll notification is sent to the driver.
        executionPlanRuntime.addCallback("tollNotifcationStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();

                    System.out.println(dt[0] + "," + dt[1] + "," + dt[2] + "," + dt[3]);
                }
            }
        });

        executionPlanRuntime.addCallback("accountBalanceResponseStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();

                    System.out.println(dt[0] + "," + dt[1] + "," + dt[2]);
                }
            }
        });

//        executionPlanRuntime.addCallback("dailyExpenseResponseStream", new StreamCallback() {
//
//            @Override
//            public void receive(org.wso2.siddhi.core.event.Event[] events) {
//                for (org.wso2.siddhi.core.event.Event evt : events) {
//                    Object[] dt = evt.getData();
//
//                    System.out.println(dt[0] + "," + dt[1] + "," + dt[2]);
//                }
//            }
//        });

        executionPlanRuntime.start();

        startTime = System.currentTimeMillis();
        DataLoderThreadSiddhi dataLoderThreadSiddhi = new DataLoderThreadSiddhi(inputHandlerPositionReportsStream, inputHandlerAccountBalanceReportRequestStream, inputHandlerDailyExpenseReportsStream);
        dataLoderThreadSiddhi.start();

        while (true) {
            try {
                if (lastEventTime1 == perfStats1.lastEventTime ) {
                    System.out.println();
                    System.out.println("***** Query 1 *****");
                    long timeDifferenceFromStart = perfStats1.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats1.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStart);
                    System.out.println("over all throughput (events/s) :" + ((perfStats1.count * 1000) / timeDifferenceFromStart));
                    System.out.println("over all avg latency (ms) :" + (perfStats1.totalLatency / perfStats1.count));
                    System.out.println();
                    break;
                } else {
                    lastEventTime1 = perfStats1.lastEventTime;
                    Thread.sleep(10*1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
