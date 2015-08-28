package org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util;

/**
 * Created by miyurud on 4/9/15.
 */
public class Constants {
    public static int EVENT_BUFFER_SIZE = 100000;
    public static final int MAIN_THREAD_SLEEP_TIME = 10*1000;
    public static final int INPUT_INJECTION_TIMESTAMP_FIELD = 0;
    public static final String CONFIG_FILENAME = "linearroad.properties";
    public static final String  LINEAR_CAR_DATA_POINTS = "org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.datasets.cardatapoints";
    public static final String  LINEAR_HISTORY = "org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.datasets.history";

    public static final int POSITION_REPORT = 0;
    public static final int ACCOUNT_BALANCE_REPORT = 1;
    public static final int DAILY_EXPENDITURE_REPORT = 2;
    public static final int TRAVEL_TIME_REPORT = 3;
}