/**
 * 
 */
package org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.input;

import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Miyuru Dayarathna
 *
 */
public class DataLoderThreadSiddhi extends Thread {
	private String filePath;
	private BufferedReader br;
	private int count;
	private String schemaPath = "";
	private InputHandler inputHandlerPositionReportsStream;
	private InputHandler inputHandlerAccountBalanceReportRequestStream;
	private InputHandler inputHandlerDailyExpenseReportsStream;
	volatile long events = 0;
	private long startTime;
	private String carDataFile;

	public DataLoderThreadSiddhi(InputHandler inputHandlerPositionReportsStream, InputHandler inputHandlerAccountBalanceReportRequestStream, InputHandler inputHandlerDailyExpenseReportsStream){
		super("Data Loader");
		this.inputHandlerPositionReportsStream = inputHandlerPositionReportsStream;
		this.inputHandlerAccountBalanceReportRequestStream = inputHandlerAccountBalanceReportRequestStream;
		this.inputHandlerDailyExpenseReportsStream = inputHandlerDailyExpenseReportsStream;

		Properties properties = new Properties();
		InputStream propertiesIS = DataLoderThreadSiddhi.class.getClassLoader().getResourceAsStream(org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util.Constants.CONFIG_FILENAME);

		if (propertiesIS == null)
		{
			throw new RuntimeException("Properties file '" + org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util.Constants.CONFIG_FILENAME + "' not found in classpath");
		}

		try{
			properties.load(propertiesIS);
		}catch(IOException ec){
			ec.printStackTrace();
		}

		this.carDataFile = properties.getProperty(org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util.Constants.LINEAR_CAR_DATA_POINTS);
	}

	public void run() {
		Object[] event = null;
		int counter = 0;
		try {
			BufferedReader in = new BufferedReader(new FileReader(carDataFile));
			String line;

			/*
				 The input file has the following format

				    Cardata points input tuple format
					0 - type of the packet (0=Position Report, 1=Account Balance, 2=Expenditure, 3=Travel Time [According to Richard's thesis. See Below...])
					1 - Seconds since start of simulation (i.e., time is measured in seconds)
					2 - Car ID (0..999,999)
					3 - An integer number of miles per hour (i.e., speed) (0..100)
					4 - Expressway number (0..9)
					5 - The lane number (There are 8 lanes altogether)(0=Ramp, 1=Left, 2=Middle, 3=Right)(4=Left, 5=Middle, 6=Right, 7=Ramp)
					6 - Direction (west = 0; East = 1)
					7 - Mile (This corresponds to the seg field in the original table) (0..99)
					8 - Distance from the last mile post (0..1759) (Arasu et al. Pos(0...527999) identifies the horizontal position of the vehicle as a measure of number of feet
					    from the western most position on the expressway)
					9 - Query ID (0..999,999)
					10 - Starting milepost (m_init) (0..99)
					11 - Ending milepost (m_end) (0..99)
					12 - day of the week (dow) (0=Sunday, 1=Monday,...,6=Saturday) (in Arasu et al. 1...7. Probably this is wrong because 0 is available as DOW)
					13 - time of the day (tod) (0:00..23:59) (in Arasu et al. 0...1440)
					14 - day

					Notes
					* While Richard thesis explain the input tuple formats, it is probably not correct.
					* The correct type number would be
					* 0=Position Report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
					* 2=Account Balance Queries (because all and only all the 4 fields available on tuples with type 2) (Type=2, Time, VID, QID)
					* 3=Daily expenditure (Type=3, Time, VID, XWay, QID, Day). Here if day=1 it is yesterday. d=69 is 10 weeks ago.
					* 4=Travel Time requests (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)

					history data input tuple format
					0 - Car ID
					1 - day
					2 - x - Expressway number
					3 - daily expenditure

					E.g.
					#(1 3 0 31)
					#(1 4 0 61)
					#(1 5 0 34)
					#(1 6 0 30)
					#(1 7 0 63)
					#(1 8 0 55)

					//Note that since the historical data table's key is made out of several fields it was decided to use a relation table
					//instead of using a hashtable
			*/


			startTime = System.currentTimeMillis();
//			while (true) {
//				if(!dataFileReader.hasNext()){
//					break;
//				}
//
//				email = dataFileReader.next();
//
//				events++;
//
//				//We are interested of only on the following set of fields.getBody();.
//				//fromAddresses, toAddresses, ccAddress, bccAddresses, subject, body
//				event = new Object[8];//We need one additional field for timestamp.
//				long cTime = System.currentTimeMillis();
//				event[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
//
//
//				try {
//					this.inputHandler.send(event);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}

			while((line = in.readLine()) != null){
//				counter++;
//				System.out.println("counter:" + counter);
				events++;
				Long iij_TimeStamp = 0l;
				Long time = -1l;
				Integer vid = -1;
				Integer qid = -1;
				Integer spd = -1;
				Integer xway = -1;
				Integer mile = -1;
				Integer ofst = -1;
				Integer lane = -1;
				Integer dir = -1;
				Integer day = -1;

				line = line.substring(2, line.length() - 1);

				String[] fields = line.split(" ");
				byte typeField = Byte.parseByte(fields[0]);

				//In the case of Storm it seems that we need not to send the type of the tuple through the network. It is because
				//the event itself has some associated type. This seems to be an advantage of Storm compared to other message passing based solutions.
				switch(typeField){
					case 0:
						//Need to calculate the offset value
						String offset = "" + ((short)(Integer.parseInt(fields[8]) - (Integer.parseInt(fields[7]) * 5280)));
						//This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
						iij_TimeStamp = System.currentTimeMillis();
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						spd = Integer.parseInt(fields[3]);
						xway = Integer.parseInt(fields[4]);
						lane = Integer.parseInt(fields[5]);
						dir = Integer.parseInt(fields[6]);
						mile = Integer.parseInt(fields[7]);
						ofst = Integer.parseInt(offset);

						event = new Object[9];

						event[0] = iij_TimeStamp;
						event[1] = time;
						event[2] = vid;
						event[3] = spd;
						event[4] = xway;
						event[5] = lane;
						event[6] = dir;
						event[7] = mile;
						event[8] = ofst;

						try {
							inputHandlerPositionReportsStream.send(event);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						break;
					case 2:
						//This is an Account Balance report (Type=2, Time, VID, QID)
						iij_TimeStamp = System.currentTimeMillis();
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						qid = Integer.parseInt(fields[9]);
						iij_TimeStamp = System.currentTimeMillis();

						//This time the size of the events array is small.
						event = new Object[4];

						event[0] = iij_TimeStamp;
						event[1] = time;
						event[2] = vid;
						event[3] = qid;

						try {
							inputHandlerAccountBalanceReportRequestStream.send(event);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						break;
					case 3 :
						//This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)
						iij_TimeStamp = System.currentTimeMillis();
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						xway = Integer.parseInt(fields[4]);
						qid = Integer.parseInt(fields[9]);
						day = Integer.parseInt(fields[14]);

						event = new Object[6];
						event[0] = iij_TimeStamp;
						event[1] = time;
						event[2] = vid;
						event[3] = xway;
						event[4] = qid;
						event[5] = day;

						try {
							inputHandlerDailyExpenseReportsStream.send(event);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						break;
					case 4:
						//This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
						break;
					case 5:
						System.out.println("Travel time query was issued : " + line);
						break;
				}
			}
			System.out.println("Done emitting the input tuples...");


			long currentTime = System.currentTimeMillis();
			System.out.println("****** Input ******");
			System.out.println("events read : " + events);
			System.out.println("time to read (ms) : " + (currentTime - startTime));
			System.out.println("read throughput (events/s) : " + (events * 1000 / (currentTime - startTime)));
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public Object[] split(CharSequence item){
		int itemLen = item.length();
		int counter = 0;
		Object[] result = null;

		for(int i = 0; i < itemLen; i++){
			if(item.charAt(i) == ','){
				counter++;
			}
		}

		result = new Object[counter];
		counter = 0;
		int counter2 = 0;

		for(int i = 0; i < itemLen; i++) {
			if (item.charAt(i) == ',') {
				result[counter2] = item.subSequence(counter, i);
				counter = i;
				counter2++;
			}
		}

		return result;
	}
}