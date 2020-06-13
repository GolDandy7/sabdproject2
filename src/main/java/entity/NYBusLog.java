package entity;
import org.apache.flink.core.fs.FileSystem;
import utils.DataParser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class NYBusLog implements Comparable {

    private long dateOccuredOn;
    private String boro;
    private long delay;
    private String delay_reason;

    public String getCompanyName() {
        return companyName;
    }

    private String companyName;
    private String time_slot;

    public NYBusLog(){}
    public NYBusLog(long dateOccuredOn, String boro, int delay, String delay_reason,String time_slot) {
        this.dateOccuredOn = dateOccuredOn;
        this.boro = boro;
        this.delay = delay;
        this.delay_reason = delay_reason;
        this.time_slot = time_slot;

    }
    public NYBusLog(long dateOccuredOn, String boro, int delay, String delay_reason,String time_slot,String companyName) {
        this.dateOccuredOn = dateOccuredOn;
        this.boro = boro;
        this.delay = delay;
        this.delay_reason = delay_reason;
        this.time_slot = time_slot;
        this.companyName = companyName;
    }

    public long getDateOccuredOn() {
        return dateOccuredOn;
    }

    public String getBoro() {
        return boro;
    }

    public long getDelay() {
        return delay;
    }
    public String getTime_slot() {
        return time_slot;
    }
    public String getDelay_reason() {
        return delay_reason;
    }
    //5 reason ,7 occured,9 boro ,10 company name 11 delay
    public static NYBusLog fromString(String row) throws ParseException, IOException {
        String[] splitted= row.split(";");

        if(splitted[7].isEmpty()){
            System.err.println("OccuredOn is empty: " + row);
        }
        Date datebus = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US).parse(splitted[7]);
        long date=datebus.getTime();
        //System.out.println(datebus+" timestamp: "+date+" reason: "+splitted[5]+" Slot:"+DataParser.getSlot(datebus));
        int delay= DataParser.getMinFromString(splitted[11]);
       /* BufferedWriter writer = new BufferedWriter(new FileWriter("log.txt",true));
        writer.write(datebus+" timestamp: "+date+" Slot: "+DataParser.getSlot(datebus)+"\n");
        writer.close();*/

        NYBusLog nyBusLog=new NYBusLog(date,splitted[9],delay,splitted[5],DataParser.getSlot(datebus),splitted[10]);
        return nyBusLog;
    }

    @Override
    public int compareTo(Object my_log) {
        Long compareLog = ((NYBusLog)my_log).getDateOccuredOn();
        /* For Ascending order*/
        int n = (int) (compareLog.intValue() - this.dateOccuredOn);
        return n;
    }
}

