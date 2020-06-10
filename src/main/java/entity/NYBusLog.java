package entity;
import utils.DataParser;

public class NYBusLog  {

    private long dateOccuredOn;
    private String boro;
    private int delay;
    private String delay_reason;



    private String time_slot;

    public NYBusLog(){}
    public NYBusLog(long dateOccuredOn, String boro, int delay, String delay_reason,String time_slot) {
        this.dateOccuredOn = dateOccuredOn;
        this.boro = boro;
        this.delay = delay;
        this.delay_reason = delay_reason;
        this.time_slot = time_slot;
    }

    public long getDateOccuredOn() {
        return dateOccuredOn;
    }

    public String getBoro() {
        return boro;
    }

    public int getDelay() {
        return delay;
    }
    public String getTime_slot() {
        return time_slot;
    }
    public String getDelay_reason() {
        return delay_reason;
    }
    //6 reason ,8 occured,10 boro 11 company name 12 delay
    public static NYBusLog fromString(String row){
        String[] splitted= row.split(";");

        if(splitted[8].isEmpty()){
            System.err.println("OccuredOn is empty: " + row);
        }
        long date=Long.parseLong(splitted[8]);
        int delay= DataParser.getMinFromString(splitted[12]);

        NYBusLog nyBusLog=new NYBusLog(date,splitted[10],delay,splitted[6],DataParser.getSlot(splitted[8]));
        return nyBusLog;
    }
}

