package utils;

import java.sql.SQLOutput;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

public class DataParser {
    private static final Integer MAX_SIZE=720; //12h
    //HP: ritardo compreso tra 5 minuti e 5 ore
    public static int getMinFromString (String delay){
        int min = 0;
        // se campo nullo o inizia con un un carattere-> delay=0
        delay=delay.trim(); //elimino spazio prima e dopo
        if(delay.isEmpty() || !Character.isDigit(delay.charAt(0)))
            return -1;
        char delay_chars[]=delay.toCharArray();
        boolean isNumber[] = new boolean[delay.length()];
         for(int i=0;i<delay_chars.length;i++){
             isNumber[i]=Character.isDigit(delay_chars[i]);
         }
        int start=0,end=-1,i=0;
        do{
            if(isNumber[i]==true){
                end=i;
            }
            i++;
        }while(i< isNumber.length && isNumber[i]==true);
        String result= delay.substring(start,end+1);
        min=Integer.parseInt(result);
        //hp: qualsiasi interno sotto i 5 minuti sono ore
        if(min<5)
            return 60*min;
        if(min>MAX_SIZE)
            return -1;
        return min;
    }

    public static String getSlot(Date date){
        Calendar calendar= Calendar.getInstance(Locale.US);
        Calendar current= Calendar.getInstance(Locale.US);
        calendar.setTime(date);
        current.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY,5);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        if(current.before(calendar)){
            return "null";
        }
        calendar.set(Calendar.HOUR_OF_DAY,19);
        if(current.after(calendar)){
            return "null";
        }
        calendar.set(Calendar.HOUR_OF_DAY,12);
        if(current.before(calendar))
            return "AM";
        else
            return "PM";

    }

    //testing del parser
    public static void main(String[] args) {

        String min="60min";
        String min1=" ";
        String min2="?";
        String min3="2h";
        String min4="55 mins";
        String min5="";
        String min6="45";
        String min7="2 hours 5 mins";
        String min8="15-30 min";
        String min9="45min-1h";
        int a=getMinFromString(min9);
        System.out.println("min:"+a);


    }
}
