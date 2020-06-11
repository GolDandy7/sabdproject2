package utils;

import java.sql.SQLOutput;
import java.util.Date;

public class DataParser {
    //HP: ritardo compreso tra 5 minuti e 5 ore
    public static int getMinFromString (String delay){
        int min = 0;
        // se campo nullo o inizia con un un carattere-> delay=0
        delay=delay.trim(); //elimino spazio prima e dopo
        if(delay.isEmpty() || !Character.isDigit(delay.charAt(0)))
            return 0;
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
        return min;
    }

    public static String getSlot(Date date){
        int h = (int)(date.getTime() % 86400000) / 3600000;
        if(h>=5 && h<12)
            return "AM";
        else if(h>=12 && h<19)
            return "PM";

        return "null";
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
