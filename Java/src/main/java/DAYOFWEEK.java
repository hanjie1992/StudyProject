import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author: hanj
 * @date: 2019/10/9
 * @description: 判断周几，以及一段时间内的天数
 */
public class DAYOFWEEK {
    public static void main(String[] args) throws ParseException {
        String str = "2019-10-12";
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        calendar.setTime(sdf.parse(str));
        int i =calendar.get(Calendar.DAY_OF_WEEK);//周日 1，周六7，周一 2，周二 3，周三 4，周四 5，周五 6
        if(i == 1){
            System.out.println("今天是星期日");
        }else{
            System.out.println("今天是星期"+(i-1));
        }

        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss E");
        System.out.println(sdf1.format(calendar.getTime()));

        SimpleDateFormat sdf2 = new SimpleDateFormat("E");
        System.out.println(sdf2.format(calendar.getTime()));


        //一段时间内的天数
        String begintTime = "2019-10-01";
        String endTime =  "2019-10-10";
        for(String days: findDaysStr(begintTime,endTime)){
            System.out.println(days);
        }
    }
    public static List<String> findDaysStr(String begintTime, String endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dBegin = null;
        Date dEnd = null;
        try {
            dBegin = sdf.parse(begintTime);
            dEnd = sdf.parse(endTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        List<String> daysStrList = new ArrayList<String>();
        daysStrList.add(sdf.format(dBegin));
        Calendar calBegin = Calendar.getInstance();
        calBegin.setTime(dBegin);
        Calendar calEnd = Calendar.getInstance();
        calEnd.setTime(dEnd);
        while (dEnd.after(calBegin.getTime())) {
            calBegin.add(Calendar.DAY_OF_MONTH, 1);
            String dayStr = sdf.format(calBegin.getTime());
            daysStrList.add(dayStr);
        }
        return daysStrList;
    }
}
