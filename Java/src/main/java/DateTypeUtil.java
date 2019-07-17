import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author: hanj
 * @date: 2019/7/17
 * @description: 传入日期给出日期类型
 */
public class DateTypeUtil {
    public static void main(String[] args) {
        int date = isHoliday("20191001");
        System.out.println(date);
    }
    /**
     * @description 传入日期给出日期类型
     * @param date 传入的日期，例如20190426
     * @return int 正常工作日对应结果为 0, 法定节假日对应结果为 1, 节假日调休补班对应的结果为 2，休息日对应结果为 3
     */
    public static int isHoliday(String date){
        int dataType = -1;
        try {
            String url="http://api.goseek.cn/Tools/holiday?date="+date;
            String result = sendGet(url);
            JSONObject jsonObject = JSONArray.parseObject(result);
            dataType = (int) jsonObject.get("data");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataType;
    }

    /**
     * @description
     * @param url 查询日期类型的外部url
     * @return java.lang.String
     */
    private static String sendGet(String url) {
        PrintWriter out = null;
        BufferedReader in=null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            HttpURLConnection coon = (HttpURLConnection) realUrl.openConnection();
            coon.setRequestMethod("GET");
            coon.setRequestProperty("Content-Type","text/html");
            coon.setRequestProperty("accept","*/*");
            coon.setRequestProperty("user-agent","Mozilla/4.0(compatible;MSIE 6.0; Windows NT 5.1;SV1)");
            coon.setRequestProperty("Connection","Keep-Alive");
            coon.setRequestProperty("Charset","UTF-8");
            coon.connect();
            in=new BufferedReader(new InputStreamReader(coon.getInputStream(),"utf-8"));
            String line;
            while ((line=in.readLine())!=null){
                result += line;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (out != null){
                    out.close();
                }
                if (in != null){
                    in.close();
                }
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        return  result;
    }
}
