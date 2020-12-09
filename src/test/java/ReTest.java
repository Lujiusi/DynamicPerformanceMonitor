

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 16:19
 * @desc
 */
public class ReTest {

    public static void main(String[] args) {
        String message = "perf.server.out_call.out_app.timer.[santy.ppdapi.com]-fat,endpoint=\\ ,host=10.114.4.138,out_app=10.114.28.150,out_call_type=http count=4,max=1.794228,mean=1.2252852303450146,median=1.013009,min=0.932689,p75=1.794228,p95=1.794228,p99=1.794228,rt=4.901140921380058,stddev=0.3709576371193016 1605758760000000000";
        System.out.println(message.substring(0, message.lastIndexOf(' ')));

        String s = "0123456";

        long a = 1606986360000L;

        Date date = new Date(a);

        String format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

        System.out.println(format);
//        System.out.println();

//        System.out.println(Arrays.asList(s.split("\\s+(?!,)")));


    }
}