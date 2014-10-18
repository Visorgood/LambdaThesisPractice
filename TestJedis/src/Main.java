import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class Main
{
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	public static void main(String[] args) throws ParseException
	{
		// app:$app_name:$user_id:$counter store list of two values: time and int
		// $counter can be: Nsh, Nsd, Nsw, Nsm, Th, Td, Tw, Tm
		// Nsh - number of session in the last hour
		// Th - total duration of usage of this app during the last hour
		// app:$app_name:users_count stores int counter of the number of unique users of this app
		
		
		Jedis jedis = new Jedis("localhost");

		String app_name = "facebook";
		String user_id = "user005";
		long time = 23235;
		long duration = 60;
		
		AppInstall(jedis, app_name, user_id);
		AppSession(jedis, app_name, user_id, time, duration);
	}
	
	static void AppInstall(Jedis jedis, String app_name, String user_id)
	{
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		if (!jedis.exists(appUserKey + ":Nsh"))
		{
			String dateTime = dateFormat.format(Calendar.getInstance().getTime());
			jedis.rpush(appUserKey + ":Nsh", dateTime, "0");
			jedis.rpush(appUserKey + ":Nsd", dateTime, "0");
			jedis.rpush(appUserKey + ":Nsw", dateTime, "0");
			jedis.rpush(appUserKey + ":Nsm", dateTime, "0");
			jedis.rpush(appUserKey + ":Th", dateTime, "0");
			jedis.rpush(appUserKey + ":Td", dateTime, "0");
			jedis.rpush(appUserKey + ":Tw", dateTime, "0");
			jedis.rpush(appUserKey + ":Tm", dateTime, "0");
			String appCounterKey = String.format("app:%s:users_count", app_name);
			jedis.incr(appCounterKey);
		}
	}
	
	static void AppSession(Jedis jedis, String app_name, String user_id, long time, long duration) throws ParseException
	{
		Date now = Calendar.getInstance().getTime();
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		List<String> counter = jedis.lrange(appUserKey + ":Nsh", 0, 1);
		if (dateFormat.parse(counter.get(0)) - now > 3)
		jedis.incr(appUserKey + ":Nsh");
		jedis.incr(appUserKey + ":Nsd");
		jedis.incr(appUserKey + ":Nsw");
		jedis.incr(appUserKey + ":Nsm");
		jedis.incrBy(appUserKey + ":Th", duration);
		jedis.incrBy(appUserKey + ":Td", duration);
		jedis.incrBy(appUserKey + ":Tw", duration);
		jedis.incrBy(appUserKey + ":Tm", duration);
	}
}
