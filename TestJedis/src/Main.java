import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;

public class Main
{
	private static long HOUR = Duration.standardHours(1).getMillis();
	private static long DAY = Duration.standardDays(1).getMillis();
	private static long WEEK = Duration.standardDays(7).getMillis();
	
	public static void main(String[] args)
	{
		// app:$app_name:$user_id:$counter store list of two values: time and int
		// $counter can be: Nsh, Nsd, Nsw, Nsm, Th, Td, Tw, Tm
		// Nsh - number of session in the last hour
		// Th - total duration of usage of this app during the last hour
		// app:$app_name:users_count stores integer counter of the number of unique users of this app
		
		
		Jedis jedis = new Jedis("localhost");

		String app_name = "facebook";
		String user_id = "user005";
		long time = 23235;
		long duration = 60;
		
		AppInstall(jedis, app_name, user_id, time);
		AppSession(jedis, app_name, user_id, time, duration);
	}
	
	static void AppInstall(Jedis jedis, String app_name, String user_id, long time)
	{
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		if (!jedis.exists(appUserKey + ":Nsh"))
		{
			String dateTime = Long.toString(time);
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
	
	static void AppSession(Jedis jedis, String app_name, String user_id, long time, long duration)
	{
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		
		String appUserKeyHour = appUserKey + ":Nsh";
		long diff = time - Long.parseLong(jedis.lrange(appUserKeyHour, 0, 1).get(0));
		if (diff >= HOUR)
		{
			long hours = diff / HOUR;
			jedis.lset(appUserKeyHour, 0, Long.toString(time + hours * HOUR));
		}
		jedis.incr(appUserKeyHour);
		
		String appUserKeyDay = appUserKey + ":Nsd";
		diff = time - Long.parseLong(jedis.lrange(appUserKeyDay, 0, 1).get(0));
		if (diff >= DAY)
		{
			long days = diff / DAY;
			jedis.lset(appUserKeyDay, 0, Long.toString(time + days * DAY));
		}
		jedis.incr(appUserKeyDay);
		
		String appUserKeyWeek = appUserKey + ":Nsw";
		diff = time - Long.parseLong(jedis.lrange(appUserKeyWeek, 0, 1).get(0));
		if (diff >= WEEK)
		{
			long weeks = diff / WEEK;
			jedis.lset(appUserKeyWeek, 0, Long.toString(time + weeks * WEEK));
		}
		jedis.incr(appUserKeyWeek);
		
		String appUserKeyMonth = appUserKey + ":Nsm";
		if (monthDiff(time, Long.parseLong(jedis.lrange(appUserKeyMonth, 0, 1).get(0))) > 0)
			jedis.lset(appUserKeyMonth, 0, Long.toString(removeTillMonth(time)));
		jedis.incr(appUserKeyMonth);
		
		jedis.incrBy(appUserKey + ":Th", duration);
		jedis.incrBy(appUserKey + ":Td", duration);
		jedis.incrBy(appUserKey + ":Tw", duration);
		jedis.incrBy(appUserKey + ":Tm", duration);
	}
	
	static long monthDiff(long t1, long t2)
	{
		DateTime dt1 = new DateTime(t1);
		DateTime dt2 = new DateTime(t2);
		return (dt1.getYear() - dt2.getYear()) * 12 + (dt1.getMonthOfYear() - dt2.getMonthOfYear());
	}
	
	static long removeTillMonth(long time)
	{
		MutableDateTime mdt = new MutableDateTime(time);
		mdt.setMillisOfSecond(0);
		mdt.setSecondOfMinute(0);
		mdt.setMinuteOfHour(0);
		mdt.setHourOfDay(0);
		mdt.setDayOfMonth(0);
		return mdt.getMillis();
	}
}
