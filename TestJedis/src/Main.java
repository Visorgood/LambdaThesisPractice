import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;

public class Main
{
	private static Jedis jedis = new Jedis("localhost");
	
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
		
		String app_name = "facebook";
		String user_id = "user006";
		long time = new DateTime().getMillis();
		long duration = 35 * 1000;
		
		AppInstall(app_name, user_id, time);
		AppSession(app_name, user_id, time, duration);
	}
	
	static void AppInstall(String app_name, String user_id, long time)
	{
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		if (!jedis.exists(appUserKey + ":Nsh"))
		{
			String hour = Long.toString(dropLessThanHour(time));
			String day = Long.toString(dropLessThanDay(time));
			String week = Long.toString(dropLessThanWeek(time));
			String month = Long.toString(dropLessThanMonth(time));
			jedis.rpush(appUserKey + ":Nsh", hour, "0");
			jedis.rpush(appUserKey + ":Nsd", day, "0");
			jedis.rpush(appUserKey + ":Nsw", week, "0");
			jedis.rpush(appUserKey + ":Nsm", month, "0");
			jedis.rpush(appUserKey + ":Th", hour, "0");
			jedis.rpush(appUserKey + ":Td", day, "0");
			jedis.rpush(appUserKey + ":Tw", week, "0");
			jedis.rpush(appUserKey + ":Tm", month, "0");
			String appCounterKey = String.format("app:%s:users_count", app_name);
			jedis.incr(appCounterKey);
		}
	}
	
	static void AppSession(String app_name, String user_id, long time, long duration)
	{
		String appUserKey = String.format("app:%s:%s", app_name, user_id);
		incrNsh(appUserKey, time);
		incrNsd(appUserKey, time);
		incrNsw(appUserKey, time);
		incrNsm(appUserKey, time);
		incrTh(appUserKey, time, duration);
		incrTd(appUserKey, time, duration);
		incrTw(appUserKey, time, duration);
		incrTm(appUserKey, time, duration);
	}
	
	static void incrNsh(String appUserKey, long time)
	{
		String appUserKeyHour = appUserKey + ":Nsh";
		List<String> counter = jedis.lrange(appUserKeyHour, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsh = Long.parseLong(counter.get(0));
		long diff = time - Nsh;
		if (diff >= HOUR)
		{
			long hours = diff / HOUR;
			jedis.lset(appUserKeyHour, 0, Long.toString(Nsh + hours * HOUR));
			c = 0;
		}
		jedis.lset(appUserKeyHour, 1, Long.toString(++c));
	}
	
	static void incrNsd(String appUserKey, long time)
	{
		String appUserKeyDay = appUserKey + ":Nsd";
		List<String> counter = jedis.lrange(appUserKeyDay, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsd = Long.parseLong(counter.get(0));
		long diff = time - Nsd;
		if (diff >= DAY)
		{
			long days = diff / DAY;
			jedis.lset(appUserKeyDay, 0, Long.toString(Nsd + days * DAY));
			c = 0;
		}
		jedis.lset(appUserKeyDay, 1, Long.toString(++c));
	}
	
	static void incrNsw(String appUserKey, long time)
	{
		String appUserKeyWeek = appUserKey + ":Nsw";
		List<String> counter = jedis.lrange(appUserKeyWeek, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsw = Long.parseLong(counter.get(0));
		long diff = time - Nsw;
		if (diff >= WEEK)
		{
			// !!! here must be changed so that weeks start from monday
			long weeks = diff / WEEK;
			jedis.lset(appUserKeyWeek, 0, Long.toString(Nsw + weeks * WEEK));
			c = 0;
		}
		jedis.lset(appUserKeyWeek, 1, Long.toString(++c));
	}
	
	static void incrNsm(String appUserKey, long time)
	{
		String appUserKeyMonth = appUserKey + ":Nsm";
		List<String> counter = jedis.lrange(appUserKeyMonth, 0, 1);
		long c = Long.parseLong(counter.get(1));
		if (monthDiff(time, Long.parseLong(counter.get(0))) > 0)
		{
			jedis.lset(appUserKeyMonth, 0, Long.toString(dropLessThanMonth(time)));
			c = 0;
		}
		jedis.lset(appUserKeyMonth, 1, Long.toString(++c));
	}
	
	static void incrTh(String appUserKey, long time, long duration)
	{
		String appUserKeyHour = appUserKey + ":Th";
		List<String> counter = jedis.lrange(appUserKeyHour, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Th = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Th;
		if (diff >= HOUR)
		{
			long hours = diff / HOUR;
			Th += hours * HOUR;
			jedis.lset(appUserKeyHour, 0, Long.toString(Th));
			d = 0;
		}
		if (time < Th)
			duration -= Th - time;
		jedis.lset(appUserKeyHour, 1, Long.toString(d + duration));
	}
	
	static void incrTd(String appUserKey, long time, long duration)
	{
		String appUserKeyDay = appUserKey + ":Td";
		List<String> counter = jedis.lrange(appUserKeyDay, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Td = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Td;
		if (diff >= DAY)
		{
			long days = diff / DAY;
			Td += days * DAY;
			jedis.lset(appUserKeyDay, 0, Long.toString(Td));
			d = 0;
		}
		if (time < Td)
			duration -= Td - time;
		jedis.lset(appUserKeyDay, 1, Long.toString(d + duration));
	}
	
	static void incrTw(String appUserKey, long time, long duration)
	{
		String appUserKeyWeek = appUserKey + ":Tw";
		List<String> counter = jedis.lrange(appUserKeyWeek, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Tw = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Tw;
		if (diff >= WEEK)
		{
			long weeks = diff / WEEK;
			Tw += weeks * WEEK;
			jedis.lset(appUserKeyWeek, 0, Long.toString(Tw));
			d = 0;
		}
		if (time < Tw)
			duration -= Tw - time;
		jedis.lset(appUserKeyWeek, 1, Long.toString(d + duration));
	}
	
	static void incrTm(String appUserKey, long time, long duration)
	{
		String appUserKeyMonth = appUserKey + ":Tm";
		List<String> counter = jedis.lrange(appUserKeyMonth, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Tm = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		if (monthDiff(endTime, Tm) > 0)
		{
			jedis.lset(appUserKeyMonth, 0, Long.toString(dropLessThanMonth(endTime)));
			d = 0;
		}
		if (time < Tm)
			duration -= Tm - time;
		jedis.lset(appUserKeyMonth, 1, Long.toString(d + duration));
	}
	
	static long monthDiff(long t1, long t2)
	{
		DateTime dt1 = new DateTime(t1);
		DateTime dt2 = new DateTime(t2);
		return (dt1.getYear() - dt2.getYear()) * 12 + (dt1.getMonthOfYear() - dt2.getMonthOfYear());
	}
	
	static long dropLessThanHour(long time)
	{
		return time - time % HOUR;
	}
	
	static long dropLessThanDay(long time)
	{
		return time - time % DAY;
	}
	
	static long dropLessThanWeek(long time)
	{
		return time - time % WEEK;
	}
	
	static long dropLessThanMonth(long time)
	{
		MutableDateTime mdt = new MutableDateTime(time);
		mdt.setMillisOfSecond(0);
		mdt.setSecondOfMinute(0);
		mdt.setMinuteOfHour(0);
		mdt.setHourOfDay(0);
		mdt.setDayOfMonth(1);
		return mdt.getMillis();
	}
}
