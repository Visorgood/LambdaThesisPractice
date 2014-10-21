import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;

public class AppsAggregations
{
	private static Jedis jedis = new Jedis("localhost");

	private static long HOUR = Duration.standardHours(1).getMillis();
	private static long DAY = Duration.standardDays(1).getMillis();
	private static long WEEK = Duration.standardDays(7).getMillis();
	
	static void AppInstall(String appName, String userId, long time)
	{
		String appUserKey = String.format("app:%s:%s", appName, userId);
		if (!jedis.exists(appUserKey + ":numberOfSessionsHourly"))
		{
			String hour = Long.toString(dropLessThanHour(time));
			String day = Long.toString(dropLessThanDay(time));
			String week = Long.toString(dropLessThanWeek(time));
			String month = Long.toString(dropLessThanMonth(time));
			jedis.rpush(appUserKey + ":numberOfSessionsHourly", hour, "0");
			jedis.rpush(appUserKey + ":numberOfSessionsDaily", day, "0");
			jedis.rpush(appUserKey + ":numberOfSessionsWeekly", week, "0");
			jedis.rpush(appUserKey + ":numberOfSessionsMonthly", month, "0");
			jedis.rpush(appUserKey + ":totalDurationHourly", hour, "0");
			jedis.rpush(appUserKey + ":totalDurationDaily", day, "0");
			jedis.rpush(appUserKey + ":totalDurationWeekly", week, "0");
			jedis.rpush(appUserKey + ":totalDurationMonthly", month, "0");
			String appCounterKey = String.format("app:%s:usersCount", appName);
			jedis.incr(appCounterKey);
		}
	}
	
	static void AppSession(String appName, String userId, long time, long duration)
	{
		String appUserKey = String.format("app:%s:%s", appName, userId);
		incrementCounterHour(appUserKey + ":numberOfSessionsHourly", time);
		incrementCounterDay(appUserKey + ":numberOfSessionsDaily", time);
		incrementCounterWeek(appUserKey + ":numberOfSessionsWeekly", time);
		incrementCounterMonth(appUserKey + ":numberOfSessionsMonthly", time);
		incrementDurationHour(appUserKey + ":totalDurationHourly", time, duration);
		incrementDurationDay(appUserKey + "totalDurationDaily", time, duration);
		incrementDurationWeek(appUserKey + ":totalDurationWeekly", time, duration);
		incrementDurationMonth(appUserKey + ":totalDurationMonthly", time, duration);
	}
	
	static void incrementCounterHour(String key, long time)
	{
		//String appUserKeyHour = appUserKey + ":Nsh";
		List<String> counter = jedis.lrange(key, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsh = Long.parseLong(counter.get(0));
		long diff = time - Nsh;
		if (diff >= HOUR)
		{
			long hours = diff / HOUR;
			jedis.lset(key, 0, Long.toString(Nsh + hours * HOUR));
			c = 0;
		}
		jedis.lset(key, 1, Long.toString(++c));
	}
	
	static void incrementCounterDay(String key, long time)
	{
		//String appUserKeyDay = appUserKey + ":Nsd";
		List<String> counter = jedis.lrange(key, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsd = Long.parseLong(counter.get(0));
		long diff = time - Nsd;
		if (diff >= DAY)
		{
			long days = diff / DAY;
			jedis.lset(key, 0, Long.toString(Nsd + days * DAY));
			c = 0;
		}
		jedis.lset(key, 1, Long.toString(++c));
	}
	
	static void incrementCounterWeek(String key, long time)
	{
		//String appUserKeyWeek = appUserKey + ":Nsw";
		List<String> counter = jedis.lrange(key, 0, 1);
		long c = Long.parseLong(counter.get(1));
		long Nsw = Long.parseLong(counter.get(0));
		long diff = time - Nsw;
		if (diff >= WEEK)
		{
			// !!! here must be changed so that weeks start from monday
			long weeks = diff / WEEK;
			jedis.lset(key, 0, Long.toString(Nsw + weeks * WEEK));
			c = 0;
		}
		jedis.lset(key, 1, Long.toString(++c));
	}
	
	static void incrementCounterMonth(String key, long time)
	{
		//String appUserKeyMonth = appUserKey + ":Nsm";
		List<String> counter = jedis.lrange(key, 0, 1);
		long c = Long.parseLong(counter.get(1));
		if (monthDiff(time, Long.parseLong(counter.get(0))) > 0)
		{
			jedis.lset(key, 0, Long.toString(dropLessThanMonth(time)));
			c = 0;
		}
		jedis.lset(key, 1, Long.toString(++c));
	}
	
	static void incrementDurationHour(String key, long time, long duration)
	{
		//String appUserKeyHour = appUserKey + ":Th";
		List<String> counter = jedis.lrange(key, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Th = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Th;
		if (diff >= HOUR)
		{
			long hours = diff / HOUR;
			Th += hours * HOUR;
			jedis.lset(key, 0, Long.toString(Th));
			d = 0;
		}
		if (time < Th)
			duration -= Th - time;
		jedis.lset(key, 1, Long.toString(d + duration));
	}
	
	static void incrementDurationDay(String key, long time, long duration)
	{
		//String appUserKeyDay = appUserKey + ":Td";
		List<String> counter = jedis.lrange(key, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Td = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Td;
		if (diff >= DAY)
		{
			long days = diff / DAY;
			Td += days * DAY;
			jedis.lset(key, 0, Long.toString(Td));
			d = 0;
		}
		if (time < Td)
			duration -= Td - time;
		jedis.lset(key, 1, Long.toString(d + duration));
	}
	
	static void incrementDurationWeek(String key, long time, long duration)
	{
		//String appUserKeyWeek = appUserKey + ":Tw";
		List<String> counter = jedis.lrange(key, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Tw = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		long diff = endTime - Tw;
		if (diff >= WEEK)
		{
			long weeks = diff / WEEK;
			Tw += weeks * WEEK;
			jedis.lset(key, 0, Long.toString(Tw));
			d = 0;
		}
		if (time < Tw)
			duration -= Tw - time;
		jedis.lset(key, 1, Long.toString(d + duration));
	}
	
	static void incrementDurationMonth(String key, long time, long duration)
	{
		//String appUserKeyMonth = appUserKey + ":Tm";
		List<String> counter = jedis.lrange(key, 0, 1);
		long d = Long.parseLong(counter.get(1));
		long Tm = Long.parseLong(counter.get(0));
		long endTime = time + duration;
		if (monthDiff(endTime, Tm) > 0)
		{
			jedis.lset(key, 0, Long.toString(dropLessThanMonth(endTime)));
			d = 0;
		}
		if (time < Tm)
			duration -= Tm - time;
		jedis.lset(key, 1, Long.toString(d + duration));
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
