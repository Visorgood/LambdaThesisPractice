import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisProxy
{
	private static long HOUR = Duration.standardHours(1).getMillis();
	private static long DAY = Duration.standardDays(1).getMillis();
	private static long WEEK = Duration.standardDays(7).getMillis();
	
	private final Jedis jedis;
	private Pipeline pipeline;
	
	public RedisProxy(String host)
	{
		jedis = new Jedis(host);
		pipeline = null;
	}
	
	public void incrementCounters(String key, long time)
	{
		pipeline = jedis.pipelined();
		pipeline.multi();
		if (!pipeline.exists(key + ":count_hourly").get())
		{
			pipeline.rpush(key + ":count_hourly", Long.toString(dropLessThanHour(time)), "0");
			pipeline.rpush(key + ":count_daily", Long.toString(dropLessThanDay(time)), "0");
			pipeline.rpush(key + ":count_weekly", Long.toString(dropLessThanWeek(time)), "0");
			pipeline.rpush(key + ":count_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyCounter(key, time, 1);
		incrementDailyCounter(key, time, 1);
		incrementWeeklyCounter(key, time, 1);
		incrementMonthlyCounter(key, time, 1);
		pipeline.exec();
	}

	public void incrementLengths(String key, long time, int length)
	{
		pipeline = jedis.pipelined();
		pipeline.multi();
		if (!pipeline.exists(key + ":length_hourly").get())
		{
			pipeline.rpush(key + ":length_hourly", Long.toString(dropLessThanHour(time)), "0");
			pipeline.rpush(key + ":length_daily", Long.toString(dropLessThanDay(time)), "0");
			pipeline.rpush(key + ":length_weekly", Long.toString(dropLessThanWeek(time)), "0");
			pipeline.rpush(key + ":length_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyCounter(key, time, length);
		incrementDailyCounter(key, time, length);
		incrementWeeklyCounter(key, time, length);
		incrementMonthlyCounter(key, time, length);
		pipeline.exec();
	}
	
	public void incrementDurations(String key, long time, long duration)
	{
		pipeline = jedis.pipelined();
		pipeline.multi();
		if (!pipeline.exists(key + ":duration_hourly").get())
		{
			pipeline.rpush(key + ":duration_hourly", Long.toString(dropLessThanHour(time)), "0");
			pipeline.rpush(key + ":duration_daily", Long.toString(dropLessThanDay(time)), "0");
			pipeline.rpush(key + ":duration_weekly", Long.toString(dropLessThanWeek(time)), "0");
			pipeline.rpush(key + ":duration_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyDuration(key, time, duration);
		incrementDailyDuration(key, time, duration);
		incrementWeeklyDuration(key, time, duration);
		incrementMonthlyDuration(key, time, duration);
		pipeline.exec();
	}
	
	public void addUserToApp(String appNameUserIdKey, String appNameKey)
	{
		//String key = String.format("app:%s:%s:%s", appName, userId, "count_hourly");
		//String usersCountKey = String.format("app:%s:%s", appName, "users_count");
		if (!pipeline.exists(appNameUserIdKey + ":count_hourly").get())
			pipeline.incr(appNameKey + ":users_count");
	}
	
	private void incrementHourlyCounter(String key, long time, long value)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingHourTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingHourTime;
		if (difference >= HOUR)
		{
			long numberOfHours = difference / HOUR;
			pipeline.lset(key, 0, Long.toString(startingHourTime + numberOfHours * HOUR));
			counterValue = 0;
		}
		counterValue += value;
		pipeline.lset(key, 1, Long.toString(counterValue));
	}
	
	private void incrementDailyCounter(String key, long time, long value)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingDayTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingDayTime;
		if (difference >= DAY)
		{
			long numberOfDays = difference / DAY;
			pipeline.lset(key, 0, Long.toString(startingDayTime + numberOfDays * DAY));
			counterValue = 0;
		}
		counterValue += value;
		pipeline.lset(key, 1, Long.toString(counterValue));
	}
	
	private void incrementWeeklyCounter(String key, long time, long value)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingWeekTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingWeekTime;
		if (difference >= WEEK)
		{
			// !!! here must be changed so that weeks start from monday
			long numberOfWeeks = difference / WEEK;
			pipeline.lset(key, 0, Long.toString(startingWeekTime + numberOfWeeks * WEEK));
			counterValue = 0;
		}
		counterValue += value;
		pipeline.lset(key, 1, Long.toString(counterValue));
	}
	
	private void incrementMonthlyCounter(String key, long time, long value)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingMonthTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		if (monthDifference(time, startingMonthTime) > 0)
		{
			pipeline.lset(key, 0, Long.toString(dropLessThanMonth(time)));
			counterValue = 0;
		}
		counterValue += value;
		pipeline.lset(key, 1, Long.toString(counterValue));
	}
	
	private void incrementHourlyDuration(String key, long time, long duration)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingHourTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + durationValue;
		long difference = endTime - startingHourTime;
		if (difference >= HOUR)
		{
			long numberOfHours = difference / HOUR;
			startingHourTime += numberOfHours * HOUR;
			pipeline.lset(key, 0, Long.toString(startingHourTime));
			durationValue = 0;
		}
		if (time < startingHourTime)
			duration -= startingHourTime - time;
		pipeline.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private void incrementDailyDuration(String key, long time, long duration)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingDayTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		long difference = endTime - startingDayTime;
		if (difference >= DAY)
		{
			long numberOfDays = difference / DAY;
			startingDayTime += numberOfDays * DAY;
			pipeline.lset(key, 0, Long.toString(startingDayTime));
			durationValue = 0;
		}
		if (time < startingDayTime)
			duration -= startingDayTime - time;
		pipeline.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private void incrementWeeklyDuration(String key, long time, long duration)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingWeekTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		long difference = endTime - startingWeekTime;
		if (difference >= WEEK)
		{
			long numberOfWeeks = difference / WEEK;
			startingWeekTime += numberOfWeeks * WEEK;
			pipeline.lset(key, 0, Long.toString(startingWeekTime));
			durationValue = 0;
		}
		if (time < startingWeekTime)
			duration -= startingWeekTime - time;
		pipeline.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private void incrementMonthlyDuration(String key, long time, long duration)
	{
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		long startingMonthTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		if (monthDifference(endTime, startingMonthTime) > 0)
		{
			pipeline.lset(key, 0, Long.toString(dropLessThanMonth(endTime)));
			durationValue = 0;
		}
		if (time < startingMonthTime)
			duration -= startingMonthTime - time;
		pipeline.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private static long monthDifference(long t1, long t2)
	{
		DateTime dt1 = new DateTime(t1);
		DateTime dt2 = new DateTime(t2);
		return (dt1.getYear() - dt2.getYear()) * 12 + (dt1.getMonthOfYear() - dt2.getMonthOfYear());
	}
	
	private static long dropLessThanHour(long time)
	{
		return time - time % HOUR;
	}
	
	private static long dropLessThanDay(long time)
	{
		return time - time % DAY;
	}
	
	private static long dropLessThanWeek(long time)
	{
		return time - time % WEEK;
	}
	
	private static long dropLessThanMonth(long time)
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
