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
		checkCountersExist(key, CounterType.Count, time);
		incrementCounter(DurationType.Hour, CounterType.Count, key, time, 1);
		incrementCounter(DurationType.Day, CounterType.Count, key, time, 1);
		incrementCounter(DurationType.Week, CounterType.Count, key, time, 1);
		incrementCounter(DurationType.Month, CounterType.Count, key, time, 1);
	}

	public void incrementLengths(String key, long time, int length)
	{
		checkCountersExist(key, CounterType.Length, time);
		incrementCounter(DurationType.Hour, CounterType.Length, key, time, length);
		incrementCounter(DurationType.Day, CounterType.Length, key, time, length);
		incrementCounter(DurationType.Week, CounterType.Length, key, time, length);
		incrementCounter(DurationType.Month, CounterType.Length, key, time, length);
	}
	
	public void incrementDurations(String key, long time, long duration)
	{
		checkCountersExist(key, CounterType.Duration, time);
		incrementDuration(DurationType.Hour, key, time, duration);
		incrementDuration(DurationType.Day, key, time, duration);
		incrementDuration(DurationType.Week, key, time, duration);
		incrementDuration(DurationType.Month, key, time, duration);
	}
	
	public void addUserToApp(String key, long userId)
	{
		pipeline.sadd(key, Long.toString(userId));
	}
	
	private void checkCountersExist(String key, CounterType counterType, long time)
	{
		String counterName = getCounterName(counterType);
		String hourlyCounterKey = String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Hour));
		String dailyCounterKey = String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Day));
		String weeklyCounterKey = String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Week));
		String monthlyCounterKey = String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Month));
		Boolean success = false;
		while (!success)
		{
			jedis.watch(hourlyCounterKey, dailyCounterKey, weeklyCounterKey, monthlyCounterKey);
			boolean exists = jedis.exists(hourlyCounterKey) && jedis.exists(dailyCounterKey) && jedis.exists(weeklyCounterKey) && jedis.exists(monthlyCounterKey);
			pipeline = jedis.pipelined();
			pipeline.multi();
			if (!exists)
			{
				pipeline.del(hourlyCounterKey);
				pipeline.del(dailyCounterKey);
				pipeline.del(weeklyCounterKey);
				pipeline.del(monthlyCounterKey);
				pipeline.rpush(hourlyCounterKey, Long.toString(dropLessThanHour(time)), "0");
				pipeline.rpush(dailyCounterKey, Long.toString(dropLessThanDay(time)), "0");
				pipeline.rpush(weeklyCounterKey, Long.toString(dropLessThanWeek(time)), "0");
				pipeline.rpush(monthlyCounterKey, Long.toString(dropLessThanMonth(time)), "0");
				
			}
			success = (pipeline.exec() != null);
		}
		pipeline.sync();
	}
	
	private void incrementCounter(DurationType durationType, CounterType counterType, String key, long time, long valueToIncrement)
	{
		key = String.format("%s:%s:%s", key, getCounterName(counterType), getDurationName(durationType));
		Boolean success = false;
		while (!success)
		{
			jedis.watch(key);
			List<String> value = jedis.lrange(key, 0, 1);
			pipeline = jedis.pipelined();
			pipeline.multi();
			Counter counter = new Counter(durationType, Long.parseLong(value.get(0)), Long.parseLong(value.get(1)));
			if (updateStartingTime(counter, time, 0))
				pipeline.lset(key, 0, Long.toString(counter.startTime));
			counter.value += valueToIncrement;
			pipeline.lset(key, 1, Long.toString(counter.value));
			success = (pipeline.exec() != null);
		}
		pipeline.sync();
	}
	
	private void incrementDuration(DurationType durationType, String key, long time, long duration)
	{
		key = String.format("%s:%s:%s", key, getCounterName(CounterType.Duration), getDurationName(durationType));
		Boolean success = false;
		while (!success)
		{
			jedis.watch(key);
			List<String> value = jedis.lrange(key, 0, 1);
			pipeline = jedis.pipelined();
			pipeline.multi();
			Counter counter = new Counter(durationType, Long.parseLong(value.get(0)), Long.parseLong(value.get(1)));
			if (updateStartingTime(counter, time, duration))
				pipeline.lset(key, 0, Long.toString(counter.startTime));
			if (time < counter.startTime)
				counter.value = duration - (counter.startTime - time);
			pipeline.lset(key, 1, Long.toString(counter.value));
			success = (pipeline.exec() != null);
		}
		pipeline.sync();
	}
	
	private class Counter
	{
		public final DurationType durationType;
		public long startTime;
		public long value;
		
		public Counter(DurationType durationType, long startTime, long value)
		{
			this.durationType = durationType;
			this.startTime = startTime;
			this.value = value;
		}
	}
	
	private enum DurationType
	{
		Hour, Day, Week, Month
	}
	
	private String getDurationName(DurationType durationType)
	{
		switch (durationType)
		{
		case Hour: return "hourly";
		case Day: return "daily";
		case Week: return "weekly";
		case Month: return "monthly";
		}
		return null;
	}
	
	private enum CounterType
	{
		Count, Length, Duration
	}
	
	private String getCounterName(CounterType counterType)
	{
		switch (counterType)
		{
		case Count: return "count";
		case Length: return "length";
		case Duration: return "duration";
		}
		return null;
	}
	
	private boolean updateStartingTime(Counter counter, long time, long duration)
	{
		boolean updated = false;
		long endTime = time + duration;
		switch (counter.durationType)
		{
		case Hour:
			if (hourDifference(endTime, counter.startTime) > 0)
			{
				counter.startTime = dropLessThanHour(endTime);
				counter.value = 0;
				updated = true;
			}
		case Day:
			if (dayDifference(endTime, counter.startTime) > 0)
			{
				counter.startTime = dropLessThanDay(endTime);
				counter.value = 0;
				updated = true;
			}
		case Week:
			if (weekDifference(endTime, counter.startTime) > 0)
			{
				counter.startTime = dropLessThanWeek(endTime);
				counter.value = 0;
				updated = true;
			}
		case Month:
			if (monthDifference(endTime, counter.startTime) > 0)
			{
				counter.startTime = dropLessThanMonth(endTime);
				counter.value = 0;
				updated = true;
			}
		}
		return updated;
	}
	
	private static long hourDifference(long t1, long t2)
	{
		return (t1 - t2) / HOUR;
	}
	
	private static long dayDifference(long t1, long t2)
	{
		return (t1 - t2) / DAY;
	}
	
	private static long weekDifference(long t1, long t2)
	{
		return (t1 - t2) / WEEK;
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


/*
private void incrementHourlyCounter(String key, long time, long value)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementDailyCounter(String key, long time, long value)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementWeeklyCounter(String key, long time, long value)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementMonthlyCounter(String key, long time, long value)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
		long startingMonthTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		if (monthDifference(time, startingMonthTime) > 0)
		{
			pipeline.lset(key, 0, Long.toString(dropLessThanMonth(time)));
			counterValue = 0;
		}
		counterValue += value;
		pipeline.lset(key, 1, Long.toString(counterValue));
		success = (pipeline.exec() != null);
	}
}

private void incrementHourlyDuration(String key, long time, long duration)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementDailyDuration(String key, long time, long duration)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementWeeklyDuration(String key, long time, long duration)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}

private void incrementMonthlyDuration(String key, long time, long duration)
{
	Boolean success = false;
	while (!success)
	{
		pipeline.watch(key);
		List<String> counter = pipeline.lrange(key, 0, 1).get();
		pipeline.multi();
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
		success = (pipeline.exec() != null);
	}
}
*/