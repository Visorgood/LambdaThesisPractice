import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;

public class Aggregations
{
	private static long HOUR = Duration.standardHours(1).getMillis();
	private static long DAY = Duration.standardDays(1).getMillis();
	private static long WEEK = Duration.standardDays(7).getMillis();
	
	private static Jedis jedis = new Jedis("localhost");

	static void AppInstall(String appName, String userId, long time)
	{
		String key = String.format("app:%s:%s:%s", appName, userId, "count_hourly");
		if (!jedis.exists(key))
		{
			String usersCountKey = String.format("app:%s:%s", appName, "users_count");
			jedis.incr(usersCountKey);
		}
	}
	
	static void AppSession(String userId, long time, long duration, String appName)
	{
		// app:$app_name:$user_id:sessions:* counters
		String key = String.format("app:%s:%s:%s", appName, userId, "sessions");
		incrementCounters(key, time);
		
		// app:$app_name:$user_id:total_time:* durations
		key = String.format("app:%s:%s:%s", appName, userId, "total_time");
		incrementDurations(key, time, duration);
		
		// user:$user_id:$app_name:app_usage:* counters
		key = String.format("user:%s:%s:%s", userId, appName, "app_usage");
		incrementCounters(key, time);
		
		// user:$user_id:$app_name:app_starts:* durations
		key = String.format("user:%s:%s:%s", userId, appName, "app_starts");
		incrementDurations(key, time, duration);
	}
	
	static void ScreenUnlock(String userId, long time)
	{
		// user:$user_id:screen_lock:* counters
		String key = String.format("user:%s:%s", userId, "screen_lock");
		incrementCounters(key, time);
	}
	
	static void SmsReceived(String userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:incoming_msg_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_msg_count");
		incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_msg_length:* lengths
		key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_msg_length");
		incrementLengths(key, time, msgLength);
	}
	
	static void SmsSent(String userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:outgoing_msg_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_msg_count");
		incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_msg_length:* lengths
		key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_msg_length");
		incrementLengths(key, time, msgLength);
	}
	
	static void CallOutgoing(String userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:outgoing_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_call_count");
		incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_call_duration:* durations
		key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_call_duration");
		incrementDurations(key, time, durationInMillis);
	}
	
	static void CallReceived(String userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:incoming_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_call_count");
		incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_call_duration:* durations
		key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_call_duration");
		incrementDurations(key, time, durationInMillis);
	}
	
	static void CallMissed(String userId, long time, String contactHash, long timestamp)
	{
		// user:$user_id:$phone_hash:missed_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "missed_call_count");
		incrementCounters(key, time);
	}
	
	private static void incrementCounters(String key, long time)
	{
		if (!jedis.exists(key + ":count_hourly"))
		{
			jedis.rpush(key + ":count_hourly", Long.toString(dropLessThanHour(time)), "0");
			jedis.rpush(key + ":count_daily", Long.toString(dropLessThanDay(time)), "0");
			jedis.rpush(key + ":count_weekly", Long.toString(dropLessThanWeek(time)), "0");
			jedis.rpush(key + ":count_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyCounter(key, time, 1);
		incrementDailyCounter(key, time, 1);
		incrementWeeklyCounter(key, time, 1);
		incrementMonthlyCounter(key, time, 1);
	}
	
	private static void incrementDurations(String key, long time, long duration)
	{
		if (!jedis.exists(key + ":duration_hourly"))
		{
			jedis.rpush(key + ":duration_hourly", Long.toString(dropLessThanHour(time)), "0");
			jedis.rpush(key + ":duration_daily", Long.toString(dropLessThanDay(time)), "0");
			jedis.rpush(key + ":duration_weekly", Long.toString(dropLessThanWeek(time)), "0");
			jedis.rpush(key + ":duration_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyDuration(key, time, duration);
		incrementDailyDuration(key, time, duration);
		incrementWeeklyDuration(key, time, duration);
		incrementMonthlyDuration(key, time, duration);
	}

	
	private static void incrementLengths(String key, long time, int length)
	{
		if (!jedis.exists(key + ":length_hourly"))
		{
			jedis.rpush(key + ":length_hourly", Long.toString(dropLessThanHour(time)), "0");
			jedis.rpush(key + ":length_daily", Long.toString(dropLessThanDay(time)), "0");
			jedis.rpush(key + ":length_weekly", Long.toString(dropLessThanWeek(time)), "0");
			jedis.rpush(key + ":length_monthly", Long.toString(dropLessThanMonth(time)), "0");
		}
		incrementHourlyCounter(key, time, length);
		incrementDailyCounter(key, time, length);
		incrementWeeklyCounter(key, time, length);
		incrementMonthlyCounter(key, time, length);
	}
	
	private static void incrementHourlyCounter(String key, long time, long value)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingHourTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingHourTime;
		if (difference >= HOUR)
		{
			long numberOfHours = difference / HOUR;
			jedis.lset(key, 0, Long.toString(startingHourTime + numberOfHours * HOUR));
			counterValue = 0;
		}
		counterValue += value;
		jedis.lset(key, 1, Long.toString(counterValue));
	}
	
	private static void incrementDailyCounter(String key, long time, long value)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingDayTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingDayTime;
		if (difference >= DAY)
		{
			long numberOfDays = difference / DAY;
			jedis.lset(key, 0, Long.toString(startingDayTime + numberOfDays * DAY));
			counterValue = 0;
		}
		counterValue += value;
		jedis.lset(key, 1, Long.toString(counterValue));
	}
	
	private static void incrementWeeklyCounter(String key, long time, long value)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingWeekTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		long difference = time - startingWeekTime;
		if (difference >= WEEK)
		{
			// !!! here must be changed so that weeks start from monday
			long numberOfWeeks = difference / WEEK;
			jedis.lset(key, 0, Long.toString(startingWeekTime + numberOfWeeks * WEEK));
			counterValue = 0;
		}
		counterValue += value;
		jedis.lset(key, 1, Long.toString(counterValue));
	}
	
	private static void incrementMonthlyCounter(String key, long time, long value)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingMonthTime = Long.parseLong(counter.get(0));
		long counterValue = Long.parseLong(counter.get(1));
		if (monthDifference(time, startingMonthTime) > 0)
		{
			jedis.lset(key, 0, Long.toString(dropLessThanMonth(time)));
			counterValue = 0;
		}
		counterValue += value;
		jedis.lset(key, 1, Long.toString(counterValue));
	}
	
	private static void incrementHourlyDuration(String key, long time, long duration)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingHourTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + durationValue;
		long difference = endTime - startingHourTime;
		if (difference >= HOUR)
		{
			long numberOfHours = difference / HOUR;
			startingHourTime += numberOfHours * HOUR;
			jedis.lset(key, 0, Long.toString(startingHourTime));
			durationValue = 0;
		}
		if (time < startingHourTime)
			duration -= startingHourTime - time;
		jedis.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private static void incrementDailyDuration(String key, long time, long duration)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingDayTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		long difference = endTime - startingDayTime;
		if (difference >= DAY)
		{
			long numberOfDays = difference / DAY;
			startingDayTime += numberOfDays * DAY;
			jedis.lset(key, 0, Long.toString(startingDayTime));
			durationValue = 0;
		}
		if (time < startingDayTime)
			duration -= startingDayTime - time;
		jedis.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private static void incrementWeeklyDuration(String key, long time, long duration)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingWeekTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		long difference = endTime - startingWeekTime;
		if (difference >= WEEK)
		{
			long numberOfWeeks = difference / WEEK;
			startingWeekTime += numberOfWeeks * WEEK;
			jedis.lset(key, 0, Long.toString(startingWeekTime));
			durationValue = 0;
		}
		if (time < startingWeekTime)
			duration -= startingWeekTime - time;
		jedis.lset(key, 1, Long.toString(durationValue + duration));
	}
	
	private static void incrementMonthlyDuration(String key, long time, long duration)
	{
		List<String> counter = jedis.lrange(key, 0, 1);
		long startingMonthTime = Long.parseLong(counter.get(0));
		long durationValue = Long.parseLong(counter.get(1));
		long endTime = time + duration;
		if (monthDifference(endTime, startingMonthTime) > 0)
		{
			jedis.lset(key, 0, Long.toString(dropLessThanMonth(endTime)));
			durationValue = 0;
		}
		if (time < startingMonthTime)
			duration -= startingMonthTime - time;
		jedis.lset(key, 1, Long.toString(durationValue + duration));
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
