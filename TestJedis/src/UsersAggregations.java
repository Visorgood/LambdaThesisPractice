import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;

public class UsersAggregations
{
	private static Jedis jedis = new Jedis("localhost");
		
	private static long HOUR = Duration.standardHours(1).getMillis();
	private static long DAY = Duration.standardDays(1).getMillis();
	private static long WEEK = Duration.standardDays(7).getMillis();
	
	static void AppSession(String user_id, String app_name, long duration, long current_time)
	{
		// APP_USAGE
		// user:$user_id:APP_USAGE:$app_name start count
		String app_usage_key = String.format("user:%s:%s:%s", user_id, "APP_USAGE", app_name);
		CountAggregation(app_usage_key, user_id, current_time);

		
		// APP_STARTS
		// user:$user_id:APP_STARTS:$app_name start duration 
		String app_sarts_key = String.format("user:%s:%s:%s", user_id, "APP_STARTS", app_name);
		LengthAggregation(app_sarts_key, user_id, duration, current_time);
	}
	
	static void ScreenUnlock(String user_id, long current_time)
	{
		// SCREEN_LOCK 
		// user:$user_id:SCREEN_LOCK start count
		String screen_lock_key = String.format("user:%s:%s", user_id, "SCREEN_LOCK");
		CountAggregation(screen_lock_key, user_id, current_time);
	}
	
	static void SmsSent(String user_id, String contactHash, int msgLength, long current_time)
	{
		// OUTGOING_MSG_COUNT  
		// user:$user_id:OUTGOING_MSG_COUNT:$phone_number start count
		String outgoing_msg_count_key = String.format("user:%s:%s:%s", user_id, "OUTGOING_MSG_COUNT", contactHash);
		CountAggregation(outgoing_msg_count_key, user_id, current_time);
		
		// OUTGOING_MSG_LENGTH
		// user:$user_id:OUTGOING_MSG_LENGTH:$phone_number start length
		String outgoing_msg_length_key = String.format("user:%s:%s:%s", user_id, "OUTGOING_MSG_LENGTH", contactHash);
		LengthAggregation(outgoing_msg_length_key, user_id, msgLength, current_time);
	}
	
	static void SmsReceived(String user_id, String contactHash, int msgLength, long current_time)
	{
		// INCOMING_MSG_COUNT  
		// user:$user_id:INCOMING_MSG_COUNT:$phone_number start count
		String incoming_msg_count_key = String.format("user:%s:%s:%s", user_id, "INCOMING_MSG_COUNT", contactHash);
		CountAggregation(incoming_msg_count_key, user_id, current_time);
		
		// INCOMING_MSG_LENGTH
		// user:$user_id:INCOMING_MSG_LENGTH:$phone_number start length
		String incoming_msg_length_key = String.format("user:%s:%s:%s", user_id, "INCOMING_MSG_LENGTH", contactHash);
		LengthAggregation(incoming_msg_length_key, user_id, msgLength, current_time);
	}
	
	static void CallOutgoing(String user_id, String contactHash, long durationInMillis, long current_time)
	{
		// OUTGOING_CALL_COUNT  
		// user:$user_id:OUTGOING_CALL_COUNT:$phone_number start count
		String outgoing_call_count_key = String.format("user:%s:%s:%s", user_id, "OUTGOING_CALL_COUNT", contactHash);
		CountAggregation(outgoing_call_count_key, user_id, current_time);
		
		// OUTGOING_CALL_DURATION
		// user:$user_id:OUTGOING_CALL_DURATION:$phone_number start length
		String outgoing_call_length_key = String.format("user:%s:%s:%s", user_id, "OUTGOING_CALL_DURATION", contactHash);
		LengthAggregation(outgoing_call_length_key, user_id, durationInMillis, current_time);
	}
	
	static void CallReceived(String user_id, String contactHash, long durationInMillis, long current_time)
	{
		// INCOMING_CALL_COUNT  
		// user:$user_id:INCOMING_CALL_COUNT:$phone_number start count
		String incoming_call_count_key = String.format("user:%s:%s:%s", user_id, "INCOMING_CALL_COUNT", contactHash);
		CountAggregation(incoming_call_count_key, user_id, current_time);
		
		// INCOMING_CALL_DURATION
		// user:$user_id:INCOMING_CALL_DURATION:$phone_number start length
		String incoming_call_length_key = String.format("user:%s:%s:%s", user_id, "INCOMING_CALL_DURATION", contactHash);
		LengthAggregation(incoming_call_length_key, user_id, durationInMillis, current_time);
	}
	
	static void CallMissed(String user_id, String contactHash, long current_time)
	{
		// MISSED_CALL_COUNT   
		// user:$user_id:MISSED_CALL_COUNT:$phone_number start count
		String missed_call_count_key = String.format("user:%s:%s:%s", user_id, "MISSED_CALL_COUNT", contactHash);
		CountAggregation(missed_call_count_key, user_id, current_time);
	}
	
	static void CountAggregation(String key, String user_id, long current_time)
	{
		if (!jedis.exists(key + ":sum_hourly"))
		{
			String hour = Long.toString(dropLessThanHour(current_time));
			String day = Long.toString(dropLessThanDay(current_time));
			String week = Long.toString(dropLessThanWeek(current_time));
			String month = Long.toString(dropLessThanMonth(current_time));
			jedis.rpush(key + ":count_hourly", hour, "0");
			jedis.rpush(key + ":count_daily", day, "0");
			jedis.rpush(key + ":count_weekly", week, "0");
			jedis.rpush(key + ":count_monthly", month, "0");
		}
		incrNsh(key, current_time);
		incrNsd(key, current_time);
		incrNsw(key, current_time);
		incrNsm(key, current_time);
	}
	
	static void LengthAggregation(String key, String user_id, long length, long current_time)
	{
		if (!jedis.exists(key + ":sum_hourly"))
		{
			String hour = Long.toString(dropLessThanHour(current_time));
			String day = Long.toString(dropLessThanDay(current_time));
			String week = Long.toString(dropLessThanWeek(current_time));
			String month = Long.toString(dropLessThanMonth(current_time));
			jedis.rpush(key + ":duration_hourly", hour, "0");
			jedis.rpush(key + ":duration_daily", day, "0");
			jedis.rpush(key + ":duration_weekly", week, "0");
			jedis.rpush(key + ":duration_monthly", month, "0");
		}
		incrTh(key, current_time, length);
		incrTd(key, current_time, length);
		incrTw(key, current_time, length);
		incrTm(key, current_time, length);
	}
	
	static void incrNsh(String appUserKey, long time)
	{
		String appUserKeyHour = appUserKey + ":count_hourly";
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
		String appUserKeyDay = appUserKey + ":count_daily";
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
		String appUserKeyWeek = appUserKey + ":count_weekly";
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
		String appUserKeyMonth = appUserKey + ":count_monthly";
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
		String appUserKeyHour = appUserKey + ":duration_hourly";
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
		String appUserKeyDay = appUserKey + ":duration_daily";
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
		String appUserKeyWeek = appUserKey + ":duration_weekly";
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
		String appUserKeyMonth = appUserKey + ":duration_monthly";
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
