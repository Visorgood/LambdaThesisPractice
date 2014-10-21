public class EventAggregator
{
	RedisProxy redisProxy;
	
	public EventAggregator(String host)
	{
		redisProxy = new RedisProxy(host);
	}
	
	void AppInstall(String appName, String userId, long time)
	{
		redisProxy.addUserToApp(appName, userId);
	}
	
	void AppSession(String userId, long time, long duration, String appName)
	{
		// app:$app_name:$user_id:sessions:* counters
		String key = String.format("app:%s:%s:%s", appName, userId, "sessions");
		redisProxy.incrementCounters(key, time);
		
		// app:$app_name:$user_id:total_time:* durations
		key = String.format("app:%s:%s:%s", appName, userId, "total_time");
		redisProxy.incrementDurations(key, time, duration);
		
		// user:$user_id:$app_name:app_usage:* counters
		key = String.format("user:%s:%s:%s", userId, appName, "app_usage");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$app_name:app_starts:* durations
		key = String.format("user:%s:%s:%s", userId, appName, "app_starts");
		redisProxy.incrementDurations(key, time, duration);
	}
	
	void ScreenUnlock(String userId, long time)
	{
		// user:$user_id:screen_lock:* counters
		String key = String.format("user:%s:%s", userId, "screen_lock");
		redisProxy.incrementCounters(key, time);
	}
	
	void SmsReceived(String userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:incoming_msg_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_msg_length:* lengths
		key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
	}
	
	void SmsSent(String userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:outgoing_msg_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_msg_length:* lengths
		key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
	}
	
	void CallOutgoing(String userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:outgoing_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_call_duration:* durations
		key = String.format("user:%s:%s:%s", userId, contactHash, "outgoing_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);
	}
	
	void CallReceived(String userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:incoming_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_call_duration:* durations
		key = String.format("user:%s:%s:%s", userId, contactHash, "incoming_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);
	}
	
	void CallMissed(String userId, long time, String contactHash, long timestamp)
	{
		// user:$user_id:$phone_hash:missed_call_count:* counters
		String key = String.format("user:%s:%s:%s", userId, contactHash, "missed_call_count");
		redisProxy.incrementCounters(key, time);
	}
}
