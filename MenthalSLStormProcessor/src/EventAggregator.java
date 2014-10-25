public class EventAggregator
{
	private final RedisProxy redisProxy;
	private static String ALL_USERS_ID = "allUsers";
	
	public EventAggregator(String host)
	{
		redisProxy = new RedisProxy(host);
	}
	
	void processAppInstall(long userId, long time, String appName)
	{
		String key = String.format("app:%s:%s", appName, "users_count");
		redisProxy.addUserToApp(key, userId);
	}
	
	void processAppSession(long userId, long time, long duration, String appName)
	{
		// app:$app_name:$user_id:sessions:* counters
		String key = String.format("app:%s:user%d:%s", appName, userId, "sessions");
		redisProxy.incrementCounters(key, time);
		
		// app:$app_name:$user_id:total_time:* durations
		key = String.format("app:%s:user%d:%s", appName, userId, "total_time");
		redisProxy.incrementDurations(key, time, duration);
		
		// user:$user_id:$app_name:app_usage:* counters
		key = String.format("user:user%d:%s:%s", userId, appName, "app_usage");
		redisProxy.incrementCounters(key, time);
		
		// user:ALL_USERS_ID:$app_name:app_usage:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, appName, "app_usage");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$app_name:app_starts:* durations
		key = String.format("user:user%d:%s:%s", userId, appName, "app_starts");
		redisProxy.incrementDurations(key, time, duration);
		
		// user:ALL_USERS_ID:$app_name:app_starts:* durations
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, appName, "app_starts");
		redisProxy.incrementDurations(key, time, duration);
	}
	
	void processScreenUnlock(long userId, long time)
	{
		// user:$user_id:screen_lock:* counters
		String key = String.format("user:user%d:%s", userId, "screen_lock");
		redisProxy.incrementCounters(key, time);
	}
	
	void processSmsReceived(long userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:incoming_msg_count:* counters
		String key = String.format("user:user%d:%s:%s", userId, contactHash, "incoming_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:ALL_USERS_ID:$phone_hash:incoming_msg_count:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "incoming_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_msg_length:* lengths
		key = String.format("user:user%d:%s:%s", userId, contactHash, "incoming_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
		
		// user:ALL_USERS_ID:$phone_hash:incoming_msg_length:* lengths
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "incoming_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
	}
	
	void processSmsSent(long userId, long time, String contactHash, int msgLength)
	{
		// user:$user_id:$phone_hash:outgoing_msg_count:* counters
		String key = String.format("user:user%d:%s:%s", userId, contactHash, "outgoing_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:ALL_USERS_ID:$phone_hash:outgoing_msg_count:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "outgoing_msg_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_msg_length:* lengths
		key = String.format("user:user%d:%s:%s", userId, contactHash, "outgoing_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
		
		// user:ALL_USERS_ID:$phone_hash:outgoing_msg_length:* lengths
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "outgoing_msg_length");
		redisProxy.incrementLengths(key, time, msgLength);
	}
	
	void processCallOutgoing(long userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:outgoing_call_count:* counters
		String key = String.format("user:user%d:%s:%s", userId, contactHash, "outgoing_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:ALL_USERS_ID:$phone_hash:outgoing_call_count:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "outgoing_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:outgoing_call_duration:* durations
		key = String.format("user:user%d:%s:%s", userId, contactHash, "outgoing_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);
		
		// user:ALL_USERS_ID:$phone_hash:outgoing_call_duration:* durations
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "outgoing_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);
	}
	
	void processCallReceived(long userId, long time, String contactHash, long startTimestamp, long durationInMillis)
	{
		// user:$user_id:$phone_hash:incoming_call_count:* counters
		String key = String.format("user:user%d:%s:%s", userId, contactHash, "incoming_call_count");
		redisProxy.incrementCounters(key, time);

		// user:ALL_USERS_ID:$phone_hash:incoming_call_count:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "incoming_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:$user_id:$phone_hash:incoming_call_duration:* durations
		key = String.format("user:user%d:%s:%s", userId, contactHash, "incoming_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);

		// user:ALL_USERS_ID:$phone_hash:incoming_call_duration:* durations
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "incoming_call_duration");
		redisProxy.incrementDurations(key, time, durationInMillis);
	}
	
	void processCallMissed(long userId, long time, String contactHash, long timestamp)
	{
		// user:$user_id:$phone_hash:missed_call_count:* counters
		String key = String.format("user:user%d:%s:%s", userId, contactHash, "missed_call_count");
		redisProxy.incrementCounters(key, time);
		
		// user:ALL_USERS_ID:$phone_hash:missed_call_count:* counters
		key = String.format("user:%s:%s:%s", ALL_USERS_ID, contactHash, "missed_call_count");
		redisProxy.incrementCounters(key, time);
	}
}
