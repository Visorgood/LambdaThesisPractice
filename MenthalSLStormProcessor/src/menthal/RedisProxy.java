package menthal;

import java.util.List;

import javax.naming.event.EventContext;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisProxy {
  private int countInterval = 10000;
  private String EVENT_COUNTER_KEY = "eventCounter";
  private String EVENT_COUNTER_LIST_KEY = "eventCounterList";

  private static long HOUR = Duration.standardHours(1).getMillis();
  private static long DAY = Duration.standardDays(1).getMillis();
  private static long WEEK = Duration.standardDays(7).getMillis();

  private final Jedis jedis; // the main redis connection provider
  private Pipeline pipeline; // class that allows to send requests to redis in batches through the
                             // network

  // to send requests to Redis we use pipelines
  // it works as follows:
  // we execute WATCH command for keys we want to retrieve and then change
  // we retrieve them, start pipeline, execute some logic as a transaction,
  // and send data to Redis using pipeline's methods exec and sync
  // if after WATCH command any of watched keys in Redis were changed by somebody else,
  // exec command will return null, and processing will start again
  // the point to use pipeline is to send requests to Redis in batch through the network
  // the point to use transaction within pipeline is to be sure, that all commands will be executed
  // as one atomic command

  public RedisProxy(String host, int eventCountLimit) {
    jedis = new Jedis(host);
    pipeline = null;
  }

  // increments all standard counters for all keys having a given base part
  public void incrementCounters(String key, long time) {
    checkCountersExist(key, CounterType.Count, time);
    incrementCounter(DurationType.Hour, CounterType.Count, key, time, 1);
    incrementCounter(DurationType.Day, CounterType.Count, key, time, 1);
    incrementCounter(DurationType.Week, CounterType.Count, key, time, 1);
    incrementCounter(DurationType.Month, CounterType.Count, key, time, 1);
  }

  // increments all length counters for all keys having a given base part
  public void incrementLengths(String key, long time, int length) {
    checkCountersExist(key, CounterType.Length, time);
    incrementCounter(DurationType.Hour, CounterType.Length, key, time, length);
    incrementCounter(DurationType.Day, CounterType.Length, key, time, length);
    incrementCounter(DurationType.Week, CounterType.Length, key, time, length);
    incrementCounter(DurationType.Month, CounterType.Length, key, time, length);
  }

  // increments all duration counters for all keys having a given base part
  public void incrementDurations(String key, long time, long duration) {
    checkCountersExist(key, CounterType.Duration, time);
    incrementDuration(DurationType.Hour, key, time, duration);
    incrementDuration(DurationType.Day, key, time, duration);
    incrementDuration(DurationType.Week, key, time, duration);
    incrementDuration(DurationType.Month, key, time, duration);
  }

  public void addUserToApp(String key, long userId) {
    pipeline = jedis.pipelined();
    pipeline.sadd(key, Long.toString(userId));
    pipeline.sync();
  }

  public void countEvent() {
    Boolean success = false;
    while (!success) {
      jedis.watch(EVENT_COUNTER_KEY);
      List<String> value = jedis.lrange(EVENT_COUNTER_KEY, 0, 1);
      pipeline = jedis.pipelined();
      pipeline.multi();
      long timestamp = Long.parseLong(value.get(0));
      long count = Long.parseLong(value.get(1));
      long now = new DateTime().now().getMillis();
      if (timestamp == 0) {
        timestamp = now;
        count = 1;
      } else {
        ++count;
        if (now - timestamp >= countInterval) {
          pipeline.zadd(EVENT_COUNTER_LIST_KEY, now, Long.toString(count));
          timestamp = now;
        }
      }
      pipeline.lset(EVENT_COUNTER_KEY, 0, Long.toString(timestamp));
      pipeline.lset(EVENT_COUNTER_KEY, 1, Long.toString(count));
      success = (pipeline.exec() != null);
    }
    pipeline.sync();
  }

  private void checkCountersExist(String key, CounterType counterType, long time) {
    String counterName = getCounterName(counterType);
    String hourlyCounterKey =
        String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Hour));
    String dailyCounterKey =
        String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Day));
    String weeklyCounterKey =
        String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Week));
    String monthlyCounterKey =
        String.format("%s:%s:%s", key, counterName, getDurationName(DurationType.Month));
    Boolean success = false;
    while (!success) {
      jedis.watch(hourlyCounterKey, dailyCounterKey, weeklyCounterKey, monthlyCounterKey);
      boolean exists =
          jedis.exists(hourlyCounterKey) && jedis.exists(dailyCounterKey)
              && jedis.exists(weeklyCounterKey) && jedis.exists(monthlyCounterKey);
      pipeline = jedis.pipelined();
      pipeline.multi();
      if (!exists) {
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

  private void incrementCounter(DurationType durationType, CounterType counterType, String key,
      long time, long valueToIncrement) {
    key =
        String.format("%s:%s:%s", key, getCounterName(counterType), getDurationName(durationType));
    Boolean success = false;
    while (!success) {
      jedis.watch(key);
      List<String> value = jedis.lrange(key, 0, 1);
      pipeline = jedis.pipelined();
      pipeline.multi();
      Counter counter =
          new Counter(durationType, Long.parseLong(value.get(0)), Long.parseLong(value.get(1)));
      if (updateStartingTime(counter, time, 0))
        pipeline.lset(key, 0, Long.toString(counter.startTime));
      counter.value += valueToIncrement;
      pipeline.lset(key, 1, Long.toString(counter.value));
      success = (pipeline.exec() != null);
    }
    pipeline.sync();
  }

  private void incrementDuration(DurationType durationType, String key, long time, long duration) {
    key =
        String.format("%s:%s:%s", key, getCounterName(CounterType.Duration),
            getDurationName(durationType));
    Boolean success = false;
    while (!success) {
      jedis.watch(key);
      List<String> value = jedis.lrange(key, 0, 1);
      pipeline = jedis.pipelined();
      pipeline.multi();
      Counter counter =
          new Counter(durationType, Long.parseLong(value.get(0)), Long.parseLong(value.get(1)));
      if (updateStartingTime(counter, time, duration))
        pipeline.lset(key, 0, Long.toString(counter.startTime));
      if (time < counter.startTime)
        counter.value = duration - (counter.startTime - time);
      pipeline.lset(key, 1, Long.toString(counter.value));
      success = (pipeline.exec() != null);
    }
    pipeline.sync();
  }

  private class Counter {
    public final DurationType durationType;
    public long startTime;
    public long value;

    public Counter(DurationType durationType, long startTime, long value) {
      this.durationType = durationType;
      this.startTime = startTime;
      this.value = value;
    }
  }

  private enum DurationType {
    Hour, Day, Week, Month
  }

  private String getDurationName(DurationType durationType) {
    switch (durationType) {
      case Hour:
        return "hourly";
      case Day:
        return "daily";
      case Week:
        return "weekly";
      case Month:
        return "monthly";
    }
    return null;
  }

  private enum CounterType {
    Count, Length, Duration
  }

  private String getCounterName(CounterType counterType) {
    switch (counterType) {
      case Count:
        return "count";
      case Length:
        return "length";
      case Duration:
        return "duration";
    }
    return null;
  }

  private boolean updateStartingTime(Counter counter, long time, long duration) {
    boolean updated = false;
    long endTime = time + duration;
    switch (counter.durationType) {
      case Hour:
        if (hourDifference(endTime, counter.startTime) > 0) {
          counter.startTime = dropLessThanHour(endTime);
          counter.value = 0;
          updated = true;
        }
      case Day:
        if (dayDifference(endTime, counter.startTime) > 0) {
          counter.startTime = dropLessThanDay(endTime);
          counter.value = 0;
          updated = true;
        }
      case Week:
        if (weekDifference(endTime, counter.startTime) > 0) {
          counter.startTime = dropLessThanWeek(endTime);
          counter.value = 0;
          updated = true;
        }
      case Month:
        if (monthDifference(endTime, counter.startTime) > 0) {
          counter.startTime = dropLessThanMonth(endTime);
          counter.value = 0;
          updated = true;
        }
    }
    return updated;
  }

  private static long hourDifference(long t1, long t2) {
    return (t1 - t2) / HOUR;
  }

  private static long dayDifference(long t1, long t2) {
    return (t1 - t2) / DAY;
  }

  private static long weekDifference(long t1, long t2) {
    return (t1 - t2) / WEEK;
  }

  private static long monthDifference(long t1, long t2) {
    DateTime dt1 = new DateTime(t1);
    DateTime dt2 = new DateTime(t2);
    return (dt1.getYear() - dt2.getYear()) * 12 + (dt1.getMonthOfYear() - dt2.getMonthOfYear());
  }

  private static long dropLessThanHour(long time) {
    return time - time % HOUR;
  }

  private static long dropLessThanDay(long time) {
    return time - time % DAY;
  }

  private static long dropLessThanWeek(long time) {
    return time - time % WEEK;
  }

  private static long dropLessThanMonth(long time) {
    MutableDateTime mdt = new MutableDateTime(time);
    mdt.setMillisOfSecond(0);
    mdt.setSecondOfMinute(0);
    mdt.setMinuteOfHour(0);
    mdt.setHourOfDay(0);
    mdt.setDayOfMonth(1);
    return mdt.getMillis();
  }
}
