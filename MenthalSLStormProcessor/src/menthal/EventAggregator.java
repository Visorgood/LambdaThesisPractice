package menthal;
public interface EventAggregator {
  void processAppInstall(long userId, long time, String appName);

  void processAppSession(long userId, long time, long duration, String appName);

  void processCallMissed(long userId, long time, String contactHash, long timestamp);

  void processCallOutgoing(long userId, long time, String contactHash, long startTimestamp,
      long durationInMillis);

  void processCallReceived(long userId, long time, String contactHash, long startTimestamp,
      long durationInMillis);

  void processDreamingStarted(long userId, long time);

  void processDreamingStopped(long userId, long time);

  void processPhoneShutdown(long userId, long time);

  void processScreenOff(long userId, long time);

  void processScreenOn(long userId, long time);

  void processScreenUnlock(long userId, long time);

  void processSmsReceived(long userId, long time, String contactHash, int msgLength);

  void processSmsSent(long userId, long time, String contactHash, int msgLength);

  void processWindowStateChanged(long userId, long time, String appName, String windowTitle);
}
