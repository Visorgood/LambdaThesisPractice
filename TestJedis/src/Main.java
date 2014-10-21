import org.joda.time.DateTime;

public class Main
{
	public static void main(String[] args)
	{
		// app:$app_name:$user_id:$counter store list of two values: time and int
		// $counter can be: Nsh, Nsd, Nsw, Nsm, Th, Td, Tw, Tm
		// Nsh - number of session in the last hour
		// Th - total duration of usage of this app during the last hour
		// app:$app_name:users_count stores integer counter of the number of unique users of this app
		
		String appName = "facebook";
		String userId = "user007";
		long time = new DateTime().getMillis();
		long duration = 35 * 1000;
		
		AppsAggregations.AppInstall(appName, userId, time);
		AppsAggregations.AppSession(appName, userId, time, duration);
	}
}
