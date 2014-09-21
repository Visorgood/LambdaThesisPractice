import java.nio.ByteBuffer;

import kafka.message.Message;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;


public class SmsReceivedEventSerializer implements Encoder<SmsReceived>
{
	public SmsReceivedEventSerializer(VerifiableProperties props)
	{
		
	}
	
	public Message toMessage(SmsReceived smsReceived)
	{
		return new Message(toBytes(smsReceived));
	}

	@Override
	public byte[] toBytes(SmsReceived smsReceived)
	{
		ByteBuffer buffer = ByteBuffer.allocate(24);
	    buffer.putLong(0, smsReceived.getId());
	    buffer.putLong(8, smsReceived.getUserId());
	    buffer.putLong(16, smsReceived.getTime());
		return buffer.array();
	}
}