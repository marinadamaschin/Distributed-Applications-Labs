package md.utm.messaging.broker; import md.utm.messaging.contracts.MessageEnvelope; public record DeadLetter(MessageEnvelope message,int attempts,String reason){}
