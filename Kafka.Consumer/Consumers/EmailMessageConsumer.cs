using Confluent.Kafka;
using Kafka.Message;
using System;

namespace Kafka.Consumer.Consumers
{
    public class EmailMessageConsumer : MessageConsumerBase<EmailMessage>
    {
        public EmailMessageConsumer() : base("emailmessage-topic") { }

        public override void OnErrorOccured(Error error)
        {
            Console.WriteLine($"Error: {error}");
        }

        public override void OnMessageDelivered(EmailMessage message)
        {
            Console.WriteLine($"To: {message.To} \nContent: {message.Content} \nSubject: {message.Subject}");
        }
    }
}
