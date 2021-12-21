using Kafka.Message;
using System;

namespace Kafka.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            IMessageProducer messageProducer = new MessageProducer();

            //produce email message
            var emailMessage = new EmailMessage
            {
                Content = "Contoso Retail Daily News Email Content",
                Subject = "Contoso Retail Daily News",
                To = "seleniumtestgulsen@gmail.com"
            };
            messageProducer.Produce("emailmessage-topic", emailMessage);

            Console.ReadLine();
        }
    }
}
