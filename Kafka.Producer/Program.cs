using Kafka.Message;
using System;

namespace Kafka.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            IMessageProducer messageProducer = new MessageProducer();

            Console.Write("Content: ");
            var content = Console.ReadLine();

            Console.Write("Subject: ");
            var subject = Console.ReadLine();

            Console.Write("To: ");
            var to = Console.ReadLine();


            //produce email message
            var emailMessage = new EmailMessage
            {
                Content = content,
                Subject = subject,
                To = to
            };
            messageProducer.Produce("emailmessage-topic", emailMessage);

            Console.ReadLine();
        }
    }
}
