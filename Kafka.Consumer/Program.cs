using Kafka.Consumer.Consumers;
using System;

namespace Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Consumer Started !");

            var emailMessageConsumer = new EmailMessageConsumer();
            emailMessageConsumer.StartConsuming();

            Console.ReadLine();
        }
    }
}
