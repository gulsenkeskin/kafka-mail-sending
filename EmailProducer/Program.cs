using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace EmailProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await ProduceAsync();
        }

        public static async Task ProduceAsync()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    while (true)
                    {
                        Console.Write("To:");
                        var to = Console.ReadLine();
                        Console.Write("\nSubject:");
                        var subject = Console.ReadLine();

                        var email = new Email
                        {
                            To = to,
                            Subject = subject
                        };

                        var emailMessage = JsonConvert.SerializeObject(email);

                        var result = await producer.ProduceAsync("email", new Message<Null, string> { Value = emailMessage });
                        Console.WriteLine($"\nGönderilen: '{result.Value}' Topic: '{result.TopicPartitionOffset}'\n");
                    }
                }
                catch (ProduceException<Null, string> ex)
                {

                    Console.WriteLine(ex.Error.Reason);
                }
            }
        }
    }
}
