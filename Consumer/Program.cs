using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Threading;

namespace Consumer
{
    class Program
    {
        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            const int commitPeriod = 5;

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {

                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }
                            //--------------------------------------

                            MailMessage message = new MailMessage("seleniumtestgulsen@gmail.com","gulsenkeskin2@gmail.com");
                            message.Subject = "subject";
                            message.Body = consumeResult.Message.Value;

                            SmtpClient client = new SmtpClient("smtp.gmail.com", 587);
                            client.EnableSsl = true;
                            client.Credentials = new System.Net.NetworkCredential("seleniumtestgulsen@gmail.com", "testselenium");
                            try
                            {
                                client.Send(message);
                                Console.WriteLine("Mail gönderildi");

                            }
                            catch (SmtpException ex)
                            {
                                Console.WriteLine("Exception caught in SendErrorLog: {0}",
                                    ex.ToString());
                            }

                            //------------------------------------

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }


        public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                EnableAutoCommit = true
            };

            using (var consumer =
                new ConsumerBuilder<Ignore, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");

        public static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                PrintUsage();
                return;
            }

            var mode = args[0];
            var brokerList = args[1];
            var topics = args.Skip(2).ToList();

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            switch (mode)
            {
                case "subscribe":
                    Run_Consume(brokerList, topics, cts.Token);
                    break;
                case "manual":
                    Run_ManualAssign(brokerList, topics, cts.Token);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
