using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Kafka.Consumer.Consumers
{
    public abstract class MessageConsumerBase<IMessage>
    {
        private readonly string _topic;

        protected MessageConsumerBase(string topic)
        {
            _topic = topic;
        }
        public void StartConsuming()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "emailmessage-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                consumer.Subscribe(_topic);

                //var keepConsuming = true;
                //consumer.OnError += (_, e) => keepConsuming = !e.IsFatal;
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumedTextMessage = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{consumedTextMessage.Message.Value}' Topic: '{consumedTextMessage.Topic}'.");

                            var message = JsonConvert.DeserializeObject<IMessage>(consumedTextMessage.Message.Value);

                            OnMessageDelivered(message);
                        }
                        catch (ConsumeException e)
                        {
                            OnErrorOccured(e.Error);
                        }
                    }

                }
                catch (OperationCanceledException)
                {
              
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
        public abstract void OnMessageDelivered(IMessage message);

        public abstract void OnErrorOccured(Error error);
    }
}
