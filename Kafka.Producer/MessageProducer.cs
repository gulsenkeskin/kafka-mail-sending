using Confluent.Kafka;
using Kafka.Message;
using Newtonsoft.Json;
using System;

namespace Kafka.Producer
{
    public class MessageProducer : IMessageProducer
    {
        public void Produce(string topic, IMessageBase message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var textMessage = JsonConvert.SerializeObject(message);

                producer.Produce(topic, new Message<Null, string> { Value = textMessage }, OnDelivery);

                producer.Flush(TimeSpan.FromSeconds(10));

            }
        }
        private void OnDelivery(DeliveryReport<Null, string> r)
        {
            Console.WriteLine(!r.Error.IsError ? $"Delivered message to {r.TopicPartitionOffset}" : $"Delivery Error: {r.Error.Reason}");
        }
    }
}
