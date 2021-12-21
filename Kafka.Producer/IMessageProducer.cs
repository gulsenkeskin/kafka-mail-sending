using Kafka.Message;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Producer
{
    public interface IMessageProducer
    {
        void Produce(string topic, IMessageBase message);
    }
}
