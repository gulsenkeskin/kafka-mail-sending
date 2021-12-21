using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Mail;
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
                var cts = new CancellationTokenSource();

                //var keepConsuming = true;
                //consumer.OnError += (_, e) => keepConsuming = !e.IsFatal;
                //CancellationTokenSource cts = new CancellationTokenSource();

                //Console.CancelKeyPress += (_, e) =>
                //{
                //    e.Cancel = true; // prevent the process from terminating.
                //    cts.Cancel();
                //};

                try
                {
                    while (true)
                    {
                        try
                        {
                            //var consumedTextMessage = consumer.Consume(cts.Token);
                            var consumedTextMessage = consumer.Consume();

                            Console.WriteLine($"Consumed message '{consumedTextMessage.Message.Value}' Topic: '{consumedTextMessage.Topic}'.");

                            var message = JsonConvert.DeserializeObject<IMessage>(consumedTextMessage.Message.Value);
                            //---------------------------
                            //****************************
                            MailMessage m = new MailMessage("seleniumtestgulsen@gmail.com", "gulsenkeskin2@gmail.com");
                            m.Subject = "subject";
                            m.Body = consumedTextMessage.Message.Value;

                            SmtpClient client = new SmtpClient("smtp.gmail.com", 587);
                            client.EnableSsl = true;
                            client.Credentials = new System.Net.NetworkCredential("seleniumtestgulsen@gmail.com", "testselenium");
                            try
                            {
                                client.Send(m);
                                Console.WriteLine("Mail gönderildi");

                            }
                            catch (SmtpException ex)
                            {
                                Console.WriteLine("Exception caught in SendErrorLog: {0}",
                                    ex.ToString());
                            }
                            //*********************************
                            //--------------------------
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
                    consumer.Close();

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
