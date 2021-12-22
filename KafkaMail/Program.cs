﻿using Confluent.Kafka;
using Kafka.Message;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nancy.Json;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Mail;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaMail
{
    class Program
    {
        static void Main(string[] args)
        {

            CreateHostBuilder(args).Build().Run();
        }
        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args).ConfigureServices((context, collection) =>
            {
                collection.AddHostedService<KafkaConsumerHostedService>();
                collection.AddHostedService<KafkaProducerHostedService>();
            });
    }
    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;
        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
                LingerMs = 5000
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }
        public async Task StartAsync(CancellationToken cancellationToken)
        {


            List<EmailMessage> messageList= new List<EmailMessage>();
            for (int i = 0; i < 100; i++)
            {
               var m= new EmailMessage
                {
                    Content = $"content {i} message",
                    Subject = $"subject {i} message",
                    To = "gulsenkeskin2@gmail.com"
                };
                messageList.Add(m);
            }
          


            foreach (var message in messageList)
            {       
                var value = JsonConvert.SerializeObject(message);
                _logger.LogInformation(value);
                await _producer.ProduceAsync(topic: "demo", new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);

                _producer.Flush(timeout: TimeSpan.FromSeconds(10));
            }


            //Console.Write("Content: ");
            //var content = Console.ReadLine();

            //Console.Write("Subject: ");
            //var subject = Console.ReadLine();

            //Console.Write("To: ");
            //var to = Console.ReadLine();
            ////produce email message
            //var emailMessage = new EmailMessage
            //{
            //    Content = content,
            //    Subject = subject,
            //    To = to
            //};

            //var value = JsonConvert.SerializeObject(emailMessage);
            //_logger.LogInformation(value);
            //await _producer.ProduceAsync(topic: "demo", new Message<Null, string>()
            //{
            //    Value = value
            //}, cancellationToken);

            //_producer.Flush(timeout: TimeSpan.FromSeconds(10));



        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private ClusterClient _cluster;
        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(new Configuration
            { Seeds = "localhost:9092" }, new ConsoleLogger());

        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest(topic: "demo");
            _cluster.MessageReceived += record =>
            {
                _logger.LogInformation($"Received: {Encoding.UTF8.GetString(record.Value as byte[])}");

                string value = Encoding.UTF8.GetString(record.Value as byte[]);

                JavaScriptSerializer ser = new JavaScriptSerializer();
                var r = ser.Deserialize<List<EmailMessage>>(value);
                var b = ser.Deserialize<EmailMessage>(value);

                sendMail(b);


            };
            return Task.CompletedTask;
        }


        public void sendMail(EmailMessage message)
        {

            MailMessage m = new MailMessage("seleniumtestgulsen@gmail.com", message.To);
            m.Subject = message.Subject;
            m.Body = message.Content;

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

        }
        //public void sendMailList(List<EmailMessage> messageList)
        //{
        //    foreach (var message in messageList)
        //    {
        //        MailMessage m = new MailMessage("seleniumtestgulsen@gmail.com", message.To);
        //        m.Subject = message.Subject;
        //        m.Body = message.Content;

        //        SmtpClient client = new SmtpClient("smtp.gmail.com", 587);
        //        client.EnableSsl = true;
        //        client.Credentials = new System.Net.NetworkCredential("seleniumtestgulsen@gmail.com", "testselenium");
        //        try
        //        {
        //            client.Send(m);
        //            Console.WriteLine("Mail gönderildi");

        //        }
        //        catch (SmtpException ex)
        //        {
        //            Console.WriteLine("Exception caught in SendErrorLog: {0}",
        //                ex.ToString());
        //        }
        //    }
        //}



        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }



    }






}
