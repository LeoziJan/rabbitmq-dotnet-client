using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using RabbitMQ.Client;

namespace GH_681
{
    public class ThreadArgs
    {
        private readonly CountdownEvent countdownEvent;
        private readonly IConnection connection;

        public ThreadArgs(CountdownEvent countdownEvent, IConnection connection)
        {
            this.countdownEvent = countdownEvent;
            this.connection = connection;
        }

        public CountdownEvent CountdownEvent
        {
            get
            {
                return countdownEvent;
            }
        }

        public IConnection Connection
        {
            get
            {
                return connection;
            }
        }
    }

    public static class Program
    {
        private static readonly ICollection<Thread> ts = new List<Thread>();
        private static readonly ConnectionFactory factory = new ConnectionFactory { HostName = "ravel" };

        private static CountdownEvent countdownEvent;
        private static byte[] data;

        private static int loops;
        private static int sleepMs;
        private static int threads;

        static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                Console.WriteLine("Usage: .exe loops sleepMs threads payloadFile");
                Environment.Exit(-1);
            }

            loops = int.Parse(args[0]);
            sleepMs = int.Parse(args[1]);
            threads = int.Parse(args[2]);
            data = File.ReadAllBytes(args[3]);

            Console.WriteLine("Setting up exchange and queue...");

            var queueArguments = new Dictionary<string, object>()
            {
                { "x-queue-mode", "lazy" }
            };

            using (IConnection connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "exch", type: "direct", autoDelete: false, durable: true, arguments: null);
                    channel.QueueDeclare(queue: "gh-681", autoDelete: false, durable: true, exclusive: false, arguments: queueArguments);
                    channel.QueueBind(queue: "gh-681", exchange: "exch", routingKey: "gh-681");
                }

                Console.WriteLine("DONE setting up exchange and queue...");

                countdownEvent = new CountdownEvent(threads);
                for (int i = 0; i < threads; i++)
                {
                    Console.WriteLine("Start WriteMessage");
                    var t = new Thread(WriteDirectMessage);
                    t.Start(new ThreadArgs(countdownEvent, connection));
                    ts.Add(t);
                }

                Console.WriteLine("Waiting for threads to finish...");
                countdownEvent.Wait();
            }
            Console.WriteLine("Finished");
        }

        private static void WriteDirectMessage(object o)
        {
            ThreadArgs args = (ThreadArgs)o;
            CountdownEvent countdownEvent = args.CountdownEvent;
            IConnection connection = args.Connection;
            Console.WriteLine("Starting WriteDirectMessage...");
            try
            {
                try
                {
                    using (var channel = connection.CreateModel())
                    {
                        IBasicProperties props = channel.CreateBasicProperties();
                        props.Persistent = true;
                        for (int i = 0; i < loops; i++)
                        {
                            channel.BasicPublish(exchange: "exch", routingKey: "gh-681", basicProperties: props, body: data);
                            if (sleepMs > 0)
                            {
                                Thread.Sleep(sleepMs);
                            }
                        }
                    }

                }
                catch (Exception ex)
                {
                    var msg = ex.ToString();
                    Console.WriteLine(msg);
                }
            }
            finally
            {
                countdownEvent.Signal();
            }
        }
    }
}