﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaConsumer
{
    class KafkaConsumer
    {
        public static DateTime dt = DateTime.Now;

        public static void kafkaConsumer()
        {
            OffsetPosition[] offsetPositions = new OffsetPosition[]
       {
            new OffsetPosition()
            {
               Offset = 1,
               PartitionId = 0
            }
       };
            var options = new KafkaOptions(new Uri("http://localhost:9092"),
            new Uri("http://localhost:9092"));
            var consumer = new KafkaNet.Consumer(new ConsumerOptions("newtest1",
               new BrokerRouter(options)), offsetPositions);


            using (FileStream fileStream = new FileStream("tweets.txt", FileMode.OpenOrCreate))
            {
                using (StreamWriter writer = new StreamWriter(fileStream))
                {
                    using (TextWriter originalConsoleOut = Console.Out)
                    {
                        foreach (var message in consumer.Consume())
                        {
                            Console.WriteLine("Response: P{0},O{1} : {2}",
                               message.Meta.PartitionId, message.Meta.Offset,
                            Encoding.UTF8.GetString(message.Value));
                            Console.SetOut(writer);
                            Console.WriteLine(originalConsoleOut);
                            //////creating a new line 
                            dt = DateTime.Now;
                            String tweet = Encoding.UTF8.GetString(message.Value);
                            int trunc = tweet.Length - 2;

                            tweet = tweet.Substring(2, trunc);
                            int nameend = tweet.IndexOf(':');
                            int namestart = tweet.IndexOf('@');

                            int indexOfAtSign = tweet.IndexOf("@");
                            int indexOfColon = tweet.IndexOf(":");
                            string name = " ";
                            string body = " ";
                            if (indexOfAtSign >= 1 && indexOfColon >= 1)
                            {
                                try
                                {
                                    name = tweet.Substring(indexOfAtSign + 1, indexOfColon - indexOfAtSign - 1);
                                }
                                catch (Exception)
                                {
                                    name = tweet.Substring(3, 10);
                                }


                                body = tweet.Substring(indexOfColon + 1, tweet.Length - indexOfColon - 1);
                            }



                            var newLine = $"{name},{ dt},{body}";
                            csv.AppendLine(newLine);
                            File.WriteAllText(@"C:\Users\billm\source\repos\ConsoleApp2\testingTweets.csv", csv.ToString());
                        }
                    }
                }
                Console.WriteLine("Hello to console only");

            }
        }


        public static StringBuilder csv;

        static void Main(string[] args)
        {

            csv = new StringBuilder();

            kafkaConsumer();
        }

    }
}