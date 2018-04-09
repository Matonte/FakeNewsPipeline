using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaConsumer
{
    class Program
    {
        static void kafkaConsumer()
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
            var consumer = new KafkaNet.Consumer(new ConsumerOptions("test",
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
                        }
                    }
                }
                Console.WriteLine("Hello to console only");

            }
        }

        static void Main(string[] args)
        {
            kafkaConsumer();
        }
    }
}
