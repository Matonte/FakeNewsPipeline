using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tweetinvi;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.IO;

namespace TwitterApp
{
    class Program
    {

       public static void twitterInfo()
        {
            var options = new KafkaOptions
           (new Uri("http://localhost:9092"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

          const String topic = "test";
	 	  const   String tokenSecret = "FbcP68UtZR8U6n0AIMkBZHxgx4hzv3ibLQrU35qQipQ7Y";
		  const   String consumerSecret = "CqsVHqSKW4S6hH6eGrq9B4PvKCZZElJ8XLFMeJihEouyJHl9xH";
	      const   String token = "2710082868-swCbtRmmODBOB6TMbsDGIQNMCUKoATTVAwbCQwi";
	      const   String consumerKey = "XC6GdAJVYF9jMQqS68bQOu6kG";

        Auth.SetUserCredentials(consumerKey, consumerSecret, token, tokenSecret);
             var stream = Tweetinvi.Stream.CreateFilteredStream();
             stream.AddTrack("Politics");

            stream.MatchingTweetReceived += (sender, arguments) =>
          {
       
           

              client.SendMessageAsync("test", new[]
                   { new  KafkaNet.Protocol.Message("tweet:  " + arguments.Tweet.Text) }).Wait();

          };
            stream.StartStreamMatchingAllConditions();
        }

    static void Main(string[] args)
        {
            twitterInfo();
        }
    }
}
