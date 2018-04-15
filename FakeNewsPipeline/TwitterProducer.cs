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
using KafkaConsumer;
namespace TwitterApp
{
    class TwitterApp
    {

       public static void twitterInfo()
        {
            var options = new KafkaOptions
           (new Uri("http://localhost:9092"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

          const   String topic = "newtest1";
	 	  const   String tokenSecret = "FbcP68UtZR8U6n0AIMkBZHxgx4hzv3ibLQrU35qQipQ7Y";
		  const   String consumerSecret = "xL20m2W34vg8dhxjxvBs0R9vvx3dZCw0fYAG9UjhVRBqgHTQ9d";
	      const   String token = "2710082868-swCbtRmmODBOB6TMbsDGIQNMCUKoATTVAwbCQwi";
	      const   String consumerKey = "XC6GdAJVYF9jMQqS68bQOu6kG";

        Auth.SetUserCredentials(consumerKey, consumerSecret, token, tokenSecret);
             var stream = Tweetinvi.Stream.CreateFilteredStream();
             stream.AddTrack("Politics");

            stream.MatchingTweetReceived += (sender, arguments) =>
          {
              Console.WriteLine(arguments.Tweet.Text);
              client.SendMessageAsync(topic, new[]
                   { new  KafkaNet.Protocol.Message( arguments.Tweet.Text) }).Wait();
              
          };
            stream.StartStreamMatchingAllConditions();
        }

    static void Main(string[] args)
        {

            twitterInfo();
           

        }
    }
}
