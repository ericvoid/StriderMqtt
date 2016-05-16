using System;
using StriderMqtt;
using System.Threading;

using System.IO;

namespace EmptyPublishing
{
	class MainClass
	{
		public static void Main(string[] args)
		{
			string qos = args[0];
			string topic = args[1];
			string client_id = args[2];
			int publishCount = Int32.Parse(args[3]);

			MqttQos Qos = ParseQos(qos);

			using (var pers = new InMemoryPersistence())
			{
				var client = new MqttClient(topic, client_id, Qos, publishCount, pers);
				while (!client.Finished)
				{
					try
					{
						client.Run();
					}
					catch (Exception ex)
					{
						Console.WriteLine("{0} : {1}", client_id, ex.ToString());
					}

					Thread.Sleep(TimeSpan.FromSeconds(1));
				}
			}

			Console.WriteLine("{0} done", client_id);
		}

		static MqttQos ParseQos(string qos)
		{
			switch (qos)
			{
				case "0":
				return MqttQos.AtMostOnce;

				case "1":
				return MqttQos.AtLeastOnce;

				case "2":
				return MqttQos.ExactlyOnce;

				default:
				throw new ArgumentException("invalid qos level");
			}
		}
	}
}
