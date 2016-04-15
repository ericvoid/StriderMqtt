using System;
using System.Threading;
using StriderMqtt;

namespace NumbersTest
{
	class MainClass
	{
		public static void Main(string[] args)
		{
			string qos = args[0];
			string topic_root = args[1];
			string client_id = args[2];
			int maxNumber = Int32.Parse(args[3]);

			// Console.WriteLine("{0} starting", client_id);
			MqttQos Qos = ParseQos(qos);

			using (var p = new SqlitePersistence(client_id + ".sqlite3"))
			// using (var p = new InMemoryPersistence())
			{
				var client = new MqttClient(p, topic_root, client_id, Qos, maxNumber);
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
