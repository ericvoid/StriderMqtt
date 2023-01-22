using System;
using System.Threading;
using StriderMqtt;
using StriderMqtt.Sqlite;

namespace NumbersTest
{
	class MainClass
	{
		public static void Main(string[] args)
		{
            string qos = "0";
            string topic = "test";
            string topic_root = "topic";
            string client_id = "dotnet";
            int publishCount = 0;
            int maxNumber = 0;

            int items = args.Length;
            for (int item = 0; item < items; item++)
            {
                switch (args[item])
                {
                    case "/Q":
                    case "--qos":
                        {
                            qos = args[item + 1];
                            qos = qos.TrimStart('"');
                            qos = qos.TrimEnd('"');
                            break;
                        }
                    case "/T":
                    case "--topic":
                        {
                            topic = args[item + 1];
                            topic = topic.TrimStart('"');
                            topic = topic.TrimEnd('"');
                            break;
                        }
                    case "/t":
                    case "--topicroot":
                        {
                            topic_root = args[item + 1];
                            topic_root = topic_root.TrimStart('"');
                            topic_root = topic_root.TrimEnd('"');
                            break;
                        }
                    case "/C":
                    case "--client":
                        {
                            client_id = args[item + 1];
                            client_id = client_id.TrimStart('"');
                            client_id = client_id.TrimEnd('"');
                            break;
                        }
                    case "/P":
                    case "--publish":
                        {
                            publishCount = Convert.ToInt32(args[item + 1]);
                            break;
                        }
                    case "/M":
                    case "--maximum":
                        {
                            maxNumber = Convert.ToInt32(args[item + 1]);
                            break;
                        }
                }
            }

            // Console.WriteLine("{0} starting", client_id);
            MqttQos Qos = ParseQos(qos);

			using (var numbersPersistence = new NumbersPersistence(client_id + ".sqlite3"))
			{
				using (var clientPersistence = new SqlitePersistence(numbersPersistence.conn))
				// using (var p = new InMemoryPersistence())
				{
					var client = new MqttClient(numbersPersistence, clientPersistence, topic_root, client_id, Qos, maxNumber);
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
