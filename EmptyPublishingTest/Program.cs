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
			string qos = "0";
			string topic = "test";
			string client_id = "dotnet";
			int publishCount = 1;

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
                }
            }


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
