using System;
using StriderMqtt;
using System.Text;

namespace NumbersTest
{
	public class MqttClient
	{
		readonly TimeSpan PollLimit = TimeSpan.FromMilliseconds(200);

		MqttQos Qos;

		string ClientId;
		string TopicToPublish;
		string TopicToSubscribe;

		IMqttPersistence clientPersistence;
		NumbersPersistence numbersPersistence;

		// the previous number that was published
		int previousPublishedNumber;

		// last number to deliver (inclusive)
		int maxNumber;

		public MqttClient(NumbersPersistence numbersPersistence, IMqttPersistence clientPersistence, string topicRoot, string clientId, MqttQos qos, int maxNumber)
		{
			this.clientPersistence = clientPersistence;
			this.numbersPersistence = numbersPersistence;

			ClientId = clientId;
			TopicToPublish = topicRoot + "/" + clientId;
			TopicToSubscribe = topicRoot + "/#";
			Qos = qos;

			this.maxNumber = maxNumber;
			this.previousPublishedNumber = numbersPersistence.GetLastNumberSent();
		}

		
		public void Run()
		{

			var connArgs = new MqttConnectionArgs()
			{
				ClientId = this.ClientId,
				Hostname = "localhost",
				CleanSession = false,
				ProtocolVersion = MqttProtocolVersion.V3_1
			};

			using (var conn = new MqttConnection(connArgs, clientPersistence))
			{
				Console.WriteLine("{0} connected", ClientId);
				try
				{
					BindEvents(conn);

					if (!conn.IsSessionPresent)
					{
						Subscribe(conn);
					}

					while (conn.Loop(PollLimit))
					{
						if (Finished && !conn.IsPublishing)
						{
							break;
						}

						bool finishedPublishing = previousPublishedNumber >= maxNumber;
						bool canPublishNext = numbersPersistence.GetLastReceived(TopicToPublish) >= previousPublishedNumber;

						if (!conn.IsPublishing && !finishedPublishing && canPublishNext)
						{
							PublishNext(conn);
						}
					}
				}
				finally
				{
					UnbindEvents(conn);
				}
			}
		}

		void BindEvents(MqttConnection conn)
		{
			conn.PublishReceived += HandlePublishReceived;
			conn.PublishSent += HandlePublishSent;
		}

		void UnbindEvents(MqttConnection conn)
		{
			conn.PublishReceived -= HandlePublishReceived;
			conn.PublishSent -= HandlePublishSent;
		}

		// incoming publish events
		// =======================

		void HandlePublishReceived (object sender, PublishReceivedEventArgs e)
		{
			var number = ReadPayload(e.Message);
			numbersPersistence.RegisterReceivedNumber(e.Topic, number);

			Console.WriteLine("{0} << broker : {1} (topic:{2})", ClientId, number, e.Topic);

			if (e.Topic == TopicToPublish)
			{
				(sender as MqttConnection).InterruptLoop = true;
			}
		}

		// outgoing publish events
		// =======================

		void HandlePublishSent (object sender, IdentifiedPacketEventArgs e)
		{
			numbersPersistence.RegisterPublishedNumber(this.previousPublishedNumber);
			(sender as MqttConnection).InterruptLoop = true;
		}


		void PublishNext(MqttConnection conn)
		{
			int i;

			this.previousPublishedNumber = i = this.previousPublishedNumber + 1;
			byte[] bytes = MakePayload(i);

			if (Qos == MqttQos.AtMostOnce)
			{
				// if is qos 0, assume the number was published
				numbersPersistence.RegisterPublishedNumber(i);
			}

			ushort packetId = conn.Publish(TopicToPublish, bytes, Qos);

			Console.WriteLine("{0} >> broker : Delivering {1} (PUBLISH packet_id:{2})", ClientId, i, packetId);
		}

		void Subscribe(MqttConnection conn)
		{
			conn.Subscribe(TopicToSubscribe, Qos);
		}

		byte[] MakePayload(int i)
		{
			return Encoding.UTF8.GetBytes(i.ToString());
		}

		int ReadPayload(byte[] p)
		{
			return Int32.Parse(Encoding.UTF8.GetString(p));
		}

		public bool Finished
		{
			get
			{
				return FinishedPublishing && FinishedReceiving;
			}
		}

		public bool FinishedPublishing
		{
			get
			{
				return previousPublishedNumber >= maxNumber;
			}
		}

		public bool FinishedReceiving
		{
			get
			{
				return numbersPersistence.IsDoneReceiving(maxNumber);
			}
		}

	}
}

