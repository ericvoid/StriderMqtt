using System;
using StriderMqtt;

namespace EmptyPublishing
{
	public class MqttClient
	{
		readonly TimeSpan PollLimit = TimeSpan.FromMilliseconds(50);

		MqttQos Qos;

		string ClientId;
		string TopicToPublish;
		string TopicToSubscribe;

		// last number to deliver (inclusive)
		int publishCount;
		int published = 0;
		int received = 0;

		IMqttPersistence persistence;

		public bool Finished
		{
			get
			{
				return this.published >= this.publishCount && this.received >= this.publishCount;
			}
		}

		public MqttClient(string topic, string clientId, MqttQos qos, int publishCount, IMqttPersistence persistence)
		{
			ClientId = clientId;
			TopicToPublish = topic;
			TopicToSubscribe = topic;
			Qos = qos;

			this.publishCount = publishCount;
			this.persistence = persistence;
		}

		public void Run()
		{
			var connArgs = new MqttConnectionArgs()
			{
				ClientId = this.ClientId,
				Hostname = "localhost",
				Port = 1883,
				Secure = true,
				CleanSession = false
			};

			using (var conn = new MqttConnection(connArgs, persistence))
			{
				Console.WriteLine("{0} connected", ClientId);
				try
				{
					BindEvents(conn);

					if (!conn.IsSessionPresent)
					{
						Subscribe(conn);
					}

					while (conn.Loop(PollLimit) && !Finished)
					{
						if (!conn.IsPublishing)
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

		void Subscribe(MqttConnection conn)
		{
			conn.Subscribe(TopicToSubscribe, Qos);
		}

		void PublishNext(MqttConnection conn)
		{
			byte[] bytes = new byte[0];

			if (Qos == MqttQos.AtMostOnce)
			{
				// if is qos 0, assume the packet will be delivered
				this.published += 1;
			}

			ushort packetId = conn.Publish(TopicToPublish, bytes, Qos);

			Console.WriteLine("{0} >> broker : Delivering {1} : PUBLISH packet_id:{2}",
				ClientId, this.published, packetId);
		}


		// EVENTS
		// ==========

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
			// do something with the received message
			received += 1;
			Console.WriteLine("{0} << broker : Received {1} (topic:{2}, mid:{3})", ClientId, received, e.Topic, e.PacketId);
			if (e.Message.Length > 0)
			{
				Console.WriteLine("    received message is not empty");
			}
		}


		// outgoing publish events
		// =======================

		void HandlePublishSent (object sender, IdentifiedPacketEventArgs e)
		{
			// a publish was sent to the broker
			this.published += 1;
			(sender as MqttConnection).InterruptLoop = true;
		}
	}
}
