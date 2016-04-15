using System;
using StriderMqtt;
using System.Text;

namespace NumbersTest
{
	public class MqttClient
	{
		readonly TimeSpan PollLimit = TimeSpan.FromMilliseconds(200);

		bool IsPublishing; // there is an outgoing message

		MqttQos Qos;

		string ClientId;
		string TopicToPublish;
		string TopicToSubscribe;

		IPersistence persistence;

		// the previous number that was published
		int previousPublishedNumber;

		// last number to deliver (inclusive)
		int maxNumber;

		string lastReceivingTopic;

		public MqttClient(IPersistence pers, string topicRoot, string clientId, MqttQos qos, int maxNumber)
		{
			persistence = pers;

			ClientId = clientId;
			TopicToPublish = topicRoot + "/" + clientId;
			TopicToSubscribe = topicRoot + "/#";
			Qos = qos;

			this.maxNumber = maxNumber;
			this.previousPublishedNumber = persistence.GetLastNumberSent();
		}

		
		public void Run()
		{
			IsPublishing = false;

			var connArgs = new MqttConnectionArgs()
			{
				ClientId = this.ClientId,
				Hostname = "localhost",
				CleanSession = false,
				Version = MqttProtocolVersion.V3_1
			};

			using (var conn = new MqttConnection(connArgs))
			{
				Console.WriteLine("{0} connected", ClientId);
				try
				{
					BindEvents(conn);

					if (conn.IsSessionPresent)
					{
						Redeliver(conn);
					}
					else
					{
						Subscribe(conn);
					}

					while (conn.Loop(PollLimit) && !Finished)
					{
						bool finishedPublishing = previousPublishedNumber >= maxNumber;
						bool canPublishNext = persistence.GetLastReceived(TopicToPublish) >= previousPublishedNumber;

						if (!IsPublishing && !finishedPublishing && canPublishNext)
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
			conn.PubackReceived += HandlePubackReceived;
			conn.PubrecReceived += HandlePubrecReceived;
			conn.PubrelReceived += HandlePubrelReceived;
			conn.PubcompReceived += HandlePubcompReceived;
		}

		void UnbindEvents(MqttConnection conn)
		{
			conn.PublishReceived -= HandlePublishReceived;
			conn.PubackReceived -= HandlePubackReceived;
			conn.PubrecReceived -= HandlePubrecReceived;
			conn.PubrelReceived -= HandlePubrelReceived;
			conn.PubcompReceived -= HandlePubcompReceived;
		}

		// incoming publish events
		// =======================

		void HandlePublishReceived (object sender, PublishReceivedEventArgs e)
		{
			if (e.Packet.QosLevel == MqttQos.ExactlyOnce && persistence.IsIncomingMessageRegistered(e.Packet.PacketId))
			{
				return;
			}

			var number = ReadPayload(e.Packet.Message);
			persistence.StoreIncomingMessage(e.Packet.PacketId, e.Packet.Topic, e.Packet.QosLevel, number);
			Console.WriteLine("{0} << broker : {1} (topic:{2})", ClientId, number, e.Packet.Topic);

			if (Qos == MqttQos.AtLeastOnce && e.Packet.Topic == TopicToPublish)
			{
				(sender as MqttConnection).InterruptLoop = true;
			}
			else if (Qos == MqttQos.ExactlyOnce)
			{
				lastReceivingTopic = e.Packet.Topic;
			}
		}

		void HandlePubrelReceived (object sender, IdentifiedPacketEventArgs e)
		{
			persistence.ReleaseIncomingPacketId(e.PacketId);

			if (lastReceivingTopic == TopicToPublish)
			{
				(sender as MqttConnection).InterruptLoop = true;
			}
		}

		// outgoing publish events
		// =======================

		void HandlePubackReceived (object sender, IdentifiedPacketEventArgs e)
		{
			persistence.SetOutgoingMessageAcknowledged(e.PacketId);
			this.IsPublishing = false;
			(sender as MqttConnection).InterruptLoop = true;
		}

		void HandlePubrecReceived (object sender, IdentifiedPacketEventArgs e)
		{
			persistence.SetOutgoingMessageReceived(e.PacketId);
		}

		void HandlePubcompReceived (object sender, IdentifiedPacketEventArgs e)
		{
			persistence.SetOutgoingMessageAcknowledged(e.PacketId);
			this.IsPublishing = false;
			(sender as MqttConnection).InterruptLoop = true;
		}


		void Redeliver(MqttConnection conn)
		{
			OutgoingMessage message = persistence.GetPendingOutgoingMessage();

			if (message == null)
			{
				return;
			}

			byte[] bytes = MakePayload(message.Number);

			if (Qos == MqttQos.AtLeastOnce ||
			    (Qos == MqttQos.ExactlyOnce && !message.Received))
			{
				var publish = new PublishPacket() {
					PacketId = message.PacketId,
					QosLevel = Qos,
					Topic = TopicToPublish,
					Message = bytes,
					DupFlag = true
				};

				Console.WriteLine("{0} >> broker : Redelivering {1} (PUBLISH packet_id:{2})", ClientId, message.Number, message.PacketId);

				conn.Publish(publish);
				this.IsPublishing = true;
			}
			else if (Qos == MqttQos.ExactlyOnce && message.Received)
			{
				Console.WriteLine("{0} >> broker : Redelivering {1} (PUBREL packet_id:{2})", ClientId, message.Number, message.PacketId);
				conn.Pubrel(message.PacketId);
				this.IsPublishing = true;
			}
		}

		void PublishNext(MqttConnection conn)
		{
			int i;

			this.previousPublishedNumber = i = this.previousPublishedNumber + 1;
			byte[] bytes = MakePayload(i);

			var publish = new PublishPacket() {
				QosLevel = Qos,
				Topic = TopicToPublish,
				Message = bytes
			};

			if (Qos == MqttQos.AtMostOnce)
			{
				// if is qos 0, assume the number was published
				persistence.RegisterPublishedNumber(i);
			}
			else
			{
				publish.PacketId = conn.GetNextPacketId();
				this.IsPublishing = true;

				// for qos 1 and 2 only register an outgoing inflight message
				// must register BEFORE publish is sent
				persistence.RegisterOutgoingMessage(new OutgoingMessage()
                {
					PacketId = publish.PacketId,
					Number = i
				});
			}

			Console.WriteLine("{0} >> broker : Delivering {1} (PUBLISH packet_id:{2})", ClientId, i, publish.PacketId);
			conn.Publish(publish);
		}

		void Subscribe(MqttConnection conn)
		{
			conn.Subscribe(new SubscribePacket()
            {
				PacketId = conn.GetNextPacketId(),
				Topics = new string[] { TopicToSubscribe },
				QosLevels = new MqttQos[] { Qos }
			});
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
				return FinishedPublishing && FinishedReceiving && !IsPublishing;
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
				return persistence.IsDoneReceiving(maxNumber);
			}
		}

	}
}

