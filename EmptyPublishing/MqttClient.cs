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

		ushort incomingPacketId = 0;

		Guid outgoingGuid;
		ushort outgoingPacketId = 0;
		bool outgoingPacketReceived = false;


		public bool IsPublishing
		{
			get
			{
				return this.outgoingPacketId > 0;
			}
		}

		public bool Finished
		{
			get
			{
				return this.published >= this.publishCount && this.received >= this.publishCount;
			}
		}


		public MqttClient(string topic, string clientId, MqttQos qos, int publishCount)
		{
			ClientId = clientId;
			TopicToPublish = topic;
			TopicToSubscribe = topic;
			Qos = qos;

			this.publishCount = publishCount;
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

			using (var conn = new MqttConnection(connArgs))
			{
				Console.WriteLine("{0} connected", ClientId);
				try
				{
					BindEvents(conn);

					if (conn.IsSessionPresent || this.outgoingPacketId > 0)
					{
						Redeliver(conn);
					}
					else
					{
						Subscribe(conn);
					}

					while (conn.Loop(PollLimit) && !Finished)
					{
						if (!IsPublishing)
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
			conn.Subscribe(new SubscribePacket()
			               {
				PacketId = conn.GetNextPacketId(),
				Topics = new string[] { TopicToSubscribe },
				QosLevels = new MqttQos[] { Qos }
			});
		}

		void PublishNext(MqttConnection conn)
		{
			this.outgoingGuid = Guid.NewGuid();
			byte[] bytes = outgoingGuid.ToByteArray();

			var publish = new PublishPacket() {
				QosLevel = Qos,
				Topic = TopicToPublish,
				Message = bytes
			};

			if (Qos == MqttQos.AtMostOnce)
			{
				// if is qos 0, assume the number was published
				this.published += 1;
			}
			else
			{
				var id = conn.GetNextPacketId();
				publish.PacketId = id;

				// for qos 1 and 2 only register an outgoing inflight message
				// must register BEFORE publish is sent
				this.outgoingPacketId = id;
				this.outgoingPacketReceived = false;
			}

			Console.WriteLine("{0} >> broker : Delivering {1} : PUBLISH packet_id:{2}",
			                  ClientId, this.outgoingGuid.ToString(), publish.PacketId);
			conn.Publish(publish);
		}

		void Redeliver(MqttConnection conn)
		{
			if (this.outgoingPacketId != 0)
			{
				byte[] bytes = outgoingGuid.ToByteArray();

				if (Qos == MqttQos.AtLeastOnce || (Qos == MqttQos.ExactlyOnce && !this.outgoingPacketReceived))
				{
					var publish = new PublishPacket() {
						PacketId = this.outgoingPacketId,
						QosLevel = Qos,
						Topic = TopicToPublish,
						Message = bytes,
						DupFlag = true
					};

					Console.WriteLine("{0} >> broker : Redelivering {1} : PUBLISH packet_id:{2}",
					                  ClientId, this.outgoingGuid.ToString(), this.outgoingPacketId);

					conn.Publish(publish);
				}
				else if (Qos == MqttQos.ExactlyOnce && this.outgoingPacketReceived)
				{
					Console.WriteLine("{0} >> broker : Redelivering {1} : PUBREL packet_id:{2}",
					                  ClientId, this.outgoingGuid.ToString(), this.outgoingPacketId);
					conn.Pubrel(this.outgoingPacketId);
				}
			}
		}


		// EVENTS
		// ==========


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
			if (e.Packet.QosLevel == MqttQos.ExactlyOnce)
			{
				if (incomingPacketId == e.Packet.PacketId)
				{
					// is duplicate, the connection class will send the pubrec
					return;
				}
				else if (incomingPacketId > 0)
				{
					throw new InvalidOperationException("Application does not support more than one inbound flow");
				}

				this.incomingPacketId = e.Packet.PacketId;
			}

			Console.WriteLine("{0} << broker : Received {1} (topic:{2})", ClientId, this.outgoingGuid.ToString(), e.Packet.Topic);

			// DO STUFF WITH e.Packet.Message;
			try
			{
				new Guid(e.Packet.Message);
			}
			catch (ArgumentNullException)
			{
				Console.WriteLine("    received message is empty");
			}
			catch (ArgumentException)
			{
				Console.WriteLine("    received message is not 16 bytes long");
			}

			if (e.Packet.QosLevel != MqttQos.ExactlyOnce)
			{
				this.received += 1;
			}
		}

		void HandlePubrelReceived (object sender, IdentifiedPacketEventArgs e)
		{
			if (e.PacketId != incomingPacketId)
			{
				throw new InvalidOperationException("Pubrel has unexpected packet id");
			}
			else
			{
				this.received += 1;

				// resets the incoming packet id
				this.incomingPacketId = 0;
			}
		}

		// outgoing publish events
		// =======================

		void HandlePubackReceived (object sender, IdentifiedPacketEventArgs e)
		{
			this.published += 1;
			this.outgoingPacketId = 0;
			this.outgoingPacketReceived = false;
			(sender as MqttConnection).InterruptLoop = true;
		}

		void HandlePubrecReceived (object sender, IdentifiedPacketEventArgs e)
		{
			this.outgoingPacketReceived = true;
		}

		void HandlePubcompReceived (object sender, IdentifiedPacketEventArgs e)
		{
			this.published += 1;
			this.outgoingPacketId = 0;
			this.outgoingPacketReceived = false;
			(sender as MqttConnection).InterruptLoop = true;
		}
	}
}
