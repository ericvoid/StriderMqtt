using System;
using StriderMqtt;
using System.Text;

namespace SqlitePersistenceSample
{
	public class MqttClient
	{
		protected TimeSpan PollLimit;

		/// <summary>
		/// Gets or sets a value indicating whether there is a message being published publishing.
		/// This client assumes only one inflight message at a time.
		/// </summary>
		private bool IsPublishing { get; set; }

		string ClientId;

		IMqttClientPersistence persistence;

		public string HostName { get; set; }
		public bool CleanSession { get; set; }

		public bool KeepAlive { get; set; }

		public MqttClient(IMqttClientPersistence pers, string clientId)
		{
			persistence = pers;
			ClientId = clientId;

			this.KeepAlive = true;
			this.PollLimit = TimeSpan.FromMilliseconds(200);
		}

		public void Run()
		{
			var connArgs = new MqttConnectionArgs()
			{
				ClientId = this.ClientId,
				Hostname = this.HostName,
				CleanSession = this.CleanSession
			};

			using (var conn = new MqttConnection(connArgs))
			{
				try
				{
					BindEvents(conn);

					this.IsPublishing = false;
					this.OnConnect(conn);

					while (conn.Loop(PollLimit) && KeepAlive)
					{
						this.Loop(conn);
					}
				}
				finally
				{
					UnbindEvents(conn);
				}
			}
		}


		// virtual methods
		// ====================

		protected virtual void OnConnect(MqttConnection conn)
		{
			// session present is mqtt 3.1.1
			if (!conn.IsSessionPresent)
			{
				// here we can make initial subscriptions
				// Subscribe(conn, topics, qosLevels);
			}
			else
			{
				// tries to redeliver if that's the case
				OutgoingMessage message = persistence.GetPendingOutgoingMessage();

				if (message != null)
				{
					Redeliver(conn, message);
				}
			}
		}

		protected virtual void Loop(MqttConnection conn)
		{
			// a simple publishing rule assuming 1 inflight mesasge
			if (!IsPublishing && !persistence.IsOutgoingQueueEmpty)
			{
				// Publish(conn, topic, qos, message);
			}
		}

		protected virtual void ProcessIncomingPublish(PublishPacket packet)
		{
			// process the received message
			// read received message in `packet.Message`;
		}


		// protected methods

		protected void Publish(MqttConnection conn, String topic, MqttQos qos, byte[] payload)
		{
			OutgoingMessage message = new OutgoingMessage()
			{
				Topic = topic,
				Qos = qos,
				Payload = payload
			};

			if (message.Qos != MqttQos.AtMostOnce)
			{
				message.PacketId = conn.GetNextPacketId();

				// persistence needed only on qos levels 1 and 2
				persistence.RegisterOutgoingMessage(message);
			}

			this.IsPublishing = true;

			conn.Publish(new PublishPacket() {
				PacketId = message.PacketId,
				QosLevel = message.Qos,
				Topic = message.Topic,
				Message = message.Payload
			});
		}

		// sends a publish with dup flag in the case of a publish redelivery
		// or a pubrel in the case of qos2 message that we know was received by the broker
		protected void Redeliver(MqttConnection conn, OutgoingMessage message)
		{
			if (message.Qos == MqttQos.AtLeastOnce ||
			    (message.Qos == MqttQos.ExactlyOnce && !message.Received))
			{
				var publish = new PublishPacket() {
					PacketId = message.PacketId,
					QosLevel = message.Qos,
					Topic = message.Topic,
					Message = message.Payload,
					DupFlag = true
				};

				conn.Publish(publish);
				this.IsPublishing = true;
			}
			else if (message.Qos == MqttQos.ExactlyOnce && message.Received)
			{
				conn.Pubrel(message.PacketId);
				this.IsPublishing = true;
			}
		}

		protected void Subscribe(MqttConnection conn, string[] topics, MqttQos[] qosLevels)
		{
			conn.Subscribe(new SubscribePacket()
			{
				PacketId = conn.GetNextPacketId(),
				Topics = topics,
				QosLevels = qosLevels
			});
		}


		// EVENTS
		// =========

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

		void HandlePublishReceived(object sender, PublishReceivedEventArgs e)
		{
			if (e.Packet.QosLevel == MqttQos.ExactlyOnce && persistence.IsIncomingMessageRegistered(e.Packet.PacketId))
			{
				// it is a duplicate qos2 packet, we can ignore it
				return;
			}
			else
			{
				// `ProcessIncomingPublish` will handle the message itself
				ProcessIncomingPublish(e.Packet);

				if (e.Packet.QosLevel == MqttQos.ExactlyOnce)
				{
					// Register the incoming packetId, so duplicate messages can be filtered.
					// This is done after "ProcessIncomingPublish" because we can't assume the
					// mesage was received in the case that method throws an exception.
					persistence.StoreIncomingMessage(e.Packet.PacketId);

					// MqttConnection object will send the PubComp packet if this event finishes
					// without exceptions.
				}

				// in the case of qos 1, MqttConnection object will send the PubAck packet
				// if this event finishes without exceptions.
			}
		}

		void HandlePubrelReceived(object sender, IdentifiedPacketEventArgs e)
		{
			persistence.ReleaseIncomingPacketId(e.PacketId);

			// `MqttConnection` class will send the PubComp
		}


		// outgoing publish events
		// =======================

		void HandlePubackReceived(object sender, IdentifiedPacketEventArgs e)
		{
			// qos level 1 flow finished, clear persistence by packetId
			persistence.SetOutgoingMessageAcknowledged(e.PacketId);
			this.IsPublishing = false;

			// interrupts the receive loop to trigger next publish
			(sender as MqttConnection).InterruptLoop = true;
		}

		void HandlePubrecReceived(object sender, IdentifiedPacketEventArgs e)
		{
			// first round of qos 2 flow done.
			persistence.SetOutgoingMessageReceived(e.PacketId);

			// MqttConnection object will send the PubRel packet if this event finishes without exceptions.
			// If an error occur before PubComp is received, we can resume sending the PubRel packet.
		}

		void HandlePubcompReceived(object sender, IdentifiedPacketEventArgs e)
		{
			// qos level 2 flow finished, clear persistence by packetId
			persistence.SetOutgoingMessageAcknowledged(e.PacketId);
			this.IsPublishing = false;

			// interrupts the receive loop to trigger next publish
			(sender as MqttConnection).InterruptLoop = true;
		}

	}
}

