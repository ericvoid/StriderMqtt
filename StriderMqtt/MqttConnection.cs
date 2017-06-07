using System;
using System.Net.Sockets;
using System.Net.Security;
using System.Threading;

namespace StriderMqtt
{
	public class MqttConnection : IDisposable
	{
        /// <summary>
        /// Defines a maximum time for polling, to give opportunity for
        /// loop cancelation, pingreq sending, and so on.
        /// </summary>
        static readonly TimeSpan MaxPollTime = TimeSpan.FromSeconds(0.1);

		private MqttConnectionArgs ConnectionArgs;
		private IMqttTransport Transport;

		private IMqttPersistence Persistence;

		// all in ms
		private int Keepalive;
		private int LastRead;
		private int LastWrite;

		public MqttProtocolVersion ProtocolVersion
		{
			get; private set;
		}

		/// <summary>
		/// Gets a value indicating whether a session in the broker is present.
		/// </summary>
		public bool IsSessionPresent { get; private set; }

		/// <summary>
		/// Gets a value indicating if there is an outgoing publish in the current connection.
		/// </summary>
		public bool IsPublishing { get; private set; }

		/// <summary>
		/// Set `true` to interrupt the `Loop` method.
		/// When Loop is called, InterruptLoop is automatically set to `false`.
		/// If you handle some event (specially Puback, Pubcomp, Suback and Unsuback)
		/// and you want to stop the Loop method to get the control back, set this field to `true`.
		/// </summary>
		bool IsInterruptLoopRequested { get; set; }

		private int inLoop = 0;

		#region events
		/// <summary>
		/// Occurs when a Publish packet is received from broker.
		/// </summary>
		public event EventHandler<PublishReceivedEventArgs> PublishReceived;

		/// <summary>
		/// Occurs when a Puback packet is received from broker.
		/// </summary>
		public event EventHandler<IdentifiedPacketEventArgs> PubackReceived;

		/// <summary>
		/// Occurs when Pubrec packet is received from broker.
		/// The Pubrel response is sent automatically if the event handlers
		/// succeed.
		/// </summary>
		public event EventHandler<IdentifiedPacketEventArgs> PubrecReceived;

		/// <summary>
		/// Occurs when a Pubrel packet received from broker.
		/// The Pubcomp response is sent automatically if the event handlers
		/// succeed.
		/// </summary>
		public event EventHandler<IdentifiedPacketEventArgs> PubrelReceived;

		/// <summary>
		/// Occurs when a Pubcomp packet is received from broker.
		/// </summary>
		public event EventHandler<IdentifiedPacketEventArgs> PubcompReceived;

		/// <summary>
		/// Occurs when a publish is sent to the broker.
		/// </summary>
		public event EventHandler<IdentifiedPacketEventArgs> PublishSent;

		/// <summary>
		/// Occurs when a Suback packet is received from broker.
		/// </summary>
		public event EventHandler<SubackReceivedEventArgs> SubackReceived;

		/// <summary>
		/// Occurs when a Unsuback packet is received from broker.
		/// </summary>
		public event EventHandler<EventArgs> UnsubackReceived;
		#endregion


		bool ReadWaitExpired
		{
			get
			{
				if (Keepalive > 0)
				{
					return Environment.TickCount - LastRead > (Keepalive * 1.5);
				}
				else
				{
					return false;
				}
			}
		}

		bool WriteWaitExpired
		{
			get
			{
				if (Keepalive > 0)
				{
					return Environment.TickCount - LastWrite > Keepalive;
				}
				else
				{
					return false;
				}
			}
		}

		bool WasConnectCalled;
		bool IsConnected;
		bool IsDisconnected;
		bool IsDisposed;


		public MqttConnection(MqttConnectionArgs args, IMqttPersistence persistence=null, IMqttTransport customTransport=null)
		{
			if (args == null)
			{
				throw new ArgumentNullException("args");
			}

			if (args.Keepalive.TotalSeconds < 0 || args.Keepalive.TotalSeconds > ushort.MaxValue)
			{
				throw new ArgumentException("Keepalive should be between 0 seconds and ushort.MaxValue (18 hours)");
			}

			this.Persistence = persistence ?? new InMemoryPersistence();

			this.Keepalive = (int)args.Keepalive.TotalMilliseconds; // converts to milliseconds
			this.IsPublishing = false;

			this.ProtocolVersion = args.ProtocolVersion;

			this.ConnectionArgs = args;
			this.Transport = customTransport;
		}

		public void Connect()
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (WasConnectCalled)
			{
				throw new InvalidOperationException("Connect was already called");
			}

			WasConnectCalled = true;

			if (Transport == null)
			{
				InitTransport();
			}

			Send(MakeConnectMessage());
			ReceiveConnack();

			if (ConnectionArgs.CleanSession)
			{
				Persistence.Clear();
			}
			else
			{
				ResumeOutgoingFlows();
			}

			IsConnected = true;
		}

		private void InitTransport()
		{
			if (ConnectionArgs.Secure)
			{
				var tlsTransport = new TlsTransport(ConnectionArgs.Hostname, ConnectionArgs.Port);
				tlsTransport.SetTimeouts(ConnectionArgs.ReadTimeout, ConnectionArgs.WriteTimeout);
				Transport = tlsTransport;
			}
			else
			{
				var tcpTransport = new TcpTransport(ConnectionArgs.Hostname, ConnectionArgs.Port);
				tcpTransport.SetTimeouts(ConnectionArgs.ReadTimeout, ConnectionArgs.WriteTimeout);
				Transport = tcpTransport;
			}
		}

		private ConnectPacket MakeConnectMessage()
		{
			var conn = new ConnectPacket();
			conn.ProtocolVersion = ConnectionArgs.ProtocolVersion;

			conn.ClientId = ConnectionArgs.ClientId;
			conn.Username = ConnectionArgs.Username;
			conn.Password = ConnectionArgs.Password;

			if (ConnectionArgs.WillMessage != null)
			{
				conn.WillFlag = true;
				conn.WillTopic = ConnectionArgs.WillMessage.Topic;
				conn.WillMessage = ConnectionArgs.WillMessage.Message;
				conn.WillQosLevel = ConnectionArgs.WillMessage.Qos;
				conn.WillRetain = ConnectionArgs.WillMessage.Retain;
			}

			conn.CleanSession = ConnectionArgs.CleanSession;
			conn.KeepAlivePeriod = (ushort)ConnectionArgs.Keepalive.TotalSeconds;

			return conn;
		}

		private void ReceiveConnack()
		{
			PacketBase packet = ReadPacket();
			this.LastRead = Environment.TickCount;

			var connack = packet as ConnackPacket;

			if (packet == null)
			{
				throw new MqttProtocolException(String.Format("First received message should be Connack, but {0} received instead", packet.GetType().Name));
			}

			if (connack.ReturnCode != ConnackReturnCode.Accepted)
			{
				throw new MqttConnectException("The connection was not accepted", connack.ReturnCode);
			}

			this.IsSessionPresent = connack.SessionPresent;
		}

		private void ResumeOutgoingFlows()
		{
			// tries to redeliver if that's the case
			foreach (var flow in Persistence.GetPendingOutgoingFlows())
			{
				Resume(flow);
			}
		}

		// sends a publish with dup flag in the case of a publish redelivery
		// or a pubrel in the case of qos2 message that we know was received by the broker
		private void Resume(OutgoingFlow flow)
		{
			if (flow.Qos == MqttQos.AtLeastOnce ||
			    (flow.Qos == MqttQos.ExactlyOnce && !flow.Received))
			{
				var publish = new PublishPacket() {
					PacketId = flow.PacketId,
					Retain = flow.Retain,
					QosLevel = flow.Qos,
					Topic = flow.Topic,
					Message = flow.Payload,
					DupFlag = true
				};

				Publish(publish);
			}
			else if (flow.Qos == MqttQos.ExactlyOnce && flow.Received)
			{
				Pubrel(flow.PacketId);
			}

			Persistence.LastOutgoingPacketId = flow.PacketId;
		}

		public ushort Publish(string topic, byte[] message, MqttQos qos=MqttQos.AtMostOnce, bool retained=false)
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (!IsConnected)
			{
				throw new InvalidOperationException("Not connected");
			}

			return Publish(new PublishPacket()
			{
				Topic = topic,
				Message = message,
				QosLevel = qos,
				Retain = retained
			});
		}

		/// <summary>
		/// Publishes the given packet to the broker.
		/// </summary>
		/// <param name="packet">Packet.</param>
		private ushort Publish(PublishPacket packet)
		{
			if (packet.QosLevel != MqttQos.AtMostOnce)
			{
				if (packet.PacketId == 0)
				{
					packet.PacketId = this.GetNextPacketId();
				}

				// persistence needed only on qos levels 1 and 2
				Persistence.RegisterOutgoingFlow(new OutgoingFlow()
                {
					PacketId = packet.PacketId,
					Retain = packet.Retain,
					Topic = packet.Topic,
					Qos = packet.QosLevel,
					Payload = packet.Message
				});
			}

			try
			{
				IsPublishing = true;
				Send(packet);

				return packet.PacketId;
			}
			catch
			{
				IsPublishing = false;
				throw;
			}
		}

		/// <summary>
		/// Sends a Pubrel packet to the broker with the given packetId.
		/// This method is intended for resuming a QoS 2 flow (when a pubrel was sent but the pubcomp packet wasn't received).
		/// </summary>
		/// <remarks>
		/// The client automatically sends the pubrel when a pubrec is received (and the `PubrecReceived` event completes without any error),
		/// so there is no need to explicitly call the `Pubrel` method in this case.
		/// </remarks>
		/// <param name="packetId">Packet identifier.</param>
		private void Pubrel(ushort packetId)
		{
			try
			{
				IsPublishing = true;
				Send(new PubrelPacket() { PacketId = packetId });
			}
			catch
			{
				IsPublishing = false;
				throw;
			}
		}

		public void Subscribe(string topic, MqttQos qosLevel)
		{
			Subscribe(new string[] { topic }, new MqttQos[] { qosLevel });
		}

		public void Subscribe(string[] topics, MqttQos[] qosLevels)
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (!IsConnected)
			{
				throw new InvalidOperationException("Not connected");
			}

			SubscribePacket packet = new SubscribePacket()
			{
				Topics = topics,
				QosLevels = qosLevels
			};

			if (packet.PacketId == 0)
			{
				packet.PacketId = this.GetNextPacketId();
			}

			Send(packet);
		}

		public void Unsubscribe(string topic)
		{
			Unsubscribe(new string[] { topic });
		}

		public void Unsubscribe(string[] topics)
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (!IsConnected)
			{
				throw new InvalidOperationException("Not connected");
			}

			UnsubscribePacket packet = new UnsubscribePacket()
			{
				Topics = topics
			};

			if (packet.PacketId == 0)
			{
				packet.PacketId = this.GetNextPacketId();
			}

			Send(packet);
		}


		private void Send(PacketBase packet)
		{
			if (Transport.IsClosed)
			{
				throw new MqttClientException("Transport is closed");
			}

			WritePacket(packet);
			LastWrite = Environment.TickCount;
		}

		private ushort GetNextPacketId()
		{
			ushort x = Persistence.LastOutgoingPacketId;
			if (x == Packet.MaxPacketId)
			{
				Persistence.LastOutgoingPacketId = 1;
				return 1;
			}
			else
			{
				x += 1;
				Persistence.LastOutgoingPacketId = x;
				return x;
			}
		}

		// -- transport reading and writing --

		PacketBase ReadPacket()
		{
			using(var reader = new PacketReader(Transport.Stream))
			{
				var packet = PacketFactory.GetInstance(reader.PacketTypeCode);
				packet.Deserialize(reader, ProtocolVersion);

				return packet;
			}
		}

		void WritePacket(PacketBase packet)
		{
			using (var writer = new PacketWriter())
			{
				packet.Serialize(writer, ProtocolVersion);
				writer.WriteTo(Transport.Stream);
			}
		}

        /// <summary>
        /// Interrupts the current loop in the `Loop` method.
        /// If you handle some event (specially Puback, Pubcomp, Suback and Unsuback)
        /// and you want to stop the Loop method to get the control back, then call this method.
		/// It can have no effect when called outside the event handlers.
        /// </summary>
        public void InterruptLoop()
        {
            IsInterruptLoopRequested = true;
        }


		/// <summary>
		/// Loop to receive packets. Use e
		/// The method exits when readLimit is reached or when an
		/// outbound flow completes. This
		/// 
		/// Throws MqttTimeoutException if keepalive period expires.
		/// </summary>
		/// <param name="readLimit">Read limit in milliseconds.</param>
		/// <returns>Returns true if is connected, false otherwise.</returns>
		public bool Loop(int readLimit)
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (!IsConnected)
			{
				throw new InvalidOperationException("Not connected");
			}

			if (readLimit < 0)
			{
				throw new ArgumentException("Poll limit should be positive");
			}

			// Loop shouldn't be called concurrently or recursivelly
			if(Interlocked.CompareExchange(ref inLoop, 1, 0) == 1)
			{
				throw new InvalidProgramException("Recursive call to Loop method not allowed");
			}

			try
			{
				int readThreshold = Environment.TickCount + readLimit;
				int pollTime;

				IsInterruptLoopRequested = false;

				while ((pollTime = readThreshold - Environment.TickCount) > 0 && !IsInterruptLoopRequested)
				{
					if (Transport.IsClosed)
					{
						return false;
					}
					else if (Poll(pollTime))
					{
						ReceivePacket();
					}
					else if (WriteWaitExpired)
					{
						Send(new PingreqPacket());
					}
					else if (ReadWaitExpired)
					{
						throw new MqttTimeoutException();
					}
				}

				return !Transport.IsClosed;
			}
			finally
			{
				inLoop = 0;
			}
		}

        /// <summary>
        /// Loop that tries to receive packets.
        /// Returns true if is connected, false otherwise.
        /// Throws MqttTimeoutException if keepalive period expires.
        /// </summary>
        /// <param name="readLimit">Poll limit TimeSpan.</param>
        public bool Loop(TimeSpan readLimit)
		{
			if (readLimit.TotalMilliseconds > Int32.MaxValue)
			{
				throw new ArgumentException("Read limit total milliseconds should be less than Int32 max value");
			}

			return Loop((int)readLimit.TotalMilliseconds);
		}

		/// <summary>
		/// Tries to receive packets, reading for `Keepalive` duration
		/// </summary>
		public bool Loop()
		{
			return Loop(Keepalive);
		}


		bool Poll(int pollTime)
		{
            return Transport.Poll(Math.Min(pollTime, (int)MaxPollTime.TotalMilliseconds));
		}


		private void ReceivePacket()
		{
			PacketBase packet = ReadPacket();
			LastRead = Environment.TickCount;

			HandleReceivedPacket(packet);
		}

		void HandleReceivedPacket(PacketBase packet)
		{
			switch (packet.PacketType)
			{
				case PublishPacket.PacketTypeCode:
					OnPublishReceived(packet as PublishPacket);
					break;
				case PubackPacket.PacketTypeCode:
					OnPubackReceived(packet as PubackPacket);
					break;
				case PubrecPacket.PacketTypeCode:
					OnPubrecReceived(packet as PubrecPacket);
					break;
				case PubrelPacket.PacketTypeCode:
					OnPubrelReceived(packet as PubrelPacket);
					break;
				case PubcompPacket.PacketTypeCode:
					OnPubcompReceived(packet as PubcompPacket);
					break;
				case SubackPacket.PacketTypeCode:
					OnSubackReceived(packet as SubackPacket);
					break;
				case UnsubackPacket.PacketTypeCode:
					OnUnsubackReceived(packet as UnsubackPacket);
					break;
				case PingrespPacket.PacketTypeCode:
					break;
				default:
					throw new MqttProtocolException(String.Format("Cannot receive message of type {0}", packet.GetType().Name));
			}
		}


		// -- incoming publish events --

		void OnPublishReceived(PublishPacket packet)
		{
			if (packet.QosLevel == MqttQos.ExactlyOnce)
			{
				OnQos2PublishReceived(packet);
			}
			else
			{
				if (PublishReceived != null)
				{
					PublishReceived(this, new PublishReceivedEventArgs(packet));
				}

				if (packet.QosLevel == MqttQos.AtLeastOnce)
				{
					Send(new PubackPacket() { PacketId = packet.PacketId });
				}
			}
		}

		void OnQos2PublishReceived(PublishPacket packet)
		{
			if (!Persistence.IsIncomingFlowRegistered(packet.PacketId))
			{
				if (PublishReceived != null)
				{
					PublishReceived(this, new PublishReceivedEventArgs(packet));
				}

				// Register the incoming packetId, so duplicate messages can be filtered.
				// This is done after "ProcessIncomingPublish" because we can't assume the
				// mesage was received in the case that method throws an exception.
				Persistence.RegisterIncomingFlow(packet.PacketId);

				// the ideal would be to run `PubishReceived` and `Persistence.RegisterIncomingFlow`
				// in a single transaction (either both or neither succeeds).
			}

			Send(new PubrecPacket() { PacketId = packet.PacketId });
		}

		void OnPubrelReceived(PubrelPacket packet)
		{
			if (PubrelReceived != null)
			{
				PubrelReceived(this, new IdentifiedPacketEventArgs(packet));
			}

			Persistence.ReleaseIncomingFlow(packet.PacketId);

			Send(new PubcompPacket() { PacketId = packet.PacketId });
		}


		// -- outgoing publish events --

		void OnPubackReceived(PubackPacket packet)
		{
			if (PubackReceived != null)
			{
				PubackReceived(this, new IdentifiedPacketEventArgs(packet));
			}

			if (PublishSent != null)
			{
				PublishSent(this, new IdentifiedPacketEventArgs(packet));
			}

			Persistence.SetOutgoingFlowCompleted(packet.PacketId);

			this.IsPublishing = false;
		}

		void OnPubrecReceived(PubrecPacket packet)
		{
			if (PubrecReceived != null)
			{
				PubrecReceived(this, new IdentifiedPacketEventArgs(packet));
			}

			Persistence.SetOutgoingFlowReceived(packet.PacketId);

			Send(new PubrelPacket() { PacketId = packet.PacketId });
		}

		void OnPubcompReceived(PubcompPacket packet)
		{
			if (PubcompReceived != null)
			{
				PubcompReceived(this, new IdentifiedPacketEventArgs(packet));
			}

			if (PublishSent != null)
			{
				PublishSent(this, new IdentifiedPacketEventArgs(packet));
			}

			Persistence.SetOutgoingFlowCompleted(packet.PacketId);

			this.IsPublishing = false;
		}


		// -- subscription events --

		void OnSubackReceived(SubackPacket packet)
		{
			if (SubackReceived != null)
			{
				SubackReceived(this, new SubackReceivedEventArgs(packet));
			}
		}

		void OnUnsubackReceived(UnsubackPacket packet)
		{
			if (UnsubackReceived != null)
			{
				UnsubackReceived(this, EventArgs.Empty);
			}
		}

		public void Disconnect()
		{
			if (IsDisposed)
			{
				throw new ObjectDisposedException("MqttConnection");
			}
			else if (!IsConnected)
			{
				throw new InvalidOperationException("Not connected");
			}

			if (!IsDisconnected)
			{
				IsDisconnected = true;
				Send(new DisconnectPacket());
			}
		}

		public void Dispose()
		{
			IsDisposed = true;

			// if the transport was created by the application, leave the application
			// clean it up.
			if (Transport != null && Transport is IInternalTransport)
			{
				(Transport as IInternalTransport).Dispose();
			}

			try
			{
				if (ConnectionArgs.CleanSession)
				{
					Persistence.Clear();
				}
			}
			catch
			{
				// The persistence should be cleared when CleanSession is set
				// but it is bad to throw exceptions from the Dispose method.
				// It also shouldn't be cleared in Disconnect, because even
				// when the connection drops, the session must me cleared.
			}
		}
	}


	public class MqttConnectionArgs
	{
		public string Hostname { get; set; }
		public int Port { get; set; }

		public bool Secure { get; set; }

		public MqttProtocolVersion ProtocolVersion { get; set; }

		public string ClientId { get; set; }
		public string Username { get; set; }
		public string Password { get; set; }

		public bool CleanSession { get; set; }

		public TimeSpan Keepalive { get; set; }

		public WillMessage WillMessage { get; set; }

		public TimeSpan ReadTimeout { get; set; }
		public TimeSpan WriteTimeout { get; set; }

		public MqttConnectionArgs ()
		{
			this.ProtocolVersion = MqttProtocolVersion.V3_1_1;
			this.Keepalive = TimeSpan.FromSeconds(60);

			this.Port = 1883;
			this.CleanSession = true;

			this.ReadTimeout = TimeSpan.FromSeconds(10);
			this.WriteTimeout = TimeSpan.FromSeconds(10);
		}
	}

	public class WillMessage
	{
		public string Topic { get; set; }
		public byte[] Message { get; set; }
		public MqttQos Qos { get; set; }
		public bool Retain { get; set; }
	}
}

