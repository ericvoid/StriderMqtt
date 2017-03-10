using System;

namespace StriderMqtt
{
	public class PublishReceivedEventArgs : EventArgs
	{
		private PublishPacket Packet { get; set; }

		/// <summary>
		/// The packet id of the received message.
		/// </summary>
		public ushort PacketId
		{
			get
			{
				return Packet.PacketId;
			}
		}

		/// <summary>
		/// A flag indicating if the message is duplicate.
		/// It is set to true when another trial was made previously.
		/// </summary>
		public bool DupFlag
		{
			get
			{
				return Packet.DupFlag;
			}
		}

		/// <summary>
		/// Quality of Service of the received message.
		/// </summary>
		public MqttQos QosLevel
		{
			get
			{
				return Packet.QosLevel;
			}
		}

		/// <summary>
		/// A flag indicating if the message is retained.
		/// </summary>
		public bool Retain
		{
			get
			{
				return Packet.Retain;
			}
		}

		/// <summary>
		/// The topic of the received message.
		/// </summary>
		public string Topic
		{
			get
			{
				return Packet.Topic;
			}
		}

		/// <summary>
		/// The Application Message received from broker.
		/// </summary>
		public byte[] Message
		{
			get
			{
				return Packet.Message;
			}
		}

		internal PublishReceivedEventArgs(PublishPacket packet) : base()
		{
			this.Packet = packet;
		}
	}

	public class IdentifiedPacketEventArgs : EventArgs
	{
		public ushort PacketId
		{
			get;
			private set;
		}

		internal IdentifiedPacketEventArgs(IdentifiedPacket packet)
		{
			this.PacketId = packet.PacketId;
		}
	}

	public class SubackReceivedEventArgs : EventArgs
	{
		public SubackReturnCode[] GrantedQosLevels
		{
			get;
			private set;
		}

		internal SubackReceivedEventArgs(SubackPacket packet)
		{
			this.GrantedQosLevels = packet.GrantedQosLevels;
		}
	}
}

