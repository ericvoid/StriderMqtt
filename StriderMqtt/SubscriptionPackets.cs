using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;

namespace StriderMqtt
{
	internal class SubscribePacket : IdentifiedPacket
    {
		internal const byte PacketTypeCode = 0x08;

		private const byte QosPartMask = 0x03;

        /// <summary>
        /// List of topics to subscribe
        /// </summary>
		internal string[] Topics { get; set; }

        /// <summary>
        /// List of QOS Levels related to topics
        /// </summary>
		internal MqttQos[] QosLevels { get; set; }


        internal SubscribePacket()
        {
            this.PacketType = PacketTypeCode;
        }

		internal override void Serialize(PacketWriter writer, MqttProtocolVersion protocolVersion)
		{
			Validate();

			if (protocolVersion == MqttProtocolVersion.V3_1_1)
			{
				writer.SetFixedHeader(PacketType, MqttQos.AtLeastOnce);
			}
			else
			{
				writer.SetFixedHeader(PacketType);
			}

			writer.AppendIntegerField(PacketId);

			for (int i = 0; i < Topics.Length; i++)
			{
				writer.AppendTextField(this.Topics[i]);
				writer.Append((byte)(((byte)this.QosLevels[i]) & QosPartMask));
			}
		}

		private void Validate()
		{
			if (Topics.Length != QosLevels.Length)
			{
				throw new ArgumentException("The length of Topics should match the length of QosLevels");
			}

			foreach (string topic in Topics)
			{
				if (String.IsNullOrEmpty(topic) || topic.Length > Packet.MaxTopicLength)
				{
					throw new ArgumentException("Invalid topic length");
				}
			}
		}

		internal override void Deserialize(PacketReader reader, MqttProtocolVersion protocolVersion)
		{
			throw new MqttProtocolException("Clients should not receive subscribe packets");
		}
    }


	internal class SubackPacket : IdentifiedPacket
	{
		internal const byte PacketTypeCode = 0x09;

		internal SubackReturnCode[] GrantedQosLevels
		{
			get;
			private set;
		}

		internal SubackPacket()
		{
			this.PacketType = PacketTypeCode;
		}


		internal override void Serialize(PacketWriter writer, MqttProtocolVersion protocolVersion)
		{
			throw new MqttProtocolException("Clients should not send unsuback packets");
		}

		internal override void Deserialize(PacketReader reader, MqttProtocolVersion protocolVersion)
		{
			if (protocolVersion == MqttProtocolVersion.V3_1_1)
			{
				if ((reader.FixedHeaderFirstByte & Packet.PacketFlagsBitMask) != Packet.ZeroedHeaderFlagBits)
				{
					throw new MqttProtocolException("Unsuback packet received with invalid header flags");
				}
			}

			this.PacketId = reader.ReadIntegerField();

			var bytes = reader.ReadToEnd();
			this.GrantedQosLevels = new SubackReturnCode[bytes.Length];

			for (int i = 0; i < bytes.Length; i++)
			{
				if (bytes[i] > (byte)SubackReturnCode.ExactlyOnceGranted && bytes[i] != (byte)SubackReturnCode.SubscriptionFailed)
				{
					throw new MqttProtocolException(String.Format("Invalid qos level '{0}' received from broker", bytes[i]));
				}

				this.GrantedQosLevels[i] = (SubackReturnCode)bytes[i];
			}
		}
	}


	internal class UnsubscribePacket : IdentifiedPacket
	{
		internal const byte PacketTypeCode = 0x0A;

		internal string[] Topics { get; set; }

		internal UnsubscribePacket()
		{
			this.PacketType = PacketTypeCode;
		}

		internal override void Serialize(PacketWriter writer, MqttProtocolVersion protocolVersion)
		{
			if (protocolVersion == MqttProtocolVersion.V3_1_1)
			{
				writer.SetFixedHeader(PacketType, MqttQos.AtLeastOnce);
			}
			else
			{
				writer.SetFixedHeader(PacketType);
			}

			writer.AppendIntegerField(PacketId);

			foreach (string topic in this.Topics)
			{
				writer.AppendTextField(topic);
			}
		}

		internal override void Deserialize(PacketReader reader, MqttProtocolVersion protocolVersion)
		{
			throw new MqttProtocolException("Clients should not send unsubscribe packets");
		}
	}


	internal class UnsubackPacket : IdentifiedPacket
	{
		internal const byte PacketTypeCode = 0x0B;

		internal UnsubackPacket()
		{
			this.PacketType = PacketTypeCode;
		}

		internal override void Serialize(PacketWriter writer, MqttProtocolVersion protocolVersion)
		{
			throw new MqttProtocolException("Clients should not send unsuback packets");
		}

		internal override void Deserialize(PacketReader reader, MqttProtocolVersion protocolVersion)
		{
			if (protocolVersion == MqttProtocolVersion.V3_1_1)
			{
				if ((reader.FixedHeaderFirstByte & Packet.PacketFlagsBitMask) != Packet.ZeroedHeaderFlagBits)
				{
					throw new MqttProtocolException("Unsuback packet received with invalid header flags");
				}
			}

			if (reader.RemainingLength != 2)
			{
				throw new MqttProtocolException("Unsuback packet received with invalid remaining length");
			}

			this.PacketId = reader.ReadIntegerField();
		}
	}
}
