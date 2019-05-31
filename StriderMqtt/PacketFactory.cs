using System;

namespace StriderMqtt
{
	internal class PacketFactory
	{
		internal static PacketBase GetInstance(byte packetTypeCode)
		{
			switch (packetTypeCode)
			{
				case ConnackPacket.PacketTypeCode:
					return new ConnackPacket();

				case PublishPacket.PacketTypeCode:
					return new PublishPacket();

				case PubackPacket.PacketTypeCode:
					return new PubackPacket();
				case PubrecPacket.PacketTypeCode:
					return new PubrecPacket();
				case PubrelPacket.PacketTypeCode:
					return new PubrelPacket();
				case PubcompPacket.PacketTypeCode:
					return new PubcompPacket();

				case SubackPacket.PacketTypeCode:
					return new SubackPacket();
				case UnsubackPacket.PacketTypeCode:
					return new UnsubackPacket();

				case PingrespPacket.PacketTypeCode:
					return new PingrespPacket();

				default:
					throw new MqttProtocolException(String.Format("Packet received with invalid code {0}", packetTypeCode.ToString("X")));
			}
		}

		internal static string GetPacketTypeName(byte packetTypeCode)
		{
			switch (packetTypeCode)
			{
				case ConnectPacket.PacketTypeCode:
					return "CONNECT";
				case ConnackPacket.PacketTypeCode:
					return "CONNACK";

				case PublishPacket.PacketTypeCode:
					return "PUBLISH";

				case PubackPacket.PacketTypeCode:
					return "PUBACK";
				case PubrecPacket.PacketTypeCode:
					return "PUBREC";
				case PubrelPacket.PacketTypeCode:
					return "PUBREL";
				case PubcompPacket.PacketTypeCode:
					return "PUBCOMP";

				case SubscribePacket.PacketTypeCode:
					return "SUBSCRIBE";
				case SubackPacket.PacketTypeCode:
					return "SUBACK";
				case UnsubscribePacket.PacketTypeCode:
					return "UNSUBSCRIBE";
				case UnsubackPacket.PacketTypeCode:
					return "UNSUBACK";

				case PingreqPacket.PacketTypeCode:
					return "PINREQ";
				case PingrespPacket.PacketTypeCode:
					return "PINGRESP";

				case DisconnectPacket.PacketTypeCode:
					return "DISCONNECT";

				default:
					return "???";
			}
		}
	}
}

