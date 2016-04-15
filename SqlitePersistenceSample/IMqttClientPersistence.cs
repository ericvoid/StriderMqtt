using System;

namespace SqlitePersistenceSample
{
	public interface IMqttClientPersistence
	{
		void StoreIncomingMessage(ushort packetId);
		void ReleaseIncomingPacketId(ushort packetId);
		bool IsIncomingMessageRegistered(ushort packetId);

		void RegisterOutgoingMessage(OutgoingMessage outgoingMessage);
		OutgoingMessage GetPendingOutgoingMessage();

		void SetOutgoingMessageReceived(ushort packetId);
		void SetOutgoingMessageAcknowledged(ushort packetId);

		bool IsOutgoingQueueEmpty { get; }
	}
}

