using System;
using StriderMqtt;

namespace NumbersTest
{
	public interface IPersistence : IDisposable
	{
		/// <summary>
		/// Stores an incoming message in persistence.
		/// In case of QoS level 2, the packet id is registered in
		/// an incoming inflight set, so duplicates can be avoided.
		/// </summary>
		void StoreIncomingMessage(ushort packetId, string topic, MqttQos qos, int number);

		/// <summary>
		/// Releases an entry in the incoming inflight set by packet identifier.
		/// Expected to be called when a Pubrel is received.
		/// </summary>
		void ReleaseIncomingPacketId(ushort packetId);

		/// <summary>
		/// Determines whether the packet id is registered in the incoming inflight set.
		/// In the case of QoS 2 flow, this method determines wether a duplicate message
		/// should be received (if not in incoming set) or ignored (if present in incoming set).
		/// </summary>
		bool IsIncomingMessageRegistered(ushort packetId);

		/// <summary>
		/// Gets the last received number by the given topic.
		/// </summary>
		int GetLastReceived(string topic);

		/// <summary>
		/// Determines whether all numbers were received by all topics.
		/// </summary>
		bool IsDoneReceiving(int maxNumber);


		/// <summary>
		/// In the case of qos 0, registers the message in the published numbers list.
		/// Otherwise the message is stored in the outgoing inflight messages queue.
		/// </summary>
		void RegisterOutgoingMessage(OutgoingMessage outgoingMessage);

		/// <summary>
		/// Gets the last number sent, in the published packets list.
		/// </summary>
		/// <returns>The last number sent.</returns>
		int GetLastNumberSent();

		/// <summary>
		/// Gets a message in the outgoing inflight messages queue.
		/// Returns null if there isn't any message in the queue.
		/// </summary>
		/// <returns>The pending outgoing message.</returns>
		OutgoingMessage GetPendingOutgoingMessage();

		/// <summary>
		/// Marks the outgoing message (in the outgoing inflight queue) as "received" by the broker.
		/// Should be called when a Pubrec is received.
		/// </summary>
		/// <param name="packetId">Packet identifier.</param>
		void SetOutgoingMessageReceived(ushort packetId);

		/// <summary>
		/// Removes the message from the outgoing inflight queue,
		/// and stores the related number in the published numbers list.
		/// </summary>
		/// <param name="packetId">Packet identifier.</param>
		void SetOutgoingMessageAcknowledged(ushort packetId);

		void RegisterPublishedNumber(int n);
	}
}

