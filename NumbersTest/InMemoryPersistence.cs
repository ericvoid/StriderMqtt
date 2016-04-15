using System;
using System.Collections.Generic;
using StriderMqtt;
using System.Linq;

namespace NumbersTest
{
	public class InMemoryPersistence : IPersistence
	{
		List<ushort> incomingPacketIds = new List<ushort>();
		Dictionary<string, List<int>> receivedNumbers = new Dictionary<string, List<int>>();

		List<OutgoingMessage> outgoingMessages = new List<OutgoingMessage>();
		int lastNumberSent;
		List<int> publishedNumbers = new List<int>();

		public InMemoryPersistence()
		{
		}

		public void StoreIncomingMessage(ushort packetId, string topic, MqttQos qos, int number)
		{
			if (qos == MqttQos.ExactlyOnce)
			{
				incomingPacketIds.Add(packetId);
			}

			RegisterReceivedNumber(topic, number);
		}

		void RegisterReceivedNumber(string topic, int number)
		{
			if (!receivedNumbers.ContainsKey(topic))
			{
				receivedNumbers[topic] = new List<int>();
			}

			receivedNumbers[topic].Add(number);
		}

		public void ReleaseIncomingPacketId(ushort packetId)
		{
			if (incomingPacketIds.Contains(packetId))
			{
				incomingPacketIds.Remove(packetId);
			}
		}

		public bool IsIncomingMessageRegistered(ushort packetId)
		{
			return incomingPacketIds.Contains(packetId);
		}


		public int GetLastReceived(string topic)
		{
			if (receivedNumbers.ContainsKey(topic))
			{
				var items = receivedNumbers[topic];
				return items.Count == 0 ? 0 : items.Last();
			}
			else
			{
				return 0;
			}
		}

		public bool IsDoneReceiving(int maxNumber)
		{
			foreach (var kvp in receivedNumbers)
			{
				if (kvp.Value.Count == 0 || kvp.Value.Last() < maxNumber)
				{
					return false;
				}
			}

			return true;
		}


		public void RegisterOutgoingMessage(OutgoingMessage outgoingMessage)
		{
			outgoingMessages.Add(outgoingMessage);
		}

		public int GetLastNumberSent()
		{
			return lastNumberSent;
		}

		public OutgoingMessage GetPendingOutgoingMessage()
		{
			return outgoingMessages.FirstOrDefault();
		}

		public void SetOutgoingMessageReceived(ushort packetId)
		{
			OutgoingMessage msg = outgoingMessages.FirstOrDefault(m => m.PacketId == packetId);
			if (msg != null)
			{
				msg.Received = true;
			}
		}

		public void SetOutgoingMessageAcknowledged(ushort packetId)
		{
			OutgoingMessage msg = outgoingMessages.FirstOrDefault(m => m.PacketId == packetId);
			if (msg != null)
			{
				publishedNumbers.Add(msg.Number);
			}

			outgoingMessages.RemoveAll(m => m.PacketId == packetId);
		}

		public void RegisterPublishedNumber(int n)
		{
			publishedNumbers.Add(n);
		}

		public void Dispose()
		{
			incomingPacketIds.Clear();
			receivedNumbers.Clear();

			outgoingMessages.Clear();
			lastNumberSent = 0;
			publishedNumbers.Clear();
		}
	}
}

