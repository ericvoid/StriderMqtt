using System;
using System.IO;
using Mono.Data.Sqlite;
using System.Collections.Generic;
using StriderMqtt;

namespace NumbersTest
{
	public class SqlitePersistence : IPersistence
	{
		readonly string ConnectionString;
		SqliteConnection conn;

		public SqlitePersistence(string filename)
		{
			this.ConnectionString = "Data Source=" + filename;

			if (!File.Exists(filename))
			{
				SqliteConnection.CreateFile(filename);
			}

			this.conn = new SqliteConnection(ConnectionString);
			this.conn.Open();

			CreateTables();
		}

		void CreateTables()
		{
			using (var trans = conn.BeginTransaction())
			{
				// stores messages that being delivered to the broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS outgoing_messages " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
						"packet_id INT, number INT, received INT)";
					command.ExecuteNonQuery();
				}

				// stores messages that were published to the broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS published_numbers " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, number INT)";
					command.ExecuteNonQuery();
				}


				// stores packet ids for messages that are arriving from broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS incoming_messages " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, packet_id INT)";
					command.ExecuteNonQuery();
				}

				// stores messages that were received from broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS received_numbers " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
						"topic TEXT, number INT)";
					command.ExecuteNonQuery();
				}

				// stores messages that were received from broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = @"CREATE TABLE IF NOT EXISTS last_received_per_topic
						(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, topic TEXT, number INT)";
					command.ExecuteNonQuery();
				}

				trans.Commit();
			}
		}

		public void StoreIncomingMessage(ushort packetId, string topic, MqttQos qos, int number)
		{
			using (var trans = conn.BeginTransaction())
			{
				if (qos == MqttQos.ExactlyOnce)
				{
					RegisterIncomingMessage(packetId, trans);
				}

				RegisterReceivedNumber(topic, number, trans);
				trans.Commit();
			}
		}

		void RegisterIncomingMessage(ushort packetId, SqliteTransaction trans)
		{
			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = "INSERT INTO incoming_messages (packet_id) VALUES (@packet_id)";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		void RegisterReceivedNumber(string topic, int number, SqliteTransaction trans)
		{
			using (var command = conn.CreateCommand()) {
				command.Transaction = trans;
				command.CommandText = "INSERT INTO received_numbers (topic, number) VALUES (@topic, @number)";
				command.Parameters.AddWithValue("@topic", topic);
				command.Parameters.AddWithValue("@number", number);
				command.ExecuteNonQuery();
			}

			UpdateLastReceivedPerTopic(topic, number, trans);
		}

		void UpdateLastReceivedPerTopic(string topic, int number, SqliteTransaction trans)
		{
			// check if row exists for the given topic to insert (if not exists) or update (if exists)
			object result;
			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = "SELECT 1 FROM last_received_per_topic WHERE topic = @topic LIMIT 1";
				command.Parameters.AddWithValue("@topic", topic);
				result = command.ExecuteScalar();
			}

			string commandText;
			if (IsNull(result))
			{
				commandText = @"INSERT INTO last_received_per_topic (topic, number) 
					VALUES (@topic, @number)";
			}
			else
			{
				commandText = @"UPDATE last_received_per_topic
					SET number = @number WHERE topic = @topic";
			}

			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = commandText;
				command.Parameters.AddWithValue("@topic", topic);
				command.Parameters.AddWithValue("@number", number);
				command.ExecuteNonQuery();
			}
		}

		public void ReleaseIncomingPacketId(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "DELETE from incoming_messages WHERE packet_id = @packet_id";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public bool IsIncomingMessageRegistered(ushort packetId)
		{
			object result;

			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT 1 FROM incoming_messages
					WHERE packet_id = @packet_id LIMIT 1";
				command.Parameters.AddWithValue("@packet_id", packetId);

				result = command.ExecuteScalar();
			}

			if (IsNull(result))
			{
				return false;
			}
			else
			{
				return ((long)result) > 0;
			}
		}

		public int GetLastReceived(string topic)
		{
			object result;
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT number FROM last_received_per_topic
					WHERE topic = @topic";
				command.Parameters.AddWithValue("@topic", topic);

				result = command.ExecuteScalar();
			}

			return IsNull(result) ? 0 : Convert.ToInt32(result);
		}

		public bool IsDoneReceiving(int maxNumber)
		{
			bool result = true;

			using (var trans = conn.BeginTransaction())
			{
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "SELECT number FROM last_received_per_topic";

					SqliteDataReader reader = command.ExecuteReader();
					while (reader.Read())
					{
						if (reader.GetInt32(0) < maxNumber)
						{
							result = false;
							break;
						}
					}
				}
				trans.Commit();
			}

			return result;
		}



		public void RegisterOutgoingMessage(OutgoingMessage outgoingMessage)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"INSERT INTO outgoing_messages (packet_id, number, received)
					VALUES (@packet_id, @number, @received)";
				command.Parameters.AddWithValue("@packet_id", outgoingMessage.PacketId);
				command.Parameters.AddWithValue("@number", outgoingMessage.Number);
				command.Parameters.AddWithValue("@received", 0);
				command.ExecuteNonQuery();
			}
		}

		public int GetLastNumberSent()
		{
			object result;

			// if there is an outgoing message, return its number
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "SELECT MAX(number) FROM outgoing_messages";
				result = command.ExecuteScalar();
			}

			if (!IsNull(result))
			{
				return Convert.ToInt32(result);
			}

			// if there is no outgoing message, return the last published number
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "SELECT MAX(number) FROM published_numbers";
				result = command.ExecuteScalar();
			}

			return IsNull(result) ? 0 : Convert.ToInt32(result);
		}

		public OutgoingMessage GetPendingOutgoingMessage()
		{
			OutgoingMessage result;

			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT packet_id, number, received
					FROM outgoing_messages LIMIT 1";

				SqliteDataReader reader = command.ExecuteReader();
				if (reader.Read())
				{
					result = new OutgoingMessage()
					{
						PacketId = (ushort)reader.GetInt32(0),
						Number = reader.GetInt32(1),
						Received = reader.GetBoolean(2)
					};
				}
				else
				{
					result = null;
				}
			}

			return result;
		}

		public void SetOutgoingMessageReceived(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"UPDATE outgoing_messages
					SET received = 1
					WHERE packet_id = @packet_id
					AND received = 0";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public void SetOutgoingMessageAcknowledged(ushort packetId)
		{
			using (var trans = conn.BeginTransaction())
			{
				int number = GetOutgoingNumber(packetId, trans);

				SetOutgoingMessageAcknowledged(packetId, trans);
				RegisterPublishedNumber(number, trans);

				trans.Commit();
			}
		}

		void SetOutgoingMessageAcknowledged(ushort packetId, SqliteTransaction trans)
		{
			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = "DELETE FROM outgoing_messages WHERE packet_id = @packet_id";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public void RegisterPublishedNumber(int n)
		{
			using (var trans = conn.BeginTransaction())
			{
				RegisterPublishedNumber(n, trans);
				trans.Commit();
			}
		}

		void RegisterPublishedNumber(int n, SqliteTransaction trans)
		{
			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = "INSERT INTO published_numbers (number) VALUES (@number)";
				command.Parameters.AddWithValue("@number", n);
				command.ExecuteNonQuery();
			}
		}

		int GetOutgoingNumber(ushort packetId, SqliteTransaction trans)
		{
			object result;

			using (var command = conn.CreateCommand())
			{
				command.Transaction = trans;
				command.CommandText = @"SELECT number FROM outgoing_messages
					WHERE packet_id = @packet_id
					LIMIT 1";
				command.Parameters.AddWithValue("@packet_id", packetId);
				result = command.ExecuteScalar();
			}

			return IsNull(result) ? -1 : Convert.ToInt32(result);
		}


		public void Dispose()
		{
			conn.Close();
			conn.Dispose();
		}

		bool IsNull(object result)
		{
			return result == null || result == DBNull.Value;
		}
	}

	public class OutgoingMessage
	{
		/// <summary>
		/// The packetId used for publishing.
		/// </summary>
		public ushort PacketId {
			get;
			set;
		}

		/// <summary>
		/// The number that will be published.
		/// </summary>
		public int Number {
			get;
			set;
		}

		/// <summary>
		/// Received Flag, to be used with QoS2.
		/// This flag determines if the `Pubrec` packet was received from broker.
		/// </summary>
		public bool Received {
			get;
			set;
		}
	}
}

