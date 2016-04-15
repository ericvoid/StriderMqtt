using System;
using System.IO;
using Mono.Data.Sqlite;
using System.Collections.Generic;
using StriderMqtt;

namespace SqlitePersistenceSample
{
	public class SqlitePersistence : IMqttClientPersistence, IDisposable
	{
		const int ReadBufferSize = 512;

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
				// stores packet ids for messages that are arriving from broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS incoming_messages " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, packet_id INT)";
					command.ExecuteNonQuery();
				}

				// stores messages that being delivered to the broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS outgoing_messages " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
							"packet_id INT, topic TEXT, qos INT, payload BLOB, " +
							"received BOOLEAN)";
					command.ExecuteNonQuery();
				}

				trans.Commit();
			}
		}

		public void StoreIncomingMessage(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "INSERT INTO incoming_messages (packet_id) VALUES (@packet_id)";
				command.Parameters.AddWithValue("@packet_id", packetId);
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



		public void RegisterOutgoingMessage(OutgoingMessage outgoingMessage)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"INSERT INTO outgoing_messages
					(packet_id, topic, qos, payload, received)
					VALUES (@packet_id, @topic, @qos, @payload, @received)";
				command.Parameters.AddWithValue("@packet_id", outgoingMessage.PacketId);
				command.Parameters.AddWithValue("@payload", outgoingMessage.Payload);
				command.Parameters.AddWithValue("@received", false);
				command.ExecuteNonQuery();
			}
		}

		public OutgoingMessage GetPendingOutgoingMessage()
		{
			OutgoingMessage result;

			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT packet_id, topic, qos, payload, received
					FROM outgoing_messages
					ORDER BY id ASC LIMIT 1";

				SqliteDataReader reader = command.ExecuteReader();
				if (reader.Read())
				{
					result = GetOutgoingMessage(reader);
				}
				else
				{
					result = null;
				}
			}

			return result;
		}

		OutgoingMessage GetOutgoingMessage(SqliteDataReader reader)
		{
			return new OutgoingMessage()
			{
				PacketId = (ushort)reader.GetInt32(0),
				Topic = reader.GetString(1),
				Qos = GetQosLevel(reader.GetByte(2)),
				Payload =  this.GetBlob(reader, 3),
				Received = reader.GetBoolean(4)
			};
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
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "DELETE FROM outgoing_messages WHERE packet_id = @packet_id";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public bool IsOutgoingQueueEmpty
		{
			get
			{
				object result;

				using (var command = conn.CreateCommand())
				{
					command.CommandText = "SELECT 1 FROM outgoing_messages LIMIT 1";
					result = command.ExecuteScalar();
				}

				return IsNull(result);

			}
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

		private byte[] GetBlob(SqliteDataReader reader, int i)
		{
			MemoryStream ms = new MemoryStream();
			byte[] buffer = new byte[ReadBufferSize];
			long offset = 0;
			int lastRead = 0;
			do
			{
				lastRead = (int)reader.GetBytes(i, offset, buffer, 0, ReadBufferSize);
				ms.Write(buffer, 0, lastRead);
				offset += lastRead;
			}
			while (lastRead == ReadBufferSize);

			return ms.ToArray();
		}

		private MqttQos GetQosLevel(byte b)
		{
			switch (b)
			{
				case 0:
					return MqttQos.AtMostOnce;
				case 1:
					return MqttQos.AtLeastOnce;
				case 2:
					return MqttQos.ExactlyOnce;
				default:
					throw new InvalidDataException("Invalid Qos Level found on persistence");
			}

		}
	}
}

