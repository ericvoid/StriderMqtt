using System;
using System.IO;
using Mono.Data.Sqlite;
using System.Collections.Generic;
using StriderMqtt;

namespace StriderMqtt.Sqlite
{
	public class SqlitePersistence : IMqttPersistence, IDisposable
	{
		const int ReadBufferSize = 512;

		bool ConnectionIsManaged;
		SqliteConnection conn;

		public SqlitePersistence(string filename)
		{
			var connectionString = "Data Source=" + filename;

			if (!File.Exists(filename))
			{
				SqliteConnection.CreateFile(filename);
			}

			this.conn = new SqliteConnection(connectionString);
			this.conn.Open();

			ConnectionIsManaged = true;
			CreateTables();
		}

		public SqlitePersistence(SqliteConnection connection)
		{
			this.conn = connection;

			ConnectionIsManaged = false;
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
					command.CommandText = "CREATE TABLE IF NOT EXISTS incoming_flows " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, packet_id INT)";
					command.ExecuteNonQuery();
				}

				// stores the last used packet id
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS last_packet_id " +
						"(_ INT UNIQUE, id INT)";
					command.ExecuteNonQuery();
				}

				// stores messages that being delivered to the broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS outgoing_flows " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, " +
					    "packet_id INT, retain BOOLEAN, topic TEXT, qos INT, " +
					    "payload BLOB, received BOOLEAN)";
					command.ExecuteNonQuery();
				}

				trans.Commit();
			}
		}

		public void RegisterIncomingFlow(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "INSERT INTO incoming_flows (packet_id) VALUES (@packet_id)";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public void ReleaseIncomingFlow(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "DELETE from incoming_flows WHERE packet_id = @packet_id";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public bool IsIncomingFlowRegistered(ushort packetId)
		{
			object result;

			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT 1 FROM incoming_flows
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


		public ushort LastOutgoingPacketId
		{
			get
			{
				using (var command = conn.CreateCommand())
				{
					command.CommandText = "SELECT id FROM last_packet_id LIMIT 1";
					object r = command.ExecuteScalar();
					return (ushort)(IsNull(r) ? 0L : (int)r);
				}
			}

			set
			{
				using (var command = conn.CreateCommand())
				{
					command.CommandText = "INSERT OR REPLACE INTO last_packet_id (_, id) VALUES (0, @id)";
					command.Parameters.AddWithValue("@id", value);
					command.ExecuteNonQuery();
				}
			}
		}

		public void RegisterOutgoingFlow(OutgoingFlow outgoingMessage)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"INSERT INTO outgoing_flows
					(packet_id, retain, topic, qos, payload, received)
					VALUES (@packet_id, @retain, @topic, @qos, @payload, @received)";
				command.Parameters.AddWithValue("@packet_id", outgoingMessage.PacketId);
				command.Parameters.AddWithValue("@retain", outgoingMessage.Retain);
				command.Parameters.AddWithValue("@topic", outgoingMessage.Topic);
				command.Parameters.AddWithValue("@qos", (int)outgoingMessage.Qos);
				command.Parameters.AddWithValue("@payload", outgoingMessage.Payload);
				command.Parameters.AddWithValue("@received", false);
				command.ExecuteNonQuery();
			}
		}

		public IEnumerable<OutgoingFlow> GetPendingOutgoingFlows()
		{
			List<OutgoingFlow> result = new List<OutgoingFlow>();

			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"SELECT packet_id, retain, topic, qos, payload, received
					FROM outgoing_flows
					ORDER BY id ASC LIMIT 1";

				SqliteDataReader reader = command.ExecuteReader();
				while (reader.Read())
				{
					result.Add(GetOutgoingMessage(reader));
				}
			}

			return result;
		}

		OutgoingFlow GetOutgoingMessage(SqliteDataReader reader)
		{
			return new OutgoingFlow()
			{
				PacketId = (ushort)reader.GetInt32(0),
				Retain = reader.GetBoolean(1),
				Topic = reader.GetString(2),
				Qos = GetQosLevel(reader.GetByte(3)),
				Payload =  this.GetBlob(reader, 4),
				Received = reader.GetBoolean(5)
			};
		}

		public void SetOutgoingFlowReceived(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = @"UPDATE outgoing_flows
					SET received = 1
					WHERE packet_id = @packet_id
					AND received = 0";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public void SetOutgoingFlowCompleted(ushort packetId)
		{
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "DELETE FROM outgoing_flows WHERE packet_id = @packet_id";
				command.Parameters.AddWithValue("@packet_id", packetId);
				command.ExecuteNonQuery();
			}
		}

		public long OutgoingFlowsCount
		{
			get
			{
				using (var command = conn.CreateCommand())
				{
					command.CommandText = "SELECT COUNT(*) FROM outgoing_flows";
					object r = command.ExecuteScalar();
					return IsNull(r) ? 0L : (long)r;
				}
			}
		}


		public void Dispose()
		{
			if (ConnectionIsManaged)
			{
				conn.Close();
				conn.Dispose();
			}
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

