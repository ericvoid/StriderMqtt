using System;
using System.IO;
using Mono.Data.Sqlite;
using System.Collections.Generic;
using StriderMqtt;

namespace NumbersTest
{
	public class NumbersPersistence : IDisposable
	{
		readonly string ConnectionString;
		public SqliteConnection conn { get; private set; }

		public NumbersPersistence(string filename)
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
				// stores messages that were published to the broker
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "CREATE TABLE IF NOT EXISTS published_numbers " +
						"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, number INT)";
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


		public void RegisterReceivedNumber(string topic, int number)
		{
			using (var trans = conn.BeginTransaction())
			{
				using (var command = conn.CreateCommand())
				{
					command.Transaction = trans;
					command.CommandText = "INSERT INTO received_numbers (topic, number) VALUES (@topic, @number)";
					command.Parameters.AddWithValue("@topic", topic);
					command.Parameters.AddWithValue("@number", number);
					command.ExecuteNonQuery();
				}

				UpdateLastReceivedPerTopic(topic, number, trans);

				trans.Commit();
			}
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



		public int GetLastNumberSent()
		{
			object result;

			// if there is no outgoing message, return the last published number
			using (var command = conn.CreateCommand())
			{
				command.CommandText = "SELECT MAX(number) FROM published_numbers";
				result = command.ExecuteScalar();
			}

			return IsNull(result) ? 0 : Convert.ToInt32(result);
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

}

