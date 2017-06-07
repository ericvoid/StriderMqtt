using System;
using System.IO;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Authentication;
using System.Collections.Generic;

namespace StriderMqtt
{
	public interface IMqttTransport
	{
		/// <summary>
		/// The Stream object to read from and write to.
		/// </summary>
		/// <value>The stream.</value>
		Stream Stream { get; }

		/// <summary>
		/// Gets a value indicating whether it is possible to read from and write to the Stream.
		/// </summary>
		/// <value><c>true</c> if Stream is connected and available; otherwise, <c>false</c>.</value>
		bool IsClosed { get; }

		/// <summary>
		/// Poll the connection for the specified pollLimit time.
		/// </summary>
		/// <param name="pollLimit">Poll limit time in milliseconds.</param>
		bool Poll(int pollLimit);
	}

	internal interface IInternalTransport : IMqttTransport, IDisposable { }

	internal class TcpTransport : IInternalTransport
	{
		private TcpClient tcpClient;
		private NetworkStream netstream;

		public Stream Stream
		{
			get
			{
				return this.netstream;
			}
		}

		public bool IsClosed
		{
			get
			{
				return tcpClient == null || !tcpClient.Connected;
			}
		}

		internal TcpTransport(string hostname, int port)
		{
			this.tcpClient = new TcpClient();
			this.tcpClient.Connect(hostname, port);
			this.netstream = this.tcpClient.GetStream();

		}

		public void SetTimeouts(TimeSpan readTimeout, TimeSpan writeTimeout)
		{
			this.netstream.ReadTimeout = (int)readTimeout.TotalMilliseconds;
			this.netstream.WriteTimeout = (int)writeTimeout.TotalMilliseconds;
		}

        public bool Poll(int pollLimit)
        {
            var limitMicros = Conversions.MillisToMicros(pollLimit);
            return tcpClient.Client.Poll(limitMicros, SelectMode.SelectRead);
        }

		public void Dispose()
		{
			this.netstream.Close();
			this.tcpClient.Close();
		}
	}


	internal class TlsTransport : IInternalTransport
	{
		private TcpClient tcpClient;
		private NetworkStream netstream;
		private SslStream sslStream;

		public Stream Stream
		{
			get
			{
				return this.sslStream;
			}
		}

		public bool IsClosed
		{
			get
			{
				return tcpClient == null || !tcpClient.Connected;
			}
		}

		internal TlsTransport(string hostname, int port)
		{
			this.tcpClient = new TcpClient();
			this.tcpClient.Connect(hostname, port);

			this.netstream = this.tcpClient.GetStream();
			this.sslStream = new SslStream(netstream, false);

			this.sslStream.AuthenticateAsClient(hostname);
		}

		public void SetTimeouts(TimeSpan readTimeout, TimeSpan writeTimeout)
		{
			this.sslStream.ReadTimeout = (int)readTimeout.TotalMilliseconds;
			this.sslStream.WriteTimeout = (int)writeTimeout.TotalMilliseconds;
		}

        public bool Poll(int pollLimit)
        {
            var limitMicros = Conversions.MillisToMicros(pollLimit);
            return tcpClient.Client.Poll(limitMicros, SelectMode.SelectRead);
        }

		public void Dispose()
		{
			this.sslStream.Close();
			this.netstream.Close();
			this.tcpClient.Close();
		}
	}

}

