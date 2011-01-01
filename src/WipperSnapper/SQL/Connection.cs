namespace WipperSnapper.SQL
{
	using System;
	using System.Data.SqlClient;
	using System.Data;

	public class Connection : IDisposable
	{
		/// <summary>
		/// Local static reference to the connection pool. 
		/// </summary>
		protected static ConnectionPool _EstablishedConnections = new ConnectionPool();

		/// <summary>
		/// Constructor that provides all the information needed to make a connection.
		/// </summary>
		/// <param name="ConnectionString">The connection string used to make the connection.</param>
		/// <param name="UniqueConnectionName">A Per-Thread unique name to unqiuely identify a connection.</param>
		/// <param name="AutoConnect">If this is true then the connection will automatically be connected upon construction.</param>
		protected Connection(string UniqueConnectionName, string ConnectionString, bool AutoConnect)
		{
			//Guarantee uniqueness
			if (Connection._EstablishedConnections.Contains(UniqueConnectionName))
			{
				throw new NotUniqueConnectionNameException(UniqueConnectionName);
			}

			this._ConnectionString = ConnectionString;
			this._UniqueConnectionName = UniqueConnectionName;

			if (AutoConnect)
			{
				this.Connect();
			}

			//Add to the connection pool always
			Connection._EstablishedConnections[UniqueConnectionName] = this;
		}

		/// <summary>
		/// Gets a connection. First checks connection pool. If not in pool then creates new connection.
		/// </summary>
		/// <param name="UniqueName">A Per-Thread unique name to unqiuely identify a connection</param>
		/// <param name="ConnectionString">The connection string used to make the connection.</param>
		/// <param name="AutoConnect">If this is true then the connection will automatically be connected upon construction.</param>
		public static Connection GetConnection(string UniqueName, string ConnectionString, bool AutoConnect)
		{
			if (Connection._EstablishedConnections.Contains(UniqueName))
			{
				// Little safety here to make sure we're not going to accidentially connect to the wrong location
				if (Connection._EstablishedConnections[UniqueName].ConnectionString != ConnectionString)
				{
					throw new Exception("Attempting to get a connection with the same unique name '" + UniqueName + "' but with different connection string");
				}

				return Connection._EstablishedConnections[UniqueName];
			}
			else
			{
				return new Connection(UniqueName, ConnectionString, AutoConnect);
			}
		}

		/// <summary>
		/// Gets the connection string for the current connection, closed or open.
		/// </summary>
		public string ConnectionString
		{
			get { return this._ConnectionString; }
		}
		protected string _ConnectionString;

		/// <summary>
		/// The inner SqlConnection. I'm still debating whether to make this public or not
		/// </summary>
		protected internal System.Data.SqlClient.SqlConnection SqlConnection
		{
			get { return this._Connection; }
		}
		protected System.Data.SqlClient.SqlConnection _Connection = null;

		/// <summary>
		/// Checks if this connection really is connected
		/// </summary>
		public bool IsConnected
		{
			get { return this._Connection != null; }
		}

		/// <summary>
		/// The Per-Thread Unique Name for the Connection. This helps identify a connection
		/// </summary>
		public string UniqueConnectionName
		{
			get { return _UniqueConnectionName; }
		}
		private string _UniqueConnectionName;


		/// <summary>
		/// Transaction for the connection
		/// </summary>
		public Transaction ActiveTransaction
		{
			get { return this._ActiveTransaction; }
			protected internal set { this._ActiveTransaction = value; }
		}
		protected Transaction _ActiveTransaction = null;

		/// <summary>
		/// Called by the Transaction class when the transaction has ended
		/// </summary>
		internal void TransactionEnded()
		{
			this._ActiveTransaction = null;
		}


		/// <summary>
		/// Initiates Connection. If we are already connected then this will disconnect first and reconnect.
		/// </summary>
		public void Connect()
		{
			// Close current connection if exists.
			this.Disconnect();

			// Override internal Sql pooling. We're doing our own.
			SqlConnectionStringBuilder connbuild = new SqlConnectionStringBuilder(this.ConnectionString);
			connbuild.Pooling = false;
			this._ConnectionString = connbuild.ToString();

			// Create new sql connection and open it
			this._Connection = new System.Data.SqlClient.SqlConnection(this.ConnectionString);
			this._Connection.Open();
		}

		/// <summary>
		/// Disconnect connection. If the connection is already closed then this does nothing.
		/// Note: This will NOT remove the connection from the Connection Pool. Only Dispose() does that.
		/// </summary>
		/// <see cref="Dispose"/>
		public void Disconnect()
		{
			if (this.IsConnected)
			{
				this._Connection.Close();
				this._Connection.Dispose();
				this._Connection = null;
			}
		}

		/// <summary>
		/// Trash the connection and remove from the Connection Pool.
		/// </summary>
		public void Dispose()
		{
			this.Disconnect();
			Connection._EstablishedConnections.Remove(this);
		}
	}

	public class NotUniqueConnectionNameException : Exception 
	{
		public NotUniqueConnectionNameException(string Name) : base("The Connection " + Name + " is not unique for this thread.") { } 
	}
}
