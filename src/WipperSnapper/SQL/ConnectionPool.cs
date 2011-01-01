namespace WipperSnapper.SQL
{
	using System;
	using System.Collections.Generic;
	using System.Threading;

	/// <summary>
	/// Database connections are expensive so unfortunately we need to pool them to reuse as much as possible
	/// This class is the center of the universe in terms of WipperSnapper's DB communication.
	/// Cowboy use of this class is strongly discouraged but it's publically exposed if you need to perform nano surgery. 
	/// </summary>
	/// <remarks>
	/// - Connections need to be assigned per thread. We can't have a connection shared between 2 threads due to transaction boundaries.
	/// - We have to be very careful with who we give connections to so that we don't violate a possible transaction by opening multiple connections in 1 transaction.
	/// - We can have multiple connections with the same database but multiple things going on in that connection.
	/// - ThreadIDs are only unique for the lifetime of the thread. I just don't think we care if a new-yet-finished thread uses the connection of the old thread. 
	/// </remarks>
	public sealed class ConnectionPool
	{
		/// <summary>
		/// The connection pool sits floating around in static memory to ensure singleton.
		/// </summary>
		private static Dictionary<ConnectionIdentifier, Connection> _pool = new Dictionary<ConnectionIdentifier,Connection>();

		public Connection this[string name]
		{
			get { return this[Thread.CurrentThread.ManagedThreadId, name]; }
			set { this[Thread.CurrentThread.ManagedThreadId, name] = value; }
		}

		public Connection this[int threadid, string name]
		{
			get { return this[new ConnectionIdentifier(threadid, name)]; }
			set { this[new ConnectionIdentifier(threadid, name)] = value; }
		}

		public Connection this[ConnectionIdentifier id]
		{
			get 
			{
				lock (ConnectionPool._pool)
				{
					return ConnectionPool._pool[id];
				}
			}
			set
			{
				lock (ConnectionPool._pool)
				{
					ConnectionPool._pool[id] = value;
				}
			}
		}

		/// <summary>
		/// See if the connection with the name exists in the pool.
		/// </summary>
		public bool Contains(string name)
		{
			return Contains(name, Thread.CurrentThread.ManagedThreadId);
		}

		/// <summary>
		/// See if the connection with the name exists in the pool.
		/// </summary>
		public bool Contains(string name, int threadID)
		{
			ConnectionIdentifier id = new ConnectionIdentifier(threadID, name);
			return Contains(id);
		}

		/// <summary>
		/// See if the connection with the name exists in the pool.
		/// </summary>
		public bool Contains(ConnectionIdentifier identifier)
		{
			lock (ConnectionPool._pool)
			{
				return ConnectionPool._pool.ContainsKey(identifier);
			}
		}

		/// <summary>
		/// Removes the connection from the Pool.
		/// </summary>
		public void Remove(Connection conn)
		{
			lock (ConnectionPool._pool)
			{
				ConnectionIdentifier id = new ConnectionIdentifier(Thread.CurrentThread.ManagedThreadId, conn.UniqueConnectionName);
				if (ConnectionPool._pool.ContainsKey(id))
				{
					ConnectionPool._pool.Remove(id);
				}
				else
				{
					throw new Exception("Could not find a connection in the ConnectionPool, Name = " + conn.UniqueConnectionName + " ConnectionString: " + conn.ConnectionString);
				}
			}
		}
	}


	public struct ConnectionIdentifier
	{
		public int ThreadID;
		public string ConnectionName;

		public ConnectionIdentifier(int ThreadID, string ConnectionName)
		{
			this.ThreadID = ThreadID;
			this.ConnectionName = ConnectionName;
		}

		/// <summary>
		/// Explicitly defining what equals means so ensure dictionary lookups.  The == and != operators are also defined to use this method.
		/// </summary>
		public override bool Equals(object obj)
		{
			// I get criticism about this but I still argue it's technically correct and I prefer a false to an exception.
			if (obj.GetType().Equals(typeof(ConnectionIdentifier)))
			{
				return false;
			}

			ConnectionIdentifier other = (ConnectionIdentifier)obj;
			return other.ConnectionName.Equals(this.ConnectionName, StringComparison.CurrentCultureIgnoreCase) && other.ThreadID.Equals(this.ThreadID);
		}
		public static bool operator ==(ConnectionIdentifier first, ConnectionIdentifier second)
		{
			return first.Equals(second);
		}
		public static bool operator !=(ConnectionIdentifier first, ConnectionIdentifier second)
		{
			return !first.Equals(second);
		}

		/// <summary>
		/// Equals method no longer uses this method but this *should* still be unique per structure.
		/// </summary>
		public override int GetHashCode()
		{
			return string.Format("{0}.{1}", this.ThreadID, this.ConnectionName).GetHashCode();
		}
	}

}
