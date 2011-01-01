namespace WipperSnapper.Session
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using WipperSnapper.SQL;
	using System.Data;

	/// <summary>
	/// Session is essentially like a TransactionScope Except:
	/// - Without the distributed capability 
	/// - It actually works without timing out for some stupid reason.
	/// - You can actually nest them! like OMFG!
	/// </summary>
	/// <remarks>
	/// All connections created/used inside the session will actually be 
	/// the same single connection as to share the same transaction.
	/// </remarks>
	public class Session : IDisposable
	{
		/// <summary>
		/// Local reference to the static session manager.
		/// </summary>
		protected static SessionManager _manager = new SessionManager();

		protected Session(string connectionString, IsolationLevel isoLevel)
		{
			//New session
			this.SessionID = Guid.NewGuid();

			//Create new connection unique to this session
			this._Connection = Connection.GetConnection(this.SessionID.ToString(), connectionString, true);
			this._Transaction = new Transaction(this._Connection, this.SessionID, isoLevel);

			//Add it to the session manager
			Session._manager.Add(this);
		}

		protected Session(Session parent, IsolationLevel isoLevel)
		{
			//Nested Session
			this.SessionID = Guid.NewGuid();

			//Create savepoint transaction
			this._Transaction = new Transaction(parent.Connection, this.SessionID, isoLevel);

			//Set parent/child stuff
			this.Parent = parent;
			this.Parent.Children.Add(this);
		}

		public static Session GetSession(string connectionString, IsolationLevel isoLevel)
		{
			//This will make each thread begin a using block atomically. (though they can execute in the using block in parallel)
			lock (Session._manager)
			{
				Session sess = SessionManager.GetSessionForThisThread();
				if (sess == null)
				{
					//If there isn't a new session then we create one
					return new Session(connectionString, isoLevel);
				}
				else
				{
					//Otherwise piggy back off existing one
					return new Session(sess, isoLevel);
				}
			}
		}

		/// <summary>
		/// Unique identifier for the session - sessions are also thread specific
		/// </summary>
		public Guid SessionID { get; set; }

		/// <summary>
		/// The Parent Session if this session is nested. Otherwise null if not nested;
		/// </summary>
		public Session Parent { get; set; }


		/// <summary>
		/// Children session
		/// </summary>
		public List<Session> Children 
		{
			get { return _Children; }
		}
		private List<Session> _Children = new List<Session>();

		/// <summary>
		/// Connection for the session
		/// </summary>
		public Connection Connection
		{
			get { return _Connection; }
		}
		private Connection _Connection = null;

		/// <summary>
		/// Transaction for the session
		/// </summary>
		public Transaction Transaction
		{
			get { return _Transaction; }
		}
		private Transaction _Transaction = null;

		/// <summary>
		/// Commit Session - save all SQL changes during this session.
		/// </summary>
		public void Commit()
		{
			this.Transaction.Commit();
		}

		/// <summary>
		/// Rollback Session - undo all SQL changes during this session.
		/// </summary>
		public void Rollback()
		{
			this.Transaction.Rollback();
		}

		/// <summary>
		/// On end of session, we need to commit or rollback the nested transaction
		/// </summary>
		public void Dispose()
		{
			this.Transaction.Dispose();
			this.Connection.Dispose();
			Session._manager.Remove();
			this._Transaction = null;
			this._Connection = null;
		}
	}
}
