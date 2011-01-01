namespace WipperSnapper.Session
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Text;
	using System.Threading;

	public sealed class SessionManager
	{
		/// <summary>
		/// The connection pool sits floating around in static memory to ensure singleton.
		/// You can't have more than 1 root session per thread.
		/// </summary>
		private static Dictionary<int, Session> _pool = new Dictionary<int, Session>();

		/// <summary>
		/// Gets the session for the current thread.
		/// If the current thread does not have an active session then return null
		/// </summary>
		public static Session GetSessionForThisThread()
		{
			lock (SessionManager._pool)
			{
				int threadid = Thread.CurrentThread.ManagedThreadId;
				if (SessionManager._pool.ContainsKey(threadid))
				{
					return SessionManager._pool[threadid];
				}
				return null;
			}
		}

		/// <summary>
		/// You can't have more than 1 ROOT session per thread so it makes sense to be able to find 
		/// the session by threadID
		/// </summary>
		public Session this[int threadID]
		{
			get
			{
				lock (SessionManager._pool)
				{
					return SessionManager._pool[threadID];
				}
			}
			set
			{
				lock (SessionManager._pool)
				{
					SessionManager._pool[threadID] = value;
				}
			}
		}

		/// <summary>
		/// See if a session exists for this thread already
		/// </summary>
		public bool Contains(int threadID)
		{
			lock (SessionManager._pool)
			{
				return SessionManager._pool.ContainsKey(threadID);
			}
		}

		public void Add(Session sess)
		{
			lock (SessionManager._pool)
			{
				int threadid = Thread.CurrentThread.ManagedThreadId;
				SessionManager._pool[threadid] = sess;
			}
		}

		/// <summary>
		/// Removes the session from the current thread
		/// </summary>
		public void Remove()
		{
			lock (SessionManager._pool)
			{
				int threadid = Thread.CurrentThread.ManagedThreadId;
				if (SessionManager._pool.ContainsKey(threadid))
				{
					SessionManager._pool.Remove(threadid);
				}
			}
		}
	}
}
