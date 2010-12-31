﻿namespace WipperSnapper.SQL
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Data.SqlClient;

	/// <summary>
	/// Command is the basis for sending commands across the SQL Connection.
	/// I'm wrapping SQLCommand and making parameters more friendly to work with.
	/// </summary>
	public class Command : IDisposable
	{
		/// <summary>
		/// Must be in context of a connection.
		/// </summary>
		public Command(Connection conn)
		{
			this._Connection = conn;
			this._Command = this.Connection.SqlConnection.CreateCommand();
		}

		/// <summary>
		/// The SQL string used if the Execute commands are called without a string parameter.
		/// </summary>
		public string Sql
		{
			get { return this._Sql; }
			set { this._Sql = value; }
		}
		protected string _Sql = null;

		/// <summary>
		/// Gets the SqlCommand this object is wrapping. This will hopefully never be public.
		/// </summary>
		protected internal System.Data.SqlClient.SqlCommand SqlCommand
		{
			get { return this._Command; }
		}
		protected System.Data.SqlClient.SqlCommand _Command;

		/// <summary>
		/// Gets the connection. Note: This is readonly as you can't change connection mid command.
		/// </summary>
		public Connection Connection
		{
			get { return this._Connection; }
		}
		protected Connection _Connection = null;


		#region Parameters
		/// <summary>
		/// Gets or sets the parameter given by <paramref name="key"/>.
		/// This will throw an exception if the parameter doesn't exist.
		/// </summary>
		/// <param name="key">Name of the parameter</param>
		/// <returns>Parameter value</returns>
		public object this[string key]
		{
			get { return this._Params[key]; }
			set { this._Params[key] = value; }
		}
		Dictionary<string, object> _Params = new Dictionary<string, object>();

		/// <summary>
		/// Checks if the parameter <paramref name="key"/> exists
		/// </summary>
		/// <returns>True if parameter exists, false otherwise.</returns>
		public bool ContainsKey(string key)
		{
			return this._Params.ContainsKey(key);
		}

		/// <summary>
		/// Copy a dictionary of parameters to the command. Very useful.
		/// </summary>
		public void AddParametersDictionary(Dictionary<string, object> parms)
		{
			if (parms == null) 
			{ 
				return; 
			}

			foreach (KeyValuePair<string, object> kv in parms)
			{
				this[kv.Key] = kv.Value;
			}
		}
		#endregion

		/// <summary>
		/// This is just setup that copies the friendly parameter dictionary down to SQL Parameters
		/// </summary>
		private void _ExecuteSetup()
		{
			// Copy parameters down
			foreach (KeyValuePair<string, object> kvp in this._Params)
			{
				object value;
				if (((object)kvp.Value) == null)
				{
					// Null isn't null, it's dbnull - duh.
					value = System.DBNull.Value;
				}
				else
				{
					value = kvp.Value;
				}
				this._Command.Parameters.Add(new SqlParameter(kvp.Key, value));
			}
		}

		/// <summary>
		/// This handles executing commands over the connection in a fail-safe(r) manner. 
		/// If the connection on the command has closed, then this will reconnect it up to 3 times before failing
		/// If the SQL execution fails for any reason, this turns the SQL Exception into a WipperSnapper Command Exception.
		/// 
		/// This is a little strange in that you pass the actual method into it. This basically wraps the method with some helpers.
		/// </summary>
		private T _ExecuteCommand<T>(Func<T> CommandToExecute)
		{
			//Call setup
			_ExecuteSetup();

			//Begin the loop.
			for (int i = 0; ; i++)
			{
				try
				{
					//Execute the command
					return CommandToExecute();
				}
				catch (System.InvalidOperationException)
				{
					if (!this.Connection.IsConnected)
					{
						if (i >= 3)
						{
							throw new WipperSnapperCommandException("Failed to execute within 3 attempts");
						}

						this.Connection.Connect();
						continue; // try again.  
					}
					else
						throw;
				}
				catch (System.Data.SqlClient.SqlException sqlexc)
				{
					//SQL Exception encountered. Wrap it in WipperSnapperCommandException
					throw new WipperSnapperCommandException("Failed to execute on attempt #" + i, sqlexc);
				}
			}

			throw new WipperSnapperCommandException("Failed to reconnect to Database");
		}

		/// <summary>
		/// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
		/// </summary>
		public int ExecuteNonQuery()
		{
			return _ExecuteCommand(this._Command.ExecuteNonQuery);
		}

		/// <summary>
		/// Executes the query, and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored.
		/// </summary>
		public object ExecuteScalar()
		{
			return _ExecuteCommand(this._Command.ExecuteScalar);
		}

		/// <summary>
		/// Executes the query, and returns a DataReader to read the result.
		/// </summary>
		public SqlDataReader ExecuteReader()
		{
			return _ExecuteCommand(this._Command.ExecuteReader);
		}

		/// <summary>
		/// Trashes the command. Does nothing to the inner connection.
		/// </summary>
		public void Dispose()
		{
			this._Command.Dispose();
		}
	}

	/// <summary>
	/// WipperSnapper's SQL Exception. For the most part it just passes on the info it can, but the type is abstracted.
	/// </summary>
	public class WipperSnapperCommandException : Exception
	{
		public WipperSnapperCommandException(string msg) : base(msg) { }

		public WipperSnapperCommandException(string msg, Exception e) : base(msg, e) { }
	}

}