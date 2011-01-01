namespace WipperSnapper.SQL
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Data.SqlClient;
	using System.Data.Sql;
	using System.Data;

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

		/// <summary>
		/// Are we in a transaction?
		/// </summary>
		public bool IsInTransaction
		{
			get { return this.Transaction != null; }
		}

		/// <summary>
		/// Gets the transaction that this command is in (based off the current connection)
		/// </summary>
		public Transaction Transaction
		{
			get { return this._Connection.ActiveTransaction; }
		}

		/// <summary>
		/// Gets the SqlTransaction object (or null) running on the current connection.
		/// </summary>
		protected System.Data.SqlClient.SqlTransaction SqlTransaction
		{
			get { return this.Transaction == null ? null : this.Transaction.SqlTransaction; }
		}


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
		public void AddInputParametersDictionary(Dictionary<string, object> parms)
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

		/// <summary>
		/// Adds a set of output parameters to the Command - useful when dealing with output parameters in stored procs.
		/// - I strongly recommend not using output parameters due to their innate ugliness.
		/// </summary>
		public void AddOutputParameters(List<string> outputparams)
		{
			_OutputParams.AddRange(outputparams);
		}
		private List<string> _OutputParams = new List<string>();

		/// <summary>
		/// After executing the SQL (stored procedure), this method will retrieve the output parameters into a dictionary
		/// where the key is the parameter name
		/// </summary>
		public Dictionary<string, object> GetOutputParameterValues()
		{
			Dictionary<string, object> output = new Dictionary<string, object>();

			if (_OutputParams == null)
			{
				return output;
			}

			foreach (string key in _OutputParams)
			{
				output[key] = this._Command.Parameters[key];
			}

			return output;
		}
		#endregion

		/// <summary>
		/// This is just setup that copies the friendly parameter dictionary down to SQL Parameters
		/// </summary>
		private void _ExecuteSetup()
		{
			// Set transaction - I'm not sure if this matters actually but we'll do it for consistency's sake at least.
			this._Command.Transaction = this.SqlTransaction;

			// Copy parameters down
			foreach (KeyValuePair<string, object> kvp in this._Params)
			{
				SqlParameter param = new SqlParameter();
				param.ParameterName = kvp.Key;

				//Special check for Table Types
				if ((kvp.Value is DataTable))
				{
					param.SqlDbType = SqlDbType.Structured;
				}

				//Add parameter direction
				if (((_OutputParams != null) && _OutputParams.Contains(kvp.Key)))
				{
					param.Direction = ParameterDirection.InputOutput;
					param.Size = 512; //This came from debugging - we need a generic big size so we don't truncate.
				}
				else
				{
					param.Direction = ParameterDirection.Input;
				}

				//Set value
				if (((object)kvp.Value) == null)
				{
					// Null isn't null, it's dbnull - duh.
					param.Value = System.DBNull.Value;
				}
				else
				{
					param.Value = kvp.Value;
				}
				this._Command.Parameters.Add(param);
			}

			//'Add outout parameters
			if (((_OutputParams != null)))
			{
				foreach (string leftOver in _OutputParams.Except(_Params.Keys))
				{
					SqlParameter param = this._Command.CreateParameter();
					param.ParameterName = leftOver;
					param.Direction = ParameterDirection.Output;
					param.Size = 512; //This came from debugging - we need a generic big size so we don't truncate.
					this._Command.Parameters.Add(param);
				}
			}

			//Set Long Timeout - sick of the 30 second crap.
			this._Command.CommandTimeout = 500;
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
