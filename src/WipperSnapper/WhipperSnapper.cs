namespace WipperSnapper
{

	using System;
	using System.Collections.Generic;
	using System.Configuration;
	using System.Data;
	using System.Reflection;
	using System.Data.SqlClient;
	using WipperSnapper.SQL;
	using WipperSnapper.Session;

	/// <summary>
	/// Whipper Snapper - someone who is unimportant but cheeky and presumptuous. 
	/// 
	/// Database abstraction is just that. It's annoying. It's *really* not the center of the universe but it's always treated as such. 
	/// Why? Because it's a pain in the ass and defines how your system is coded. I'm sick of that nonsense. So, this is a library to
	/// take away the pain of having to worry about how to get shit to your database.
	/// 
	/// This library does NOT attempt to define how you get your shit IN your database, only how to get shit TO your database. 
	/// Defining how to get shit IN your database is presumptuous. You're a programmer, you can figure that part out on your own.
	/// For better or worse.
	/// 
	/// To use this you only need to put the code in your solution and setup 1 config and a connection string. End of story. 
	/// - No inheriting multiple base classs, implementing an interface, and writing your own factory.
	/// - No writing types out into your web config
	/// - No f*cking XML!  For the love of Christ i'm just transferring data. No godamn xml!
	/// - No pre/post compilation.
	/// - No "oh you shouldn't use this in a batch process" 
	/// - No "oh you're using that wrong, didn't you know?
	/// 
	/// Future Items:
	/// - Add in ORM feature - SQL building based on attributed objects
	/// </summary>
	public sealed class WhipperSnapper
	{

		/// <summary>
		/// The whipper snapper instance (as well as the connections/transactions it uses) are tied to a particular connection string key.
		/// </summary>
		private string _ConnectionStringKey;

		/// <summary>
		/// Local reference to a session. This will only have a value if THIS WhipperSnapper instance created the session OR was constructed based on a session.
		/// </summary>
		private Session.Session _CreatedSession;

		/// <summary>
		/// The actual connection string for this instance.
		/// </summary>
		private string ConnectionString
		{
			get
			{
				//Go get the connection string. If it's not there throw an informative exception (vs a null ref)
				ConnectionStringSettings connString = ConfigurationManager.ConnectionStrings[_ConnectionStringKey];
				if (connString == null || string.IsNullOrWhiteSpace(connString.ConnectionString))
				{
					throw new Exception("Missing Connection String For: " + _ConnectionStringKey);
				}
				return connString.ConnectionString;
			}
		}

		/// <summary>
		/// Private constructor - we have to go through the factory properties/methods
		/// </summary>
		private WhipperSnapper(string ConnectionStringKey)
		{
			// Set the connection string key
			this._ConnectionStringKey = ConnectionStringKey;

			// Check for active session. If we're in a session and we're trying to open a connection to a different database - that isn't supported
			Session.Session sess = Session.SessionManager.GetSessionForThisThread();
			if (sess != null)
			{
				// Make sure at least the data source matches..
				SqlConnectionStringBuilder sessCs = new SqlConnectionStringBuilder(sess.Connection.ConnectionString);
				SqlConnectionStringBuilder myCs = new SqlConnectionStringBuilder(this.ConnectionString);

				if (!string.Equals(sessCs.DataSource, myCs.DataSource, StringComparison.InvariantCultureIgnoreCase))
				{
					throw new Exception("Trying to open a connection to '" + this.ConnectionString + "' when we're IN an session for the connection '" + sess.Connection.ConnectionString + "'. WhipperSnapper (ADO.NET actually) doesn't support cross-database transactions. We can't do this.");
				}
			}
		}

		/// <summary>
		/// Get a WhipperSnapper instance for the Default connection. This relies on an app setting 'DefaultConnectionStringKey' to be present. 
		/// </summary>
		public static WhipperSnapper Default
		{
			get
			{
				string DefaultConnectionKey = ConfigurationManager.AppSettings["DefaultConnectionStringKey"];
				if (string.IsNullOrEmpty(DefaultConnectionKey))
				{
					throw new Exception("Missing AppSetting Key: DefaultConnectionStringKey.  This should be the connection string 'key' to what you consider the default connection.");
				}
				return new WhipperSnapper(DefaultConnectionKey);
			}
		}

		/// <summary>
		/// Gets a WhipperSnapper instance specifically for a connection string defined by <paramref name="ConnectionStringKey"/>
		/// </summary>
		/// <param name="ConnectionStringKey">The connection string to be used for this whippersnapper instance</param>
		/// <returns></returns>
		public static WhipperSnapper ForDatabase(string ConnectionStringKey)
		{
			return new WhipperSnapper(ConnectionStringKey);
		}

		#region Public Instance Methods

		/// <summary>
		/// Finds a single DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
		/// The DTO is automatically created and populated through reflection
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to.</typeparam>
		/// <param name="procedure">The name of the stored procedure to execute</param>
		/// <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
		/// <returns>A single DTO result</returns>
		public T FindSingle<T>(string procedure, Dictionary<string, object> parameters)
		{
			return FindSingle<T>(procedure, parameters, MapToDTO<T>);
		}


		/// <summary>
		/// Finds a single DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
		/// The DTO is creatd and mapped based on the function <paramref name="mapFunc" />
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to.</typeparam>
		/// <param name="procedure">The name of the stored procedure to execute</param>
		/// <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
		/// <param name="mapFunc">The function used to create and map the result of the stored procedure to the DTO </param>
		/// <returns>A single DTO result</returns>
		public T FindSingle<T>(string procedure, Dictionary<string, object> parameters, MapToDTODelegate<T> mapFunc)
		{
			List<T> result = FindMultiple<T>(procedure, parameters, mapFunc);

			if ((result.Count > 1))
			{
				throw new MultipleRecordsFoundException();

			}
			else if ((result.Count == 0))
			{
				return default(T);
			}
			else
			{
				return result[0];
			}
		}

		/// <summary>
		/// Finds multiple DTOs from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
		/// The DTOs are automatically created and mapped from the sql results
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to.</typeparam>
		/// <param name="procedure">The name of the stored procedure to execute</param>
		/// <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
		/// <returns>A list of DTOs</returns>
		public List<T> FindMultiple<T>(string procedure, Dictionary<string, object> parameters)
		{
			return FindMultiple<T>(procedure, parameters, MapToDTO<T>);
		}

		/// <summary>
		/// Finds multiple DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
		/// The DTOs are creatd and mapped based on the function <paramref name="mapFunc" />
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to.</typeparam>
		/// <param name="procedure">The name of the stored procedure to execute</param>
		/// <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
		/// <param name="mapFunc">The function used to create and map 1 row result of the stored procedure to one DTO </param>
		/// <returns>A list of DTOs</returns>
		public List<T> FindMultiple<T>(string procedure, Dictionary<string, object> parameters, MapToDTODelegate<T> mapFunc)
		{
			// Create return set
			List<T> ret = new List<T>();

			// Open/Retrieve connection
			using (Connection conn = GetConnection())
			{
				// Creates a command to the stored procedure
				using (Command cmd = GetCommand(conn, procedure, parameters, null))
				{
					// Query and read
					using (IDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							ret.Add(mapFunc(reader));
						}
					}
				}
			}

			//Return the set
			return ret;
		}

		/// <summary>
		/// Executes a stored procedure that doesn't return anything back. No output parameters or data reader
		/// </summary>
		/// <param name="procedure">The procedure to execute</param>
		/// <param name="parameters">Parameters to pass it</param>
		public void ExecuteVoidProcedure(string procedure, Dictionary<string, object> parameters)
		{
			ExecuteVoidProcedure(procedure, parameters, null);
		}


		/// <summary>
		/// Executes a stored procedure and returns the output parameters whose names are given in <paramref name="outputParams" />
		/// </summary>
		/// <param name="procedure">The procedure to execute</param>
		/// <param name="parameters">Parameters to pass it</param>
		/// <param name="outputParams">list of output parameters</param>
		public Dictionary<string, object> ExecuteVoidProcedure(string procedure, Dictionary<string, object> parameters, List<string> outputParams)
		{
			//Open/Retrieve Connection
			using (Connection conn = GetConnection())
			{
				//Query
				using (Command cmd = GetCommand(conn, procedure, parameters, outputParams))
				{
					//Write back
					cmd.ExecuteNonQuery();
					return cmd.GetOutputParameterValues();
				}
			}
		}

		/// <summary>
		/// Executes a stored procedure (with parameters) expecting a single scalar return. 
		/// </summary>
		/// <typeparam name="T">A Value Type returned</typeparam>
		/// <param name="procedure">The procedure to execute</param>
		/// <param name="parameters">Parameters to pass into the stored procedure</param>
		/// <returns>A scalar value type result of the stored procedure</returns>
		public T ExecuteScalarProcedure<T>(string procedure, Dictionary<string, object> parameters) where T : struct
		{
			//Open/Retrieve Connection
			using (Connection conn = GetConnection())
			{
				//Query
				using (Command cmd = GetCommand(conn, procedure, parameters, null))
				{
					dynamic scalar = cmd.ExecuteScalar();

					//It is possible to receive null at this point, in which case just return the default value of T 
					if ((object.ReferenceEquals(scalar, DBNull.Value)))
					{
						scalar = default(T);
					}

					try
					{
						//Attempt to cast
						return (T)scalar;
					}
					catch (InvalidCastException)
					{
						//If cast failed, then provide a more detailed message stating what type was received and what type was expected
						throw new InvalidCastException(
						    String.Format("Cannot cast what was received from the database: '{0}' to the specified type '{1}'",
									   scalar.GetType.FullName, typeof(T).FullName));
					}
				}
			}
		}


		/// <summary>
		/// Bulk insert data from a list of objects into a table
		/// </summary>
		/// <remarks>
		/// The table columns and the object properties MUST line up. 
		/// Use overload if you wish to provide column mappings
		/// </remarks>
		public void BulkInsert<T>(string tablename, List<T> data)
		{
			BulkInsert(tablename, data.ToDataTable());
		}

		/// <summary>
		/// Bulk insert data from a list of objects into a table. This method allows you to provide your own property to column mapping
		/// </summary>
		public void BulkInsert<T>(string tablename, List<T> data, Dictionary<string, string> mappings)
		{
			BulkInsert(tablename, data.ToDataTable(), mappings);
		}

		/// <summary>
		/// Bulk insert data from a datatable into a table in the DB
		/// </summary>
		/// <remarks>
		/// The table columns and the datatable columns MUST line up. 
		/// Use overload if you wish to provide column mappings
		/// </remarks>
		public void BulkInsert(string tablename, DataTable data)
		{
			BulkInsert(tablename, data, null);
		}

		/// <summary>
		/// Bulk insert data from a datatable into a table. This method allows you to provide your own property to column mapping
		/// </summary>
		public void BulkInsert(string tablename, DataTable data, Dictionary<string, string> mappings)
		{
			dynamic connstring = GetConnection().ConnectionString;

			using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connstring, SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints))
			{
				bulkcopy.DestinationTableName = tablename;
				bulkcopy.BulkCopyTimeout = 512;

				//Bind mappings if they exist

				if ((mappings != null))
				{
					foreach (KeyValuePair<string, string> kvp in mappings)
					{
						bulkcopy.ColumnMappings.Add(kvp.Key, kvp.Value);
					}
				}

				bulkcopy.WriteToServer(data);
				bulkcopy.Close();
			}
		}

		/// <summary>
		/// Begin a transaction session
		/// This will assume an unspecied IsolationLevel
		/// </summary>
		/// <returns>The session created - nested or otherwise</returns>
		/// <remarks>
		/// When a session is created using this method then that session is stored with the WhipperSnapper instance.
		/// So if that instance is used in multiple thread simultaneously then all DB interaction done through the 
		/// WhipperSnapper instance will be on the same transaction and connection.
		/// 
		/// Note - only one session will be created per WhipperSnapper instance. You cannot utilize this method to create subsequent nested sessions.
		/// </remarks>
		public Session.Session BeginSession()
		{
			if (_CreatedSession == null)
			{
				this._CreatedSession = Session.Session.GetSession(this.ConnectionString, IsolationLevel.Unspecified);
			}
			return _CreatedSession;
		}

		/// <summary>
		/// Begin a transaction session
		/// </summary>
		/// <param name="isolevel">The isolation level for the transaction to use</param>
		/// <returns>The session created - nested or otherwise</returns>
		/// <remarks>
		/// When a session is created using this method then that session is stored with the WhipperSnapper instance.
		/// So if that instance is used in multiple thread simultaneously then all DB interaction done through the 
		/// WhipperSnapper instance will be on the same transaction and connection.
		/// 
		/// Note - only one session will be created per WhipperSnapper instance. You cannot utilize this method to create subsequent nested sessions.
		/// </remarks>
		public Session.Session BeginSession(IsolationLevel isolevel)
		{
			if (_CreatedSession == null)
			{
				this._CreatedSession = Session.Session.GetSession(this.ConnectionString, isolevel);
			}
			return _CreatedSession;
		}

		#endregion

		/// <summary>
		/// Gets a Database object. Connection Abstraction should be done here
		/// </summary>
		private Connection GetConnection()
		{
			//If we created a session IN this WhipperSnapper instance then use that connection
			if (this._CreatedSession != null)
			{
				if (!this._CreatedSession.IsActive)
				{
					//If the session isn't active then it's been disposed - so we do some late cleanup here
					this._CreatedSession = null;
				}
				else
				{
					//If the session is active with a live transaction, then default to that.
					return _CreatedSession.Connection;
				}
			}

			//If I'm in a session that wasn't created by this instance, default to that connection.
			WipperSnapper.Session.Session ses = SessionManager.GetSessionForThisThread();
			if (ses != null)
			{
				return ses.Connection;
			}

			//If not transaction present then call into the connection manager to fetch/create the connection
			return Connection.GetConnection(_ConnectionStringKey, this.ConnectionString, true);
		}

		/// <summary>
		/// Creates a db command and maps parameters accordingly
		/// </summary>
		/// <param name="db">The database the command will query against</param>
		/// <param name="procedure">The stored procedure to execute</param>
		/// <param name="parameters">Dictionary of all parameters</param>
		/// <returns>a DBCommand</returns>
		private Command GetCommand(Connection conn, string procedure, Dictionary<string, object> parameters, List<string> outputParams)
		{
			// Create a command on the stored procedure
			Command cmd = new Command(conn);

			// Set sql to execute the stored proc
			cmd.Sql = procedure;

			// Add input parameters - null check
			if (parameters == null) parameters = new Dictionary<string, object>();
			if (outputParams == null) outputParams = new List<string>();

			// Set the parameters
			cmd.AddInputParametersDictionary(parameters);
			cmd.AddOutputParameters(outputParams);

			return cmd;
		}


		#region Dynamic Mapping

		/// <summary>
		/// Default map to DTO delegate, uses reflection and maps all columns in reader to properties of the same name
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to</typeparam>
		/// <param name="reader">The datareader result of the sql query</param>
		/// <returns>The populated DTO</returns>
		/// <remarks>You can write your own MapToDTODelegate</remarks>
		static internal T MapToDTO<T>(IDataReader reader)
		{
			//Get the type
			Type type = typeof(T);

			//Interfaces can't be mapped automatically since we can't instantiate them
			if ((type.IsInterface))
			{
				throw new Exception("Interfaces can't be mapped automatically since we can't instantiate them");
			}

			//If we're dealing with primitives then we're going to do something different.
			if ((type.IsPrimitive))
			{
				return _Primitive_MapToDTO<T>(type, reader);
			}

			//Enums are special
			if ((type.IsEnum))
			{
				return _Enum_MapToDTO<T>(type, reader);
			}

			//By default use the complex type mapper
			return _Complex_MapToDTO<T>(type, reader);
		}

		/// <summary>
		/// Primitive types are handled differently. They don't have properties so the values in the columns
		/// of the data reader ARE of the primitive type
		/// </summary>
		/// <remarks>
		/// Multiple columns don't automatically make sense for primitive types 
		/// so for the sake of sanity we only read the first colummn
		/// </remarks>
		private static T _Primitive_MapToDTO<T>(Type type, IDataReader reader)
		{
			return (T)reader.GetValue(0);
		}

		/// <summary>
		/// Enums are different yet from primitives and complex types. They're like primitives in that
		/// the value in the datareader IS the enum value. However unlike primitives they can't just 
		/// be directly converted. We have to take them through the System.Enum object.
		/// </summary>
		private static T _Enum_MapToDTO<T>(Type type, IDataReader reader)
		{
			return (T)Enum.Parse(type, reader.GetValue(0).ToString());
		}

		/// <summary>
		/// Default behavior of our automatic mapper. This will assume a complex type and attempt 
		/// to match properties of the object against columns of the data row. 
		/// </summary>
		private static T _Complex_MapToDTO<T>(Type type, IDataReader reader)
		{

			//Create a new instance (default constructor called)
			dynamic inst = Activator.CreateInstance(type);

			//Set the properties of the object

			foreach (PropertyInfo prop in type.GetProperties(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public))
			{
				//Property exists in the reader

				if ((ReaderHasKey(reader, prop.Name)))
				{
					//Special Null check
					dynamic val = reader[prop.Name];
					if ((object.ReferenceEquals(val, DBNull.Value)))
					{
						val = default(T);
					}

					//Special Type check
					dynamic propType = prop.PropertyType;

					if ((propType.IsEnum))
					{
						//Convert the stored value back into the enum
						prop.SetValue(inst, Enum.Parse(prop.PropertyType, val.ToString()), null);
					}
					else
					{
						//Set the value raw
						prop.SetValue(inst, val, null);
					}
				}
			}
			return (T)inst;
		}

		/// <summary>
		/// Checks to see if a reader has a key
		/// </summary>
		private static bool ReaderHasKey(IDataReader reader, string key)
		{
			for (int i = 0; i <= reader.FieldCount - 1; i++)
			{
				string name = reader.GetName(i);
				if ((name.Equals(key, StringComparison.InvariantCultureIgnoreCase)))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Maps a sql row to a DTO. This delegate allows you to inject logic into the sql selection process
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to</typeparam>
		/// <param name="reader">The datareader result of the sql query</param>
		/// <returns>The newly created and ppulated DTO</returns>
		public delegate T MapToDTODelegate<T>(IDataReader reader);

		#endregion


		/// <summary>
		/// This is used for bulk insertting. 
		/// This will build a dictionary of property name to property name mapping based on all the properties of a DTO.
		/// This makes for a great starting point to maniuplate single columns of mapping without having to remap the rest of the properties.
		/// 
		/// This WILL add inherited properties in the mapping.
		///  </summary>
		/// <param name="t">The type to build the mapping on</param>
		public static Dictionary<string, string> BuildDefaultBulkInsertMapping(Type t)
		{
			Dictionary<string, string> dict = new Dictionary<string, string>();

			foreach (PropertyInfo prop in t.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy))
			{
				dict.Add(prop.Name, prop.Name);
			}

			return dict;
		}
	}

	/// <summary>
	/// A specific exception defining a specific occurance
	/// </summary>
	public class MultipleRecordsFoundException : Exception
	{
		public MultipleRecordsFoundException()
			: base("Found many results when expecting only a single result")
		{
		}
	}

}