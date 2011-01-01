namespace WipperSnapper
{

	using System;
	using System.Collections.Generic;
	using System.Configuration;
	using System.Data;
	using System.Data.Common;
	using System.Reflection;
	using System.Data.SqlClient;
	using System.Linq;
	using WipperSnapper.SQL;

	/// <summary>
	/// This is a simple library that helps relieve the pain of having to map SQL Query output to managed types. 
	/// You're able to execute stored procedures directly and even provide your own DataReader - To - Object mapping
	/// function should you so choose to. 
	///
	/// This library is currently dependent on Enterprise Library and thus has practically no reliable transaction support.
	/// 
	/// Future Items:
	/// - Remove the epic fail dependency to Enterprise Library
	/// - Add in managed connection pool
	/// - Add in managed SQL transactions with REAL nested transaction behavior: a concept that apparently is foreign to any other .NET library.
	/// - Add in ORM feature - SQL building based on attributed objects
	/// - Rewrite in C# to get around VB.NET's lack of real null support.
	/// </summary>
	public static class DynamicService
	{

		/// <summary>
		/// Finds a single DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
		/// The DTO is automatically created and populated through reflection
		/// </summary>
		/// <typeparam name="T">The type of DTO to map to.</typeparam>
		/// <param name="procedure">The name of the stored procedure to execute</param>
		/// <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
		/// <returns>A single DTO result</returns>
		public static T FindSingle<T>(string procedure, Dictionary<string, object> parameters)
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
		public static T FindSingle<T>(string procedure, Dictionary<string, object> parameters, MapToDTODelegate<T> mapFunc)
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
		public static List<T> FindMultiple<T>(string procedure, Dictionary<string, object> parameters)
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
		public static List<T> FindMultiple<T>(string procedure, Dictionary<string, object> parameters, MapToDTODelegate<T> mapFunc)
		{
			//'Create return set
			List<T> ret = new List<T>();

			//'Creates a command to the stored procedure
			using (Command cmd = GetCommand(procedure, parameters, null))
			{
				//Query and read
				using (IDataReader reader = cmd.ExecuteReader())
				{
					while (reader.Read())
					{
						ret.Add(mapFunc(reader));
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
		public static void ExecuteVoidProcedure(string procedure, Dictionary<string, object> parameters)
		{
			ExecuteVoidProcedure(procedure, parameters, null);
		}


		/// <summary>
		/// Executes a stored procedure and returns the output parameters whose names are given in <paramref name="outputParams" />
		/// </summary>
		/// <param name="procedure">The procedure to execute</param>
		/// <param name="parameters">Parameters to pass it</param>
		/// <param name="outputParams">list of output parameters</param>
		public static Dictionary<string, object> ExecuteVoidProcedure(string procedure, Dictionary<string, object> parameters, List<string> outputParams)
		{
			//Query
			using (Command cmd = GetCommand(procedure, parameters, outputParams))
			{
				//Write back
				return cmd.GetOutputParameterValues();
			}
		}

		/// <summary>
		/// Executes a stored procedure (with parameters) expecting a single scalar return. 
		/// </summary>
		/// <typeparam name="T">A Value Type returned</typeparam>
		/// <param name="procedure">The procedure to execute</param>
		/// <param name="parameters">Parameters to pass into the stored procedure</param>
		/// <returns>A scalar value type result of the stored procedure</returns>
		public static T ExecuteScalarProcedure<T>(string procedure, Dictionary<string, object> parameters) where T : struct
		{
			//Query
			using (Command cmd = GetCommand(procedure, parameters, null))
			{
				dynamic scalar = cmd.ExecuteScalar();

				//Write back
				return (T)scalar;
			}
		}


		/// <summary>
		/// Bulk insert data from a list of objects into a table
		/// </summary>
		/// <remarks>
		/// The table columns and the object properties MUST line up. 
		/// Use overload if you wish to provide column mappings
		/// </remarks>
		public static void BulkInsert<T>(string tablename, List<T> data)
		{
			BulkInsert(tablename, data.ToDataTable());
		}

		/// <summary>
		/// Bulk insert data from a list of objects into a table. This method allows you to provide your own property to column mapping
		/// </summary>
		public static void BulkInsert<T>(string tablename, List<T> data, Dictionary<string, string> mappings)
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
		public static void BulkInsert(string tablename, DataTable data)
		{
			BulkInsert(tablename, data, null);
		}

		/// <summary>
		/// Bulk insert data from a datatable into a table. This method allows you to provide your own property to column mapping
		/// </summary>
		public static void BulkInsert(string tablename, DataTable data, Dictionary<string, string> mappings)
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
		/// Gets a Database object. Connection Abstraction should be done here
		/// </summary>
		private static Connection GetConnection()
		{
			//We'll know which connection string based on an app setting
			string dbName = ConfigurationManager.AppSettings["ConnectionStringKey"];
			if(string.IsNullOrWhiteSpace(dbName))
			{
				throw new Exception("Missing App Setting: ConnectionStringKey");
			}

			//Go get the connection string. If it's not there throw an informative exception (vs a null ref)
			ConnectionStringSettings connString = ConfigurationManager.ConnectionStrings[dbName];
			if (connString == null || string.IsNullOrWhiteSpace(connString.ConnectionString))
			{
				throw new Exception("Missing Connection String For: " + dbName);
			}

			return Connection.GetConnection(dbName, connString.ConnectionString, true);
		}

		/// <summary>
		/// Creates a db command and maps parameters accordingly
		/// </summary>
		/// <param name="db">The database the command will query against</param>
		/// <param name="procedure">The stored procedure to execute</param>
		/// <param name="parameters">Dictionary of all parameters</param>
		/// <returns>a DBCommand</returns>
		private static Command GetCommand(string procedure, Dictionary<string, object> parameters, List<string> outputParams)
		{
			// Get a connection
			Connection conn = GetConnection();

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
						val = null;
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