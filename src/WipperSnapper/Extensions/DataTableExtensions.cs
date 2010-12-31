namespace WipperSnapper
{
	using System;
	using System.Data;
	using System.Collections.Generic;
	using System.Linq;
	using System.Reflection;
	using System.Runtime.CompilerServices;

	public static class DataTableExtensions
	{
		/// <summary>
		/// Converts a DataTable to a List of objects of type <typeparamref name="T" />.
		/// <typeparamref name="T" /> must have a default constructor. 
		/// </summary>
		/// <typeparam name="T">The type that maps to a row in the datatable. Must have a default constructor</typeparam>
		/// <returns>A list of objects defined from the data table</returns>
		public static List<T> ToList<T>(this DataTable table)
		{
			// Setup
			List<T> ret = new List<T>();
			Type tp = typeof(T);

			// NonPublic, Public, and Static. This may cause unforseen side effects but I'll leave it for now.
			BindingFlags PropSearchBindingFlags = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.IgnoreCase;

			// Populate the list
			foreach(DataRow r in table.Rows)
			{
				// Instance of the row
				T newObj = (T)Activator.CreateInstance(tp);
				foreach (DataColumn c in table.Columns)
				{

					// Map the columns to the properties instead of properties to the columns. Saves a try/catch.
					PropertyInfo prop = tp.GetProperty(c.ColumnName, PropSearchBindingFlags);
					if (prop != null)
					{
						prop.SetValue(newObj, r[c.ColumnName], null);
					}
				}

				// Wee ha
				ret.Add(newObj);
			}

			return ret;
		}
	}
}
