namespace WipperSnapper
{
	using System;
	using System.Data;
	using System.Collections.Generic;
	using System.Linq;
	using System.Reflection;
	using System.Runtime.CompilerServices;
	using System.ComponentModel;

	public static class EnumerableExtensions
	{
		/// <summary>
		/// Converts an enumerable collection to a DataTable. 
		/// This was apparently too much for the .NET team to tackle yet again.
		/// http://stackoverflow.com/questions/564366/generic-list-to-datatable
		/// </summary>
		/// <typeparam name="T">The type in the list</typeparam>
		/// <param name="mycol">The collection in question</param>
		/// <returns>A datatable corresponding to the data in the enumerable collection</returns>
		public static DataTable ToDataTable<T>(this IEnumerable<T> mycol)
		{
			// Get the properties
			List<PropertyDescriptor> props = new List<PropertyDescriptor>();

			//Filter complex types
			foreach (PropertyDescriptor prop in TypeDescriptor.GetProperties(typeof(T))) 
			{
				if ((prop.PropertyType.IsPrimitive || 
					prop.PropertyType.Equals(typeof(Guid)) || 
					prop.PropertyType.Equals(typeof(string)) || 
					prop.PropertyType.Equals(typeof(DateTime)) || 
					prop.PropertyType.Equals(typeof(decimal)))) 
				{
					props.Add(prop);
				}
			}

			DataTable table = new DataTable();

			//Create a column for each property
			for (int i = 0; i <= props.Count - 1; i++) 
			{
				dynamic prop = props[i];
				table.Columns.Add(prop.Name, prop.PropertyType);
			}

			object[] values = new object[props.Count];

			//Each item becomes a row
			foreach (T item in mycol) 
			{
				//Map the property
				for (int i = 0; i <= values.Length - 1; i++) 
				{
					values[i] = props[i].GetValue(item);
				}

				//Add as a row
				table.Rows.Add(values);
			}

			return table;
		}

		/// <summary>
		/// Takes a list and returns a collection of lists grouped by the index.
		/// </summary>
		public static Dictionary<TResult, List<T>> ToIndexedList<T, TResult>(this IEnumerable<T> mycol, Func<T, TResult> keySelector)
		{
			Dictionary<TResult, List<T>> indexedList = new Dictionary<TResult, List<T>>();
			foreach (T item in mycol)
			{
				TResult key = keySelector(item);
				if ((!indexedList.ContainsKey(key)))
				{
					indexedList.Add(key, new List<T>());
				}
				indexedList[key].Add(item);
			}
			return indexedList;
		}
	}
}
