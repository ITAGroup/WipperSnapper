Imports System.Reflection
Imports System.Runtime.CompilerServices
Imports System.ComponentModel

''' <summary>
''' Enumerable collections Extensions
''' </summary>
''' <remarks></remarks>
Public Module EnumerableExtensions

    ''' <summary>
    ''' Converts an enumerable collection to a DataTable. 
    ''' 
    ''' This was apparently too much for the .NET team to tackle yet again.
    ''' http://stackoverflow.com/questions/564366/generic-list-to-datatable
    ''' </summary>
    ''' <typeparam name="T">The type in the list</typeparam>
    ''' <param name="mycol">The collection in question</param>
    ''' <returns>A datatable corresponding to the data in the enumerable collection</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Public Function ToDataTable(Of T)(ByVal mycol As IEnumerable(Of T)) As DataTable

        'Get the properties
        Dim props As New List(Of PropertyDescriptor)

        'Filter complex types
        For Each prop As PropertyDescriptor In TypeDescriptor.GetProperties(GetType(T))
            If (prop.PropertyType.IsPrimitive _
                OrElse prop.PropertyType.Equals(GetType(Guid)) _
                OrElse prop.PropertyType.Equals(GetType(String)) _
                OrElse prop.PropertyType.Equals(GetType(DateTime)) _
                OrElse prop.PropertyType.Equals(GetType(Decimal)) _
                ) Then
                props.Add(prop)
            End If
        Next

        Dim table As New DataTable()

        'Create a column for each property
        For i As Integer = 0 To props.Count - 1
            Dim prop = props(i)
            table.Columns.Add(prop.Name, prop.PropertyType)
        Next

        Dim values(props.Count - 1) As Object

        'Each item becomes a row
        For Each item In mycol

            'Map the property
            For i As Integer = 0 To values.Length - 1
                values(i) = props(i).GetValue(item)
            Next

            'Add as a row
            table.Rows.Add(values)
        Next

        Return table
    End Function

    ''' <summary>
    ''' Takes a list and returns a collection of lists grouped by the index.
    ''' </summary>
    <Extension()> _
    Public Function ToIndexedList(Of T, TResult)(ByVal mycol As IEnumerable(Of T), ByVal keySelector As Func(Of T, TResult)) As Dictionary(Of TResult, List(Of T))
        Dim indexedList As New Dictionary(Of TResult, List(Of T))()

        For Each item In mycol
            Dim key = keySelector(item)

            If (Not indexedList.ContainsKey(key)) Then
                indexedList.Add(key, New List(Of T))
            End If

            indexedList(key).Add(item)
        Next

        Return indexedList
    End Function


End Module
