Imports System.Reflection
Imports System.Runtime.CompilerServices

''' <summary>
''' Fun extensions for DataTables
''' </summary>
''' <remarks></remarks>
Public Module DataTableExtensions

    ''' <summary>
    ''' Converts a DataTable to a List of objects of type <typeparamref name="T" />.
    ''' <typeparamref name="T" /> must have a default constructor. 
    ''' </summary>
    ''' <typeparam name="T">The type that maps to a row in the datatable. Must have a default constructor</typeparam>
    ''' <returns>A list of objects defined from the data table</returns>
    <Extension()> _
    Public Function ToList(Of T)(ByVal table As DataTable) As List(Of T)

        'Setup
        Dim ret As New List(Of T)
        Dim tp = GetType(T)

        'NonPublic, Public, and Static. This may cause unforseen side effects but I'll leave it for now.
        Dim PropSearchBindingFlags = BindingFlags.NonPublic Or _
                                     BindingFlags.Public Or _
                                     BindingFlags.Instance Or _
                                     BindingFlags.Static Or _
                                     BindingFlags.IgnoreCase

        'Populate the list
        For Each r As DataRow In table.Rows

            'Instance of the row
            Dim newObj As T = Activator.CreateInstance(tp)
            For Each c As DataColumn In table.Columns

                'Map the columns to the properties instead of properties to the columns. Saves a try/catch.
                Dim prop = tp.GetProperty(c.ColumnName, PropSearchBindingFlags)
                If (prop IsNot Nothing) Then
                    prop.SetValue(newObj, r.Item(c.ColumnName), Nothing)
                End If

            Next

            'Wee ha
            ret.Add(newObj)
        Next

        Return ret
    End Function

End Module
