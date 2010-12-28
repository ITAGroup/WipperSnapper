Imports System.Data.Common
Imports Microsoft.Practices.EnterpriseLibrary.Data
Imports System.Reflection
Imports System.Data.SqlClient

''' <summary>
''' This is a simple library that helps relieve the pain of having to map SQL Query output to managed types. 
''' You're able to execute stored procedures directly and even provide your own DataReader - To - Object mapping
''' function should you so choose to. 
'''
''' This library is currently dependent on Enterprise Library and thus has practically no reliable transaction support.
''' 
''' Future Items:
''' - Remove the epic fail dependency to Enterprise Library
''' - Add in managed connection pool
''' - Add in managed SQL transactions with REAL nested transaction behavior: a concept that apparently is foreign to any other .NET library.
''' - Add in ORM feature - SQL building based on attributed objects
''' - Rewrite in C# to get around VB.NET's lack of real null support.
''' </summary>
Public Module DynamicService

    ''' <summary>
    ''' Finds a single DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
    ''' The DTO is automatically created and populated through reflection
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to.</typeparam>
    ''' <param name="procedure">The name of the stored procedure to execute</param>
    ''' <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
    ''' <returns>A single DTO result</returns>
    Public Function FindSingle(Of T)(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object)) As T
        Return FindSingle(Of T)(procedure, parameters, AddressOf MapToDTO)
    End Function


    ''' <summary>
    ''' Finds a single DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
    ''' The DTO is creatd and mapped based on the function <paramref name="mapFunc" />
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to.</typeparam>
    ''' <param name="procedure">The name of the stored procedure to execute</param>
    ''' <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
    ''' <param name="mapFunc">The function used to create and map the result of the stored procedure to the DTO </param>
    ''' <returns>A single DTO result</returns>
    Public Function FindSingle(Of T)(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object), ByVal mapFunc As MapToDTODelegate(Of T)) As T

        Dim result = FindMultiple(Of T)(procedure, parameters, mapFunc)

        If (result.Count > 1) Then
            Throw New MultipleRecordsFoundException()

        ElseIf (result.Count = 0) Then
            Return Nothing

        Else
            Return result(0)

        End If

    End Function

    ''' <summary>
    ''' Finds multiple DTOs from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
    ''' The DTOs are automatically created and mapped from the sql results
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to.</typeparam>
    ''' <param name="procedure">The name of the stored procedure to execute</param>
    ''' <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
    ''' <returns>A list of DTOs</returns>
    Public Function FindMultiple(Of T)(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object)) As List(Of T)
        Return FindMultiple(Of T)(procedure, parameters, AddressOf MapToDTO)
    End Function


    ''' <summary>
    ''' Finds multiple DTO from a stored procedure <paramref name="procedure" /> with the parameters <paramref name="parameters" />.
    ''' The DTOs are creatd and mapped based on the function <paramref name="mapFunc" />
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to.</typeparam>
    ''' <param name="procedure">The name of the stored procedure to execute</param>
    ''' <param name="parameters">Parameters indexed by name to be passed to the procedure</param>
    ''' <param name="mapFunc">The function used to create and map 1 row result of the stored procedure to one DTO </param>
    ''' <returns>A list of DTOs</returns>
    Public Function FindMultiple(Of T)(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object), ByVal mapFunc As MapToDTODelegate(Of T)) As List(Of T)
        ''Create database connection (could be config driven)
        Dim db As Database = GetDatabase()

        ''Create return set
        Dim ret As New List(Of T)

        ''Creates a command to the stored procedure
        Using cmd As DbCommand = GetCommand(db, procedure, parameters, Nothing)
            'Query and read
            Using reader As IDataReader = db.ExecuteReader(cmd)
                While reader.Read
                    ret.Add(mapFunc(reader))
                End While
            End Using
        End Using

        'Return the set
        Return ret
    End Function

    ''' <summary>
    ''' Executes a stored procedure that doesn't return anything back. No output parameters or data reader
    ''' </summary>
    ''' <param name="procedure">The procedure to execute</param>
    ''' <param name="parameters">Parameters to pass it</param>
    Public Sub ExecuteVoidProcedure(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object))
        ExecuteVoidProcedure(procedure, parameters, Nothing)
    End Sub


    ''' <summary>
    ''' Executes a stored procedure and returns the output parameters whose names are given in <paramref name="outputParams" />
    ''' </summary>
    ''' <param name="procedure">The procedure to execute</param>
    ''' <param name="parameters">Parameters to pass it</param>
    ''' <param name="outputParams">list of output parameters</param>
    Public Function ExecuteVoidProcedure(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object), ByVal outputParams As List(Of String)) As Dictionary(Of String, Object)

        'Grab connection
        Dim db As Database = GetDatabase()

        'Query
        Using cmd = GetCommand(db, procedure, parameters, outputParams)
            db.ExecuteNonQuery(cmd)

            'Write back
            Return WriteBackParameters(db, cmd, outputParams)
        End Using

    End Function

    ''' <summary>
    ''' Executes a stored procedure (with parameters) expecting a single scalar return. 
    ''' </summary>
    ''' <typeparam name="T">A Value Type returned</typeparam>
    ''' <param name="procedure">The procedure to execute</param>
    ''' <param name="parameters">Parameters to pass into the stored procedure</param>
    ''' <returns>A scalar value type result of the stored procedure</returns>
    Public Function ExecuteScalarProcedure(Of T As Structure)(ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object)) As T

        'Grab connection
        Dim db As Database = GetDatabase()

        'Query
        Using cmd = GetCommand(db, procedure, parameters, Nothing)
            Dim scalar = db.ExecuteScalar(cmd)

            'Write back
            Return DirectCast(scalar, T)
        End Using

    End Function


    ''' <summary>
    ''' Bulk insert data from a list of objects into a table
    ''' </summary>
    ''' <remarks>
    ''' The table columns and the object properties MUST line up. 
    ''' Use overload if you wish to provide column mappings
    ''' </remarks>
    Public Sub BulkInsert(Of T)(ByVal tablename As String, ByVal data As List(Of T))
        BulkInsert(tablename, data.ToDataTable())
    End Sub

    ''' <summary>
    ''' Bulk insert data from a list of objects into a table. This method allows you to provide your own property to column mapping
    ''' </summary>
    Public Sub BulkInsert(Of T)(ByVal tablename As String, ByVal data As List(Of T), ByVal mappings As Dictionary(Of String, String))
        BulkInsert(tablename, data.ToDataTable(), mappings)
    End Sub

    ''' <summary>
    ''' Bulk insert data from a datatable into a table in the DB
    ''' </summary>
    ''' <remarks>
    ''' The table columns and the datatable columns MUST line up. 
    ''' Use overload if you wish to provide column mappings
    ''' </remarks>
    Public Sub BulkInsert(ByVal tablename As String, ByVal data As DataTable)
        BulkInsert(tablename, data, Nothing)
    End Sub

    ''' <summary>
    ''' Bulk insert data from a datatable into a table. This method allows you to provide your own property to column mapping
    ''' </summary>
    Public Sub BulkInsert(ByVal tablename As String, ByVal data As DataTable, ByVal mappings As Dictionary(Of String, String))
        Dim connstring = GetDatabase().ConnectionString

        Using bulkcopy As New SqlBulkCopy(connstring, SqlBulkCopyOptions.UseInternalTransaction Or SqlBulkCopyOptions.FireTriggers Or SqlBulkCopyOptions.CheckConstraints)
            bulkcopy.DestinationTableName = tablename
            bulkcopy.BulkCopyTimeout = 300

            'Bind mappings if they exist
            If (mappings IsNot Nothing) Then

                For Each kvp In mappings
                    bulkcopy.ColumnMappings.Add(kvp.Key, kvp.Value)
                Next

            End If

            bulkcopy.WriteToServer(data)
            bulkcopy.Close()
        End Using
    End Sub

    ''' <summary>
    ''' Gets a Database object. Connection Abstraction should be done here
    ''' </summary>
    Private Function GetDatabase() As Database
        ''Create database connection (could be config driven)
        Return DatabaseFactory.CreateDatabase("Kohler")
    End Function

    ''' <summary>
    ''' Creates a db command and maps parameters accordingly
    ''' </summary>
    ''' <param name="db">The database the command will query against</param>
    ''' <param name="procedure">The stored procedure to execute</param>
    ''' <param name="parameters">Dictionary of all parameters</param>
    ''' <returns>a DBCommand</returns>
    Private Function GetCommand(ByVal db As Database, ByVal procedure As String, ByVal parameters As Dictionary(Of String, Object), ByVal outputParams As List(Of String)) As DbCommand

        'Create a command on the stored procedure
        Dim cmd As DbCommand = db.GetStoredProcCommand(procedure)

        ''Add input parameters
        'null check
        If (parameters Is Nothing) Then parameters = New Dictionary(Of String, Object)()
        For Each kvp As KeyValuePair(Of String, Object) In parameters

            'Dim param = cmd.CreateParameter()
            Dim param = New SqlParameter()
            param.ParameterName = kvp.Key
            If (TypeOf kvp.Value Is DataTable) Then
                param.SqlDbType = SqlDbType.Structured
            End If

            'Add parameter direction
            If (Not outputParams Is Nothing AndAlso outputParams.Contains(kvp.Key)) Then
                param.Direction = ParameterDirection.InputOutput
                param.Size = 512
                'eh why not
            Else
                param.Direction = ParameterDirection.Input
            End If

            'DB null if null
            If (kvp.Value Is Nothing) Then
                param.Value = DBNull.Value
            Else
                param.Value = kvp.Value
            End If

            cmd.Parameters.Add(param)
        Next

        ''Add outout parameters
        If (Not outputParams Is Nothing) Then
            For Each leftOver In outputParams.Except(parameters.Keys)
                Dim param = cmd.CreateParameter()

                param.ParameterName = leftOver
                param.Direction = ParameterDirection.Output
                param.Size = 512
                'eh why not

                cmd.Parameters.Add(param)
            Next
        End If

        'Set Long Timeout
        cmd.CommandTimeout = 500

        Return cmd
    End Function


    ''' <summary>
    ''' Default map to DTO delegate, uses reflection and maps all columns in reader to properties of the same name
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to</typeparam>
    ''' <param name="reader">The datareader result of the sql query</param>
    ''' <returns>The populated DTO</returns>
    ''' <remarks>You can write your own MapToDTODelegate</remarks>
    Friend Function MapToDTO(Of T)(ByVal reader As IDataReader) As T

        'Get the type
        Dim type As Type = GetType(T)

        'Interfaces can't be mapped automatically since we can't instantiate them
        If (type.IsInterface) Then
            Throw New Exception("Interfaces can't be mapped automatically since we can't instantiate them")
        End If

        'If we're dealing with primitives then we're going to do something different.
        If (type.IsPrimitive) Then
            Return _Primitive_MapToDTO(Of T)(type, reader)
        End If

        'Enums are special
        If (type.IsEnum) Then
            Return _Enum_MapToDTO(Of T)(type, reader)
        End If

        'By default use the complex type mapper
        Return _Complex_MapToDTO(Of T)(type, reader)
    End Function

    ''' <summary>
    ''' Primitive types are handled differently. They don't have properties so the values in the columns
    ''' of the data reader ARE of the primitive type
    ''' </summary>
    ''' <remarks>
    ''' Multiple columns don't automatically make sense for primitive types 
    ''' so for the sake of sanity we only read the first colummn
    ''' </remarks>
    Private Function _Primitive_MapToDTO(Of T)(ByVal type As Type, ByVal reader As IDataReader) As T
        Return CType(reader.GetValue(0), T)
    End Function

    ''' <summary>
    ''' Enums are different yet from primitives and complex types. They're like primitives in that
    ''' the value in the datareader IS the enum value. However unlike primitives they can't just 
    ''' be directly converted. We have to take them through the System.Enum object.
    ''' </summary>
    Private Function _Enum_MapToDTO(Of T)(ByVal type As Type, ByVal reader As IDataReader) As T
        Return CType([Enum].Parse(type, reader.GetValue(0).ToString()), T)
    End Function

    ''' <summary>
    ''' Default behavior of our automatic mapper. This will assume a complex type and attempt 
    ''' to match properties of the object against columns of the data row. 
    ''' </summary>
    Private Function _Complex_MapToDTO(Of T)(ByVal type As Type, ByVal reader As IDataReader) As T

        'Create a new instance (default constructor called)
        Dim inst = Activator.CreateInstance(type)

        'Set the properties of the object
        For Each prop As PropertyInfo In type.GetProperties(BindingFlags.Instance Or BindingFlags.NonPublic Or BindingFlags.Public)

            'Property exists in the reader
            If (ReaderHasKey(reader, prop.Name)) Then

                'Special Null check
                Dim val = reader(prop.Name)
                If (val Is DBNull.Value) Then
                    val = Nothing
                End If

                'Special Type check
                Dim propType = prop.PropertyType

                If (propType.IsEnum) Then
                    'Convert the stored value back into the enum
                    prop.SetValue(inst, [Enum].Parse(prop.PropertyType, val.ToString()), Nothing)
                Else
                    'Set the value raw
                    prop.SetValue(inst, val, Nothing)
                End If

            End If
        Next

        Return CType(inst, T)
    End Function

    ''' <summary>
    ''' Writes output parameters back to the dictionary
    ''' (really, it just writes everything back to the parameter dictionary)
    ''' </summary>
    Private Function WriteBackParameters(ByVal db As Database, ByVal cmd As DbCommand, ByVal outputParams As List(Of String)) As Dictionary(Of String, Object)
        Dim ret As New Dictionary(Of String, Object)

        If (outputParams Is Nothing) Then
            Return ret
        End If

        For Each key As String In outputParams
            ret(key) = db.GetParameterValue(cmd, key)
        Next

        Return ret
    End Function

    ''' <summary>
    ''' Checks to see if a reader has a key
    ''' </summary>
    Private Function ReaderHasKey(ByVal reader As IDataReader, ByVal key As String) As Boolean
        For i = 0 To reader.FieldCount - 1
            Dim name = reader.GetName(i)
            If (name.Equals(key, StringComparison.InvariantCultureIgnoreCase)) Then
                Return True
            End If
        Next
        Return False
    End Function

    ''' <summary>
    ''' Maps a sql row to a DTO. This delegate allows you to inject logic into the sql selection process
    ''' </summary>
    ''' <typeparam name="T">The type of DTO to map to</typeparam>
    ''' <param name="reader">The datareader result of the sql query</param>
    ''' <returns>The newly created and ppulated DTO</returns>
    Public Delegate Function MapToDTODelegate(Of T)(ByVal reader As IDataReader) As T

    ''' <summary>
    ''' This is used for bulk insertting. 
    ''' This will build a dictionary of property name to property name mapping based on all the properties of a DTO.
    ''' This makes for a great starting point to maniuplate single columns of mapping without having to remap the rest of the properties.
    ''' 
    ''' This WILL add inherited properties in the mapping.
    '''  </summary>
    ''' <param name="t">The type to build the mapping on</param>
    Public Function BuildDefaultBulkInsertMapping(ByVal t As Type) As Dictionary(Of String, String)
        Dim dict As New Dictionary(Of String, String)

        For Each prop In t.GetProperties(BindingFlags.Instance Or BindingFlags.Public Or BindingFlags.FlattenHierarchy)
            dict.Add(prop.Name, prop.Name)
        Next

        Return dict
    End Function

End Module

''' <summary>
''' A specific exception defining a specific occurance
''' </summary>
Public Class MultipleRecordsFoundException
    Inherits Exception

    Public Sub New()
        MyBase.New("Found many results when expecting only a single result")
    End Sub
End Class
