namespace WipperSnapper.SQL
{
	using System;
	using System.Data;
	using System.Data.SqlClient;

	/// <summary>
	/// MSSQL supports nested transactions but ADO.NET does not.
	/// So instead of not supporting nesting this utilizes the save point feature.
	/// 
	/// What this means is that a second transaction merely marks a save point at its creation instead of issuing "BEGIN TRAN".
	/// If the second transaction is rolledback then the the transaction is rolled back to the save point.
	/// If the second transaction is committed then nothing happens since committing save points is implicit.
	/// </summary>
	public class Transaction : IDisposable
	{
		public Transaction(Connection conn, Guid TransactionID)
		{
			this.Initialize(conn, TransactionID, System.Data.IsolationLevel.Unspecified);
		}

		public Transaction(Connection conn, Guid TransactionID, System.Data.IsolationLevel isolationLevel)
		{
			this.Initialize(conn, TransactionID, isolationLevel);
		}

		/// <summary>
		/// Handles starting the transaction
		/// </summary>
		protected void Initialize(Connection conn, Guid TransactionID, IsolationLevel isoLevel)
		{
			// New Transaction ID
			this._Id = TransactionID;
			this._Connection = conn;

			// That means no transaction exists on the connection - brand spankin' new transaction.
			if (this._Connection.ActiveTransaction == null)
			{
				this.Connection.ActiveTransaction = this;
				this._Type = TransactionType.Transaction;
				this._Transaction = this._Connection.SqlConnection.BeginTransaction(isoLevel);
			}
			else
			{
				//Nested transaction
				this._Type = TransactionType.SavePoint;
				this._Connection.ActiveTransaction.SqlTransaction.Save(this.Name);
			}

		}

		/// <summary>
		/// Identifies this transaction uniquely.
		/// This is handy when dealing with Session.
		/// </summary>
		public Guid Id
		{
			get { return this._Id; }
		}
		protected Guid _Id;

		/// <summary>
		/// Type of transaction
		/// </summary>
		/// <see cref="TransactionType"/>
		public TransactionType Type
		{
			get { return this._Type; }
		}
		protected TransactionType _Type;

		/// <summary>
		/// Transaction is ended? That means committed or rolledback
		/// </summary>
		public bool IsEnded
		{
			get { return this._IsEnded; }
		}
		protected bool _IsEnded = false;

		/// <summary>
		/// Gets the flag indicating if the transaction has ended with a commit.
		/// </summary>
		public bool IsCommitted
		{
			get
			{
				if (!this.IsEnded)
				{
					throw new Exception("Tried to access IsCommitted flag when the transaction has not ended yet");
				}
				return this._IsCommitted;
			}
		}
		protected bool _IsCommitted = false;

		/// <summary>
		/// The transaction has ended with a rollback.
		/// </summary>
		public bool IsRolledback
		{
			get
			{
				if (!this.IsEnded)
				{
					throw new Exception("Tried to access IsRolledback flag when the transaction has not ended yet");
				}
				return (this.IsEnded && !this.IsCommitted);
			}
		}

		/// <summary>
		/// Name of this transaction used in save point and in session.
		/// </summary>
		public string Name
		{
			get { return this.Id.ToString(); }
		}

		/// <summary>
		/// Isolation level of this transaction.
		/// Supposedly this is applied at the point of execution of the transaction.
		/// I have yet to verify that.
		/// </summary>
		public System.Data.IsolationLevel IsolationLevel
		{
			get { return this._Transaction.IsolationLevel; }
		}

		/// <summary>
		/// Gets the connection the transaction is on - very important to keep track of.
		/// </summary>
		public Connection Connection
		{
			get { return this._Connection; }
		}
		protected Connection _Connection;

		/// <summary>
		/// Gets the SqlTransaction this object is wrapping.
		/// This is null if the Type is a SavePoint since it's not a real SqlTransaction backing it.
		/// </summary>
		protected internal SqlTransaction SqlTransaction
		{
			get { return this._Transaction; }
		}
		protected SqlTransaction _Transaction;

		/// <summary>
		/// Commit this transaction.
		/// </summary>
		public void Commit()
		{
			switch (this.Type)
			{
				case TransactionType.Transaction:
					this.SqlTransaction.Commit();
					this.Connection.TransactionEnded();
					break;
				case TransactionType.SavePoint:
					// Save points are not committed. They simply issue a SAVE TRANSACTION *NAME* which is just a checkpoint
					// to rollback to. So - to commit we don't do anything.
					break;
				default:
					throw new Exception("Unrecognized transaction type \"" + this.Type.ToString());
			}

			this._IsEnded = true;
			this._IsCommitted = true;
		}

		/// <summary>
		/// Rollback the outermost transaction this transaction belongs to.
		/// </summary>
		public void Rollback()
		{
			switch (this.Type)
			{
				case TransactionType.Transaction:
					this.SqlTransaction.Rollback();
					this.Connection.TransactionEnded();
					break;
				case TransactionType.SavePoint:

					// Rolling back a save point means rolling back to that particular point in the transaction (obviously)
					this.Connection.ActiveTransaction.SqlTransaction.Rollback(this.Name);
					break;
				default:
					throw new Exception("Unrecognized transaction type \"" + this.Type.ToString() + "\", cannot roll it back");
			}

			this._IsEnded = true;
			this._IsCommitted = false;
		}

		/// <summary>
		/// Disposing of the transaction
		/// </summary>
		public void Dispose()
		{
			// If already ended then nothing to be done when disposing (the transaction has already been comitted or rolledback.
			if (this.IsEnded) { return; }
			this.Rollback();
		}
	}

	/// <summary>
	/// Enum to distinguish the type of transaction.
	/// </summary>
	public enum TransactionType
	{
		/// <summary>
		/// Standard transaction
		/// </summary>
		Transaction = 0,

		/// <summary>
		/// "Fake" transaction in the sense that it's just savepoint reference and not an actual "BEGIN TRANSACTION" statement basically
		/// This is used internally.
		/// </summary>
		SavePoint = 1
	}
}
