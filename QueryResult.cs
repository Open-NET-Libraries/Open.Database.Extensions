using System;

namespace Open.Database.Extensions
{
	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	public class QueryResult<TResult>
		where TResult : class

	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		internal QueryResult(int[] ordinals, string[] names, TResult result)
		{
			Ordinals = ordinals ?? throw new ArgumentNullException(nameof(ordinals));
			Names = names ?? throw new ArgumentNullException(nameof(names));
			Result = result ?? throw new ArgumentNullException(nameof(result));
			if (ordinals.Length != names.Length) throw new ArgumentException("Mismatched array lengths of ordinals and names.");
			ColumnCount = ordinals.Length;
		}

		/// <summary>
		/// The number of columns.
		/// </summary>
		public readonly int ColumnCount;

		/// <summary>
		/// The ordinal values requested.
		/// </summary>
		public readonly int[] Ordinals;

		/// <summary>
		/// The column names requested.
		/// </summary>
		public readonly string[] Names;

		/// <summary>
		/// The values requested.  A Queue is used since values are typically used first in first out and dequeuing results helps reduced redunant memory usage.
		/// </summary>
		public readonly TResult Result;

	}
}
