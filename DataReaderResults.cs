using System.Collections.Generic;

namespace Open.Database.Extensions
{
	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	public class DataReaderResults
    {
		/// <summary>
		/// The ordinal values requested.
		/// </summary>
		public int[] Ordinals;

		/// <summary>
		/// The column names requested.
		/// </summary>
		public string[] Names;

		/// <summary>
		/// The values requested.  A Queue is used since values are typically used first in first out and dequeuing results helps reduced redunant memory usage.
		/// </summary>
		public Queue<object[]> Values; 
    }
}
