namespace Open.Database.Extensions;

/// <summary>
/// A column of a result set: its name and its ordinal.
/// </summary>
/// <remarks>
/// <para>
/// Two values are equal when their ordinals are equal and their names are equal, casing included.
/// </para>
/// <para>
/// A <see langword="default"/> value names no column: its <see cref="Name"/> is <see langword="null"/>,
/// and a method that reads columns throws <see cref="ArgumentException"/> when it is given one.
/// </para>
/// </remarks>
public readonly record struct ColumnOrdinal
{
	/// <summary>
	/// Names a column and its ordinal.
	/// </summary>
	/// <param name="name">The column's name.</param>
	/// <param name="ordinal">The column's zero-based ordinal.</param>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
	public ColumnOrdinal(string name, int ordinal)
	{
		Name = name ?? throw new ArgumentNullException(nameof(name));
		Ordinal = ordinal;
	}

	// Both properties are get-only: an init accessor would let a with expression pass the null check by.

	/// <summary>
	/// The column's name.
	/// </summary>
	/// <value><see langword="null"/> only for a <see langword="default"/> value.</value>
	public string Name { get; }

	/// <summary>
	/// The column's zero-based ordinal.
	/// </summary>
	public int Ordinal { get; }

	/// <summary>
	/// Splits the value into its name and its ordinal.
	/// </summary>
	/// <param name="name">The column's name.</param>
	/// <param name="ordinal">The column's zero-based ordinal.</param>
	public void Deconstruct(out string name, out int ordinal)
	{
		name = Name;
		ordinal = Ordinal;
	}

	/// <summary>
	/// The value as <c>(name, ordinal)</c>, such as <c>(Id, 0)</c>.
	/// </summary>
	/// <returns>The text. A <see langword="default"/> value has an empty name: <c>(, 0)</c>.</returns>
	public override string ToString()
		=> $"({Name}, {Ordinal})";
}
