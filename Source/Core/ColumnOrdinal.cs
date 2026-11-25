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
/// <param name="Name">The column's name.</param>
/// <param name="Ordinal">The column's zero-based ordinal.</param>
/// <exception cref="ArgumentNullException"><paramref name="Name"/> is <see langword="null"/>.</exception>
public readonly record struct ColumnOrdinal(string Name, int Ordinal)
{
	// Both properties are get-only: an init accessor would let a with expression pass the null check by.

	/// <summary>
	/// The column's name.
	/// </summary>
	/// <value><see langword="null"/> only for a <see langword="default"/> value.</value>
	public string Name { get; } = Name ?? throw new ArgumentNullException(nameof(Name));

	/// <summary>
	/// The column's zero-based ordinal.
	/// </summary>
	public int Ordinal { get; } = Ordinal;

	/// <summary>
	/// The value as <c>(name, ordinal)</c>, such as <c>(Id, 0)</c>.
	/// </summary>
	/// <returns>The text. A <see langword="default"/> value has an empty name: <c>(, 0)</c>.</returns>
	public override string ToString()
		=> $"({Name}, {Ordinal})";
}
