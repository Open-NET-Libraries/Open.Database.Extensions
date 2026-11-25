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
/// <param name="name">The column's name.</param>
/// <param name="ordinal">The column's zero-based ordinal.</param>
/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
public readonly struct ColumnOrdinal(string name, int ordinal) : IEquatable<ColumnOrdinal>
{
	/// <summary>
	/// The column's name.
	/// </summary>
	/// <value><see langword="null"/> only for a <see langword="default"/> value.</value>
	public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));

	/// <summary>
	/// The column's zero-based ordinal.
	/// </summary>
	public int Ordinal { get; } = ordinal;

	/// <summary>
	/// Splits the value, as in <c>var (name, ordinal) = column;</c>.
	/// </summary>
	/// <param name="name">The <see cref="Name"/>.</param>
	/// <param name="ordinal">The <see cref="Ordinal"/>.</param>
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

	/// <inheritdoc />
	public bool Equals(ColumnOrdinal other)
		=> Ordinal == other.Ordinal
		&& string.Equals(Name, other.Name, StringComparison.Ordinal);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> obj is ColumnOrdinal other && Equals(other);

	/// <inheritdoc />
	public override int GetHashCode()
		=> unchecked((Name?.GetHashCode() ?? 0) * 31 + Ordinal);

	/// <summary>Whether two values are equal.</summary>
	/// <param name="left">The first value.</param>
	/// <param name="right">The second value.</param>
	/// <returns><see langword="true"/> when they are equal.</returns>
	public static bool operator ==(ColumnOrdinal left, ColumnOrdinal right)
		=> left.Equals(right);

	/// <summary>Whether two values differ.</summary>
	/// <param name="left">The first value.</param>
	/// <param name="right">The second value.</param>
	/// <returns><see langword="true"/> when they differ.</returns>
	public static bool operator !=(ColumnOrdinal left, ColumnOrdinal right)
		=> !left.Equals(right);
}
