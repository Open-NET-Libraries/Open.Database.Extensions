using System.Buffers;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

[ExcludeFromCodeCoverage]
public static class ToChannelExtensionsContractTests
{
	[Fact]
	public static void ToChannel_IDataReader_NullReader_ThrowsArgumentNullException()
	{
		// Arrange
		IDataReader reader = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannel(true));
	}

	[Fact]
	public static void ToChannelT_IDataReader_NullReader_ThrowsArgumentNullException()
	{
		// Arrange
		IDataReader reader = null;
		CancellationToken cancellationToken = default;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannel<object>(true, cancellationToken));
	}

	[Fact]
	public static void ToChannel_IDataReader_NullReaderWithArrayPool_ThrowsArgumentNullException()
	{
		// Arrange
		IDataReader reader = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannel(true, ArrayPool<object>.Shared));
	}

	[Fact]
	public static void ToChannel_IDataReader_NullReaderWithTransform_ThrowsArgumentNullException()
	{
		// Arrange
		IDataReader reader = null;
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannel(true, transform));
	}

	[Fact]
	public static void ToChannel_IDataReader_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		IDataReader reader = Substitute.For<IDataReader>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannel(true, transform));
	}

	[Fact]
	public static void ToChannel_IDbCommand_NullCommand_ThrowsArgumentNullException()
	{
		// Arrange
		IDbCommand command = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true));
	}

	[Fact]
	public static void ToChannel_IDbCommand_NullArrayPool_ThrowsArgumentNullException()
	{
		// Arrange
		IDbCommand command = Substitute.For<IDbCommand>();
		ArrayPool<object> arrayPool = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true, arrayPool));
	}

	[Fact]
	public static void ToChannel_IDbCommand_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		IDbCommand command = Substitute.For<IDbCommand>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true, transform));
	}

	[Fact]
	public static void ToChannel_IExecuteReader_NullCommand_ThrowsArgumentNullException()
	{
		// Arrange
		IExecuteReader command = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true));
	}

	[Fact]
	public static void ToChannel_IExecuteReader_NullArrayPool_ThrowsArgumentNullException()
	{
		// Arrange
		IExecuteReader command = Substitute.For<IExecuteReader>();
		ArrayPool<object> arrayPool = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true, arrayPool));
	}

	[Fact]
	public static void ToChannel_IExecuteReader_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		IExecuteReader command = Substitute.For<IExecuteReader>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannel(true, transform));
	}

#if !NETSTANDARD2_0
	[Fact]
	public static void ToChannelAsync_DbDataReader_NullReader_ThrowsArgumentNullException()
	{
		// Arrange
		DbDataReader reader = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannelAsync(true));
	}

	[Fact]
	public static void ToChannelAsync_DbDataReader_NullArrayPool_ThrowsArgumentNullException()
	{
		// Arrange
		DbDataReader reader = Substitute.For<DbDataReader>();
		ArrayPool<object> arrayPool = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannelAsync(true, arrayPool));
	}

	[Fact]
	public static void ToChannelAsync_DbDataReader_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		DbDataReader reader = Substitute.For<DbDataReader>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => reader.ToChannelAsync(true, transform));
	}

	[Fact]
	public static void ToChannelAsync_DbCommand_NullCommand_ThrowsArgumentNullException()
	{
		// Arrange
		DbCommand command = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannelAsync(true));
	}

	[Fact]
	public static void ToChannelAsync_DbCommand_NullArrayPool_ThrowsArgumentNullException()
	{
		// Arrange
		DbCommand command = Substitute.For<DbCommand>();
		ArrayPool<object> arrayPool = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannelAsync(true, arrayPool));
	}

	[Fact]
	public static void ToChannelAsync_DbCommand_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		DbCommand command = Substitute.For<DbCommand>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannelAsync(true, transform));
	}

	[Fact]
	public static void ToChannelAsync_IExecuteReaderAsync_NullCommand_ThrowsArgumentNullException()
	{
		// Arrange
		IExecuteReaderAsync command = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannelAsync(true));
	}

	[Fact]
	public static void ToChannelAsync_IExecuteReaderAsync_NullTransform_ThrowsArgumentNullException()
	{
		// Arrange
		IExecuteReaderAsync command = Substitute.For<IExecuteReaderAsync>();
		Func<IDataRecord, object> transform = null;

		// Act & Assert
		Assert.Throws<ArgumentNullException>(() => command.ToChannelAsync(true, transform));
	}
#endif
}
