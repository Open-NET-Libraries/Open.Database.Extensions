using NSubstitute;
using System;
using System.Data;
using System.Threading.Tasks;
using Xunit;

namespace Open.Database.Extensions.Core.Tests;

public class Basic
{
	[Fact]
	public void ExpressiveCommandValidation()
	{
		var factory = DbConnectionFactory.Create(() =>
		{
			var conn = Substitute.For<IDbConnection>();
			return conn;
		});

		Assert.Throws<ArgumentNullException>(() => factory.Command(null));
		Assert.Throws<ArgumentException>(() => factory.Command(string.Empty));
	}

	[System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Compile test.")]
	static async Task AmbiguityValidation(IDbCommand command)
		=> await command.Connection.EnsureOpenAsync();
}
