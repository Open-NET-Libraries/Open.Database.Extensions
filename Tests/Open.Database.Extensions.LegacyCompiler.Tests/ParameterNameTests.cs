using System;
using System.Data;
using System.Linq;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// A compiler older than C# 13 ignores [OverloadResolutionPriority] and used to bind AddParam("author", "ann") to the deriving overload
	// AddParam<T>(T value, string expression), which sent the parameter "ann" with the value "author". Nothing may be inverted at any language version.
	public static class ParameterNameTests
	{
		sealed class NoConnections : IDbConnectionFactory
		{
			public IDbConnection Create() => throw new NotSupportedException();
		}

		sealed class Search
		{
			public string Author = "ann";
			public int Limit = 5;
		}

		static ExpressiveCommand NewCommand()
			=> new ExpressiveCommand(new NoConnections(), CommandType.Text, "select 1");

		// What the command will send, as name=value pairs.
		static string Sent(ExpressiveCommand command)
			=> string.Join(", ", command.Params.Select(p => p.Name + "=" + (p.Value is DBNull ? "<null>" : p.Value)));

		// ---- the name you write is the name that is sent ----

		[Fact]
		public static void AddParam_StringValue_UsesTheNameGiven()
		{
			const string s = "ann";
			const string maybe = null;
			const string name = "author";

			Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", "ann")));
			Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", s)));
			Assert.Equal("author=<null>", Sent(NewCommand().AddParam("author", maybe)));
			Assert.Equal("author=<null>", Sent(NewCommand().AddParam("author", (string)null)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParam(name, s)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParam(name: "author", value: s)));
		}

		[Fact]
		public static void AddParamIf_StringValue_UsesTheNameGiven()
		{
			const string s = "ann";
			const string maybe = null;

			Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", "ann")));
			Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", s)));
			Assert.Equal("author=<null>", Sent(NewCommand().AddParamIf(true, "author", maybe)));
			Assert.Equal("author=<null>", Sent(NewCommand().AddParamIf(true, "author", (string)null)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(condition: true, name: "author", value: s)));
			Assert.Equal("", Sent(NewCommand().AddParamIf(false, "author", "ann")));
			Assert.Equal("", Sent(NewCommand().AddParamIf(false, "author", s)));
		}

		[Fact]
		public static void AddParam_OtherValues_UseTheNameGiven()
		{
			object o = "ann";

			Assert.Equal("author=5", Sent(NewCommand().AddParam("author", 5)));
			Assert.Equal("author=5", Sent(NewCommand().AddParam("author", 5L)));
			Assert.Equal("author=5", Sent(NewCommand().AddParam("author", (int?)5)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", o)));
			Assert.Equal("author=5", Sent(NewCommand().AddParamIf(true, "author", 5)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", o)));
		}

		// ---- naming from the argument still works where the compiler supplies the text ----
		// A compiler that fills [CallerArgumentExpression] does so whatever the language version, so these hold on every target here.

		[Fact]
		public static void AddParam_FromTheArgument_UsesTheArgumentsName()
		{
			const string author = "ann";
			var search = new Search();

			Assert.Equal("author=ann", Sent(NewCommand().AddParam(author)));
			Assert.Equal("Author=ann", Sent(NewCommand().AddParam(search.Author)));
			Assert.Equal("Limit=5", Sent(NewCommand().AddParam(search.Limit)));
			Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, author)));
			Assert.Equal("", Sent(NewCommand().AddParamIf(false, search.Author)));
		}

		[Fact]
		public static void AddParams_SendsTheMembersOfTheObject()
		{
			var search = new Search();

			Assert.Equal("Author=ann, Limit=5", string.Join(", ", NewCommand().AddParams(search).Params.Select(p => p.Name + "=" + p.Value).OrderBy(x => x, StringComparer.Ordinal)));
		}

		// ---- a compiler that does not supply the text ----
		// The text arrives as null, as it does from a compiler older than C# 10 and when a caller passes null on purpose.

		[Fact]
		public static void NoArgumentText_IsAnError_ThatNamesTheWayOut_AndNeverSendsAWrongName()
		{
			ExpressiveCommand command = NewCommand();

			foreach (Action call in new Action[]
			{
				() => command.AddParam(value: 1, expression: null),
				() => command.AddParamIf(true, value: 1, expression: null),
				() => command.AddParamIf(false, value: 1, expression: null),
			})
			{
				ArgumentException ex = Assert.Throws<ArgumentException>(call);
				Assert.Contains("C# 10", ex.Message);
				Assert.Contains("AddParam(\"name\", value)", ex.Message);
			}

			Assert.Empty(command.Params);
		}
	}
}
