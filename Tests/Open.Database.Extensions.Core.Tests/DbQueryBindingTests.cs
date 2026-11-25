using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Open.Database.Extensions.Tests;

#nullable enable

// What the definition-first query binds to, compiled with Roslyn (a call that
// becomes ambiguous or stops compiling is a failure here):
// - every Using and UsingAsync that existed binds the same extension as
//   before (DbQuery.Using is an instance method of a type no extension has
//   as a receiver, so nothing can move);
// - a type that is both a factory and a pool is ambiguous (CS0121), and
//   a bare Func is not a source;
// - Map on every shape binds the one CoreExtensions.Map(this IExecuteReader);
// - AddParam binds the named form, the string form and the derived form, at
//   C# 12 (which ignores OverloadResolutionPriority) and at the preview.
[ExcludeFromCodeCoverage]
public static class DbQueryBindingTests
{
	// CS1701 and CS1702 (assembly version unification) are suppressed as the
	// compiler does by default.
	static readonly Dictionary<string, ReportDiagnostic> Suppressed = new()
	{
		["CS1701"] = ReportDiagnostic.Suppress,
		["CS1702"] = ReportDiagnostic.Suppress,
	};

	static readonly Lazy<MetadataReference[]> References = new(static () =>
	{
		var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string tpa)
		{
			foreach (string path in tpa.Split(Path.PathSeparator))
			{
				// The legacy SqlClient adapter defines types of the same name as the
				// SQL Server package (CS0433); neither is needed here.
				string file = Path.GetFileName(path);
				if (file.StartsWith("Open.Database.Extensions.", StringComparison.OrdinalIgnoreCase)
					&& !file.Equals("Open.Database.Extensions.Core.dll", StringComparison.OrdinalIgnoreCase))
				{
					continue;
				}

				paths.Add(path);
			}
		}

		paths.Add(typeof(DbQuery).Assembly.Location);
		return [.. paths.Select(static p => (MetadataReference)MetadataReference.CreateFromFile(p))];
	});

	const string Header = """
		#nullable enable
		using System;
		using System.Data;
		using System.Data.Common;
		using System.Threading.Tasks;
		using Microsoft.Data.Sqlite;
		using Open.Database.Extensions;
		readonly record struct AuthorViews(int AuthorId, int Views);
		sealed record Post(int Id);
		abstract class Both : IDbConnectionFactory<SqliteConnection>, IDbConnectionPool<SqliteConnection>
		{
			public abstract SqliteConnection Create();
			IDbConnection IDbConnectionFactory.Create() => Create();
			public abstract SqliteConnection Take();
			IDbConnection IDbConnectionPool.Take() => Take();
			public abstract void Give(IDbConnection connection);
		}
		""";

	static (CSharpCompilation Compilation, SyntaxTree Tree) Compile(
		string body,
		LanguageVersion version = LanguageVersion.Preview)
	{
		string source = Header + "\nstatic class Probe\n{\n\tstatic readonly DbQuery Q = DbQuery.Text(\"x\");\n\tstatic readonly DbQuery<AuthorViews> T = DbQuery.Text(\"x\").Params<AuthorViews>();\n" + body + "\n}\n";
		SyntaxTree tree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(version));
		var compilation = CSharpCompilation.Create(
			"DbQueryProbe",
			[tree],
			References.Value,
			new CSharpCompilationOptions(
				OutputKind.DynamicallyLinkedLibrary,
				specificDiagnosticOptions: Suppressed,
				nullableContextOptions: NullableContextOptions.Enable));
		return (compilation, tree);
	}

	static Diagnostic[] Errors(CSharpCompilation compilation)
		=> [.. compilation
			.GetDiagnostics(TestContext.Current.CancellationToken)
			.Where(static d => d.Severity >= DiagnosticSeverity.Warning)];

	static string Show(IEnumerable<Diagnostic> diagnostics)
		=> string.Join(" ; ", diagnostics.Select(static d => d.ToString()));

	static IMethodSymbol[] Bound(
		CSharpCompilation compilation,
		SyntaxTree tree,
		params string[] names)
	{
		SemanticModel model = compilation.GetSemanticModel(tree);
		return [.. tree.GetRoot()
			.DescendantNodes()
			.OfType<InvocationExpressionSyntax>()
			.Where(i => i.Expression is MemberAccessExpressionSyntax m
				&& names.Contains(m.Name.Identifier.Text))
			.Select(i => (IMethodSymbol)model.GetSymbolInfo(i, TestContext.Current.CancellationToken).Symbol!)];
	}

	static string Shape(IMethodSymbol method)
		=> (method.ReducedFrom ?? method).OriginalDefinition
			.ToDisplayString(SymbolDisplayFormat.CSharpShortErrorMessageFormat);

	[Fact]
	public static void EveryExistingUsing_StillBindsTheExtensionItBound()
	{
		(CSharpCompilation compilation, SyntaxTree tree) = Compile("""
			static async Task M(
				IDbConnectionFactory f,
				IDbConnectionFactory<SqliteConnection> ft,
				DbConnectionFactory<SqliteConnection> dft,
				IDbConnectionPool p,
				IDbConnectionPool<SqliteConnection> pt,
				Func<SqliteConnection> fn)
			{
				f.Using(c => { });
				f.Using(c => 1);
				ft.Using(c => { });
				ft.Using(c => 1);
				dft.Using(c => 1);
				p.Using(c => { });
				p.Using(c => 1);
				pt.Using(c => { });
				pt.Using(c => 1);
				fn.Using(c => { });
				fn.Using(c => 1);
				await f.UsingAsync(c => default(ValueTask));
				await f.UsingAsync(c => new ValueTask<int>(1));
				await ft.UsingAsync(c => default(ValueTask));
				await ft.UsingAsync(c => new ValueTask<int>(1));
				await p.UsingAsync(c => default(ValueTask));
				await p.UsingAsync(c => new ValueTask<int>(1));
				await pt.UsingAsync(c => default(ValueTask));
				await pt.UsingAsync(c => new ValueTask<int>(1));
				await fn.UsingAsync(c => default(ValueTask));
			}
			""");

		Assert.Empty(Errors(compilation));
		IMethodSymbol[] bound = Bound(compilation, tree, "Using", "UsingAsync");
		Assert.Equal(20, bound.Length);
		Assert.All(
			bound,
			static m => Assert.Equal("ConnectionExtensions", m.ContainingType.Name));
	}

	[Fact]
	public static void EveryUsingOfADefinition_BindsAnInstanceMethodOfTheDefinition()
	{
		(CSharpCompilation compilation, SyntaxTree tree) = Compile("""
			static void M(
				DbConnection c,
				SqliteConnection sc,
				DbTransaction tx,
				IDbConnectionFactory<SqliteConnection> ft,
				DbConnectionFactory<SqliteConnection> dft,
				IDbConnectionPool<SqliteConnection> pt,
				Func<SqliteConnection> fn)
			{
				Q.Using(c);
				Q.Using(sc);
				Q.Using(tx);
				Q.Using(ft);
				Q.Using(dft);
				Q.Using(pt);
				Q.Using(ft.AsPool());
				Q.Using(new DbConnectionFactory<SqliteConnection>(fn));
				Q.Using(new DbConnectionFactory<SqliteConnection>(fn).AsPool());
				T.Using(c);
				T.Using(tx);
				T.Using(ft);
				T.Using(pt);
			}
			""");

		Assert.Empty(Errors(compilation));
		IMethodSymbol[] bound = Bound(compilation, tree, "Using");
		Assert.Equal(13, bound.Length);
		Assert.All(
			bound,
			static m => Assert.True(
				m.ContainingType.Name is "DbQuery" && !m.IsExtensionMethod,
				Shape(m)));
	}

	[Fact]
	public static void ATypeThatIsBothAFactoryAndAPool_IsAmbiguous()
	{
		(CSharpCompilation compilation, _) = Compile(
			"static void M(Both b) { Q.Using(b); }");

		Diagnostic[] errors = Errors(compilation);
		Assert.NotEmpty(errors);
		Assert.All(errors, d => Assert.True(d.Id == "CS0121", Show(errors)));
	}

	// Each case fails with the one error named, and with nothing else: the
	// probe's own declarations compile, so the error is the call's.
	[Theory]
	[InlineData("static void M() { Q.Using(c => 1); }", "CS1660")]
	[InlineData("static void M(Func<SqliteConnection> f) { Q.Using(f); }", "CS1503")]
	[InlineData("static void M() { Q.Using(() => new SqliteConnection()); }", "CS1660")]
	[InlineData("static void M(IDbConnectionFactory f) { Q.Using(f); }", "CS1503")]
	[InlineData("static void M(IDbConnectionPool p) { Q.Using(p); }", "CS1503")]
	public static void ABareFuncOrAnUntypedSource_IsNotASource(string body, string id)
	{
		(CSharpCompilation compilation, _) = Compile(body);

		Diagnostic[] errors = Errors(compilation);
		Assert.NotEmpty(errors);
		Assert.All(errors, d => Assert.True(d.Id == id, Show(errors)));
	}

	// The spelling of a call and of a bound definition is the spelling of a command: the same text binds, and the compiler
	// picks the member of the call itself, which holds the call as it is, where a command reaches the one extension on
	// IExecuteReader (which would box a struct).
	static string Arguments(IMethodSymbol method)
	{
		IMethodSymbol definition = (method.ReducedFrom ?? method).OriginalDefinition;
		IEnumerable<IParameterSymbol> parameters = method.ReducedFrom is null
			? definition.Parameters
			: definition.Parameters.Skip(1);
		return definition.Name + "(" + string.Join(", ", parameters.Select(static p => p.Type.ToDisplayString())) + ")";
	}

	[Fact]
	public static void Map_OnACallOrABoundDefinition_BindsTheirOwnMember_OnACommandTheExtension()
	{
		(CSharpCompilation compilation, SyntaxTree tree) = Compile("""
			static readonly RowMap<Post> PostMap = new();

			static void M(DbConnection c, DbQueryCall call, ExpressiveCommand today)
			{
				Q.Using(c).Map<Post>();
				Q.Using(c).Map(PostMap);
				Q.Using(c).Map(r => r.GetInt32(0));
				call.Map<Post>();
				call.Map(PostMap);
				call.Map(r => r.GetInt32(0));
				Q.Using(c).AddParam("a", 1).Map<Post>();
				T.Using(c).AddParams(new(1, 2)).Map<Post>();
				today.Map<Post>();
				today.Map(PostMap);
				today.Map(r => r.GetInt32(0));
				call.AsObjectArrays();
				Q.Using(c).AsDictionaryRows();
				today.AsObjectArrays();
			}
			""");

		Assert.Empty(Errors(compilation));
		IMethodSymbol[] bound = Bound(compilation, tree, "Map");
		Assert.Equal(11, bound.Length);

		// A bound definition and every call bind the member of their own type.
		string[] owners = ["DbQueryUsing", "DbQueryUsing", "DbQueryUsing", "DbQueryCall", "DbQueryCall", "DbQueryCall", "DbQueryCall", "DbQueryCall"];
		for (int i = 0; i < owners.Length; i++)
		{
			Assert.Null(bound[i].ReducedFrom);
			Assert.Equal(owners[i], bound[i].ContainingType.Name);
		}

		// A command binds the one extension on IExecuteReader.
		INamedTypeSymbol extensions = compilation.GetTypeByMetadataName(
			"Open.Database.Extensions.CoreExtensions")!;
		for (int i = 8; i < bound.Length; i++)
		{
			IMethodSymbol definition = (bound[i].ReducedFrom ?? bound[i]).OriginalDefinition;
			Assert.Same(extensions, definition.ContainingType);
			Assert.Equal("IExecuteReader", definition.Parameters[0].Type.Name);
		}

		// The same arguments, so the same text, whatever the receiver.
		foreach (int i in new[] { 3, 6, 7, 8 }) Assert.Equal(Arguments(bound[0]), Arguments(bound[i]));
		foreach (int i in new[] { 4, 9 }) Assert.Equal(Arguments(bound[1]), Arguments(bound[i]));
		foreach (int i in new[] { 5, 10 }) Assert.Equal(Arguments(bound[2]), Arguments(bound[i]));

		Assert.Equal(["DbQueryCall", "CoreExtensions"], Bound(compilation, tree, "AsObjectArrays").Select(static m => (m.ReducedFrom ?? m).ContainingType.Name));
		Assert.Equal(["DbQueryUsing"], Bound(compilation, tree, "AsDictionaryRows").Select(static m => (m.ReducedFrom ?? m).ContainingType.Name));
	}

	public static TheoryData<LanguageVersion> Versions
		=> [LanguageVersion.CSharp12, LanguageVersion.Preview];

	[Theory]
	[MemberData(nameof(Versions))]
	public static void AddParam_BindsTheNamedTheStringAndTheDerivedForm(LanguageVersion version)
	{
		(CSharpCompilation compilation, SyntaxTree tree) = Compile(
			"""
			static void M(DbConnection c, string s, int n)
			{
				Q.Using(c).AddParam("name", "text");
				Q.Using(c).AddParam("name", s);
				Q.Using(c).AddParam("n", n);
				Q.Using(c).AddParam(s);
				Q.Using(c).AddParam(n);
				Q.Using(c).AddParams(n).AddParam("a", "b").AddParam(s);
			}
			""",
			version);

		Assert.Empty(Errors(compilation));
		string[] shapes = [.. Bound(compilation, tree, "AddParam").Select(Shape)];
		string all = string.Join(" | ", shapes);
		Assert.True(shapes.Length == 7, all);

		// A chain lists its last call first.
		string[] expected =
		[
			"AddParam(string, string?)",
			"AddParam(string, string?)",
			"AddParam<T>(string, T)",
			"AddParam<T>(T, string?)",
			"AddParam<T>(T, string?)",
			"AddParam<T>(T, string?)",
			"AddParam(string, string?)",
		];
		for (int i = 0; i < expected.Length; i++)
			Assert.True(shapes[i].Contains(expected[i], StringComparison.Ordinal), $"{i}: {all}");
	}

	[Fact]
	public static void TheTypedForm_TakesATargetTypedNew_AndNothingElse()
	{
		(CSharpCompilation compilation, _) = Compile("""
			static void M(DbConnection c, int authorId, int views)
			{
				T.Using(c).AddParams(new(AuthorId: authorId, Views: views));
				T.Using(c).AddParams(new(authorId, views));
				T.Using(c).AddParams(new AuthorViews(authorId, views));
			}
			""");

		Assert.Empty(Errors(compilation));

		(CSharpCompilation wrong, _) = Compile("""
			readonly record struct Other(int A);
			static void M(DbConnection c) { T.Using(c).AddParams(new Other(1)); }
			""");
		Diagnostic[] wrongErrors = Errors(wrong);
		Assert.NotEmpty(wrongErrors);
		Assert.All(wrongErrors, d => Assert.True(d.Id == "CS1503", Show(wrongErrors)));

		(CSharpCompilation notAStruct, _) = Compile("""
			sealed record Boxed(int A);
			static DbQuery<Boxed> B = Q.Params<Boxed>();
			""");
		Diagnostic[] boxedErrors = Errors(notAStruct);
		Assert.NotEmpty(boxedErrors);
		Assert.All(boxedErrors, d => Assert.True(d.Id == "CS0453", Show(boxedErrors)));
	}

	// A scalar chain ends in ExecuteScalarAsync only, a write in
	// ExecuteNonQueryAsync only, neither reads rows, and values without a
	// source run nothing.
	[Theory]
	[InlineData("Q.Scalar<int>().Using(c).ExecuteScalarAsync()", true)]
	[InlineData("W.Using(c).ExecuteNonQueryAsync()", true)]
	[InlineData("Q.Scalar<int>().Using(c).ExecuteNonQueryAsync()", false)]
	[InlineData("Q.Scalar<int>().Using(c).Map<int>()", false)]
	[InlineData("Q.Scalar<int>().Using(c).ToListAsync()", false)]
	[InlineData("W.Using(c).ExecuteScalarAsync()", false)]
	[InlineData("W.Using(c).Map<int>()", false)]
	[InlineData("W.Using(c).ToListAsync()", false)]
	[InlineData("W.AddParam(\"a\", 1).ExecuteNonQueryAsync()", false)]
	public static void AScalarOrAWrite_HasOnlyItsOwnTerminal(string expression, bool compiles)
	{
		(CSharpCompilation compilation, _) = Compile($$"""
			static readonly DbNonQuery W = DbNonQuery.Text("x");
			static void M(DbConnection c) { var call = {{expression}}; }
			""");
		Diagnostic[] errors = [.. Errors(compilation).Where(static d => d.Severity == DiagnosticSeverity.Error)];

		if (compiles)
		{
			Assert.True(errors.Length == 0, Show(errors));
			return;
		}

		// No such member, or only an extension that does not take this receiver or lacks an argument.
		Assert.NotEmpty(errors);
		Assert.All(errors, d => Assert.True(d.Id is "CS1061" or "CS1929" or "CS0411" or "CS7036", Show(errors)));
	}

	[Theory]
	[InlineData(LanguageVersion.CSharp12)]
	[InlineData(LanguageVersion.Preview)]
	public static void AddParam_OnAScalarOrAWrite_BindsAsOnAQuery(LanguageVersion version)
	{
		(CSharpCompilation compilation, SyntaxTree tree) = Compile("""
			static readonly DbNonQuery W = DbNonQuery.Text("x");
			static void M(DbConnection c, string s, int n)
			{
				W.Using(c).AddParam("name", s);
				W.Using(c).AddParam("n", n);
				W.Using(c).AddParam(s);
				W.AddParam("name", s);
				W.AddParam("n", n);
				W.AddParam(s);
				Q.Scalar<int>().AddParam(n).Using(c).AddParam(s);
			}
			""",
			version);

		Assert.Empty(Errors(compilation));
		IMethodSymbol[] bound = Bound(compilation, tree, "AddParam");
		string all = string.Join(" | ", bound.Select(Shape));

		// The first parameter tells the named forms from the derived one; a
		// chain lists its last call first.
		Assert.True(
			bound.Select(static m => m.Parameters[0].Name)
				.SequenceEqual(["name", "name", "value", "name", "name", "value", "value", "value"]),
			all);
	}
}
