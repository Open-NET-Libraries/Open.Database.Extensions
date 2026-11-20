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
				nullableContextOptions: NullableContextOptions.Enable,
				specificDiagnosticOptions: Suppressed));
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
		Assert.Contains(errors, static d => d.Id == "CS0121");
	}

	[Theory]
	[InlineData("static void M() { Q.Using(c => 1); }")]
	[InlineData("static void M(Func<SqliteConnection> f) { Q.Using(f); }")]
	[InlineData("static void M() { Q.Using(() => new SqliteConnection()); }")]
	[InlineData("static void M(IDbConnectionFactory f) { Q.Using(f); }")]
	[InlineData("static void M(IDbConnectionPool p) { Q.Using(p); }")]
	public static void ABareFuncOrAnUntypedSource_IsNotASource(string body)
	{
		(CSharpCompilation compilation, _) = Compile(body);

		Assert.Contains(Errors(compilation), static d => d.Severity == DiagnosticSeverity.Error);
	}

	[Fact]
	public static void Map_OnEveryShape_BindsTheOneMapOnIExecuteReader()
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
			}
			""");

		Assert.Empty(Errors(compilation));
		IMethodSymbol[] bound = Bound(compilation, tree, "Map");
		Assert.Equal(11, bound.Length);

		INamedTypeSymbol extensions = compilation.GetTypeByMetadataName(
			"Open.Database.Extensions.CoreExtensions")!;
		Assert.All(bound, m =>
		{
			IMethodSymbol definition = (m.ReducedFrom ?? m).OriginalDefinition;
			Assert.Same(extensions, definition.ContainingType);
			Assert.Equal("IExecuteReader", definition.Parameters[0].Type.Name);
		});

		// The same symbol as today's command for the same shape.
		Assert.Equal(Shape(bound[0]), Shape(bound[3]));
		Assert.Equal(Shape(bound[0]), Shape(bound[8]));
		Assert.Equal(Shape(bound[1]), Shape(bound[9]));
		Assert.Equal(Shape(bound[2]), Shape(bound[10]));
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
		Assert.Contains(Errors(wrong), static d => d.Id == "CS1503");

		(CSharpCompilation notAStruct, _) = Compile("""
			sealed record Boxed(int A);
			static DbQuery<Boxed> B = Q.Params<Boxed>();
			""");
		Assert.Contains(Errors(notAStruct), static d => d.Severity == DiagnosticSeverity.Error);
	}
}
