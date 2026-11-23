using System.Text.RegularExpressions;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// The default generated reader reads a non-nullable member unchecked (its typed getter alone) and tests only a nullable member for NULL. Proved on the
/// generated file of this build (EmitCompilerGeneratedFiles), the text of what a consumer compiles: a text scan, because the claim is about which calls
/// the generator writes, whatever a compiler or a JIT then makes of them.
/// </summary>
public class GeneratedSourceTests
{
	static string GeneratedSource()
	{
		// bin/{Configuration}/{tfm}/ -> the project's obj/{Configuration}/{tfm}/generated/...
		var output = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
		string tfm = output.Name;
		string configuration = output.Parent!.Name;
		string project = output.Parent.Parent!.Parent!.FullName;
		string path = Path.Combine(
			project, "obj", configuration, tfm, "generated",
			"Open.Database.Extensions.Generator", "Open.Database.Extensions.Generator.MapGenerator", "OdxMaps.g.cs");

		return File.ReadAllText(path);
	}

	// The text of one model's binder class (its row readers included), up to the next top-level class of the file.
	static string Binder(string source, string model)
	{
		var start = Regex.Match(source, $@"file sealed class M\d+_{model}\b");
		Assert.True(start.Success, $"No binder for {model} in the generated file.");
		int end = source.IndexOf("\n\tfile ", start.Index + 1, StringComparison.Ordinal);
		return end < 0 ? source[start.Index..] : source[start.Index..end];
	}

	// Every statement that stores the member in a row reader (item.Name = ... in each of the read modes).
	static List<string> Stores(string binder, string member)
		=> [.. binder.Split('\n').Where(line => line.Contains($"item.{member} = ", StringComparison.Ordinal))];

	[Fact]
	public void The_generator_writes_no_NULL_check_helper_for_a_non_nullable_member()
	{
		string source = GeneratedSource();

		// The helper and its call sites are gone, and no row reader reads a cell for NULL on its own line before a read.
		Assert.DoesNotContain("NotNull(", source, StringComparison.Ordinal);
		Assert.DoesNotContain("NullRead(", source, StringComparison.Ordinal);
		Assert.DoesNotContain("Conditional(\"DEBUG\")", source, StringComparison.Ordinal);
		Assert.DoesNotContain("Debug.Assert", source, StringComparison.Ordinal);
		Assert.DoesNotContain("if (b.Reader.IsDBNull(", source, StringComparison.Ordinal);
	}

	[Theory]
	[InlineData("GenRowT20", new[] { "Id", "Created", "Title", "Price", "IsActive", "Key", "Score", "Big", "Body", "Tag", "Ratio", "Count" }, new[] { "Qty", "Note", "Blob", "Amount", "Stamp", "Flag", "Link", "Summary" })]
	[InlineData("PlainPost", new[] { "Id", "Created", "Title", "Price", "IsActive", "Key", "Score", "Big", "State" }, new[] { "Qty", "Note", "Blob", "Label" })]
	public void A_row_reader_tests_for_NULL_in_the_nullable_members_only(string model, string[] nonNullable, string[] nullable)
	{
		string binder = Binder(GeneratedSource(), model);

		foreach (string member in nonNullable)
		{
			var stores = Stores(binder, member);
			Assert.NotEmpty(stores);
			foreach (string store in stores)
				Assert.DoesNotContain("DBNull", store, StringComparison.Ordinal);
		}

		// A nullable member's read tests the cell: IsDBNull for a typed read, a DBNull compare on the value for a general one (byte[]?).
		foreach (string member in nullable)
		{
			var stores = Stores(binder, member);
			Assert.NotEmpty(stores);
			foreach (string store in stores)
				Assert.Contains("DBNull", store, StringComparison.Ordinal);
		}
	}
}
