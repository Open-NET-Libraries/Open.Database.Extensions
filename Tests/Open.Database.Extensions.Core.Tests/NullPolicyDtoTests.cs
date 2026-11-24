using Open.Database.Extensions.Tests.NullPolicyMatrix;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Drives the shared DTO case (Tests/Shared/NullPolicyMatrix/NullPolicyMatrix.Dto.cs) through the runtime reader:
// one class that is the row model and the API response, with ODX's, DataAnnotations' and System.Text.Json's attributes together.
[ExcludeFromCodeCoverage]
public static class NullPolicyDtoTests
{
	static FakeDbCommand Command(object?[][] rows)
	{
		ImmutableArray<ColumnDef> columns = [.. Dto.ColumnNames.Select((name, i) => new ColumnDef(name, Dto.ColumnTypes[i]))];
		return new(new FakeDbConnection(), () => TypedReader.Create(columns, rows));
	}

	[Fact]
	public static void OneDto_MapsByOurRules_SerializesByTheJsonAttributes_AndValidatesByTheDataAnnotations()
	{
		List<PostDto> mapped = Command(Dto.Rows).Map<PostDto>().ToList();
		Assert.Equal(Dto.Expected.Length, mapped.Count);

		for (int i = 0; i < mapped.Count; i++)
		{
			PostDto dto = mapped[i];
			DtoExpected expected = Dto.Expected[i];

			// The mapping follows ODX's declarations only: [NullValue] over the type's Throw, [JsonIgnore] still mapped, [NotMapped] left alone,
			// and [Required], [MaxLength] and [Range] do not stop a value they reject.
			Assert.Equal(
				(expected.Id, expected.Title, expected.Score, expected.Secret, expected.Author, "not mapped"),
				(dto.Id, dto.Title, dto.Score, dto.Secret, dto.Author, dto.Note));

			// The JSON follows the JSON attributes: the names, and no Secret.
			Assert.Equal(expected.Json, JsonSerializer.Serialize(dto));

			// The validation runs on the mapped DTO and reports what its attributes say.
			var results = new List<ValidationResult>();
			bool valid = Validator.TryValidateObject(dto, new ValidationContext(dto), results, validateAllProperties: true);
			Assert.Equal(expected.Invalid.Length == 0, valid);
			Assert.Equal(expected.Invalid, results.SelectMany(r => r.MemberNames));
		}

		// The type's Throw covers the [JsonIgnore] member: a NULL there throws, naming it.
		ValueMappingException ex = Assert.Throws<ValueMappingException>(() => Command(Dto.FailingRows).Map<PostDto>().ToList());
		Assert.Equal(Dto.FailingMessage, ex.Message);
	}
}
