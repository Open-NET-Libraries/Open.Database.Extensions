# Where documentation lives

Every push to `master` rebuilds and publishes the site (`.github/workflows/docs.yml`). Nothing generated is committed.

- **API reference:** XML doc comments (`///`) in the code. DocFX renders `summary`, `remarks`, `param`, `returns`, `exception`, `example`, `seealso`, `cref` and `inheritdoc`.
- **Code examples:** compiled samples in `docsite/samples/` inside `#region name` ... `#endregion`. In an article use `[!code-csharp[](../samples/File.cs#name)]`. In an XML comment use `<example><code source="<path from the .cs file>/docsite/samples/File.cs" region="name" language="csharp"/></example>`.
- **Articles:** add a markdown file to `docsite/articles/` and one `- href: file.md` line to `docsite/articles/toc.yml` (the menu title is the file's first heading).
- **Namespace and package overviews:** a markdown file with `uid: <namespace>` and `summary: *content` front matter. Put it in `Source/<Project>/docs/` next to the code, or in `docsite/api/overview/`; both are picked up without a config edit.
- **Diagrams:** a standalone `.svg` in `docsite/images/`, colors as CSS variables, no external references.
- **README.md:** a pitch with absolute links; its code blocks are copied from sample regions.
- **Build locally:** `dotnet tool restore`, then `dotnet docfx docsite/docfx.json --serve`.
- **Failures:** warnings are listed in the workflow summary and never block a deploy. A DocFX error or a sample that does not compile does, and the live site stays as it was.