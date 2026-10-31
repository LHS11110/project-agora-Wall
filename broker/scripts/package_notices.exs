# Run after dependency compilation; preserve upstream texts stripped by mix release.
target = "_build/prod/third-party-notices"
File.mkdir_p!(target)
File.cp!("LICENSE", Path.join(target, "project-LICENSE"))
entries = for dependency <- Enum.sort(File.ls!("deps")), File.dir?(Path.join("deps", dependency)) do
  root = Path.join("deps", dependency)
  files = Enum.filter(File.ls!(root), fn name -> Regex.match?(~r/^(license|licence|notice|copying)([._ -].*)?$/i, name) and File.regular?(Path.join(root, name)) end)
  if files == [], do: raise("Missing upstream license text for #{dependency}")
  output = Path.join(target, dependency)
  File.mkdir_p!(output)
  for name <- files, do: File.cp!(Path.join(root, name), Path.join(output, name))
  "#{dependency}: #{Enum.join(files, ", ")}"
end
File.cp!("mix.lock", Path.join(target, "mix.lock"))
File.write!(Path.join(target, "INDEX.txt"), "Dependency versions: mix.lock\n" <> Enum.join(entries, "\n") <> "\n")
