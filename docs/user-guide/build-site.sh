#!/usr/bin/env bash
# Build the bilingual user guide with Zensical and place it under site/docs/,
# next to the landing page, so the whole site/ folder can be served as-is.
#
#   /docs/     -> redirect to /docs/en/
#   /docs/en/  -> English guide (docs/user-guide/en)
#   /docs/uk/  -> Ukrainian guide (docs/user-guide/uk)
#
# Usage: docs/user-guide/build-site.sh [output-dir]   (default: site/docs)
set -euo pipefail

guide_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$guide_dir/../.." && pwd)"
out_dir="${1:-$repo_root/site/docs}"

cd "$guide_dir"
rm -rf .build
for lang in en uk; do
  zensical build --clean -f "zensical.$lang.yml"
done

cat > .build/docs/index.html <<'HTML'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>StudioFS Docs</title>
  <meta http-equiv="refresh" content="0; url=en/">
  <link rel="canonical" href="en/">
  <script>
    var lang = (navigator.language || "").toLowerCase();
    location.replace(lang.indexOf("uk") === 0 ? "uk/" : "en/");
  </script>
</head>
<body><a href="en/">StudioFS documentation</a></body>
</html>
HTML

rm -rf "$out_dir"
mkdir -p "$(dirname "$out_dir")"
cp -R .build/docs "$out_dir"
echo "User guide built into $out_dir"
