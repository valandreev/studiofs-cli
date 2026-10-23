# Supplemental license texts

For crates whose published `.crate` archive contains no license or notice file,
`generate-third-party-notices.py` reads `<crate>-<version>/` here and reproduces
every file in the notices. Each folder needs:

- the verbatim upstream license/notice file(s) (e.g. `LICENSE-MIT`, `LICENSE-APACHE`),
  taken from the upstream repository at the tag or commit matching that crate version;
- `SOURCE.txt` with the repository URL, the tag or commit, the upstream file paths,
  and the `license` expression from the crate's `Cargo.toml` that the texts cover.

Do not write license text from memory and do not alter upstream text.
