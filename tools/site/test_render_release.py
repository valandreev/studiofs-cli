import unittest

from render_release import render_page, render_release_badge


class ReleaseBadgeTests(unittest.TestCase):
    def test_v021_has_slogan_and_links_to_exact_release(self):
        badge = render_release_badge(
            {"tag_name": "v0.2.1", "draft": False, "prerelease": False}
        )
        self.assertIn('href="https://github.com/valandreev/studiofs-cli/releases/tag/v0.2.1"', badge)
        self.assertIn("Giving penguins the love they deserve.", badge)
        self.assertNotIn("Native builds", badge)

    def test_later_stable_release_has_no_v021_slogan(self):
        badge = render_release_badge(
            {"tag_name": "v0.2.2", "draft": False, "prerelease": False}
        )
        self.assertIn("<b>v0.2.2</b>", badge)
        self.assertNotIn("Giving penguins", badge)
        self.assertIn("Native builds for Windows, Linux &amp; macOS", badge)

    def test_rejects_malformed_or_unpublished_metadata(self):
        for metadata in (
            None,
            {"tag_name": 'v0.2.1"><script>', "draft": False, "prerelease": False},
            {"tag_name": "v١.٢.٣", "draft": False, "prerelease": False},
            {"tag_name": "v0.2.1", "draft": True, "prerelease": False},
            {"tag_name": "v0.2.1", "draft": False, "prerelease": True},
        ):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                render_release_badge(metadata)

    def test_page_replacement_requires_one_marker(self):
        metadata = {"tag_name": "v1.0.0", "draft": False, "prerelease": False}
        page = render_page(metadata, "before <!-- RELEASE_BADGE --> after")
        self.assertIn("releases/tag/v1.0.0", page)
        with self.assertRaises(ValueError):
            render_page(metadata, "no marker")


if __name__ == "__main__":
    unittest.main()
