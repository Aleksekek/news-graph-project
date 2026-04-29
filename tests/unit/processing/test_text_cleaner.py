"""
Тесты для src/processing/ner/text_cleaner.py
"""

import pytest

from src.processing.ner.text_cleaner import clean_article_text, clean_html, normalize_whitespace


class TestCleanHtml:
    def test_strips_tags(self):
        assert clean_html("<p>Привет, <b>мир</b>!</p>") == "Привет, мир !"

    def test_empty_string(self):
        assert clean_html("") == ""

    def test_plain_text_unchanged(self):
        text = "Обычный текст без тегов"
        assert clean_html(text) == text

    def test_nested_tags(self):
        result = clean_html("<div><p><span>Текст</span></p></div>")
        assert "Текст" in result
        assert "<" not in result

    def test_html_entities_decoded(self):
        result = clean_html("<p>AT&amp;T &mdash; компания</p>")
        assert "&amp;" not in result
        assert "AT&T" in result


class TestNormalizeWhitespace:
    def test_multiple_spaces(self):
        assert normalize_whitespace("один   два") == "один два"

    def test_newlines_collapsed(self):
        assert normalize_whitespace("строка\n\nдругая") == "строка другая"

    def test_tabs_collapsed(self):
        assert normalize_whitespace("один\t\tдва") == "один два"

    def test_leading_trailing_stripped(self):
        assert normalize_whitespace("  текст  ") == "текст"

    def test_empty_string(self):
        assert normalize_whitespace("") == ""


class TestCleanArticleText:
    def test_strips_html_from_both(self):
        title, text = clean_article_text("<b>Заголовок</b>", "<p>Текст статьи</p>")
        assert "<b>" not in title
        assert "<p>" not in text
        assert "Заголовок" in title
        assert "Текст статьи" in text

    def test_none_title_handled(self):
        title, text = clean_article_text(None, "Текст")
        assert title == ""
        assert text == "Текст"

    def test_none_text_handled(self):
        title, text = clean_article_text("Заголовок", None)
        assert title == "Заголовок"
        assert text == ""

    def test_both_none(self):
        title, text = clean_article_text(None, None)
        assert title == ""
        assert text == ""

    def test_normalizes_whitespace(self):
        title, text = clean_article_text("  Заголовок  ", "  Много   пробелов  ")
        assert title == "Заголовок"
        assert text == "Много пробелов"
