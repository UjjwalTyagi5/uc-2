-- Migration: Create function to clean HTML tags and entities
-- Purpose: Remove HTML tags and decode common HTML entities from text

CREATE OR ALTER FUNCTION [ras_procurement].[fn_clean_html]
(
    @input NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @input IS NULL
        RETURN NULL;

    DECLARE @output NVARCHAR(MAX) = @input;

    -- Decode HTML entities (common)
    SET @output = REPLACE(@output, '&nbsp;', ' ');
    SET @output = REPLACE(@output, '&ensp;', ' ');
    SET @output = REPLACE(@output, '&emsp;', ' ');
    SET @output = REPLACE(@output, '&thinsp;', ' ');
    SET @output = REPLACE(@output, '&amp;', '&');
    SET @output = REPLACE(@output, '&lt;', '<');
    SET @output = REPLACE(@output, '&gt;', '>');
    SET @output = REPLACE(@output, '&quot;', '"');
    SET @output = REPLACE(@output, '&#39;', '''');
    SET @output = REPLACE(@output, '&apos;', '''');
    SET @output = REPLACE(@output, '&rsquo;', '''');
    SET @output = REPLACE(@output, '&lsquo;', '''');
    SET @output = REPLACE(@output, '&rdquo;', '"');
    SET @output = REPLACE(@output, '&ldquo;', '"');
    SET @output = REPLACE(@output, '&ndash;', '-');
    SET @output = REPLACE(@output, '&mdash;', '-');
    SET @output = REPLACE(@output, '&bull;', '•');
    SET @output = REPLACE(@output, '&hellip;', '...');
    SET @output = REPLACE(@output, '&copy;', '©');
    SET @output = REPLACE(@output, '&reg;', '®');
    SET @output = REPLACE(@output, '&trade;', '™');
    SET @output = REPLACE(@output, '&deg;', '°');
    SET @output = REPLACE(@output, '&euro;', '€');
    SET @output = REPLACE(@output, '&pound;', '£');
    SET @output = REPLACE(@output, '&yen;', '¥');

    -- Remove HTML tags (common and variants)
    SET @output = REPLACE(@output, '<br />', ' ');
    SET @output = REPLACE(@output, '<br/>', ' ');
    SET @output = REPLACE(@output, '<br>', ' ');
    SET @output = REPLACE(@output, '<BR />', ' ');
    SET @output = REPLACE(@output, '<BR/>', ' ');
    SET @output = REPLACE(@output, '<BR>', ' ');
    SET @output = REPLACE(@output, '<strong>', '');
    SET @output = REPLACE(@output, '</strong>', '');
    SET @output = REPLACE(@output, '<STRONG>', '');
    SET @output = REPLACE(@output, '</STRONG>', '');
    SET @output = REPLACE(@output, '<em>', '');
    SET @output = REPLACE(@output, '</em>', '');
    SET @output = REPLACE(@output, '<EM>', '');
    SET @output = REPLACE(@output, '</EM>', '');
    SET @output = REPLACE(@output, '<b>', '');
    SET @output = REPLACE(@output, '</b>', '');
    SET @output = REPLACE(@output, '<B>', '');
    SET @output = REPLACE(@output, '</B>', '');
    SET @output = REPLACE(@output, '<i>', '');
    SET @output = REPLACE(@output, '</i>', '');
    SET @output = REPLACE(@output, '<I>', '');
    SET @output = REPLACE(@output, '</I>', '');
    SET @output = REPLACE(@output, '<p>', '');
    SET @output = REPLACE(@output, '</p>', ' ');
    SET @output = REPLACE(@output, '<P>', '');
    SET @output = REPLACE(@output, '</P>', ' ');
    SET @output = REPLACE(@output, '<div>', '');
    SET @output = REPLACE(@output, '</div>', ' ');
    SET @output = REPLACE(@output, '<DIV>', '');
    SET @output = REPLACE(@output, '</DIV>', ' ');
    SET @output = REPLACE(@output, '<span>', '');
    SET @output = REPLACE(@output, '</span>', '');
    SET @output = REPLACE(@output, '<SPAN>', '');
    SET @output = REPLACE(@output, '</SPAN>', '');
    SET @output = REPLACE(@output, '<a ', '');
    SET @output = REPLACE(@output, '</a>', '');
    SET @output = REPLACE(@output, '<A ', '');
    SET @output = REPLACE(@output, '</A>', '');
    SET @output = REPLACE(@output, '<ul>', '');
    SET @output = REPLACE(@output, '</ul>', '');
    SET @output = REPLACE(@output, '<ol>', '');
    SET @output = REPLACE(@output, '</ol>', '');
    SET @output = REPLACE(@output, '<li>', '');
    SET @output = REPLACE(@output, '</li>', ' ');

    -- Remove special characters that cause issues
    SET @output = REPLACE(@output, '{', '');
    SET @output = REPLACE(@output, '}', '');
    SET @output = REPLACE(@output, '[', '');
    SET @output = REPLACE(@output, ']', '');
    SET @output = REPLACE(@output, '|', '');
    SET @output = REPLACE(@output, '`', '');
    SET @output = REPLACE(@output, '~', '');
    SET @output = REPLACE(@output, '^', '');
    SET @output = REPLACE(@output, '?', '');
    SET @output = REPLACE(@output, '§', '');
    SET @output = REPLACE(@output, '¶', '');
    SET @output = REPLACE(@output, '†', '');
    SET @output = REPLACE(@output, '‡', '');
    SET @output = REPLACE(@output, '‰', '');

    -- Replace control characters with spaces
    SET @output = REPLACE(@output, CHAR(9), ' ');    -- Tab
    SET @output = REPLACE(@output, CHAR(10), ' ');   -- Line feed
    SET @output = REPLACE(@output, CHAR(11), ' ');   -- Vertical tab
    SET @output = REPLACE(@output, CHAR(12), ' ');   -- Form feed
    SET @output = REPLACE(@output, CHAR(13), ' ');   -- Carriage return
    SET @output = REPLACE(@output, CHAR(160), ' ');  -- Non-breaking space
    SET @output = REPLACE(@output, CHAR(8192), ' '); -- En quad
    SET @output = REPLACE(@output, CHAR(8193), ' '); -- Em quad
    SET @output = REPLACE(@output, CHAR(8194), ' '); -- En space
    SET @output = REPLACE(@output, CHAR(8195), ' '); -- Em space
    SET @output = REPLACE(@output, CHAR(8201), ' '); -- Thin space
    SET @output = REPLACE(@output, CHAR(8203), ' '); -- Zero-width space
    SET @output = REPLACE(@output, CHAR(8204), ' '); -- Zero-width non-joiner
    SET @output = REPLACE(@output, CHAR(8205), ' '); -- Zero-width joiner

    -- Remove duplicate punctuation
    WHILE CHARINDEX('--', @output) > 0
        SET @output = REPLACE(@output, '--', '-');
    WHILE CHARINDEX('..', @output) > 0
        SET @output = REPLACE(@output, '..', '.');
    WHILE CHARINDEX('  ', @output) > 0
        SET @output = REPLACE(@output, '  ', ' ');

    -- Trim leading/trailing spaces
    SET @output = LTRIM(RTRIM(@output));

    RETURN @output;
END;
