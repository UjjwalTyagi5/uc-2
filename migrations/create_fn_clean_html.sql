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

    -- Decode HTML entities
    SET @output = REPLACE(@output, '&nbsp;', ' ');
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

    -- Remove HTML tags
    SET @output = REPLACE(@output, '<br />', ' ');
    SET @output = REPLACE(@output, '<br/>', ' ');
    SET @output = REPLACE(@output, '<br>', ' ');
    SET @output = REPLACE(@output, '<strong>', '');
    SET @output = REPLACE(@output, '</strong>', '');
    SET @output = REPLACE(@output, '<em>', '');
    SET @output = REPLACE(@output, '</em>', '');
    SET @output = REPLACE(@output, '<b>', '');
    SET @output = REPLACE(@output, '</b>', '');
    SET @output = REPLACE(@output, '<i>', '');
    SET @output = REPLACE(@output, '</i>', '');
    SET @output = REPLACE(@output, '<p>', '');
    SET @output = REPLACE(@output, '</p>', ' ');
    SET @output = REPLACE(@output, '<div>', '');
    SET @output = REPLACE(@output, '</div>', ' ');

    -- Remove special characters
    SET @output = REPLACE(@output, '{', '');
    SET @output = REPLACE(@output, '}', '');
    SET @output = REPLACE(@output, '[', '');
    SET @output = REPLACE(@output, ']', '');
    SET @output = REPLACE(@output, '|', '');
    SET @output = REPLACE(@output, '`', '');
    SET @output = REPLACE(@output, '~', '');
    SET @output = REPLACE(@output, '^', '');

    -- Replace control characters with spaces
    SET @output = REPLACE(@output, CHAR(9), ' ');    -- Tab
    SET @output = REPLACE(@output, CHAR(10), ' ');   -- Line feed
    SET @output = REPLACE(@output, CHAR(13), ' ');   -- Carriage return

    -- Remove duplicate dashes
    WHILE CHARINDEX('--', @output) > 0
        SET @output = REPLACE(@output, '--', '-');

    -- Clean up multiple spaces
    WHILE CHARINDEX('  ', @output) > 0
        SET @output = REPLACE(@output, '  ', ' ');

    -- Trim leading/trailing spaces
    SET @output = LTRIM(RTRIM(@output));

    RETURN @output;
END;
