-- Migration: Create function to clean HTML tags and entities
-- Purpose: Simple HTML cleaning - remove tags and decode basic entities

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
    SET @output = REPLACE(@output, '<p>', ' ');
    SET @output = REPLACE(@output, '</p>', ' ');
    SET @output = REPLACE(@output, '<div>', ' ');
    SET @output = REPLACE(@output, '</div>', ' ');

    -- Replace control characters
    SET @output = REPLACE(@output, CHAR(9), ' ');    -- Tab
    SET @output = REPLACE(@output, CHAR(10), ' ');   -- Line feed
    SET @output = REPLACE(@output, CHAR(13), ' ');   -- Carriage return

    -- Clean up multiple spaces
    WHILE CHARINDEX('  ', @output) > 0
        SET @output = REPLACE(@output, '  ', ' ');

    RETURN LTRIM(RTRIM(@output));
END;
