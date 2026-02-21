IF OBJECT_ID('dbo.AppSettings', 'U') IS NULL
BEGIN
    RETURN;
END;

DECLARE @DropSql nvarchar(max);

DECLARE @Columns TABLE (ColumnName sysname);
INSERT INTO @Columns (ColumnName)
VALUES
('ChangePasswordOtpLockMinutes'),
('ChangePasswordOtpMaxAttempts'),
('ChangePasswordOtpMaxPerHourPerIp'),
('ChangePasswordOtpMaxPerHourPerUser'),
('ChangePasswordOtpCooldownSeconds'),
('ChangePasswordOtpExpiryMinutes'),
('InviteLockMinutes'),
('InviteMaxAttempts'),
('InviteOtpLockMinutes'),
('InviteOtpMaxAttempts'),
('InviteOtpMaxPerHourPerIp'),
('InviteOtpMaxPerHourPerInvite'),
('InviteOtpCooldownSeconds'),
('InviteOtpExpiryMinutes'),
('InviteExpiryHours'),
('EmailCodeMaxFailedAttemptsPerCode'),
('EmailCodeExpiryMinutes'),
('EmailCodeMaxPerHourPerIp'),
('EmailCodeMaxPerHourPerEmail'),
('EmailCodeCooldownSeconds'),
('LoginLockoutMinutes'),
('LoginLockoutThreshold'),
('PasswordRequiresSpecialCharacter'),
('PasswordRequiresCapitalLetter'),
('PasswordRequiresNumber'),
('MinimumPasswordLength');

DECLARE @ColumnName sysname;
DECLARE ColumnCursor CURSOR FAST_FORWARD FOR
SELECT ColumnName FROM @Columns;

OPEN ColumnCursor;
FETCH NEXT FROM ColumnCursor INTO @ColumnName;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF COL_LENGTH('dbo.AppSettings', @ColumnName) IS NOT NULL
    BEGIN
        DECLARE @ConstraintName sysname;
        SELECT @ConstraintName = dc.name
        FROM sys.default_constraints dc
        INNER JOIN sys.columns c
            ON c.default_object_id = dc.object_id
        WHERE dc.parent_object_id = OBJECT_ID('dbo.AppSettings')
          AND c.name = @ColumnName;

        IF @ConstraintName IS NOT NULL
        BEGIN
            SET @DropSql = N'ALTER TABLE dbo.AppSettings DROP CONSTRAINT ' + QUOTENAME(@ConstraintName) + N';';
            EXEC sp_executesql @DropSql;
        END;

        SET @DropSql = N'ALTER TABLE dbo.AppSettings DROP COLUMN ' + QUOTENAME(@ColumnName) + N';';
        EXEC sp_executesql @DropSql;
    END;

    FETCH NEXT FROM ColumnCursor INTO @ColumnName;
END;

CLOSE ColumnCursor;
DEALLOCATE ColumnCursor;
