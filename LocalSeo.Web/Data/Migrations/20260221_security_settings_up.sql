IF OBJECT_ID('dbo.AppSettings', 'U') IS NULL
BEGIN
    RETURN;
END;

IF COL_LENGTH('dbo.AppSettings', 'MinimumPasswordLength') IS NULL
    ALTER TABLE dbo.AppSettings ADD MinimumPasswordLength int NOT NULL CONSTRAINT DF_AppSettings_MinimumPasswordLength_Migration DEFAULT(12);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresNumber') IS NULL
    ALTER TABLE dbo.AppSettings ADD PasswordRequiresNumber bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresNumber_Migration DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresCapitalLetter') IS NULL
    ALTER TABLE dbo.AppSettings ADD PasswordRequiresCapitalLetter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresCapitalLetter_Migration DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresSpecialCharacter') IS NULL
    ALTER TABLE dbo.AppSettings ADD PasswordRequiresSpecialCharacter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresSpecialCharacter_Migration DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'LoginLockoutThreshold') IS NULL
    ALTER TABLE dbo.AppSettings ADD LoginLockoutThreshold int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutThreshold_Migration DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'LoginLockoutMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD LoginLockoutMinutes int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutMinutes_Migration DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeCooldownSeconds') IS NULL
    ALTER TABLE dbo.AppSettings ADD EmailCodeCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeCooldownSeconds_Migration DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxPerHourPerEmail') IS NULL
    ALTER TABLE dbo.AppSettings ADD EmailCodeMaxPerHourPerEmail int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerEmail_Migration DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxPerHourPerIp') IS NULL
    ALTER TABLE dbo.AppSettings ADD EmailCodeMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerIp_Migration DEFAULT(50);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeExpiryMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD EmailCodeExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeExpiryMinutes_Migration DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxFailedAttemptsPerCode') IS NULL
    ALTER TABLE dbo.AppSettings ADD EmailCodeMaxFailedAttemptsPerCode int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxFailedAttemptsPerCode_Migration DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'InviteExpiryHours') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteExpiryHours int NOT NULL CONSTRAINT DF_AppSettings_InviteExpiryHours_Migration DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpExpiryMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpExpiryMinutes_Migration DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpCooldownSeconds') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpCooldownSeconds_Migration DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxPerHourPerInvite') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpMaxPerHourPerInvite int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerInvite_Migration DEFAULT(3);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxPerHourPerIp') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerIp_Migration DEFAULT(25);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxAttempts') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxAttempts_Migration DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpLockMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpLockMinutes_Migration DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'InviteMaxAttempts') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteMaxAttempts_Migration DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'InviteLockMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD InviteLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteLockMinutes_Migration DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpExpiryMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpExpiryMinutes_Migration DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpCooldownSeconds') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpCooldownSeconds_Migration DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxPerHourPerUser') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxPerHourPerUser int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerUser_Migration DEFAULT(3);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxPerHourPerIp') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerIp_Migration DEFAULT(25);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxAttempts') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxAttempts_Migration DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpLockMinutes') IS NULL
    ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpLockMinutes_Migration DEFAULT(15);
