# Authentication Cookie And Trusted Device

## Trust this device
- The login form includes a `Trust this device` option.
- When selected, the authentication cookie is issued as persistent and expires in 30 days.
- When not selected, the authentication cookie is a session cookie (no `Expires`/`Max-Age`).
- Helper text shown to users: `Keeps you signed in for 30 days. Don't use on shared devices.`

## Cookie settings
- `HttpOnly = true`
- `SecurePolicy = Always`
- `SameSite = Lax`
- `ExpireTimeSpan = 30 days` (applies to persistent cookies)
- `SlidingExpiration = false` (hard-cap behavior for persistent sign-in)

## Session invalidation on password change
- This app uses session-version claim validation on every authenticated request (`AuthClaimTypes.SessionVersion`).
- Password change increments the user session version.
- Existing cookies with old session version are rejected and signed out on validation.
- This is the app's security-stamp equivalent behavior for cookie invalidation.
