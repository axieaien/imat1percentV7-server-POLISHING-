# I'm at 1 Percent V7 — Live Battery Assistance (100% free to run)

Node.js + WebSocket server and a single-page front-end. No paid services, no Firebase.

## Run locally
```bash
npm install
npm start        # http://localhost:3000
```
Data is saved to `data.json` next to the server.

## Free hosting that keeps your data
Render's free plan wipes its disk whenever the app sleeps or redeploys, so V7 saves its data to a **private GitHub Gist** instead (free). Everything is **encrypted (AES-256-GCM)** before upload with a key derived from `JWT_SECRET`.

| Env var | What it is |
|---|---|
| `JWT_SECRET` | **Required in production.** Long random string. Also the encryption key. Never change it or old data can't be read. |
| `GIST_ID` | ID of your secret gist (the last part of its URL) |
| `GITHUB_TOKEN` | Classic token with only the `gist` scope |
| `ADMIN_EMAIL` | Optional. Your account email(s), comma separated → unlocks the Moderation panel in Profile |
| `DATA_DIR` | Optional. Local folder for `data.json` when not using a Gist |

The gist must contain one file named `imat1-data.txt` with the text `init`. If the stored data can't be read at startup the server refuses to start rather than overwrite it.

**Keep-alive (free):** Render's free plan sleeps after ~15 min of no traffic (first visit then takes ~30–60 s). To keep it awake, create a free UptimeRobot monitor for `https://YOUR-APP.onrender.com/healthz` at a 5-minute interval.

## Features
- Accounts (bcrypt + JWT), change password, delete account, rate limiting, CSP + security headers, input validation
- Live map centered on you with three selectable search radii only: 100 m, 500 m, and 1 km; 3 CDN fallbacks for the map library, tile fallback, Google/Apple Maps links when routing is unavailable
- Location permission handling for iOS / Android / desktop, HTTPS and in-app-browser checks, manual "pin on map" fallback
- Requests (Pending → Accepted → Resolved / Cancelled / auto-Expired) for power banks, chargers, laptop chargers, USB-C/Lightning/Micro-USB cables, wall chargers, charging adapters, universal travel adapters, and charging outlets; helper alerts, chat with unread badges, read receipts, typing indicator
- Block / report users, admin moderation panel (remove & ban), automatic clean-up of old data
- Installable PWA, offline banner, reconnect resync
- Location and presence are memory-only (never saved) and expire ~90 s after a user goes quiet

## API summary
| Method | Path | Purpose |
|---|---|---|
| POST | /api/auth/register · /login | `{token,user}` |
| GET / PUT / DELETE | /api/me | profile · update · delete account |
| POST | /api/me/password | change password |
| POST | /api/location · /heartbeat · /offline | presence |
| GET | /api/users | others' public profile/presence |
| GET / POST | /api/requests | visible requests · create |
| POST | /api/requests/:id/accept · /resolve · /cancel | lifecycle |
| GET | /api/conversations · /api/messages?with= | chat |
| POST | /api/messages · /api/messages/read | send · mark read |
| POST | /api/block · /api/report · GET /api/blocked | safety |
| GET | /api/admin/reports · POST /api/admin/dismiss · /remove | moderation (admin only) |
| GET | /healthz | health check |
| WS | /ws?token=… | realtime events |

## Not included
Password reset by email (removed on purpose — needs an email service), automated tests.

## V7 updates
- Added laptop charger and compatible charging accessories to request choices.
- Restricted the location search radius to 100 m, 500 m, or 1 km (default: 500 m). The radius is applied to nearby map markers and helper/request lists whenever your location is available.


## V7 features

- Smart match suggestions rank online available helpers within the selected 100 m, 500 m, or 1 km radius using proximity, verification status, and average ratings. Walking time is a rough estimate (straight-line distance adjusted by 1.25, at 80 m/min); use the linked walking directions for route-specific guidance.
- 1% Emergency Mode marks an urgent request and triggers an urgent real-time notification to available helpers.
- Student verification is admin-reviewed. A campus administrator must verify the student through the institution's normal in-person process before approving; do not upload or store student ID photos in this app. Set `ADMIN_EMAIL` to one or more comma-separated administrator email addresses.
- Helpers can receive ratings after the requester marks an accepted request as resolved. Ratings are averaged and shown on helper listings.
- Group assistance allows 2–5 helpers to join one request.
- Live WebSocket messages and browser notifications provide message/request alerts. Browser notification permission is controlled by the user/device.
- Battery-saving tips and emergency guidance are available from the Battery Guide page.

The 100 m, 500 m, and 1 km location filters remain the only radius choices. Location visibility still requires explicit browser permission and active sharing.


## V7 additions
- One-tap 1% SOS on the dashboard (asks for confirmation, then sends an urgent request using the selected radius).
- Voluntary battery percentage sharing in Profile; the app does not read device battery automatically.
- Borrow & Lend board for students to list spare charging equipment and message owners.
- Community Helpers Board based on completed requests, thanks, ratings, and verification.
- Thank-you action after a request is resolved.
- Activity History page for the signed-in student's own requests.
- Dark mode preference stored locally on the device.
- Existing live helper map, route guidance, smart matching, group assistance, messaging, verification, and emergency notifications are retained.

Safety notes: SOS does not contact emergency services; it alerts eligible helpers inside the app. Students should arrange handovers in supervised public areas. Battery sharing is voluntary and manually entered. Walking route availability depends on third-party map services and internet access.
