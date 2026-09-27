-- Initial schema for the mediasoup-meet backend.
-- Kept deliberately narrow: every column is used by the app, no speculative fields.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    display_name    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE meetings (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code                    TEXT NOT NULL UNIQUE,
    title                   TEXT NOT NULL,
    host_id                 UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    max_participants        SMALLINT NOT NULL DEFAULT 50,
    waiting_room_enabled    BOOLEAN NOT NULL DEFAULT true,
    status                  TEXT NOT NULL DEFAULT 'scheduled'
                                CHECK (status IN ('scheduled', 'live', 'ended')),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at              TIMESTAMPTZ,
    ended_at                TIMESTAMPTZ
);

CREATE INDEX idx_meetings_host_id ON meetings(host_id);
CREATE INDEX idx_meetings_code ON meetings(code);

-- One row per (meeting, join) so a user re-joining the same meeting is logged again.
CREATE TABLE meeting_participants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    meeting_id      UUID NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            TEXT NOT NULL DEFAULT 'participant' CHECK (role IN ('host', 'participant')),
    joined_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    left_at         TIMESTAMPTZ
);

CREATE INDEX idx_meeting_participants_meeting_id ON meeting_participants(meeting_id);
CREATE INDEX idx_meeting_participants_user_id ON meeting_participants(user_id);

CREATE TABLE recordings (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    meeting_id              UUID NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
    participant_user_id     UUID REFERENCES users(id) ON DELETE SET NULL,
    kind                    TEXT NOT NULL CHECK (kind IN ('audio', 'video')),
    file_path               TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'recording'
                                CHECK (status IN ('recording', 'processing', 'ready', 'failed')),
    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at                TIMESTAMPTZ
);

CREATE INDEX idx_recordings_meeting_id ON recordings(meeting_id);
