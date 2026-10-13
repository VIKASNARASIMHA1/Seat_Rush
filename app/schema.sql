CREATE TABLE IF NOT EXISTS events (
  id serial PRIMARY KEY, name text NOT NULL, venue text NOT NULL,
  starts_at timestamptz NOT NULL, waiting_room boolean NOT NULL DEFAULT false);

CREATE TABLE IF NOT EXISTS seats (
  event_id int NOT NULL REFERENCES events(id), seat_id text NOT NULL,
  price_cents int NOT NULL,
  status text NOT NULL DEFAULT 'AVAILABLE' CHECK (status IN ('AVAILABLE','HELD','SOLD')),
  hold_id uuid, hold_expires_at timestamptz, order_id uuid,
  PRIMARY KEY (event_id, seat_id));
CREATE INDEX IF NOT EXISTS seats_status_idx ON seats (event_id, status, seat_id);
CREATE INDEX IF NOT EXISTS seats_hold_idx ON seats (hold_id) WHERE hold_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS holds (
  id uuid PRIMARY KEY, event_id int NOT NULL, user_id text NOT NULL,
  seat_ids text[] NOT NULL, expires_at timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'ACTIVE');
CREATE INDEX IF NOT EXISTS holds_active_idx ON holds (expires_at) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS orders (
  id uuid PRIMARY KEY, idempotency_key text NOT NULL UNIQUE, hold_id uuid NOT NULL UNIQUE,
  user_id text NOT NULL, event_id int NOT NULL, seat_ids text[] NOT NULL,
  amount_cents int NOT NULL, status text NOT NULL,
  payment_ref text, failure_reason text, created_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS notifications (
  id bigserial PRIMARY KEY, event_key text NOT NULL UNIQUE, user_id text,
  kind text NOT NULL, body text, created_at timestamptz NOT NULL DEFAULT now());
