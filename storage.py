"""PostgreSQL (Supabase) persistence. Parameterized queries; transaction-sized synchronous work."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

# Schema is applied once on Supabase; kept here for documentation / local tests.
SCHEMA = '''
CREATE TABLE IF NOT EXISTS users (
 id BIGINT PRIMARY KEY, joined TEXT NOT NULL, last_seen TEXT NOT NULL,
 blocked INTEGER NOT NULL DEFAULT 0, reachable INTEGER NOT NULL DEFAULT 1,
 notifications INTEGER NOT NULL DEFAULT 1, pending TEXT
);
CREATE TABLE IF NOT EXISTS activity (
 user_id BIGINT REFERENCES users(id) ON DELETE CASCADE, day TEXT,
 PRIMARY KEY(user_id,day)
);
CREATE TABLE IF NOT EXISTS admins (user_id BIGINT PRIMARY KEY, role TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS movies (
 id SERIAL PRIMARY KEY, code TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('movie','series')), genre TEXT NOT NULL DEFAULT '',
 year INTEGER NOT NULL DEFAULT 0, country TEXT NOT NULL DEFAULT '',
 description TEXT NOT NULL DEFAULT '', duration INTEGER NOT NULL DEFAULT 0,
 poster TEXT NOT NULL DEFAULT '', trailer TEXT NOT NULL DEFAULT '',
 hidden INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS assets (
 id SERIAL PRIMARY KEY, movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
 language TEXT NOT NULL, quality TEXT NOT NULL, season INTEGER NOT NULL DEFAULT 0,
 episode INTEGER NOT NULL DEFAULT 0, file_id TEXT NOT NULL, media_type TEXT NOT NULL,
 UNIQUE(movie_id,language,quality,season,episode)
);
CREATE TABLE IF NOT EXISTS marks (
 user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
 movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE, kind TEXT NOT NULL,
 PRIMARY KEY(user_id,movie_id,kind)
);
CREATE TABLE IF NOT EXISTS ratings (
 user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
 movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
 score INTEGER CHECK(score BETWEEN 1 AND 5), PRIMARY KEY(user_id,movie_id)
);
CREATE TABLE IF NOT EXISTS deliveries (
 id SERIAL PRIMARY KEY, user_id BIGINT REFERENCES users(id),
 movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE, sent_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS deliveries_movie ON deliveries(movie_id);
CREATE INDEX IF NOT EXISTS deliveries_user_movie ON deliveries(user_id,movie_id,id);
CREATE INDEX IF NOT EXISTS ratings_movie ON ratings(movie_id);
CREATE INDEX IF NOT EXISTS activity_day ON activity(day);
CREATE INDEX IF NOT EXISTS movies_visible ON movies(hidden,id);
CREATE INDEX IF NOT EXISTS assets_episode ON assets(movie_id,season,episode);
CREATE INDEX IF NOT EXISTS marks_follow ON marks(movie_id,kind,user_id);
CREATE TABLE IF NOT EXISTS channels (
 id SERIAL PRIMARY KEY, chat_id BIGINT UNIQUE NOT NULL, title TEXT NOT NULL,
 url TEXT NOT NULL, required INTEGER NOT NULL DEFAULT 0, source INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS states (user_id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS keyboards (
 user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reports (
 id SERIAL PRIMARY KEY, user_id BIGINT REFERENCES users(id),
 movie_id INTEGER REFERENCES movies(id) ON DELETE SET NULL, text TEXT, created TEXT,
 resolved INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS reports_status ON reports(resolved,id);
CREATE TABLE IF NOT EXISTS imports (
 chat_id BIGINT, message_id BIGINT, PRIMARY KEY(chat_id,message_id)
);
CREATE TABLE IF NOT EXISTS jobs (
 id SERIAL PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL, created TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS queue (
 id SERIAL PRIMARY KEY, job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
 user_id BIGINT REFERENCES users(id), state TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0, next_at DOUBLE PRECISION NOT NULL DEFAULT 0,
 UNIQUE(job_id,user_id)
);
CREATE INDEX IF NOT EXISTS queue_pending ON queue(state,next_at);
'''


class Store:
    def __init__(self, dsn, timezone='Asia/Tashkent'):
        """dsn: PostgreSQL connection string (DATABASE_URL)."""
        self.dsn = dsn
        self.timezone = ZoneInfo(timezone)
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False

    @staticmethod
    def _sql(sql):
        """Convert SQLite-style ? placeholders to psycopg2 %s."""
        return sql.replace('?', '%s')

    def _cursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def now(self):
        return datetime.now(self.timezone).isoformat(timespec='seconds')

    def all(self, sql, args=()):
        with self._cursor() as cur:
            cur.execute(self._sql(sql), args)
            return [dict(r) for r in cur.fetchall()]

    def one(self, sql, args=()):
        with self._cursor() as cur:
            cur.execute(self._sql(sql), args)
            r = cur.fetchone()
            return dict(r) if r else None

    def run(self, sql, args=()):
        with self.conn:
            with self._cursor() as cur:
                cur.execute(self._sql(sql), args)
                return cur.rowcount

    def execute_returning(self, sql, args=()):
        with self.conn:
            with self._cursor() as cur:
                cur.execute(self._sql(sql), args)
                row = cur.fetchone()
                return list(row.values())[0] if row else None

    def touch(self, uid):
        now = self.now()
        with self.conn:
            with self._cursor() as cur:
                cur.execute(
                    'INSERT INTO users(id,joined,last_seen) VALUES(%s,%s,%s) '
                    'ON CONFLICT(id) DO UPDATE SET last_seen=EXCLUDED.last_seen, reachable=1',
                    (uid, now, now)
                )
                cur.execute(
                    'INSERT INTO activity(user_id,day) VALUES(%s,%s) ON CONFLICT DO NOTHING',
                    (uid, now[:10])
                )
        return self.one('SELECT * FROM users WHERE id=%s', (uid,))

    def role(self, uid, owners):
        if uid in owners:
            return 'owner'
        r = self.one('SELECT role FROM admins WHERE user_id=%s', (uid,))
        return r['role'] if r else ''

    def state(self, uid):
        row = self.one('SELECT payload FROM states WHERE user_id=%s', (uid,))
        return json.loads(row['payload']) if row else None

    def set_state(self, uid, state):
        if state is None:
            self.run('DELETE FROM states WHERE user_id=%s', (uid,))
        else:
            self.run(
                'INSERT INTO states(user_id,payload) VALUES(%s,%s) '
                'ON CONFLICT(user_id) DO UPDATE SET payload=EXCLUDED.payload',
                (uid, json.dumps(state, ensure_ascii=False))
            )

    def keyboard(self, uid):
        row = self.one('SELECT payload FROM keyboards WHERE user_id=%s', (uid,))
        return json.loads(row['payload']) if row else None

    def set_keyboard(self, uid, keyboard):
        self.run(
            'INSERT INTO keyboards(user_id,payload) VALUES(%s,%s) '
            'ON CONFLICT(user_id) DO UPDATE SET payload=EXCLUDED.payload',
            (uid, json.dumps(keyboard, ensure_ascii=False))
        )

    def movie(self, mid, public=True):
        return self.one(
            'SELECT * FROM movies WHERE id=%s' + (' AND hidden=0' if public else ''),
            (mid,)
        )

    def by_code(self, code, public=True):
        return self.one(
            'SELECT * FROM movies WHERE code=%s' + (' AND hidden=0' if public else ''),
            (code,)
        )

    def create_movie(self, data):
        keys = ['code', 'title', 'kind', 'genre', 'year', 'country', 'description', 'duration', 'poster', 'trailer']
        vals = [data.get(k, 0 if k in ('year', 'duration') else '') for k in keys]
        return self.execute_returning(
            f'INSERT INTO movies({",".join(keys)},created) VALUES({",".join(["%s"]*11)}) RETURNING id',
            vals + [self.now()]
        )

    def create_with_asset(self, movie, asset):
        if self.by_code(movie['code'], False):
            raise ValueError('Bu kod boshqa admin tomonidan band qilindi. /cancel qilib boshqa kod tanlang.')
        with self.conn:
            with self._cursor() as cur:
                keys = ['code', 'title', 'kind', 'genre', 'year', 'country', 'description', 'duration', 'poster', 'trailer']
                cur.execute(
                    f'INSERT INTO movies({",".join(keys)},created) VALUES({",".join(["%s"]*11)}) RETURNING id',
                    [movie[k] for k in keys] + [self.now()]
                )
                mid = cur.fetchone()['id']
                season, episode = asset.get('season', 0), asset.get('episode', 0)
                if (movie['kind'] == 'series' and (season < 1 or episode < 1)) or (
                        movie['kind'] == 'movie' and (season or episode)):
                    raise ValueError('Fasl va qism kino turiga mos emas.')
                cur.execute(
                    'INSERT INTO assets(movie_id,language,quality,season,episode,file_id,media_type) '
                    'VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (mid, asset['language'], asset['quality'], season, episode, asset['file_id'], asset['media_type'])
                )
        return mid

    def add_asset(self, mid, data):
        movie = self.movie(mid, public=False)
        if not movie:
            raise ValueError('Kino topilmadi.')
        season, episode = data.get('season', 0), data.get('episode', 0)
        if (movie['kind'] == 'series' and (season < 1 or episode < 1)) or (
                movie['kind'] == 'movie' and (season or episode)):
            raise ValueError('Fasl/qism kino turiga mos emas.')
        with self.conn:
            with self._cursor() as cur:
                cur.execute(
                    'SELECT id FROM assets WHERE movie_id=%s AND season=%s AND episode=%s',
                    (mid, season, episode)
                )
                fresh = cur.fetchone() is None
                cur.execute(
                    'INSERT INTO assets(movie_id,language,quality,season,episode,file_id,media_type) '
                    'VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                    (mid, data['language'], data['quality'], season, episode, data['file_id'], data['media_type'])
                )
                aid = cur.fetchone()['id']
                if fresh and movie['kind'] == 'series' and not movie['hidden']:
                    self._episode_job(mid, season, episode, cur)
                return aid

    def import_post(self, chat, message, data):
        with self.conn:
            with self._cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM imports WHERE chat_id=%s AND message_id=%s',
                    (chat, message)
                )
                if cur.fetchone():
                    return None
                movie = self.by_code(data['code'], public=False)
                if movie and (movie['title'] != data['title'] or movie['kind'] != data['kind']):
                    raise ValueError('Bu kod boshqa nomdagi kinoga tegishli.')
                if movie:
                    mid = movie['id']
                else:
                    keys = ['code', 'title', 'kind', 'genre', 'year', 'country', 'description', 'duration', 'poster', 'trailer']
                    cur.execute(
                        f'INSERT INTO movies({",".join(keys)},created) VALUES({",".join(["%s"]*11)}) RETURNING id',
                        [data[k] for k in keys] + [self.now()]
                    )
                    mid = cur.fetchone()['id']
                cur.execute(
                    'SELECT id FROM assets WHERE movie_id=%s AND season=%s AND episode=%s',
                    (mid, data['season'], data['episode'])
                )
                fresh = cur.fetchone() is None
                cur.execute(
                    'INSERT INTO assets(movie_id,language,quality,season,episode,file_id,media_type) '
                    'VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (mid, data['language'], data['quality'], data['season'], data['episode'], data['file_id'], data['media_type'])
                )
                cur.execute('INSERT INTO imports(chat_id,message_id) VALUES(%s,%s)', (chat, message))
                if fresh and data['kind'] == 'series' and not (movie and movie['hidden']):
                    self._episode_job(mid, data['season'], data['episode'], cur)
                return mid

    def _episode_job(self, mid, season, episode, cur=None):
        payload = json.dumps({'movie_id': mid, 'season': season, 'episode': episode})
        own = cur is None
        if own:
            cur = self._cursor()
        try:
            cur.execute(
                'INSERT INTO jobs(kind,payload,created) VALUES(%s,%s,%s) RETURNING id',
                ('episode', payload, self.now())
            )
            jid = cur.fetchone()['id']
            cur.execute(
                'INSERT INTO queue(job_id,user_id) '
                'SELECT %s, u.id FROM users u JOIN marks m ON m.user_id=u.id '
                "WHERE m.movie_id=%s AND m.kind='follow' AND u.notifications=1 "
                'AND u.blocked=0 AND u.reachable=1',
                (jid, mid)
            )
            if own:
                self.conn.commit()
        finally:
            if own:
                cur.close()

    def broadcast(self, chat_id, message_id, creator):
        with self.conn:
            with self._cursor() as cur:
                cur.execute(
                    'INSERT INTO jobs(kind,payload,created) VALUES(%s,%s,%s) RETURNING id',
                    ('broadcast', json.dumps({'chat_id': chat_id, 'message_id': message_id, 'creator': creator}), self.now())
                )
                jid = cur.fetchone()['id']
                cur.execute(
                    'INSERT INTO queue(job_id,user_id) SELECT %s, id FROM users '
                    'WHERE notifications=1 AND blocked=0 AND reachable=1',
                    (jid,)
                )
        return jid

    def mark(self, uid, mid, kind):
        if kind not in ('favorite', 'later', 'follow'):
            raise ValueError('Noma’lum ro‘yxat.')
        with self.conn:
            with self._cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM marks WHERE user_id=%s AND movie_id=%s AND kind=%s',
                    (uid, mid, kind)
                )
                exists = cur.fetchone() is not None
                if exists:
                    cur.execute(
                        'DELETE FROM marks WHERE user_id=%s AND movie_id=%s AND kind=%s',
                        (uid, mid, kind)
                    )
                else:
                    cur.execute(
                        'INSERT INTO marks(user_id,movie_id,kind) VALUES(%s,%s,%s)',
                        (uid, mid, kind)
                    )
        return not exists

    def backup(self, destination):
        raise NotImplementedError(
            'Supabase uchun zaxira: Dashboard → Database → Backups yoki pg_dump ishlating.'
        )

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
