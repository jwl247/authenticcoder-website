// ============================================================
// packages-worker — Phoenix DevOps OS
// UnitedSys | United Systems | jwl247
// DB: phoenix_dev_db (primary), phoenix-catalog (secondary)
// Deploy: wrangler deploy
// Tail:   wrangler tail packages-worker
// ============================================================

const JSON_HEADERS = { 'Content-Type': 'application/json' };

const ok  = (data, status = 200) => new Response(JSON.stringify({ ok: true,  ...data }), { status, headers: JSON_HEADERS });
const err = (msg,  status = 400) => new Response(JSON.stringify({ ok: false, error: msg }), { status, headers: JSON_HEADERS });

function authed(request, env) {
  const token = request.headers.get('X-Phoenix-Auth');
  return token && token === env.PHOENIX_AUTH;
}

export default {
  async fetch(request, env) {
    const url    = new URL(request.url);
    const path   = url.pathname.replace(/\/$/, '') || '/';
    const method = request.method.toUpperCase();
    const DEV    = env.DEV_DB;     // phoenix_dev_db  — primary
    const CAT    = env.CATALOG_DB; // phoenix-catalog — secondary

    try {

      // ── /health ─────────────────────────────────────────────────────────
      if (path === '/health' && method === 'GET') {
        const [cp, cu, gl, pk, ve, in_] = await Promise.all([
          DEV.prepare('SELECT COUNT(*) as c FROM clonepool').first(),
          DEV.prepare('SELECT COUNT(*) as c FROM custody').first(),
          DEV.prepare('SELECT COUNT(*) as c FROM glossary').first(),
          DEV.prepare('SELECT COUNT(*) as c FROM packages').first(),
          DEV.prepare('SELECT COUNT(*) as c FROM versions').first(),
          DEV.prepare('SELECT COUNT(*) as c FROM installed').first(),
        ]);
        return ok({
          worker:    'packages-worker',
          version:   '2.1.0',
          status:    'online',
          ts:        new Date().toISOString(),
          clonepool: cp?.c  ?? 0,
          custody:   cu?.c  ?? 0,
          glossary:  gl?.c  ?? 0,
          packages:  pk?.c  ?? 0,
          versions:  ve?.c  ?? 0,
          installed: in_?.c ?? 0,
        });
      }

      // ── /clonepool ───────────────────────────────────────────────────────
      if (path === '/clonepool' && method === 'GET') {
        const state = url.searchParams.get('state');
        const tier  = url.searchParams.get('tier');
        const limit = parseInt(url.searchParams.get('limit') || '50');
        let q = 'SELECT id, hex_id, b58, name, original_name, pool_path, state, tier, size, version, intaked_at, notes FROM clonepool WHERE 1=1';
        const binds = [];
        if (state) { q += ' AND state = ?'; binds.push(state); }
        if (tier)  { q += ' AND tier = ?';  binds.push(parseInt(tier)); }
        q += ' ORDER BY intaked_at DESC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(q).bind(...binds).all();
        return ok({ clonepool: results, count: results.length });
      }

      if (path.match(/^\/clonepool\/(.+)$/) && method === 'GET') {
        const hex_id = path.split('/')[2];
        const row = await DEV.prepare('SELECT * FROM clonepool WHERE hex_id = ?').bind(hex_id).first();
        if (!row) return err('not found', 404);
        return ok({ entry: row });
      }

      if (path === '/clonepool' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { hex_id, b58, name, original_name, pool_path, sidecar_path,
                header_qr, footer_qr, hash_sha3, hash_blake2, sha3_fp,
                blake2_fp, state = 'white', tier = 1, size, version,
                source_path, notes } = body;
        if (!hex_id || !b58 || !name) return err('hex_id, b58, name required');
        await DEV.prepare(`
          INSERT INTO clonepool
            (hex_id, b58, name, original_name, pool_path, sidecar_path,
             header_qr, footer_qr, hash_sha3, hash_blake2, sha3_fp,
             blake2_fp, state, tier, size, version, source_path, notes)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `).bind(hex_id, b58, name, original_name ?? null, pool_path ?? null,
                sidecar_path ?? null, header_qr ?? null, footer_qr ?? null,
                hash_sha3 ?? null, hash_blake2 ?? null, sha3_fp ?? null,
                blake2_fp ?? null, state, tier, size ?? null, version ?? null,
                source_path ?? null, notes ?? null).run();
        return ok({ inserted: hex_id }, 201);
      }

      if (path.match(/^\/clonepool\/(.+)$/) && method === 'PUT') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const hex_id = path.split('/')[2];
        const body   = await request.json();
        const fields = ['state','tier','version','pool_path','notes'];
        const sets = [], binds = [];
        for (const f of fields) {
          if (body[f] !== undefined) { sets.push(`${f} = ?`); binds.push(body[f]); }
        }
        if (!sets.length) return err('no updatable fields provided');
        sets.push("updated_at = datetime('now')");
        binds.push(hex_id);
        await DEV.prepare(`UPDATE clonepool SET ${sets.join(', ')} WHERE hex_id = ?`).bind(...binds).run();
        return ok({ updated: hex_id });
      }

      // ── /packages ────────────────────────────────────────────────────────
      if (path === '/packages' && method === 'GET') {
        const state    = url.searchParams.get('state');
        const platform = url.searchParams.get('platform');
        const limit    = parseInt(url.searchParams.get('limit') || '100');
        let q = 'SELECT id, name, version, backend, platform, state, tier, environment, owner, description, installed_at FROM packages WHERE 1=1';
        const binds = [];
        if (state)    { q += ' AND state = ?';    binds.push(state); }
        if (platform) { q += ' AND platform = ?'; binds.push(platform); }
        q += ' ORDER BY name ASC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(q).bind(...binds).all();
        return ok({ packages: results, count: results.length });
      }

      if (path.match(/^\/packages\/([^/]+)$/) && method === 'GET') {
        const name = decodeURIComponent(path.split('/')[2]);
        const pkg  = await DEV.prepare('SELECT * FROM packages WHERE name = ?').bind(name).first();
        if (!pkg) return err('package not found', 404);
        const [vers, files, deps] = await Promise.all([
          DEV.prepare('SELECT id, version, store_path, hash_sha3, size, created_at, note, signed_by FROM versions WHERE package = ? ORDER BY created_at DESC').bind(name).all(),
          DEV.prepare('SELECT id, filename, filepath, filetype, size_bytes FROM files WHERE package_id = ?').bind(pkg.id).all(),
          DEV.prepare('SELECT depends_on_name, min_version, max_version, layer FROM dependencies WHERE package_id = ?').bind(pkg.id).all(),
        ]);
        return ok({ package: pkg, versions: vers.results, files: files.results, dependencies: deps.results });
      }

      if (path === '/packages' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { name, version, backend, platform, hash_sha3, hash_blake2,
                manifest, hex_id, b58, pool_path, sidecar_path,
                state = 'white', tier = 1, environment = 'prod',
                owner, notes, description = '' } = body;
        if (!name) return err('name required');
        await DEV.prepare(`
          INSERT INTO packages
            (name, version, backend, platform, hash_sha3, hash_blake2,
             manifest, hex_id, b58, pool_path, sidecar_path, state, tier,
             environment, owner, notes, description)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(name) DO UPDATE SET
            version = excluded.version, state = excluded.state,
            updated_at = datetime('now')
        `).bind(name, version ?? null, backend ?? null, platform ?? null,
                hash_sha3 ?? null, hash_blake2 ?? null, manifest ?? null,
                hex_id ?? null, b58 ?? null, pool_path ?? null,
                sidecar_path ?? null, state, tier, environment,
                owner ?? null, notes ?? null, description).run();
        return ok({ upserted: name }, 201);
      }

      // ── /versions ────────────────────────────────────────────────────────
      // POST must come before the regex GET to avoid route collision
      if (path === '/versions' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { package: pkg, version, store_path, hash_sha3, hash_blake2, size, note = '', signed_by } = body;
        if (!pkg || !version || !store_path) return err('package, version, store_path required');
        await DEV.prepare(`
          INSERT INTO versions (package, version, store_path, hash_sha3, hash_blake2, size, note, signed_by)
          VALUES (?,?,?,?,?,?,?,?)
        `).bind(pkg, version, store_path, hash_sha3 ?? null, hash_blake2 ?? null, size ?? null, note, signed_by ?? null).run();
        return ok({ pushed: { package: pkg, version } }, 201);
      }

      if (path === '/versions' && method === 'GET') {
        const pkg   = url.searchParams.get('package');
        const limit = parseInt(url.searchParams.get('limit') || '50');
        let q = 'SELECT id, package, version, store_path, hash_sha3, size, created_at, note, signed_by FROM versions WHERE 1=1';
        const binds = [];
        if (pkg) { q += ' AND package = ?'; binds.push(pkg); }
        q += ' ORDER BY created_at DESC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(q).bind(...binds).all();
        return ok({ versions: results, count: results.length });
      }

      // ── /custody ─────────────────────────────────────────────────────────
      if (path === '/custody' && method === 'GET') {
        const hex_id = url.searchParams.get('hex_id');
        const state  = url.searchParams.get('state');
        const limit  = parseInt(url.searchParams.get('limit') || '100');
        let q = 'SELECT id, name, hex_id, state, action, actor, validated, intaked_at FROM custody WHERE 1=1';
        const binds = [];
        if (hex_id) { q += ' AND hex_id = ?'; binds.push(hex_id); }
        if (state)  { q += ' AND state = ?';  binds.push(state); }
        q += ' ORDER BY intaked_at DESC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(q).bind(...binds).all();
        return ok({ custody: results, count: results.length });
      }

      if (path === '/custody' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { name, hex_id, qr_top, qr_bottom, state = 'white', action, actor, validated = 0 } = body;
        if (!name || !hex_id || !action) return err('name, hex_id, action required');
        await DEV.prepare(`
          INSERT INTO custody (name, hex_id, qr_top, qr_bottom, state, action, actor, validated)
          VALUES (?,?,?,?,?,?,?,?)
        `).bind(name, hex_id, qr_top ?? null, qr_bottom ?? null, state, action, actor ?? null, validated).run();
        return ok({ logged: { hex_id, action } }, 201);
      }

      // ── /glossary ────────────────────────────────────────────────────────
      if (path === '/glossary' && method === 'GET') {
        const q     = url.searchParams.get('q');
        const cat   = url.searchParams.get('category');
        const limit = parseInt(url.searchParams.get('limit') || '100');
        let sql = 'SELECT id, hex, b58, name, category_hex, description, state, version, platform, intaked_at FROM glossary WHERE 1=1';
        const binds = [];
        if (q)   { sql += ' AND (name LIKE ? OR description LIKE ?)'; binds.push(`%${q}%`, `%${q}%`); }
        if (cat) { sql += ' AND category_hex = ?'; binds.push(cat); }
        sql += ' ORDER BY name ASC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(sql).bind(...binds).all();
        return ok({ glossary: results, count: results.length });
      }

      if (path.match(/^\/glossary\/([^/]+)$/) && method === 'GET') {
        const hex = path.split('/')[2];
        const row = await DEV.prepare('SELECT * FROM glossary WHERE hex = ?').bind(hex).first();
        if (!row) return err('not found', 404);
        return ok({ term: row });
      }

      if (path === '/glossary' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { hex, b58, name, category_hex, description = '', state = 'white',
                version, platform, backend, size, pool_path, sidecar, notes } = body;
        if (!hex || !name) return err('hex and name required');
        await DEV.prepare(`
          INSERT INTO glossary (hex, b58, name, category_hex, description, state, version, platform, backend, size, pool_path, sidecar, notes)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(hex) DO UPDATE SET
            name = excluded.name, description = excluded.description,
            state = excluded.state, amended = 1
        `).bind(hex, b58 ?? null, name, category_hex ?? null, description, state,
                version ?? null, platform ?? null, backend ?? null, size ?? null,
                pool_path ?? null, sidecar ?? null, notes ?? null).run();
        return ok({ upserted: hex }, 201);
      }

      if (path.match(/^\/glossary\/([^/]+)$/) && method === 'PUT') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const hex  = path.split('/')[2];
        const body = await request.json();
        const allowed = ['name','description','state','category_hex','version','platform','notes'];
        const sets = [], binds = [];
        for (const f of allowed) {
          if (body[f] !== undefined) { sets.push(`${f} = ?`); binds.push(body[f]); }
        }
        if (!sets.length) return err('no updatable fields');
        sets.push('amended = 1');
        binds.push(hex);
        await DEV.prepare(`UPDATE glossary SET ${sets.join(', ')} WHERE hex = ?`).bind(...binds).run();
        return ok({ updated: hex });
      }

      if (path.match(/^\/glossary\/([^/]+)$/) && method === 'DELETE') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const hex = path.split('/')[2];
        await DEV.prepare('DELETE FROM glossary WHERE hex = ?').bind(hex).run();
        return ok({ deleted: hex });
      }

      // ── /installed ───────────────────────────────────────────────────────
      if (path === '/installed' && method === 'GET') {
        const layer = url.searchParams.get('layer');
        const limit = parseInt(url.searchParams.get('limit') || '100');
        let q = 'SELECT i.id, p.name, i.version, i.install_path, i.installed_by, i.layer, i.backend, i.installed_at FROM installed i JOIN packages p ON p.id = i.package_id WHERE 1=1';
        const binds = [];
        if (layer) { q += ' AND i.layer = ?'; binds.push(layer); }
        q += ' ORDER BY i.installed_at DESC LIMIT ?';
        binds.push(limit);
        const { results } = await DEV.prepare(q).bind(...binds).all();
        return ok({ installed: results, count: results.length });
      }

      if (path === '/installed' && method === 'POST') {
        if (!authed(request, env)) return err('unauthorized', 401);
        const body = await request.json();
        const { package_name, version, install_path, installed_by, layer, backend = 'direct' } = body;
        if (!package_name || !install_path) return err('package_name and install_path required');
        const pkg = await DEV.prepare('SELECT id FROM packages WHERE name = ?').bind(package_name).first();
        if (!pkg) return err(`package '${package_name}' not registered`, 404);
        await DEV.prepare(`
          INSERT INTO installed (package_id, version, install_path, installed_by, layer, backend)
          VALUES (?,?,?,?,?,?)
        `).bind(pkg.id, version ?? null, install_path, installed_by ?? 'phoenix-installer', layer ?? null, backend).run();
        return ok({ registered: package_name }, 201);
      }

      // ── /categories ──────────────────────────────────────────────────────
      if (path === '/categories' && method === 'GET') {
        const { results } = await DEV.prepare('SELECT * FROM categories ORDER BY hex ASC').all();
        return ok({ categories: results, count: results.length });
      }

      // ── /toc ─────────────────────────────────────────────────────────────
      if (path === '/toc' && method === 'GET') {
        const { results } = await DEV.prepare(
          'SELECT name, version, state, tier, platform, description FROM packages ORDER BY tier ASC, name ASC'
        ).all();
        return ok({ toc: results, count: results.length, generated_at: new Date().toISOString() });
      }

      // ── /search ──────────────────────────────────────────────────────────
      if (path === '/search' && method === 'GET') {
        const q = url.searchParams.get('q');
        if (!q || q.trim().length < 2) return err('query too short — min 2 chars');
        const term = `%${q.trim()}%`;
        const [pkgs, pool, gloss] = await Promise.all([
          DEV.prepare('SELECT name, version, description, state FROM packages WHERE name LIKE ? OR description LIKE ? LIMIT 20').bind(term, term).all(),
          DEV.prepare('SELECT hex_id, name, original_name, state, version FROM clonepool WHERE name LIKE ? OR original_name LIKE ? LIMIT 20').bind(term, term).all(),
          DEV.prepare('SELECT hex, name, description FROM glossary WHERE name LIKE ? OR description LIKE ? LIMIT 20').bind(term, term).all(),
        ]);
        return ok({
          query:     q,
          packages:  pkgs.results,
          clonepool: pool.results,
          glossary:  gloss.results,
          total:     pkgs.results.length + pool.results.length + gloss.results.length,
        });
      }

      // ── 404 ──────────────────────────────────────────────────────────────
      return err(`no route for ${method} ${path}`, 404);

    } catch (e) {
      console.error('packages-worker error:', e.message);
      return err(`internal error: ${e.message}`, 500);
    }
  },
};
