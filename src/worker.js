// Serves the page from the assets binding and keeps one shared timer state
// in a Durable Object that every open page talks to over a WebSocket at /ws.
//
// Control needs the key in the TIMER_KEY secret, offered as ?key= on the
// socket URL. A socket without it (or with the wrong one) can only watch.
// With no secret configured at all, every socket can control.
//
// An agenda, when one is active, replaces the speaker clock: a list of
// sections with planned lengths and a fixed meeting end. Each section's budget
// is set when it starts: its plan, shrunk in proportion when the time left
// until the end no longer covers the plan of it and everything after it.
// Pausing stops the section's clock but not the meeting's end, so the pause
// comes out of the sections still to come.

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      const id = env.TIMER.idFromName('room');
      return env.TIMER.get(id).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

function freshState() {
  return { phase: 'idle', startedAt: null, elapsedMs: 0, questionsAt: null, barStartMs: 0 };
}

function cleanSections(list) {
  if (!Array.isArray(list)) return [];
  return list.slice(0, 50)
    .map((x) => ({ name: String((x && x.name) || '').trim().slice(0, 60), plannedMs: Number(x && x.plannedMs) }))
    .filter((x) => x.name && Number.isFinite(x.plannedMs) && x.plannedMs > 0);
}

export class TimerRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.state = freshState();
    // null, or { active, endAt, lengthMs, end, text, sections: [{ name, plannedMs }], index, starts: [{ at, budgetMs }], pausedAt }.
    // pausedAt is when the current section was paused, or null while it runs.
    // index is -1 before the first section starts and sections.length when done;
    // end and text are what was typed, kept to prefill the editor. lengthMs is
    // how long the meeting is meant to be; endAt minus it is the scheduled start.
    this.agenda = null;
    ctx.blockConcurrencyWhile(async () => {
      const saved = await ctx.storage.get('state');
      if (saved) this.state = saved;
      this.agenda = (await ctx.storage.get('agenda')) || null;
    });
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected a WebSocket', { status: 426 });
    }
    const offered = new URL(request.url).searchParams.get('key') || '';
    const required = this.env.TIMER_KEY || '';
    const canWrite = required === '' || offered === required;

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ canWrite });  // survives hibernation
    server.send(JSON.stringify({ type: 'state', state: this.state, agenda: this.agenda, now: Date.now(), cause: 'sync', canWrite }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    const now = Date.now();
    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', t: msg.t, now }));
      return;
    }

    const attachment = ws.deserializeAttachment();
    if (!attachment || !attachment.canWrite) {
      ws.send(JSON.stringify({ type: 'denied' }));
      return;
    }

    const inAgenda = !!(this.agenda && this.agenda.active);
    if (inAgenda && ['reset', 'questions'].includes(msg.type)) return;
    const inSection = inAgenda && this.agenda.index >= 0 && this.agenda.index < this.agenda.sections.length;

    const s = this.state;
    const elapsed = () => (s.phase === 'running' ? now - s.startedAt : s.elapsedMs);
    switch (msg.type) {
      case 'start':
        if (inAgenda) {  // resume the paused section
          const a = this.agenda;
          if (a.pausedAt == null) return;
          a.starts[a.index].at += now - a.pausedAt;
          a.pausedAt = null;
          break;
        }
        if (s.phase === 'running') return;
        s.startedAt = now - s.elapsedMs;
        s.phase = 'running';
        break;
      case 'pause':
        if (inAgenda) {
          if (!inSection || this.agenda.pausedAt != null) return;
          this.agenda.pausedAt = now;
          break;
        }
        if (s.phase !== 'running') return;
        s.elapsedMs = now - s.startedAt;
        s.startedAt = null;
        s.phase = 'paused';
        break;
      case 'reset':
        this.state = freshState();
        break;
      case 'questions': {
        const e = elapsed();
        s.questionsAt = e;
        if (s.phase !== 'running') {
          s.elapsedMs = e;
          s.startedAt = now - e;
          s.phase = 'running';
        }
        break;
      }
      case 'set': {
        if (inAgenda) {  // the current section has leftMs to go; more than its budget raises the budget
          const a = this.agenda;
          const left = Number(msg.leftMs);
          if (!inSection || !Number.isFinite(left) || left < 0) return;
          const start = a.starts[a.index];
          start.budgetMs = Math.max(start.budgetMs, left);
          start.at = (a.pausedAt ?? now) - (start.budgetMs - left);
          break;
        }
        const e = Number(msg.elapsedMs);
        if (!Number.isFinite(e)) return;
        s.questionsAt = null;
        s.barStartMs = Math.min(0, e);
        if (s.phase === 'running') s.startedAt = now - e;
        else s.elapsedMs = e;
        break;
      }
      // Load an agenda, or with keep: true edit the one that is running: the
      // sections up to the current one stay as they are, the rest are replaced.
      case 'agenda': {
        const endAt = Number(msg.endAt);
        let sections = cleanSections(msg.sections);
        if (!Number.isFinite(endAt) || sections.length === 0) return;
        const a = this.agenda;
        const keep = !!msg.keep && inAgenda && a.index >= 0 && a.index < a.sections.length;
        if (keep) sections = a.sections.slice(0, a.index + 1).concat(sections.slice(a.index + 1));
        this.agenda = {
          active: true,
          endAt,
          lengthMs: Number(msg.lengthMs) > 0 ? Number(msg.lengthMs) : 60 * 60 * 1000,
          end: String(msg.end || '').slice(0, 10),
          text: String(msg.text || '').slice(0, 5000),
          sections,
          index: keep ? a.index : -1,
          starts: keep ? a.starts : [],
          pausedAt: keep ? a.pausedAt ?? null : null,
        };
        if (!keep) this.state = freshState();
        break;
      }
      case 'next': {
        const a = this.agenda;
        if (!inAgenda || a.index >= a.sections.length) return;
        a.index += 1;
        a.pausedAt = null;
        if (a.index < a.sections.length) {
          const rest = a.sections.slice(a.index).reduce((sum, x) => sum + x.plannedMs, 0);
          const share = Math.min(1, Math.max(0, a.endAt - now) / rest);
          a.starts.push({ at: now, budgetMs: a.sections[a.index].plannedMs * share });
        }
        break;
      }
      // Undo a Next: the previous section resumes with its original start and budget.
      case 'back': {
        const a = this.agenda;
        if (!inAgenda || a.index < 0) return;
        a.index -= 1;
        a.pausedAt = null;
        a.starts.length = a.index + 1;
        break;
      }
      // Back to the speaker clock. The typed agenda stays for next time.
      case 'clear':
        if (!inAgenda) return;
        Object.assign(this.agenda, { active: false, index: -1, starts: [], pausedAt: null });
        this.state = freshState();
        break;
      default:
        return;
    }

    await this.ctx.storage.put({ state: this.state, agenda: this.agenda });
    this.broadcast({ type: 'state', state: this.state, agenda: this.agenda, now, cause: msg.type });
  }

  broadcast(obj) {
    const data = JSON.stringify(obj);
    for (const socket of this.ctx.getWebSockets()) {
      try { socket.send(data); } catch (_) { /* a closing socket */ }
    }
  }

  webSocketClose(ws) {
    try { ws.close(); } catch (_) { /* already closed */ }
  }

  webSocketError(ws) {
    try { ws.close(); } catch (_) { /* already closed */ }
  }
}
