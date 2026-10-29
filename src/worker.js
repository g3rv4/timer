// Serves the page from the assets binding and keeps one shared timer state
// in a Durable Object that every open page talks to over a WebSocket at /ws.
// No auth: anyone who can load the page can control the timer.

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

export class TimerRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.state = freshState();
    ctx.blockConcurrencyWhile(async () => {
      const saved = await ctx.storage.get('state');
      if (saved) this.state = saved;
    });
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected a WebSocket', { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.send(JSON.stringify({ type: 'state', state: this.state, now: Date.now(), cause: 'sync' }));
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

    const s = this.state;
    const elapsed = () => (s.phase === 'running' ? now - s.startedAt : s.elapsedMs);
    switch (msg.type) {
      case 'start':
        if (s.phase === 'running') return;
        s.startedAt = now - s.elapsedMs;
        s.phase = 'running';
        break;
      case 'pause':
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
        const e = Number(msg.elapsedMs);
        if (!Number.isFinite(e)) return;
        s.questionsAt = null;
        s.barStartMs = Math.min(0, e);
        if (s.phase === 'running') s.startedAt = now - e;
        else s.elapsedMs = e;
        break;
      }
      default:
        return;
    }

    await this.ctx.storage.put('state', this.state);
    this.broadcast({ type: 'state', state: this.state, now, cause: msg.type });
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
