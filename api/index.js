export const config = { runtime: "edge" };

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function handler(req) {
  if (!TARGET_BASE) {
    return new Response("Misconfigured: TARGET_DOMAIN is not set", { status: 500 });
  }

  try {
    const pathStart = req.url.indexOf("/", 8);
    const targetUrl =
      pathStart === -1 ? TARGET_BASE + "/" : TARGET_BASE + req.url.slice(pathStart);

    const out = new Headers();
    let clientIp = null;
    for (const [k, v] of req.headers) {
      if (STRIP_HEADERS.has(k)) continue;
      if (k.startsWith("x-vercel-")) continue;
      if (k === "x-real-ip") {
        clientIp = v;
        continue;
      }
      if (k === "x-forwarded-for") {
        if (!clientIp) clientIp = v;
        continue;
      }
      out.set(k, v);
    }
    if (clientIp) out.set("x-forwarded-for", clientIp);

    const method = req.method;
    const hasBody = method !== "GET" && method !== "HEAD";

    return await fetch(targetUrl, {
      method,
      headers: out,
      body: hasBody ? req.body : undefined,
      duplex: "half",
      redirect: "manual",
    });
  } catch (err) {
    console.error("relay error:", err);
    return new Response("Bad Gateway: Tunnel Failed", { status: 502 });
  }
}  "proxy-connection",
  "keep-alive",
  "via",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
  "x-forwarded-for",
  "x-real-ip",
]);

let inFlight = 0;

export default async function handler(req, res) {
  const requestId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = Date.now();
  let slotAcquired = false;

  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }
  if (!RELAY_PATH) {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_PATH is not set");
  }
  if (RELAY_PATH === "/") {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_PATH cannot be '/'");
  }
  if (RELAY_KEY && RELAY_KEY.length < 16) {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_KEY is too short");
  }

  try {
    const host = req.headers.host || "localhost";
    const url = new URL(req.url || "/", `https://${host}`);

    if (!isAllowedRelayPath(url.pathname)) {
      res.statusCode = 404;
      return res.end("Not Found");
    }

    if (!ALLOWED_METHODS.has(req.method)) {
      res.statusCode = 405;
      res.setHeader("allow", "GET, HEAD, POST");
      return res.end("Method Not Allowed");
    }

    if (RELAY_KEY) {
      const token = (req.headers["x-relay-key"] || "").toString();
      if (token !== RELAY_KEY) {
        res.statusCode = 403;
        return res.end("Forbidden");
      }
    }
    if (!tryAcquireSlot()) {
      res.statusCode = 503;
      res.setHeader("retry-after", "1");
      return res.end("Server Busy: Too Many Inflight Requests");
    }
    slotAcquired = true;

    const targetUrl = `${TARGET_BASE}${url.pathname}${url.search || ""}`;

    const headers = {};
    const clientIp = toHeaderValue(req.headers["x-real-ip"] || req.headers["x-forwarded-for"]);
    for (const key of Object.keys(req.headers)) {
      const lower = key.toLowerCase();
      const value = req.headers[key];
      if (STRIP_HEADERS.has(lower)) continue;
      if (lower.startsWith(PLATFORM_HEADER_PREFIX)) continue;
      if (lower === "x-relay-key") continue;
      if (!shouldForwardHeader(lower)) continue;
      const normalizedValue = toHeaderValue(value);
      if (normalizedValue) headers[lower] = normalizedValue;
    }
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const abortCtrl = new AbortController();
    const timeoutRef = setTimeout(() => abortCtrl.abort("upstream_timeout"), UPSTREAM_TIMEOUT_MS);

    try {
      const fetchOpts = {
        method: req.method,
        headers,
        redirect: "manual",
        signal: abortCtrl.signal,
      };

      if (hasBody) {
        const uploadNodeStream = MAX_UP_BPS > 0
          ? req.pipe(createThrottleTransform(MAX_UP_BPS))
          : req;
        fetchOpts.body = Readable.toWeb(uploadNodeStream);
        fetchOpts.duplex = "half";
      }

      const upstream = await fetch(targetUrl, fetchOpts);

      res.statusCode = upstream.status;
      for (const [headerName, headerValue] of upstream.headers) {
        const k = headerName.toLowerCase();
        if (k === "transfer-encoding" || k === "connection") continue;
        try {
          res.setHeader(headerName, headerValue);
        } catch {}
      }

      if (!upstream.body) {
        res.end();
      } else {
        const upstreamNode = Readable.fromWeb(upstream.body);
        const downloadStream = MAX_DOWN_BPS > 0
          ? upstreamNode.pipe(createThrottleTransform(MAX_DOWN_BPS))
          : upstreamNode;
        await pipeline(downloadStream, res);
      }

      const durationMs = Date.now() - startedAt;
      console.info("relay ok", {
        requestId,
        path: url.pathname,
        method: req.method,
        status: upstream.status,
        durationMs,
      });
    } finally {
      clearTimeout(timeoutRef);
    }
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    if (err?.name === "AbortError") {
      console.error("relay timeout", {
        requestId,
        method: req.method,
        durationMs,
        timeoutMs: UPSTREAM_TIMEOUT_MS,
      });
      if (!res.headersSent) {
        res.statusCode = 504;
        return res.end("Gateway Timeout: Upstream Timeout");
      }
      return;
    }

    console.error("relay error", {
      requestId,
      method: req.method,
      durationMs,
      error: String(err),
    });
    if (!res.headersSent) {
      res.statusCode = 502;
      return res.end("Bad Gateway: Tunnel Failed");
    }
  } finally {
    if (slotAcquired) releaseSlot();
  }
}

function shouldForwardHeader(headerName) {
  if (FORWARD_HEADER_EXACT.has(headerName)) return true;
  for (const prefix of FORWARD_HEADER_PREFIXES) {
    if (headerName.startsWith(prefix)) return true;
  }
  return false;
}

function isAllowedRelayPath(pathname) {
  return pathname === RELAY_PATH || pathname.startsWith(`${RELAY_PATH}/`);
}

function normalizeRelayPath(rawPath) {
  if (!rawPath) return "";
  const path = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;
  if (path.length > 1 && path.endsWith("/")) return path.slice(0, -1);
  return path;
}

function parsePositiveInt(rawValue, fallbackValue, minValue) {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) return fallbackValue;
  if (value < minValue) return fallbackValue;
  return Math.trunc(value);
}

function parseNonNegativeInt(rawValue, fallbackValue) {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) return fallbackValue;
  if (value < 0) return fallbackValue;
  return Math.trunc(value);
}

function toHeaderValue(value) {
  if (!value) return "";
  return Array.isArray(value) ? value.join(", ") : String(value);
}

function tryAcquireSlot() {
  if (inFlight >= MAX_INFLIGHT) return false;
  inFlight += 1;
  return true;
}

function releaseSlot() {
  inFlight = Math.max(0, inFlight - 1);
}

function createThrottleTransform(bytesPerSecond) {
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond <= 0) {
    return new PassThrough();
  }

  const burstCap = Math.max(bytesPerSecond, 262144);
  let tokens = burstCap;
  let lastRefill = Date.now();

  function refillTokens() {
    const now = Date.now();
    const elapsedMs = now - lastRefill;
    if (elapsedMs <= 0) return;

    const refillAmount = (elapsedMs * bytesPerSecond) / 1000;
    tokens = Math.min(burstCap, tokens + refillAmount);
    lastRefill = now;
  }

  return new Transform({
    transform(chunk, _encoding, callback) {
      if (!chunk || chunk.length === 0) {
        callback();
        return;
      }

      let offset = 0;

      const pump = () => {
        refillTokens();

        if (tokens < 1) {
          setTimeout(pump, 5);
          return;
        }

        const writableSize = Math.min(chunk.length - offset, Math.max(1, Math.floor(tokens)));
        const piece = chunk.subarray(offset, offset + writableSize);
        tokens -= writableSize;
        offset += writableSize;

        this.push(piece);

        if (offset >= chunk.length) {
          callback();
          return;
        }

        setImmediate(pump);
      };

      pump();
    },
  });
}
  "proxy-connection",
  "keep-alive",
  "via",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
  "x-forwarded-for",
  "x-real-ip",
]);

let inFlight = 0;

export default async function handler(req, res) {
  const requestId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = Date.now();

  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }
  if (!RELAY_PATH) {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_PATH is not set");
  }
  if (RELAY_PATH === "/") {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_PATH cannot be '/'");
  }
  if (RELAY_KEY && RELAY_KEY.length < 16) {
    res.statusCode = 500;
    return res.end("Misconfigured: RELAY_KEY is too short");
  }

  if (!tryAcquireSlot()) {
    res.statusCode = 503;
    res.setHeader("retry-after", "1");
    return res.end("Server Busy: Too Many Inflight Requests");
  }

  try {
    const host = req.headers.host || "localhost";
    const url = new URL(req.url || "/", `https://${host}`);

    if (!isAllowedRelayPath(url.pathname)) {
      res.statusCode = 404;
      return res.end("Not Found");
    }

    if (!ALLOWED_METHODS.has(req.method)) {
      res.statusCode = 405;
      res.setHeader("allow", "GET, HEAD, POST");
      return res.end("Method Not Allowed");
    }

    if (RELAY_KEY) {
      const token = (req.headers["x-relay-key"] || "").toString();
      if (token !== RELAY_KEY) {
        res.statusCode = 403;
        return res.end("Forbidden");
      }
    }

    const targetUrl = `${TARGET_BASE}${url.pathname}${url.search || ""}`;

    const headers = {};
    const clientIp = toHeaderValue(req.headers["x-real-ip"] || req.headers["x-forwarded-for"]);
    for (const key of Object.keys(req.headers)) {
      const lower = key.toLowerCase();
      const value = req.headers[key];
      if (STRIP_HEADERS.has(lower)) continue;
      if (lower.startsWith(PLATFORM_HEADER_PREFIX)) continue;
      if (lower === "x-relay-key") continue;
      if (!shouldForwardHeader(lower)) continue;
      const normalizedValue = toHeaderValue(value);
      if (normalizedValue) headers[lower] = normalizedValue;
    }
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const abortCtrl = new AbortController();
    const timeoutRef = setTimeout(() => abortCtrl.abort("upstream_timeout"), UPSTREAM_TIMEOUT_MS);

    try {
      const fetchOpts = {
        method: req.method,
        headers,
        redirect: "manual",
        signal: abortCtrl.signal,
      };

      if (hasBody) {
        const uploadNodeStream = MAX_UP_BPS > 0
          ? req.pipe(createThrottleTransform(MAX_UP_BPS))
          : req;
        fetchOpts.body = Readable.toWeb(uploadNodeStream);
        fetchOpts.duplex = "half";
      }

      const upstream = await fetch(targetUrl, fetchOpts);

      res.statusCode = upstream.status;
      for (const [headerName, headerValue] of upstream.headers) {
        const k = headerName.toLowerCase();
        if (k === "transfer-encoding" || k === "connection") continue;
        try {
          res.setHeader(headerName, headerValue);
        } catch {}
      }

      if (!upstream.body) {
        res.end();
      } else {
        const upstreamNode = Readable.fromWeb(upstream.body);
        const downloadStream = MAX_DOWN_BPS > 0
          ? upstreamNode.pipe(createThrottleTransform(MAX_DOWN_BPS))
          : upstreamNode;
        await pipeline(downloadStream, res);
      }

      const durationMs = Date.now() - startedAt;
      console.info("relay ok", {
        requestId,
        path: url.pathname,
        method: req.method,
        status: upstream.status,
        durationMs,
      });
    } finally {
      clearTimeout(timeoutRef);
    }
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    if (err?.name === "AbortError") {
      console.error("relay timeout", {
        requestId,
        method: req.method,
        durationMs,
        timeoutMs: UPSTREAM_TIMEOUT_MS,
      });
      if (!res.headersSent) {
        res.statusCode = 504;
        return res.end("Gateway Timeout: Upstream Timeout");
      }
      return;
    }

    console.error("relay error", {
      requestId,
      method: req.method,
      durationMs,
      error: String(err),
    });
    if (!res.headersSent) {
      res.statusCode = 502;
      return res.end("Bad Gateway: Tunnel Failed");
    }
  } finally {
    releaseSlot();
  }
}

function shouldForwardHeader(headerName) {
  if (FORWARD_HEADER_EXACT.has(headerName)) return true;
  for (const prefix of FORWARD_HEADER_PREFIXES) {
    if (headerName.startsWith(prefix)) return true;
  }
  return false;
}

function isAllowedRelayPath(pathname) {
  return pathname === RELAY_PATH || pathname.startsWith(`${RELAY_PATH}/`);
}

function normalizeRelayPath(rawPath) {
  if (!rawPath) return "";
  const path = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;
  if (path.length > 1 && path.endsWith("/")) return path.slice(0, -1);
  return path;
}

function parsePositiveInt(rawValue, fallbackValue, minValue) {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) return fallbackValue;
  if (value < minValue) return fallbackValue;
  return Math.trunc(value);
}

function parseNonNegativeInt(rawValue, fallbackValue) {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) return fallbackValue;
  if (value < 0) return fallbackValue;
  return Math.trunc(value);
}

function toHeaderValue(value) {
  if (!value) return "";
  return Array.isArray(value) ? value.join(", ") : String(value);
}

function tryAcquireSlot() {
  if (inFlight >= MAX_INFLIGHT) return false;
  inFlight += 1;
  return true;
}

function releaseSlot() {
  inFlight = Math.max(0, inFlight - 1);
}

function createThrottleTransform(bytesPerSecond) {
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond <= 0) {
    return new PassThrough();
  }

  const burstCap = Math.max(bytesPerSecond, 262144);
  let tokens = burstCap;
  let lastRefill = Date.now();

  function refillTokens() {
    const now = Date.now();
    const elapsedMs = now - lastRefill;
    if (elapsedMs <= 0) return;

    const refillAmount = (elapsedMs * bytesPerSecond) / 1000;
    tokens = Math.min(burstCap, tokens + refillAmount);
    lastRefill = now;
  }

  return new Transform({
    transform(chunk, _encoding, callback) {
      if (!chunk || chunk.length === 0) {
        callback();
        return;
      }

      let offset = 0;

      const pump = () => {
        refillTokens();

        if (tokens < 1) {
          setTimeout(pump, 5);
          return;
        }

        const writableSize = Math.min(chunk.length - offset, Math.max(1, Math.floor(tokens)));
        const piece = chunk.subarray(offset, offset + writableSize);
        tokens -= writableSize;
        offset += writableSize;

        this.push(piece);

        if (offset >= chunk.length) {
          callback();
          return;
        }

        setImmediate(pump);
      };

      pump();
    },
  });
}
