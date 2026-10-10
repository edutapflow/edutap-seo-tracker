// Entry point for the Cloudflare Worker.
// /api/... requests go to the small functions in web/functions/api.
// Everything else (the website pages) is served from the "web" folder.
import { onRequest as middleware } from "../web/functions/api/_middleware.js";
import * as login from "../web/functions/api/login.js";
import * as logout from "../web/functions/api/logout.js";
import * as data from "../web/functions/api/data.js";
import * as history from "../web/functions/api/history.js";
import * as credits from "../web/functions/api/credits.js";
import * as runs from "../web/functions/api/runs.js";

const ROUTES = {
  "/api/login": login, "/api/logout": logout, "/api/data": data,
  "/api/history": history, "/api/credits": credits, "/api/runs": runs,
};

function notAllowed() {
  return new Response(JSON.stringify({ error: "Not allowed" }), {
    status: 405, headers: { "Content-Type": "application/json" },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const mod = ROUTES[url.pathname];
    if (!mod) {
      if (url.pathname.startsWith("/api/")) {
        return new Response(JSON.stringify({ error: "Not found" }), {
          status: 404, headers: { "Content-Type": "application/json" },
        });
      }
      return env.ASSETS.fetch(request);
    }
    const method = request.method.toUpperCase();
    const handler = method === "GET" ? mod.onRequestGet : method === "POST" ? mod.onRequestPost : null;
    const context = { request, env, data: {}, waitUntil: (p) => ctx.waitUntil(p) };
    context.next = async () => (handler ? handler(context) : notAllowed());
    return middleware(context);
  },
};
