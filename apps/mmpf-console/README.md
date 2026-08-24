# MMPF Console

MMPF Console is a standalone SvelteKit application for Abashiri. It reads management data only through Abashiri and never calls object storage or cluster-internal listeners directly. Optional preview links navigate the browser to separately configured Biei and Ishikari public origins.

The first slice supports bearer authentication in tab-scoped `sessionStorage`, shows the authenticated actor and grants, polls Abashiri's bounded operational overview, and browses one authorized resource namespace at a time. A reload in the same tab resumes the bearer session; closing the tab or signing out clears it. The namespace-scoped styles and tilesets can be filtered and paged in the browser. OIDC and trusted-proxy sessions can be added through the same authentication capability contract without changing the operations UI.

## Development

Run Abashiri on `127.0.0.1:8080`, then start the SvelteKit development server. Requests below `/api` are proxied to Abashiri with the prefix removed.

```sh
npm install
npm run dev
```

## Runtime configuration

The container reads the following environment variables at startup. The entrypoint validates them and writes the browser-readable configuration; credentials, query strings, and fragments are rejected, and polling is bounded to 2–60 seconds. Preview origins are optional.

```sh
MMPF_CONSOLE_API_BASE_URL=/api
MMPF_CONSOLE_POLL_INTERVAL_MS=5000
MMPF_CONSOLE_STYLE_PREVIEW_BASE_URL=https://biei.example.test
MMPF_CONSOLE_TILESET_PREVIEW_BASE_URL=https://ishikari.example.test
```

For the preferred same-origin deployment, route `/*` to the static Console and route `/api/*` to Abashiri with `/api` stripped. Management routes must bypass delivery caches. A separate-origin deployment additionally needs Abashiri CORS support with an exact Console origin allowlist; wildcard credentialed CORS is not acceptable.

The included container serves the static application on port 8080 with a restrictive content security policy. Plain static hosting can still replace `console-config.json` directly, but Kubernetes and container deployments need only set the environment variables above.

SvelteKit emits the UI base path into its static assets at build time. The default is the origin root; a deployment below a prefix builds with `--build-arg MMPF_CONSOLE_BASE_PATH=/console` and configures its ingress to strip that prefix before forwarding to the container.
