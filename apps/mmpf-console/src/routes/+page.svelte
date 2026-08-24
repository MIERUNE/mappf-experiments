<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import SourceCard from "../components/SourceCard.svelte";
  import { AbashiriClient, ApiError } from "../lib/api";
  import { loadConsoleConfig, type ConsoleConfig } from "../lib/config";
  import { canReadInventory, canReadOperations } from "../lib/permissions";
  import { formatAge } from "../lib/status";
  import type {
    AuthCapabilities,
    Identity,
    NamespaceInventory,
    OperationalOverview,
    ResourceInventory,
    StyleInventoryItem,
    TilesetInventoryItem,
  } from "../lib/types";

  const PAGE_SIZE = 20;
  const CREDENTIAL_STORAGE_KEY = "mmpf.console.bearer";

  let config: ConsoleConfig | undefined;
  let client: AbashiriClient | undefined;
  let capabilities: AuthCapabilities | undefined;
  let tokenInput = "";
  let token: string | undefined;
  let identity: Identity | undefined;
  let overview: OperationalOverview | undefined;
  let namespaceInventory: NamespaceInventory | undefined;
  let inventory: ResourceInventory | undefined;
  let selectedNamespace = "";
  let filter = "";
  let stylePage = 1;
  let tilesetPage = 1;
  let inventoryLoading = false;
  let operationsAvailable = true;
  let inventoryAvailable = true;
  let loading = true;
  let refreshing = false;
  let error = "";
  let requestId = "";
  let timer: ReturnType<typeof setInterval> | undefined;
  let now = Date.now();

  onMount(async () => {
    try {
      config = await loadConsoleConfig();
      client = new AbashiriClient(config.apiBaseUrl);
      try {
        capabilities = await client.capabilities();
      } catch (cause) {
        if (!(cause instanceof ApiError && cause.status === 404)) throw cause;
      }
      const storedCredential = readStoredCredential();
      if (storedCredential) {
        await resumeBearerSession(storedCredential);
      } else if (capabilities?.methods.some((method) => method.type !== "bearer")) {
        await resumeSession();
      }
    } catch (cause) {
      setError(cause);
    } finally {
      loading = false;
    }
  });

  onDestroy(stopPolling);

  async function resumeSession() {
    if (!client) return;
    try {
      identity = await client.whoami();
      await loadAuthorizedViews();
    } catch {
      // A configured session method may not have an active browser session yet.
    }
  }

  async function resumeBearerSession(storedCredential: string) {
    if (!client) return;
    try {
      identity = await client.whoami(storedCredential);
      token = storedCredential;
      await loadAuthorizedViews();
    } catch {
      clearStoredCredential();
    }
  }

  async function signIn() {
    const candidate = tokenInput.trim();
    if (!client || candidate.length === 0) return;
    loading = true;
    clearError();
    try {
      const nextIdentity = await client.whoami(candidate);
      token = candidate;
      storeCredential(candidate);
      tokenInput = "";
      identity = nextIdentity;
      await loadAuthorizedViews();
    } catch (cause) {
      setError(cause);
    } finally {
      loading = false;
    }
  }

  function signOut() {
    stopPolling();
    clearStoredCredential();
    token = undefined;
    identity = undefined;
    overview = undefined;
    namespaceInventory = undefined;
    inventory = undefined;
    selectedNamespace = "";
    filter = "";
    tokenInput = "";
    clearError();
  }

  async function refresh() {
    if (!client || !identity || !canReadOperations(identity) || refreshing || document.hidden) return;
    refreshing = true;
    clearError();
    try {
      overview = await client.operations(token);
      operationsAvailable = true;
      now = Date.now();
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 404) {
        operationsAvailable = false;
        overview = undefined;
      } else {
        setError(cause);
      }
    } finally {
      refreshing = false;
    }
  }

  async function loadInventory() {
    if (!client || !identity || !canReadInventory(identity) || !selectedNamespace) return;
    inventoryLoading = true;
    inventory = undefined;
    try {
      inventory = await client.inventory(selectedNamespace, token);
      inventoryAvailable = true;
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 404) {
        inventoryAvailable = false;
        inventory = undefined;
      } else {
        setError(cause);
      }
    } finally {
      inventoryLoading = false;
    }
  }

  async function loadNamespaces() {
    if (!client || !identity || !canReadInventory(identity)) return;
    try {
      namespaceInventory = await client.namespaces(token);
      inventoryAvailable = true;
      if (!namespaceInventory.namespaces.includes(selectedNamespace)) {
        selectedNamespace = namespaceInventory.namespaces[0] ?? "";
      }
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 404) {
        inventoryAvailable = false;
        namespaceInventory = undefined;
        selectedNamespace = "";
      } else {
        setError(cause);
      }
    }
  }

  async function changeNamespace() {
    filter = "";
    stylePage = 1;
    tilesetPage = 1;
    await loadInventory();
  }

  async function refreshAll() {
    if (identity && canReadOperations(identity)) await refresh();
    if (identity && canReadInventory(identity)) {
      await loadNamespaces();
      await loadInventory();
    }
  }

  function startPolling() {
    stopPolling();
    if (identity && canReadOperations(identity)) {
      timer = setInterval(refresh, config?.pollIntervalMs ?? 5_000);
    }
  }

  async function loadAuthorizedViews() {
    if (!identity) return;
    if (canReadOperations(identity)) {
      await refresh();
    } else {
      operationsAvailable = false;
      overview = undefined;
    }
    if (canReadInventory(identity)) {
      await loadNamespaces();
      await loadInventory();
    } else {
      inventoryAvailable = false;
      inventory = undefined;
    }
    startPolling();
  }

  function stopPolling() {
    if (timer) clearInterval(timer);
    timer = undefined;
  }

  function clearError() {
    error = "";
    requestId = "";
  }

  function readStoredCredential(): string | undefined {
    try {
      return sessionStorage.getItem(CREDENTIAL_STORAGE_KEY) ?? undefined;
    } catch {
      return undefined;
    }
  }

  function storeCredential(credential: string) {
    try {
      sessionStorage.setItem(CREDENTIAL_STORAGE_KEY, credential);
    } catch {
      // Storage-disabled browsers retain the credential in memory only.
    }
  }

  function clearStoredCredential() {
    try {
      sessionStorage.removeItem(CREDENTIAL_STORAGE_KEY);
    } catch {
      // There is nothing else to clear when browser storage is unavailable.
    }
  }

  function setError(cause: unknown) {
    error = cause instanceof Error ? cause.message : "An unexpected error occurred";
    requestId = cause instanceof ApiError ? cause.requestId ?? "" : "";
  }

  function formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
  }

  function previewPath(resourceId: string): string {
    return resourceId.split("/").map(encodeURIComponent).join("/");
  }

  function stylePreviewUrl(style: StyleInventoryItem): string | undefined {
    return config?.stylePreviewBaseUrl
      ? `${config.stylePreviewBaseUrl}/styles/${previewPath(style.delivery_style_id)}/preview`
      : undefined;
  }

  function tilesetPreviewUrl(tileset: TilesetInventoryItem): string | undefined {
    return config?.tilesetPreviewBaseUrl
      ? `${config.tilesetPreviewBaseUrl}/tilesets/${previewPath(tileset.tileset_id)}/preview`
      : undefined;
  }

  // Older or temporarily unreachable Abashiri deployments may not expose
  // capability discovery yet. Keep the baseline bearer form usable so an
  // operator can retry without rebuilding the Console.
  $: bearerAvailable = capabilities?.methods.some((method) => method.type === "bearer") ?? true;
  $: normalizedFilter = filter.trim().toLocaleLowerCase();
  $: matchingStyles =
    inventory?.styles.filter((style) =>
      style.delivery_style_id.toLocaleLowerCase().includes(normalizedFilter),
    ) ?? [];
  $: matchingTilesets =
    inventory?.tilesets.filter((tileset) =>
      tileset.tileset_id.toLocaleLowerCase().includes(normalizedFilter),
    ) ?? [];
  $: stylePageCount = Math.max(1, Math.ceil(matchingStyles.length / PAGE_SIZE));
  $: tilesetPageCount = Math.max(1, Math.ceil(matchingTilesets.length / PAGE_SIZE));
  $: stylePage = Math.min(stylePage, stylePageCount);
  $: tilesetPage = Math.min(tilesetPage, tilesetPageCount);
  $: visibleStyles = matchingStyles.slice((stylePage - 1) * PAGE_SIZE, stylePage * PAGE_SIZE);
  $: visibleTilesets = matchingTilesets.slice(
    (tilesetPage - 1) * PAGE_SIZE,
    tilesetPage * PAGE_SIZE,
  );
</script>

<svelte:head><title>MMPF Console</title></svelte:head>

<div class="shell">
  <header class="topbar">
    <a class="brand" href="./" aria-label="MMPF Console home">
      <span class="brand-mark">M</span>
      <span><strong>MMPF</strong><small>Console</small></span>
    </a>
    {#if identity}
      <div class="operator">
        <span><small>Signed in as</small>{identity.actor.subject}</span>
        <button class="quiet" type="button" on:click={signOut}>Sign out</button>
      </div>
    {/if}
  </header>

  <main>
    {#if loading && !identity}
      <section class="center-card"><div class="spinner"></div><p>Connecting to Abashiri…</p></section>
    {:else if !identity}
      <section class="login-grid">
        <div class="intro">
          <p class="eyebrow">Management plane</p>
          <h1>See the platform.<br />Operate with confidence.</h1>
          <p>MMPF Console uses Abashiri as its only management boundary. Delivery remains independent.</p>
        </div>
        <form class="login-card" on:submit|preventDefault={signIn}>
          <p class="eyebrow">Console access</p>
          <h2>Connect to Abashiri</h2>
          {#if bearerAvailable}
            <label for="credential">Bearer credential</label>
            <input id="credential" bind:value={tokenInput} type="password" autocomplete="off" spellcheck="false" placeholder="Paste credential" />
            <p class="privacy">The credential stays in this tab’s session, survives reloads, and is cleared when the tab closes or you sign out.</p>
            <button class="primary" type="submit" disabled={tokenInput.trim().length === 0 || loading}>{loading ? "Connecting…" : "Continue"}</button>
          {:else}
            <p class="empty">No interactive authentication method is available on this Abashiri deployment.</p>
          {/if}
          {#if error}<div class="error" role="alert"><strong>{error}</strong>{#if requestId}<small>Request {requestId}</small>{/if}</div>{/if}
        </form>
      </section>
    {:else}
      <section class="page-heading">
        <div>
          <p class="eyebrow">{canReadOperations(identity) ? "Operations" : "Resources"}</p>
          <h1>{canReadOperations(identity) ? "Delivery overview" : "Your resources"}</h1>
          <p>{canReadOperations(identity) ? "Observed through Abashiri’s bounded operational adapters." : "Styles and tilesets granted to your namespaces."}</p>
        </div>
        <button class="quiet refresh" type="button" on:click={refreshAll} disabled={refreshing}>{refreshing ? "Refreshing…" : "Refresh"}</button>
      </section>

      <section class="identity-strip">
        <div><span>Actor</span><strong>{identity.actor.kind}:{identity.actor.subject}</strong></div>
        <div><span>Namespaces</span><strong>{identity.namespaces.length}</strong></div>
        <div><span>Granted actions</span><strong>{identity.actions.length}</strong></div>
        <div><span>Registry revision</span><strong>{identity.registry_revision}</strong></div>
      </section>

      {#if error}<div class="error page-error" role="alert"><strong>{error}</strong>{#if requestId}<small>Request {requestId}</small>{/if}</div>{/if}

      {#if canReadOperations(identity)}
        {#if !operationsAvailable}
          <section class="center-card">
            <p>Operational monitoring is not enabled on this Abashiri deployment.</p>
          </section>
        {:else if overview}
          <div class="overview-meta">
            <span class:complete={overview.complete} class="state"><i></i>{overview.complete ? "All sources current" : "Partial observation"}</span>
            <span>Updated {formatAge(overview.observed_at_unix_ms, now)}</span>
          </div>
          <section class="source-grid">
            {#each overview.sources as source (source.source_id)}
              <SourceCard {source} {now} />
            {/each}
          </section>
        {:else if !error}
          <section class="center-card"><div class="spinner"></div><p>Loading delivery state…</p></section>
        {/if}
      {/if}

      <section class="inventory-section">
        <header>
          <div>
            <p class="eyebrow">Resources</p>
            <h2>Resource inventory</h2>
          </div>
          {#if inventory}<span>{inventory.visibility === "all" ? "Operator view" : "Granted view"} · {inventory.namespace}</span>{/if}
        </header>
        {#if !canReadInventory(identity)}
          <div class="inventory-empty">This credential has no resource-read grants.</div>
        {:else if !inventoryAvailable}
          <div class="inventory-empty">Resource inventory is not enabled on this Abashiri deployment.</div>
        {:else if namespaceInventory && namespaceInventory.namespaces.length === 0}
          <div class="inventory-empty">No namespaces are available to this credential.</div>
        {:else if namespaceInventory}
          <div class="inventory-controls">
            <label>
              <span>Namespace</span>
              <select bind:value={selectedNamespace} on:change={changeNamespace}>
                {#each namespaceInventory.namespaces as namespace}
                  <option value={namespace}>{namespace}</option>
                {/each}
              </select>
            </label>
            <label>
              <span>Filter resources</span>
              <input
                bind:value={filter}
                on:input={() => {
                  stylePage = 1;
                  tilesetPage = 1;
                }}
                type="search"
                placeholder="Filter by ID"
              />
            </label>
          </div>
          {#if inventory}
          <div class="inventory-grid">
            <article class="inventory-card">
              <h3>Published styles <span>{matchingStyles.length} / {inventory.styles.length}</span></h3>
              {#if matchingStyles.length === 0}
                <p class="inventory-empty">No matching styles.</p>
              {:else}
                <div class="resource-list">
                  {#each visibleStyles as style (style.delivery_style_id)}
                    <div class="resource-row">
                      <div>
                        {#if stylePreviewUrl(style)}
                          <a href={stylePreviewUrl(style)} target="_blank" rel="noreferrer">{style.delivery_style_id}</a>
                        {:else}
                          <strong>{style.delivery_style_id}</strong>
                        {/if}
                        <small>{style.management ? `Managed as ${style.management.namespace ?? style.management.account_id}/${style.management.style_id}` : "Delivery only"}</small>
                      </div>
                      <code>{formatBytes(style.size_bytes)}</code>
                    </div>
                  {/each}
                </div>
                <footer class="pagination">
                  <button type="button" on:click={() => (stylePage -= 1)} disabled={stylePage === 1}>Previous</button>
                  <span>{stylePage} / {stylePageCount}</span>
                  <button type="button" on:click={() => (stylePage += 1)} disabled={stylePage === stylePageCount}>Next</button>
                </footer>
              {/if}
            </article>
            <article class="inventory-card">
              <h3>Tilesets <span>{matchingTilesets.length} / {inventory.tilesets.length}</span></h3>
              {#if matchingTilesets.length === 0}
                <p class="inventory-empty">No matching tilesets.</p>
              {:else}
                <div class="resource-list">
                  {#each visibleTilesets as tileset (tileset.tileset_id)}
                    <div class="resource-row">
                      <div>
                        {#if tilesetPreviewUrl(tileset)}
                          <a href={tilesetPreviewUrl(tileset)} target="_blank" rel="noreferrer">{tileset.tileset_id}</a>
                        {:else}
                          <strong>{tileset.tileset_id}</strong>
                        {/if}
                        <small>{tileset.management ? `Managed as ${tileset.management.namespace ?? tileset.management.account_id}/${tileset.management.tileset_id}` : "Delivery only"}</small>
                      </div>
                      <time datetime={tileset.updated_at}>{new Date(tileset.updated_at).toLocaleDateString()}</time>
                    </div>
                  {/each}
                </div>
                <footer class="pagination">
                  <button type="button" on:click={() => (tilesetPage -= 1)} disabled={tilesetPage === 1}>Previous</button>
                  <span>{tilesetPage} / {tilesetPageCount}</span>
                  <button type="button" on:click={() => (tilesetPage += 1)} disabled={tilesetPage === tilesetPageCount}>Next</button>
                </footer>
              {/if}
            </article>
          </div>
          {:else if inventoryLoading}
            <div class="inventory-empty">Loading {selectedNamespace} resources…</div>
          {/if}
        {:else}
          <div class="inventory-empty">Loading resource inventory…</div>
        {/if}
      </section>
    {/if}
  </main>
</div>
