<script lang="ts">
  import { formatAge, summarizeStatus } from "../lib/status";
  import type { OperationalSource } from "../lib/types";

  export let source: OperationalSource;
  export let now: number;

  $: snapshot = source.state === "unavailable" ? undefined : source.snapshot;
  $: summary = snapshot ? summarizeStatus(snapshot) : {};
  $: stateLabel = source.state === "fresh" ? "Healthy" : source.state === "stale" ? "Stale" : "Unavailable";
</script>

<article class:unavailable={source.state === "unavailable"} class="source-card">
  <header>
    <div>
      <p class="eyebrow">{snapshot?.service ?? source.source_id}</p>
      <h2>{source.source_id}</h2>
    </div>
    <span class:healthy={source.state === "fresh"} class:stale={source.state === "stale"} class="state">
      <i></i>{stateLabel}
    </span>
  </header>

  {#if snapshot}
    <div class="facts">
      {#if summary.mode}<div><span>Mode</span><strong>{summary.mode}</strong></div>{/if}
      {#if summary.liveMembers !== undefined}<div><span>Live nodes</span><strong>{summary.liveMembers}</strong></div>{/if}
      {#if summary.availableSlots !== undefined}<div><span>Render slots</span><strong>{summary.availableSlots}/{summary.totalSlots}</strong></div>{/if}
      {#if summary.runningWork !== undefined}<div><span>CPU work</span><strong>{summary.runningWork}/{summary.concurrency}</strong></div>{/if}
    </div>
    <div class="footnote">
      <span>{snapshot.observer_node_id}</span>
      <time datetime={new Date(snapshot.observed_at_unix_ms).toISOString()}>{formatAge(snapshot.observed_at_unix_ms, now)}</time>
    </div>
    {#if summary.draining}<p class="warning">This service is draining.</p>{/if}
    {#if summary.ready === false}<p class="warning">This service is not ready.</p>{/if}
    <details>
      <summary>Raw service status</summary>
      <pre>{JSON.stringify(snapshot.status, null, 2)}</pre>
    </details>
  {:else}
    <p class="empty">Abashiri could not reach this service and has no recent snapshot.</p>
  {/if}
</article>
