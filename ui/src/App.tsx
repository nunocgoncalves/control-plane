import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRevision, downloadArtifact, loadDashboard, loadDetail, Period, respondToBlocker, saveFeedback, subscribe, uploadArtifact } from "./api";
import { localized, stateLabel, t } from "./i18n";
import type { Blocker, DetailData, JSONSchema, Locale, TimelineEvent, WorkArtifact, WorkItem, WorkState } from "./types";
import "./styles.css";

const states: WorkState[] = ["todo", "in_progress", "blocked", "done"];
const categoryKeys = ["incorrect_classification", "missing_information", "wrong_action", "poor_output"] as const;

function startOfDay(date = new Date()): Date { const d = new Date(date); d.setHours(0, 0, 0, 0); return d; }
function endOfDay(date = new Date()): Date { const d = startOfDay(date); d.setDate(d.getDate() + 1); return d; }
function presetPeriod(preset: string): Period {
  const to = endOfDay(); const from = startOfDay();
  if (preset === "week") { const day = (from.getDay() + 6) % 7; from.setDate(from.getDate() - day); }
  if (preset === "month") from.setDate(1);
  return { from, to };
}
function money(amount: string | number, currency = "EUR", locale: Locale = "en"): string {
  return new Intl.NumberFormat(locale === "pt" ? "pt-PT" : "en-GB", { style: "currency", currency, maximumFractionDigits: 2 }).format(Number(amount));
}
function when(value: string | undefined, locale: Locale): string {
  if (!value) return "";
  return new Intl.DateTimeFormat(locale === "pt" ? "pt-PT" : "en-GB", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}
function sourceLabel(kind: string): string {
  const labels: Record<string, string> = { outlook: "Outlook", graph_email: "Outlook", email: "Email", api: "API", schedule: "Scheduled", scheduled: "Scheduled", artifact: "Artifact" };
  return labels[kind] || kind.replaceAll("_", " ");
}
function initials(name: string): string { return name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "I"; }
function safeObject(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }

function Mark() {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h5l3 4M4 12h6M4 19h5l3-4M12 9l4 3-4 3"/><circle cx="18" cy="12" r="2.4"/></svg>;
}
function SearchIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>; }
function CloseIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"/></svg>; }

interface ConnectionProps { locale: Locale; setLocale: (locale: Locale) => void; onConnect: (token: string) => Promise<void>; loading: boolean; error: string }
function Connection({ locale, setLocale, onConnect, loading, error }: ConnectionProps) {
  const c = t(locale); const [value, setValue] = useState("");
  return <main className="connection-shell">
    <div className="language-switch" aria-label="Language"><button className={locale === "en" ? "active" : ""} onClick={() => setLocale("en")}>EN</button><button className={locale === "pt" ? "active" : ""} onClick={() => setLocale("pt")}>PT</button></div>
    <form className="connection-card" onSubmit={(event) => { event.preventDefault(); void onConnect(value.trim()); }}>
      <div className="brand-mark"><Mark /></div><span className="eyebrow">Iterabase</span><h1>{c.connectTitle}</h1><p>{c.connectBody}</p>
      <label htmlFor="api-key">{c.apiKey}</label><input id="api-key" type="password" autoComplete="off" value={value} onChange={(event) => setValue(event.target.value)} required />
      {error && <div className="form-error" role="alert">{error}</div>}
      <button className="button primary" disabled={loading || !value.trim()}>{loading ? c.connecting : c.connect}</button>
    </form>
  </main>;
}

interface HeaderProps { locale: Locale; setLocale: (locale: Locale) => void; search: string; setSearch: (value: string) => void; items: WorkItem[]; disconnect: () => void }
function Header({ locale, setLocale, search, setSearch, items, disconnect }: HeaderProps) {
  const c = t(locale); const active = items.filter((item) => item.state === "in_progress").length; const blocked = items.filter((item) => item.state === "blocked").length;
  return <header className="island">
    <div className="brand-mark small"><Mark /></div><span className="context">{c.dashboard}</span><span className="divider" />
    <label className="global-search"><SearchIcon /><span className="sr-only">{c.search}</span><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={c.search} /></label>
    <span className="divider"/><span className="live-count"><i />{active}</span><span className={`blocked-count ${blocked ? "has-blocked" : ""}`}>{blocked}</span>
    <div className="language-switch compact"><button className={locale === "en" ? "active" : ""} onClick={() => setLocale("en")}>EN</button><button className={locale === "pt" ? "active" : ""} onClick={() => setLocale("pt")}>PT</button></div>
    <button className="icon-button sign-out" title={c.disconnect} aria-label={c.disconnect} onClick={disconnect}>↪</button>
  </header>;
}

function ValueHero({ data, locale, onExplain }: { data: ReturnType<typeof useDashboardData>["data"]; locale: Locale; onExplain: () => void }) {
  const c = t(locale); if (!data) return null;
  const total = data.summary.value.totals[0];
  return <section className="summary-grid">
    <article className="card hero">
      <div><span className="eyebrow">{c.hero}</span><button className="estimate-badge" onClick={onExplain}>ⓘ {c.estimated}</button></div>
      <strong>{data.summary.value.configured && total ? money(total.amount, total.currency, locale) : "—"}</strong>
      <p>{data.summary.value.configured ? c.valueHelp : c.notConfigured}</p>
      <div className="status-counts">{states.map((state) => <div key={state}><span><i className={`dot ${state}`} />{stateLabel(state, locale)}</span><b>{data.summary.counts[state] || 0}</b></div>)}</div>
    </article>
    <article className="card trend-card"><div className="section-heading"><span className="eyebrow">{c.trend}</span></div><Trend data={data.summary.trend} locale={locale}/></article>
  </section>;
}
function Trend({ data, locale }: { data: Array<{ date: string; amount: string; currency: string }>; locale: Locale }) {
  const c = t(locale); if (!data.length) return <div className="empty-chart">{c.noTrend}</div>;
  const values = data.map((point) => Math.abs(Number(point.amount))); const max = Math.max(...values, 1);
  return <div className="trend" role="img" aria-label={c.trend}>{data.map((point, index) => <div className="trend-point" key={`${point.date}-${point.currency}`} title={`${point.date}: ${money(point.amount, point.currency, locale)}`}><span>{money(point.amount, point.currency, locale)}</span><i style={{ height: `${Math.max(5, values[index] / max * 112)}px` }} /><small>{new Intl.DateTimeFormat(locale === "pt" ? "pt-PT" : "en-GB", { month: "short", day: "numeric" }).format(new Date(`${point.date}T12:00:00`))}</small></div>)}</div>;
}

function WorkCard({ item, locale, open }: { item: WorkItem; locale: Locale; open: () => void }) {
  return <button className={`work-card card edge-${item.state}`} onClick={open}>
    <div className="card-meta"><span>{item.presentation.workflowTitle}</span><span>{sourceLabel(item.source.kind)}</span></div>
    <strong>{item.title}</strong>
    {item.state === "in_progress" && item.currentStep && <div className="step-chip"><i />{localized(item.currentStep.label, locale)}</div>}
    {item.state === "blocked" && item.blocker && <div className="blocker-chip"><span>△</span><div><small>{item.blocker.kind.replaceAll("_", " ")}</small>{localized(item.blocker.title, locale)}</div></div>}
    <div className="assignee"><span className="avatar">{initials(item.presentation.personaName)}</span><span>{item.presentation.personaName}</span><time>{when(item.finishedAt || item.startedAt || item.createdAt, locale)}</time></div>
    {item.estimatedValue && <div className={`item-value ${item.valueDisputed ? "disputed" : ""}`}>{money(item.estimatedValue, item.valueCurrency, locale)} <small>{item.valueDisputed ? t(locale).disputed : t(locale).estimated}</small></div>}
  </button>;
}

function Board({ items, locale, open }: { items: WorkItem[]; locale: Locale; open: (item: WorkItem) => void }) {
  const c = t(locale); const failed = items.filter((item) => item.state === "failed");
  return <>
    {failed.length > 0 && <section className="failed-strip" aria-labelledby="failed-title"><div><span className="failure-icon">!</span><div><h2 id="failed-title">{c.failed} <b>{failed.length}</b></h2><p>{c.failedHelp}</p></div></div><div className="failed-items">{failed.map((item) => <button key={item.id} onClick={() => open(item)}><span>{item.title}</span><small>{c.open} →</small></button>)}</div></section>}
    <section className="board" aria-label={c.board}>{states.map((state) => { const column = items.filter((item) => item.state === state); return <div className="column" key={state}><div className="column-title"><i className={`dot ${state}`} /><h2>{stateLabel(state, locale)}</h2><span>{column.length}</span></div><div className="column-items">{column.map((item) => <WorkCard key={item.id} item={item} locale={locale} open={() => open(item)}/>)}{!column.length && <div className="empty-column">{c.empty}</div>}</div></div>; })}</section>
  </>;
}

function useDashboardData(token: string | null, period: Period, search: string) {
  const [data, setData] = useState<{ summary: Awaited<ReturnType<typeof loadDashboard>>["summary"]; items: WorkItem[] } | null>(null);
  const [error, setError] = useState(""); const [loading, setLoading] = useState(false); const timer = useRef<number | undefined>(undefined);
  const refresh = useCallback(async (quiet = false) => { if (!token) return; if (!quiet) setLoading(true); try { setData(await loadDashboard(token, period, search)); setError(""); } catch (err) { setError(err instanceof Error ? err.message : "Error"); throw err; } finally { setLoading(false); } }, [token, period.from.getTime(), period.to.getTime(), search]);
  useEffect(() => { if (!token) { setData(null); return; } const id = window.setTimeout(() => { void refresh().catch(() => undefined); }, 180); return () => clearTimeout(id); }, [refresh, token]);
  useEffect(() => token ? subscribe(token, () => { clearTimeout(timer.current); timer.current = window.setTimeout(() => { void refresh(true).catch(() => undefined); }, 200); }) : undefined, [token, refresh]);
  return { data, error, loading, refresh };
}

function DetailPanel({ token, item, locale, close, changed }: { token: string; item: WorkItem; locale: Locale; close: () => void; changed: () => void }) {
  const c = t(locale); const panelRef = useRef<HTMLElement>(null); const [detail, setDetail] = useState<DetailData | null>(null); const [error, setError] = useState("");
  const [notice, setNotice] = useState(""); const [feedbackOpen, setFeedbackOpen] = useState(false); const [category, setCategory] = useState<string>(categoryKeys[0]);
  const [explanation, setExplanation] = useState(""); const [retryOpen, setRetryOpen] = useState(false); const [guidance, setGuidance] = useState(""); const [confirmed, setConfirmed] = useState<string[]>([]); const [busy, setBusy] = useState(false);
  const refresh = useCallback(async () => { try { setDetail(await loadDetail(token, item)); setError(""); } catch (err) { setError(err instanceof Error ? err.message : "Error"); } }, [token, item.id]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    panelRef.current?.focus();
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") close(); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [close]);
  const outcomeNode = detail?.nodes.filter((node) => node.summary).at(-1); const output = outcomeNode?.output;
  const resultArtifacts = detail?.artifacts.filter((artifact) => artifact.role === "output" || artifact.role === "evidence") || [];
  async function saveOnly() { if (!detail) return; setBusy(true); try { await saveFeedback(token, detail.item, category, explanation); setNotice(c.feedbackSaved); setFeedbackOpen(false); await refresh(); changed(); } catch (err) { setError(err instanceof Error ? err.message : "Error"); } finally { setBusy(false); } }
  async function startRetry() { if (!detail || !guidance.trim()) return; setBusy(true); try { let feedback = detail.feedback.find((entry) => entry.attemptId === detail.item.currentAttemptId && !entry.revisedAttemptId); if (!feedback) feedback = await saveFeedback(token, detail.item, category, explanation); await createRevision(token, detail.item, feedback.id, guidance.trim(), detail.consequences); setNotice(c.revisionStarted); setRetryOpen(false); setFeedbackOpen(false); await refresh(); changed(); } catch (err) { setError(err instanceof Error ? err.message : "Error"); } finally { setBusy(false); } }
  return <div className="overlay" onMouseDown={(event) => { if (event.target === event.currentTarget) close(); }}><aside ref={panelRef} tabIndex={-1} className="detail-panel" role="dialog" aria-modal="true" aria-labelledby="detail-title">
    <header><div><span className={`status-badge ${detail?.item.state || item.state}`}>{stateLabel(detail?.item.state || item.state, locale)}</span><span className="workflow-label">{item.presentation.workflowTitle}</span><h2 id="detail-title">{item.title}</h2><div className="panel-persona"><span className="avatar">{initials(item.presentation.personaName)}</span>{item.presentation.personaName}<span>·</span>{sourceLabel(item.source.kind)}</div></div><button className="icon-button" onClick={close} aria-label={c.close}><CloseIcon /></button></header>
    <div className="panel-body">{!detail && !error && <div className="loading">{c.loading}</div>}{error && <div className="form-error" role="alert">{error}</div>}{notice && <div className="notice" role="status">✓ {notice}</div>}
      {detail && <>
        {detail.item.state === "failed" && <Section title={c.failed}><PlainValue value={detail.item.failureSummary} empty={c.noDetails}/></Section>}
        {detail.blocker && <BlockerForm token={token} blocker={detail.blocker} locale={locale} onDone={async () => { setNotice(c.responseSent); await refresh(); changed(); }}/>}
        {detail.item.state === "done" && <Section title={c.outcome}><h3>{outcomeNode?.summary || (locale === "pt" ? "Trabalho concluído" : "Work completed")}</h3><PlainValue value={output} empty={c.noDetails}/></Section>}
        {resultArtifacts.length > 0 && <Section title={c.result}><ArtifactList token={token} artifacts={resultArtifacts} locale={locale}/></Section>}
        <Section title={c.source}><div className="source-card"><div><strong>{detail.item.source.title}</strong>{detail.item.source.subtitle && <span>{detail.item.source.subtitle}</span>}</div>{detail.item.source.originalUrl && <a href={detail.item.source.originalUrl} target="_blank" rel="noreferrer">{c.openOriginal} ↗</a>}</div></Section>
        {!!detail.item.source.evidence?.length && <Section title={c.evidence}><div className="evidence-grid">{detail.item.source.evidence.map((field, index) => <div key={index}><small>{localized(field.label, locale)}</small><strong>{field.value}</strong></div>)}</div></Section>}
        {detail.item.estimatedValue && <Section title={c.value}><div className={`panel-value ${detail.item.valueDisputed ? "disputed" : ""}`}><strong>{money(detail.item.estimatedValue, detail.item.valueCurrency, locale)}</strong><span>{detail.item.valueDisputed ? c.disputed : c.estimated}</span></div></Section>}
        <Section title={c.timeline}><Timeline events={detail.timeline} nodes={detail.nodes} locale={locale}/></Section>
        {detail.attempts.length > 1 && <Section title={c.attempts}><div className="attempt-list">{detail.attempts.map((attempt, index) => <div key={attempt.id}><span>{index === 0 ? c.original : c.revised}</span><strong>{c.attempt} {attempt.number}</strong><small>{when(attempt.createdAt, locale)}</small>{attempt.actionableGuidance && <p>{attempt.actionableGuidance}</p>}</div>)}</div></Section>}
      </>}
    </div>
    {detail?.item.state === "done" && <footer className="feedback-footer">{!feedbackOpen ? <button className="button secondary" onClick={() => setFeedbackOpen(true)}>⚑ {c.feedback}</button> : <div className="feedback-form"><div className="footer-title"><strong>{c.whatWrong}</strong><button className="icon-button" aria-label={c.close} onClick={() => setFeedbackOpen(false)}><CloseIcon /></button></div><div className="categories">{categoryKeys.map((key) => <button className={category === key ? "selected" : ""} onClick={() => setCategory(key)} key={key}>{c[key]}</button>)}</div><textarea value={explanation} onChange={(event) => setExplanation(event.target.value)} placeholder={c.explanation}/><div className="footer-actions"><button className="button secondary" disabled={busy} onClick={() => void saveOnly()}>{c.saveFeedback}</button><button className="button primary" disabled={busy} onClick={() => setRetryOpen(true)}>{c.tryAgain}</button></div></div>}</footer>}
    {retryOpen && detail && <div className="modal-layer"><div className="modal" role="dialog" aria-modal="true"><h2>{c.tryAgain}</h2><p>{c.guidance}</p><textarea autoFocus value={guidance} onChange={(event) => setGuidance(event.target.value)} placeholder={c.guidanceRequired}/>{detail.consequences.length > 0 && <div className="consequence-box"><strong>△ {c.consequences}</strong><p>{c.consequenceWarning}</p>{detail.consequences.map((consequence) => <label key={consequence.invocationId}><input type="checkbox" checked={confirmed.includes(consequence.invocationId)} onChange={(event) => setConfirmed((current) => event.target.checked ? [...current, consequence.invocationId] : current.filter((id) => id !== consequence.invocationId))}/><span>{localized(consequence.summary, locale)}</span></label>)}</div>}<div className="modal-actions"><button className="button ghost" onClick={() => setRetryOpen(false)}>{c.cancel}</button><button className="button primary" disabled={busy || !guidance.trim() || confirmed.length !== detail.consequences.length} onClick={() => void startRetry()}>{c.startRevision}</button></div></div></div>}
  </aside></div>;
}

function Section({ title, children }: { title: string; children: ReactNode }) { return <section className="panel-section"><h2>{title}</h2>{children}</section>; }
function PlainValue({ value, empty }: { value: unknown; empty: string }) {
  if (value === null || value === undefined || (typeof value === "object" && !Object.keys(safeObject(value)).length)) return <p className="muted">{empty}</p>;
  if (typeof value !== "object") return <p>{String(value)}</p>;
  return <dl className="result-grid">{Object.entries(safeObject(value)).filter(([key]) => !/prompt|model|token|tool|worker|runtime/i.test(key)).map(([key, item]) => <div key={key}><dt>{key.replaceAll("_", " ")}</dt><dd>{typeof item === "object" ? JSON.stringify(item) : String(item)}</dd></div>)}</dl>;
}
function ArtifactList({ token, artifacts, locale }: { token: string; artifacts: WorkArtifact[]; locale: Locale }) { const c = t(locale); return <div className="artifact-list">{artifacts.map((artifact) => <button key={`${artifact.artifactId}-${artifact.role}`} onClick={() => void downloadArtifact(token, artifact)}><span>▤</span><div><strong>{typeof artifact.metadata?.name === "string" ? artifact.metadata.name : c.artifact}</strong><small>{artifact.mimeType}{artifact.sizeBytes ? ` · ${artifact.sizeBytes} ${c.bytes}` : ""}</small></div><span>{c.download} ↓</span></button>)}</div>; }
function eventLabel(event: TimelineEvent, locale: Locale): string {
  const labels: Record<string, [string, string]> = {
    work_item_created: ["Work item created", "Item de trabalho criado"], attempt_created: ["Attempt created", "Tentativa criada"], node_started: ["Work started", "Trabalho iniciado"],
    node_completed: [String(event.params.summary || "Business step completed"), String(event.params.summary || "Etapa de negócio concluída")], work_blocked: ["Waiting for a person", "A aguardar uma pessoa"], blocker_resolved: ["Response received", "Resposta recebida"],
    work_completed: ["Work completed", "Trabalho concluído"], work_failed: ["Could not complete", "Não foi possível concluir"], feedback_recorded: ["Feedback captured", "Feedback registado"],
    revision_requested: ["Revised attempt requested", "Nova tentativa pedida"], value_credited: ["Estimated value recorded", "Valor estimado registado"], value_deducted: ["Estimated value deducted", "Valor estimado deduzido"],
    consequence_confirmation_required: ["Confirmation required", "Confirmação necessária"], consequence_repetition_confirmed: ["Repeated actions confirmed", "Ações repetidas confirmadas"],
  };
  return (labels[event.code] || ["Work updated", "Trabalho atualizado"])[locale === "pt" ? 1 : 0];
}
function Timeline({ events, nodes, locale }: { events: TimelineEvent[]; nodes: DetailData["nodes"]; locale: Locale }) { const nodeMap = new Map(nodes.map((node) => [node.id, node])); return <ol className="timeline">{events.map((event) => <li key={event.id}><i className={event.code.includes("failed") ? "failed" : event.code.includes("blocked") ? "blocked" : ""}/><div><strong>{eventLabel(event, locale)}</strong>{event.nodeExecutionId && nodeMap.get(event.nodeExecutionId) && <span>{localized(nodeMap.get(event.nodeExecutionId)?.businessLabel, locale)}</span>}<time>{when(event.createdAt, locale)}</time></div></li>)}</ol>; }

function schemaFields(schema: JSONSchema): Array<[string, NonNullable<JSONSchema["properties"]>[string]]> { return Object.entries(schema.properties || {}); }
function BlockerForm({ token, blocker, locale, onDone }: { token: string; blocker: Blocker; locale: Locale; onDone: () => Promise<void> }) {
  const c = t(locale); const [values, setValues] = useState<Record<string, unknown>>({}); const [file, setFile] = useState<File | null>(null); const [busy, setBusy] = useState(false); const [error, setError] = useState("");
  const fields = schemaFields(blocker.responseSchema); const required = blocker.responseSchema.required || [];
  const valid = required.every((key) => values[key] !== undefined && values[key] !== "") && (blocker.kind !== "artifact" || !!file);
  async function submit(event: FormEvent) { event.preventDefault(); if (!valid) return; setBusy(true); try { const refs: Array<{ artifactId: string; metadata?: unknown }> = []; if (file) { const artifact = await uploadArtifact(token, file); refs.push({ artifactId: artifact.id, metadata: { name: file.name } }); } await respondToBlocker(token, blocker, values, refs); await onDone(); } catch (err) { setError(err instanceof Error ? err.message : "Error"); } finally { setBusy(false); } }
  return <section className="blocker-box"><span className="eyebrow">{blocker.kind.replaceAll("_", " ")}</span><h3>{localized(blocker.title, locale)}</h3><p>{localized(blocker.description, locale)}</p><form onSubmit={(event) => void submit(event)}>{fields.map(([key, field]) => <label key={key}>{field.title || key.replaceAll("_", " ")}{field.enum ? <select value={String(values[key] || "")} onChange={(event) => setValues((current) => ({ ...current, [key]: event.target.value }))}><option value=""/>{field.enum.map((option) => <option key={option}>{option}</option>)}</select> : field.type === "boolean" ? <input type="checkbox" checked={Boolean(values[key])} onChange={(event) => setValues((current) => ({ ...current, [key]: event.target.checked }))}/> : <textarea value={String(values[key] || "")} onChange={(event) => setValues((current) => ({ ...current, [key]: event.target.value }))}/>}</label>)}{blocker.kind === "artifact" && <label>{c.uploadArtifact}<input type="file" onChange={(event) => setFile(event.target.files?.[0] || null)}/></label>}{error && <div className="form-error">{error}</div>}<button className="button primary" disabled={!valid || busy}>{c.submitResponse}</button></form></section>;
}

function ValueModal({ locale, data, close }: { locale: Locale; data: NonNullable<ReturnType<typeof useDashboardData>["data"]>; close: () => void }) { const c = t(locale); const model = data.summary.value.models[0]; const total = data.summary.value.totals[0]; return <div className="modal-layer" onMouseDown={(event) => { if (event.target === event.currentTarget) close(); }}><div className="modal value-modal" role="dialog" aria-modal="true"><button className="icon-button modal-close" aria-label={c.close} onClick={close}><CloseIcon /></button><span className="eyebrow">{c.valueHelp}</span><h2>{data.summary.value.configured && total ? money(total.amount, total.currency, locale) : c.notConfigured}</h2>{model ? <><p>{typeof model.explanation === "object" ? localized(model.explanation, locale) : model.explanation || c.valueHelp}</p><div className="formula"><span>{model.formula || "labor_time_saved"}</span><strong>{Math.round((model.baselineSeconds || 0) / 60)} min × {money(model.loadedHourlyCost || 0, model.currency || "EUR", locale)}/hour</strong></div></> : <p>{c.unconfiguredBody}</p>}</div></div>; }

export default function App() {
  const [locale, setLocale] = useState<Locale>("en"); const [token, setToken] = useState<string | null>(null); const [candidateToken, setCandidateToken] = useState<string | null>(null);
  const [connectError, setConnectError] = useState(""); const [preset, setPreset] = useState("week"); const [customFrom, setCustomFrom] = useState(""); const [customTo, setCustomTo] = useState("");
  const [search, setSearch] = useState(""); const [selected, setSelected] = useState<WorkItem | null>(null); const [valueOpen, setValueOpen] = useState(false);
  const period = useMemo(() => preset === "custom" && customFrom && customTo ? { from: new Date(`${customFrom}T00:00:00`), to: new Date(`${customTo}T23:59:59.999`) } : presetPeriod(preset), [preset, customFrom, customTo]);
  const { data, error, loading, refresh } = useDashboardData(token || candidateToken, period, search);
  useEffect(() => { if (candidateToken && data) { setToken(candidateToken); setCandidateToken(null); setConnectError(""); } }, [candidateToken, data]);
  useEffect(() => { if (candidateToken && error) { setConnectError(t(locale).invalidKey); setCandidateToken(null); } }, [candidateToken, error, locale]);
  async function connect(value: string) { setConnectError(""); setCandidateToken(value); }
  if (!token) return <Connection locale={locale} setLocale={setLocale} onConnect={connect} loading={!!candidateToken && loading} error={connectError}/>;
  const c = t(locale); const items = data?.items || [];
  return <div className="app-shell"><Header locale={locale} setLocale={setLocale} search={search} setSearch={setSearch} items={items} disconnect={() => { setToken(null); setSelected(null); }}/><main className="dashboard-main"><ValueHero data={data} locale={locale} onExplain={() => setValueOpen(true)}/>
    <section className="controls"><div><span className="eyebrow">{c.date}</span><select aria-label={c.date} value={preset} onChange={(event) => setPreset(event.target.value)}><option value="today">{c.today}</option><option value="week">{c.week}</option><option value="month">{c.month}</option><option value="custom">{c.custom}</option></select>{preset === "custom" && <><label><span className="sr-only">{c.from}</span><input type="date" value={customFrom} onChange={(event) => setCustomFrom(event.target.value)}/></label><label><span className="sr-only">{c.to}</span><input type="date" value={customTo} onChange={(event) => setCustomTo(event.target.value)}/></label></>}</div><span>{items.length} {c.itemsShown}</span></section>
    {error && <div className="page-error" role="alert">{c.refreshError}</div>}{loading && !data ? <div className="loading page-loading">{c.loading}</div> : <Board items={items} locale={locale} open={setSelected}/>}</main>
    {selected && <DetailPanel token={token} item={selected} locale={locale} close={() => setSelected(null)} changed={() => void refresh(true)}/>} {valueOpen && data && <ValueModal locale={locale} data={data} close={() => setValueOpen(false)}/>}</div>;
}
